# Databricks notebook source
import pandas as pd

# COMMAND ----------

query='''
WITH new_address AS(
  SELECT
    producer_created_date,
  merchant_id,
    checkout_id ---count(distinct checkout_id) as new_address
  FROM
    aggregate_pa.cx_1cc_events_dump_v1
  WHERE
    producer_created_date between date('2022-07-01')
   and date('2022-09-30')
    AND event_name IN (
      'render:1cc_add_new_address_screen_loaded_completed'
    )
    AND cast(get_json_object(properties, '$.data.meta.loggedIn') as boolean) is null
  and merchant_id not in ('Hb4PVe74lPmk0k',
'IH7E2OJQGEKKTN',
'FFAcTJFtJQ3Cyn',
'ChdCdGm7TvuVk6',
'7E6oragoxHFlvV')
),
tw AS (
  SELECT
    checkout_id,
    get_json_object(properties, '$.data.is_COD_available') AS tw_response,
    row_number() over(
      partition BY checkout_id
      ORDER BY
        event_timestamp DESC
    ) AS rn
  FROM
    aggregate_pa.cx_1cc_events_dump_v1
  WHERE
 producer_created_date between date('2022-07-01')
   and date('2022-09-30')
    AND event_name IN ('metric:1cc_thridwartch_api_call_completed')
  ---  AND get_json_object(properties, '$.data.meta.loggedIn') IS NULL
  
),
save_address AS (
  SELECT
    checkout_id,
    case when get_json_object(properties, '$.data.address_id') is null then 0 else 1 end AS address_saved,
    row_number() over(
      partition BY checkout_id
      ORDER BY
        event_timestamp DESC
    ) AS rn
  FROM
    aggregate_pa.cx_1cc_events_dump_v1
  WHERE
 producer_created_date between date('2022-07-01')
   and date('2022-09-30')
    AND event_name IN ('metric:1cc_add_new_address_save_api_completed')
    --AND get_json_object(properties, '$.data.meta.loggedIn') IS NULL
),
method_select as(
    SELECT
    checkout_id,
  case when cast(get_json_object(properties, '$.data.method') as varchar(10)) = 'cod' then 'cod' else 'non-cod' end as method
  FROM
    aggregate_pa.cx_1cc_events_dump_v1
  WHERE
 producer_created_date between date('2022-07-01')
   and date('2022-09-30')
  and event_name = 'behav:1cc_payment_home_screen_method_selected'
),
 submits as (

   SELECT
    checkout_id,
  case when cast(get_json_object(properties, '$.data.data.method') as varchar(10)) = 'cod' then 'cod' else 'non-cod' end as method_submit
  FROM
    aggregate_pa.cx_1cc_events_dump_v1
  WHERE
 producer_created_date between date('2022-07-01')
   and date('2022-09-30')
  and event_name = 'submit'
)


SELECT
producer_created_date,
 merchant_id,
  tw_response,
  address_saved,
  method,
  method_submit,
  COUNT(DISTINCT new_address.checkout_id)
FROM
  new_address
  LEFT JOIN tw ON new_address.checkout_id = tw.checkout_id
  LEFT JOIN save_address on new_address.checkout_id = save_address.checkout_id
  LEFT JOIN method_select on new_address.checkout_id = method_select.checkout_id
  LEFT JOIN submits on new_address.checkout_id = submits.checkout_id
WHERE
  tw.rn = 1
  and save_address.rn = 1
GROUP BY
  1,
  2,
  3,4,5,6
'''

# COMMAND ----------

original_df = sqlContext.sql(query)
print((original_df.count(), len(original_df.columns)))
original_df = original_df.toPandas()

# COMMAND ----------

original_df.head()

# COMMAND ----------

original_df.rename(columns = {'count(DISTINCT checkout_id)':'checkout_id'}, inplace = True)
original_df.head()

# COMMAND ----------

original_df['mnth'] = pd.to_datetime(original_df['producer_created_date'], format='%Y-%m-%d').dt.month 

# COMMAND ----------


