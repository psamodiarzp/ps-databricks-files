# Databricks notebook source
# MAGIC %sql
# MAGIC select * from aggregate_pa.magic_checkout_fact limit 10

# COMMAND ----------

raw_data_db = sqlContext.sql("""
WITH cte AS(
  SELECT
    a.checkout_id,
  ramp_up_date,
    max(
      get_json_object(properties, '$.data.meta.v2_result')
    ) AS v2_result,
   max(
      get_json_object(properties, '$.magicExperiments.checkout_redesign')
    ) AS experiment_result,
   max(get_json_object(properties,'$.data.meta.v2_eligible')) as v2_eligible
  
  FROM
    aggregate_pa.cx_1cc_events_dump_v1 a
  inner join batch_sheets.checkout_v2_rampup_sheet b on a.merchant_id = b.merchant_id
  WHERE
    producer_created_date BETWEEN date('2024-07-01')
    AND date('2024-07-07')
  and producer_created_date > date(ramp_up_date)
  and rampup_100_percent_flag = 'false'
  and blacklisted_flag = 'false'
  GROUP BY
    1,2
),

submits as(

select 
   checkout_id,
   coalesce(get_json_object (properties,'$.data.data.upi_app'),
                split_part(try_cast(get_json_object(properties,'$.data.data.vpa')as string),'@',2), /* collect VPA handle */
                get_json_object (properties,'$.data.provider'),
                get_json_object (properties,'$.data.bank'),
                get_json_object (properties,'$.data.wallet')) as submit_instrument,
  
  coalesce(
  get_json_object(properties,'$.data.method'),
    get_json_object(properties,'$.data.data.method')
  ) as method
    FROM
    aggregate_pa.cx_1cc_events_dump_v1 a
  WHERE
    producer_created_date BETWEEN date('2024-07-01')
    AND date('2024-07-07')
  and event_name = 'submit'



),
cte2 AS(
select 
  p.id,
  p.method, 
p.authorized_at,
  checkout_id,
  error_code,
  error_description,
  internal_error_code
  from realtime_hudi_api.payments p
  inner join realtime_hudi_api.payment_analytics pa on p.id = pa.payment_id
  inner join realtime_hudi_api.order_meta om on p.order_id = om.order_id

  where p.created_date BETWEEN ('2024-07-01')
    AND ('2024-07-07')
  AND pa.created_date BETWEEN ('2024-07-01')
    AND ('2024-07-07')
      AND om.created_date BETWEEN ('2024-07-01')
    AND ('2024-07-07')
    
), rto_db as(
select order_id ,
 cod_intelligence_enabled,
  cod_eligible
  from aggregate_pa.magic_rto_reimbursement_fact
  where order_created_date BETWEEN date('2024-07-01')
    AND date('2024-07-07')


)
select 
cte.checkout_id,
merchant_id,
open,
summary_screen_loaded,
summary_screen_continue_cta_clicked,
payment_home_screen_loaded,
submit, 
browser_major_version,
browser_name,
os_brand_family,
os_major_version,
mcf.platform,
cte.experiment_result,
cte.v2_result,
cte.v2_eligible,
submit_instrument,
submits.method as checkout_method,
cte2.method as payment_method,
id as payment_attempts,
case when authorized_at is not null or cte2.method='cod' then id else null end as payment_success,
cte2.error_code,
  cte2.error_description,
  cte2.internal_error_code,
   cod_intelligence_enabled,
  cod_eligible
 
from cte 
left join cte2 on cte.checkout_id = cte2.checkout_id
left join submits on cte.checkout_id = submits.checkout_id
left join aggregate_pa.magic_checkout_fact mcf on cte.checkout_id = mcf.checkout_id
left join rto_db on mcf.order_id = rto_db.order_id
where mcf.producer_created_date between date('2024-07-01')
    AND date('2024-07-07')
""")

raw_data_df = raw_data_db.toPandas()
raw_data_df.head()

# COMMAND ----------

raw_data_df.to_csv('/dbfs/FileStore/cod_df.csv', index=False)
"https://razorpay-dev.cloud.databricks.com/files/cod_df.csv"

# COMMAND ----------

raw_data_df.columns

# COMMAND ----------

cod_pivot = raw_data_df.groupby(by=['merchant_id','v2_result',
       'v2_eligible','browser_name', 'cod_intelligence_enabled','payment_home_screen_loaded','submit', 'payment_method',
       'cod_eligible'],dropna=False).agg({'checkout_id':'nunique','payment_attempts':'nunique','payment_success':'nunique'}).reset_index()
cod_pivot.head()

# COMMAND ----------

cod_pivot.to_csv('/dbfs/FileStore/cod_pivot.csv', index=False)
"https://razorpay-dev.cloud.databricks.com/files/cod_pivot.csv"

# COMMAND ----------


