# Databricks notebook source
import pandas as pd

# COMMAND ----------

import datetime

# COMMAND ----------

#getting general stats of the situation
step1 = """
WITH non_prefill AS(
  SELECT
    -- count(distinct checkout_id)
    checkout_id,
    producer_created_date,
    CASE
      WHEN (
        CAST(
          get_json_object(properties, '$.data.prefill_contact_number') AS string
        ) IS NULL
        OR CAST(
          get_json_object(properties, '$.data.prefill_contact_number') AS string
        ) = ''
      ) THEN 0
      ELSE 1
    END AS prefill_contact_number,
  get_json_object(context,'$.user_agent_parsed.os.family') as os_family,
  case when lower(browser_name) like '%safari%' then 'safari' else 'non-safari' end as browser
  FROM
    aggregate_pa.cx_1cc_events_dump_v1
  WHERE
    event_name = 'render:1cc_summary_screen_loaded_completed'
    --AND producer_created_date = date('2023-02-01') 
  and producer_created_date >= date('2022-12-01')
    and producer_created_date < date('2023-02-07')
),
contact_screen AS(
  SELECT
    checkout_id,
    get_json_object(properties, '$.data.contact_number') AS contact_number,
    producer_created_date
  FROM
    aggregate_pa.cx_1cc_events_dump_v1
  WHERE
    event_name = 'behav:1cc_summary_screen_contact_number_entered'
   -- AND producer_created_date = date('2023-02-01') 
  and producer_created_date >= date('2022-12-01')
    and producer_created_date < date('2023-02-07')
    AND (
      get_json_object(properties, '$.data.contact_number') IS NOT NULL
      AND get_json_object(properties, '$.data.contact_number') <> ''
    )
)
SELECT
  prefill_contact_number,
  os_family,
  browser,
  COUNT(DISTINCT non_prefill.checkout_id),
  COUNT(DISTINCT contact_screen.checkout_id)
FROM
  non_prefill
  LEFT JOIN contact_screen ON non_prefill.checkout_id=contact_screen.checkout_id 
 
  group by 1,2,3

"""
df = spark.sql(step1).toPandas()
df.head()
#df.createOrReplaceTempView("dfView")
#spark.sql("""select count(*) from dfView""").show()
#df.head()


# COMMAND ----------

df.head()

# COMMAND ----------

sql4 = """select * from aggregate_pa.cx_1cc_events_dump_v1 limit 10"""
df = spark.sql(sql4)
df.createOrReplaceTempView("dfView")
spark.sql("""select count(*) from dfView""").show()

# COMMAND ----------

Step 2: get all phone numbers from Magic

# COMMAND ----------

magic_numbers_sql = """
WITH non_prefill AS(
  SELECT
    -- count(distinct checkout_id)
    checkout_id,
    producer_created_date,
    CASE
      WHEN (
        CAST(
          get_json_object(properties, '$.data.prefill_contact_number') AS string
        ) IS NULL
        OR CAST(
          get_json_object(properties, '$.data.prefill_contact_number') AS string
        ) = ''
      ) THEN 0
      ELSE 1
    END AS prefill_contact_number,
    case when lower(get_json_object(context,'$.user_agent_parsed.os.family'))='ios' or lower(browser_name) like '%safari%' then 'not-applicable' else 'applicable' end as prefill_applicable
  FROM
    aggregate_pa.cx_1cc_events_dump_v1
  WHERE
    event_name = 'render:1cc_summary_screen_loaded_completed'
 --   AND producer_created_date = date('2023-02-01') 
  and producer_created_date >= date('2022-12-01')
    and producer_created_date < date('2023-02-07')
),
contact_screen AS(
  SELECT
    a.checkout_id,
    get_json_object(properties, '$.data.contact_number') AS contact_number,
    a.producer_created_date
  FROM
    aggregate_pa.cx_1cc_events_dump_v1 a
  inner join non_prefill on a.checkout_id = non_prefill.checkout_id
  WHERE
    event_name = 'behav:1cc_summary_screen_contact_number_entered'
  --  AND a.producer_created_date = date('2023-02-01') --   
  and a.producer_created_date >= date('2022-12-01')
    and a.producer_created_date < date('2023-02-07')
    AND (
      get_json_object(properties, '$.data.contact_number') IS NOT NULL
      AND get_json_object(properties, '$.data.contact_number') <> ''
    )
  AND prefill_contact_number = 0 --make sure only non-prefilled is there
  AND prefill_applicable='applicable' --removing iOS/Safari for simplicity
)
SELECT * 
from contact_screen
"""
magic_numbers = spark.sql(magic_numbers_sql).toPandas()
magic_numbers.head()

# COMMAND ----------

cx_numbers_sql = """
WITH contact_cta AS (
  SELECT
    DISTINCT get_json_object (context, '$.checkout_id') AS checkout_id
  FROM
    events.lumberjack AS checkout_events
  WHERE
    (
      (
        UPPER(
          LOWER(get_json_object(context, '$.library'))
        ) IN (UPPER('checkoutjs'), UPPER('hosted'))
      )
    )
   -- AND producer_created_date = '2023-02-01' 
    AND producer_created_date BETWEEN  '2022-10-01' AND '2022-12-31'
    AND Lower (checkout_events.event_name) = ('behav:contact_details:cta_click')
),
contact_fill AS(
  SELECT
    get_json_object (context, '$.checkout_id') AS checkout_id,
    producer_created_date,
    substr(
      get_json_object(properties, '$.data.value'),
      4,
      10
    ) AS contact_number
  FROM
    events.lumberjack AS checkout_events
  WHERE
    (
      (
        UPPER(
          LOWER(get_json_object(context, '$.library'))
        ) IN (UPPER('checkoutjs'), UPPER('hosted'))
      )
    )
    --AND producer_created_date = '2023-02-01'
     AND producer_created_date BETWEEN  '2022-10-01' AND '2022-12-31'
    AND Lower (checkout_events.event_name) = ('behav:contact:fill')
    AND get_json_object(properties, '$.data.valid') = 'true'
  AND length(substr(
      get_json_object(properties, '$.data.value'),
      4,
      10
    )) = 10
  group by 1,2,3
)
SELECT
  *
FROM
  contact_fill
  INNER JOIN contact_cta ON contact_fill.checkout_id = contact_cta.checkout_id

"""
cx_numbers_2021 = spark.sql(cx_numbers_sql).toPandas()
cx_numbers_2021.head()

# COMMAND ----------

len(pd.unique(magic_numbers['contact_number']))

# COMMAND ----------

cx_numbers.size

# COMMAND ----------

cx_numbers_2021.to_csv('/dbfs/FileStore/cx_numbers_2021.csv')

# COMMAND ----------

feb_magic = magic_numbers[magic_numbers['producer_created_date'] >= datetime.date(2023,2,1)]

# COMMAND ----------

feb_magic.head()

# COMMAND ----------

feb_vs_std_jan = pd.merge(feb_magic, cx_numbers, on='contact_number', how='inner')
feb_vs_std_jan.head()

# COMMAND ----------

len(pd.unique(feb_vs_std_jan['contact_number']))

# COMMAND ----------

len(pd.unique(feb_vs_std_jan['checkout_id_x']))

# COMMAND ----------

len(pd.unique(feb_magic['checkout_id']))

# COMMAND ----------

cx_combined = pd.concat([cx_numbers,cx_numbers_2021])
cx_combined.shape

# COMMAND ----------

print(cx_combined.shape)
print(cx_numbers.shape)
print(cx_numbers_2021.shape)

# COMMAND ----------

jan_magic = magic_numbers[(magic_numbers['producer_created_date'] > datetime.date(2023,1,1)) & (magic_numbers['producer_created_date'] < datetime.date(2023,2,6))]
jan_magic.shape

# COMMAND ----------

jan_magic_vs_std = pd.merge(jan_magic, cx_combined, on='contact_number', how='inner')
jan_magic_vs_std.shape

# COMMAND ----------

jan_magic_vs_std.info()

# COMMAND ----------

jan_magic_vs_std['producer_created_date_y'] =  pd.to_datetime(jan_magic_vs_std['producer_created_date_y'])

# COMMAND ----------

jan_magic_vs_std['producer_created_date_x'] =  pd.to_datetime(jan_magic_vs_std['producer_created_date_x'])

# COMMAND ----------

jan_magic_vs_std = jan_magic_vs_std[jan_magic_vs_std['producer_created_date_y'] < '2023-01-01']
jan_magic_vs_std.head()

# COMMAND ----------

min(jan_magic_vs_std['producer_created_date_y'])

# COMMAND ----------

len(pd.unique(jan_magic['contact_number']))

# COMMAND ----------

len(pd.unique(jan_magic_vs_std['contact_number']))

# COMMAND ----------


