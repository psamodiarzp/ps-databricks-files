# Databricks notebook source
import pandas as pd

# COMMAND ----------

base_db = sqlContext.sql(
    """
    with cte as (
  select
    merchant_id,
    session_id,
    producer_created_date,
    event_timestamp,
    event_name,
    event_timestamp_raw,
    browser_name,
    CAST(
      get_json_object(context, '$["device.id"]') AS string
    ) as new_device_id
  from
    aggregate_pa.cx_1cc_events_dump_v1
  WHERE
    producer_created_date >= DATE_SUB(CURRENT_DATE,7)
    and library <> 'magic-x'
),
step1 as(
  SELECT
    *,
    event_timestamp - (
      lead(event_timestamp) OVER (
        partition BY merchant_id,
        producer_created_date,
        new_device_id
        ORDER BY
          event_timestamp_raw DESC
      )
    ) AS lag_diff
  FROM
    cte
),
step2 as(
  SELECT
    *,
    CASE
      WHEN lag_diff >= 1800
      OR lag_diff IS NULL THEN 1
      ELSE 0
    END AS is_new_session
  FROM
    step1
)
SELECT
  *,
  concat(
    CAST(
      sum(is_new_session) over(
        partition BY merchant_id,
        producer_created_date,
        new_device_id
        ORDER BY
          event_timestamp_raw
      ) AS string
    ),
    '-',
    CAST(producer_created_date AS string),
    '-',
    new_device_id
  ) AS new_session_id
FROM
  step2
    """
)
base_df = base_db.toPandas()
base_df.head()


# COMMAND ----------

#MID count null check
base_df[base_df['merchant_id'].isna()].count()

# COMMAND ----------

#Date wise count
date_count_df = base_df.groupby(by=['producer_created_date']).agg({'session_id':'nunique','new_session_id':'nunique'}).reset_index()
date_count_df['diff%'] = round(date_count_df['new_session_id']*1.0 / date_count_df['session_id'] * 100,2)
date_count_df.head()

# COMMAND ----------

browser_count_df = base_df.groupby(by=['browser_name']).agg({'session_id':'nunique','new_session_id':'nunique'}).reset_index()
browser_count_df['diff%'] = round(browser_count_df['new_session_id']*1.0 / browser_count_df['session_id'] * 100,2)
browser_count_df.head()

# COMMAND ----------

event_count_df = base_df.groupby(by=['event_name']).agg({'session_id':'nunique','new_session_id':'nunique'}).reset_index()
event_count_df['diff%'] = round(event_count_df['new_session_id']*1.0 / event_count_df['session_id'] * 100,2)
event_count_df.head()

# COMMAND ----------

event_count_df[(event_count_df['event_name'] == 'open') | (event_count_df['event_name'] == 'submit')].sort_values(by=['session_id'], ascending=False)

# COMMAND ----------

62242/180472

# COMMAND ----------

89117/285808

# COMMAND ----------

mx_count_df = base_df.groupby(by=['merchant_id']).agg({'session_id':'nunique','new_session_id':'nunique'}).reset_index()
mx_count_df['diff%'] = round(mx_count_df['new_session_id']*1.0 / mx_count_df['session_id'] * 100,2)
mx_count_df = mx_count_df.sort_values(by=['session_id'], ascending=False)
mx_count_df.head(20)

# COMMAND ----------

modified_base_df = base_df.fillna('null')
mx_cr_df = modified_base_df[(modified_base_df['event_name'] == 'open') | (modified_base_df['event_name'] == 'submit')].groupby(by=['merchant_id','event_name']).agg({'session_id':'nunique','new_session_id':'nunique'}).reset_index()
mx_cr_df['diff%'] = round(mx_cr_df['new_session_id']*1.0 / mx_cr_df['session_id'] * 100,2)
#mx_cr_df = mx_cr_df.sort_values(by=['session_id'], ascending=False)
mx_cr_df.head(20)

# COMMAND ----------

mx_cr_pivot_df = mx_cr_df.pivot(index=['merchant_id'], columns=['event_name'], values=['session_id',	'new_session_id',	'diff%']).reset_index()
mx_cr_pivot_df.columns = ['_'.join(col) for col in mx_cr_pivot_df.columns]
mx_cr_pivot_df['new_session_id_cr'] = round(mx_cr_pivot_df['new_session_id_submit']*1.0/mx_cr_pivot_df['new_session_id_open'] *100,2)
mx_cr_pivot_df['session_id_cr'] = round(mx_cr_pivot_df['session_id_submit']*1.0/mx_cr_pivot_df['session_id_open'] *100,2)
mx_cr_pivot_df['cr_diff'] = round(mx_cr_pivot_df['new_session_id_cr'] - mx_cr_pivot_df['session_id_cr'],2)
mx_cr_pivot_df = mx_cr_pivot_df.sort_values(by=['session_id_open'], ascending=False)
mx_cr_pivot_df

# COMMAND ----------

mx_cr_pivot_df.to_csv('/dbfs/FileStore/mx_cr_pivot_df.csv', index=False)
"https://razorpay-dev.cloud.databricks.com/files/mx_cr_pivot_df.csv"

# COMMAND ----------


