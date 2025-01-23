# Databricks notebook source
from datetime import date, timedelta
import pandas as pd

# COMMAND ----------


def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)

# COMMAND ----------

query='''
     WITH base as(
SELECT created_date,
team_owner as mid_segment,
a.method,
SUM(payment_attempt) as sum_payment_attempt,
SUM(payment_success) as sum_payment_success
FROM aggregate_pa.magic_payment_fact a
left join aggregate_ba.final_team_tagging b on a.merchant_id = b.merchant_id
WHERE created_date >= '2023-02-01'
GROUP BY 1, 2, 3
), 
base_3 as(
select 
unix_timestamp(created_date) as unix_created_date,
created_date,
mid_segment,
method,
sum_payment_attempt,
sum_payment_success
FROM base
)
 
select 
  b1.* 
from 
  base_3 b1 
'''


# COMMAND ----------

df = spark.sql(query)
df.createOrReplaceTempView("dfView")
spark.sql("""select count(*) from dfView""").show(5)
db = df.toPandas()
db.head()

# COMMAND ----------

db = df.toPandas()
db.head(50)

# COMMAND ----------

db[db['mid_segment']=='Enterprise']

# COMMAND ----------

import datetime
import time

import pytz  # to define the time based dynamic filename
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.session import SparkSession
from pyspark.sql.types import *
from pyspark.sql.types import ArrayType
from pyspark.sql.types import IntegerType
from pyspark.sql.types import StringType

# ------- Enabling Spark configurations
sc = SparkContext.getOrCreate()
spark = SparkSession(sc)
spark = SparkSession.builder.enableHiveSupport().getOrCreate()

enableLegacyTimeParser = """ set spark.sql.legacy.timeParserPolicy=LEGACY"""
enableDynamicPartitioning = """ set hive.exec.dynamic.partition=true"""
enableNonStrict = """ set hive.exec.dynamic.partition.mode=nonstrict"""

spark.sql(enableLegacyTimeParser)
spark.sql(enableDynamicPartitioning)
spark.sql(enableNonStrict)

print('Spark configurations enabled')


# declare the vars
s3_loc_for_Magic_Checkout = 'rzp-1642-prod-anodot/persistent/PG/Magic_Checkout/q1'
datetime_ist = datetime.datetime.now(
    pytz.timezone(
        'Asia/Kolkata',
    ),
)  # get the current timestamp in ist TZ
date_folder = datetime_ist.strftime('%Y-%m-%d')
# format the time in yyyymmddhh format as it's per the recommendations
current_hour = datetime_ist.strftime('%Y%m%d%H')
# filename to be pushed
filename = f'anodot_metric_data_population_Magic_Checkout_q1{current_hour}_datafile'
# write/read location of s3 bucket
final_filename = f's3://{s3_loc_for_Magic_Checkout}'

# Read existing data before writing to check duplicacy
date_N_days_ago = datetime.datetime.now() - datetime.timedelta(days=1)
yesterdate = date_N_days_ago.strftime('%Y-%m-%d')

try:
    read_data = spark.read.parquet(f's3://{s3_loc_for_Magic_Checkout}/{yesterdate}')

    read_data.createOrReplaceTempView('existing_data_temp')

    # pull data from DWH to append; Pulls past day's data

    query1 = '''
   WITH base as(
  SELECT 
    unix_timestamp(
      current_timestamp()
    ) as current_timestamp, 
    unix_timestamp(producer_created_date) as unix_producer_created_date, 
    producer_created_date, 
    merchant_id, 
    (
      CASE WHEN magic_checkout_fact.summary_screen_continue_cta_clicked = 1 THEN 'Summary Screen CTA clicked' WHEN magic_checkout_fact.summary_screen_loaded = 0 THEN 'Summary Screen did not load' WHEN magic_checkout_fact.summary_screen_continue_cta_clicked = 0 
      AND (
        magic_checkout_fact.contact_email_entered = 1 
        OR magic_checkout_fact.contact_number_entered = 1 
        OR magic_checkout_fact.clicked_change_contact = 1 
        OR (
          magic_checkout_fact.contact_fill_began = 1 
          AND magic_checkout_fact.prefill_contact_number = 0
        ) 
        OR (
          magic_checkout_fact.email_fill_began = 1 
          AND magic_checkout_fact.prefill_email = 0
        )
      ) = TRUE 
      AND (
        magic_checkout_fact.have_coupon_clicked = 1 
        OR magic_checkout_fact.coupon_screen_loaded = 1
      ) = FALSE THEN 'Interacted w contact but not coupons' WHEN magic_checkout_fact.summary_screen_continue_cta_clicked = 0 
      AND (
        magic_checkout_fact.contact_email_entered = 1 
        OR magic_checkout_fact.contact_number_entered = 1 
        OR magic_checkout_fact.clicked_change_contact = 1 
        OR (
          magic_checkout_fact.contact_fill_began = 1 
          AND magic_checkout_fact.prefill_contact_number = 0
        ) 
        OR (
          magic_checkout_fact.email_fill_began = 1 
          AND magic_checkout_fact.prefill_email = 0
        )
      ) = FALSE 
      AND (
        magic_checkout_fact.have_coupon_clicked = 1 
        OR magic_checkout_fact.coupon_screen_loaded = 1
      ) = TRUE THEN 'Interacted w coupons but not contact' WHEN magic_checkout_fact.summary_screen_continue_cta_clicked = 0 
      AND (
        magic_checkout_fact.contact_email_entered = 1 
        OR magic_checkout_fact.contact_number_entered = 1 
        OR magic_checkout_fact.clicked_change_contact = 1 
        OR (
          magic_checkout_fact.contact_fill_began = 1 
          AND magic_checkout_fact.prefill_contact_number = 0
        ) 
        OR (
          magic_checkout_fact.email_fill_began = 1 
          AND magic_checkout_fact.prefill_email = 0
        )
      ) = TRUE 
      AND (
        magic_checkout_fact.have_coupon_clicked = 1 
        OR magic_checkout_fact.coupon_screen_loaded = 1
      ) = TRUE THEN 'Interacted w both coupons and contact' WHEN magic_checkout_fact.summary_screen_continue_cta_clicked = 0 
      AND (
        magic_checkout_fact.contact_email_entered = 1 
        OR magic_checkout_fact.contact_number_entered = 1 
        OR magic_checkout_fact.clicked_change_contact = 1 
        OR (
          magic_checkout_fact.contact_fill_began = 1 
          AND magic_checkout_fact.prefill_contact_number = 0
        ) 
        OR (
          magic_checkout_fact.email_fill_began = 1 
          AND magic_checkout_fact.prefill_email = 0
        )
      ) = FALSE 
      AND (
        magic_checkout_fact.have_coupon_clicked = 1 
        OR magic_checkout_fact.coupon_screen_loaded = 1
      ) = FALSE THEN 'Bounced w/o any interaction' WHEN magic_checkout_fact.summary_screen_continue_cta_clicked = 0 
      AND magic_checkout_fact.edit_address_clicked = 1 THEN 'Exited to Edit Address' ELSE 'Others' END
    ) AS summary_screen_dropoffs, 
    submit, 
    open, 
    auto_coupon_applied, 
    merchant_coupon_applied, 
    manual_coupon_applied, 
    validation_successful, 
    summary_screen_loaded 
  FROM 
    aggregate_pa.magic_checkout_fact 
  where 
    producer_created_date = DATE_ADD(CURRENT_DATE,-1)
), 
base_3 as(
  SELECT 
    unix_producer_created_date, 
    producer_created_date, 
    merchant_id, 
    platform,
    max(current_timestamp) as current_timestamp, 
    sum(
      case when summary_screen_dropoffs = 'Bounced w/o any interaction' then 1 else 0 end
    ) as bounce_off, 
    sum(submit) as cnt_submit, 
    sum(open) as cnt_open, 
    sum(summary_screen_loaded) as cnt_summary_screen_loaded, 
    sum(
      auto_coupon_applied + merchant_coupon_applied + manual_coupon_applied
    ) as cnt_coupon_applied, 
    sum(validation_successful) as coupon_validation_successful, 
    count(*) as total 
  from 
    raw 
  group by 
    1, 
    2, 
    3,
    4
)
 
select 
  b1.* 
from 
  base_3 b1 
  left join existing_data_temp b2 on b1.producer_created_date = b2.producer_created_date 
  and b1.merchant_id = b2.merchant_id and b1.platform = b2.platform 
  where b2.total is null 
    '''
except:
    query1='''
     WITH base as(
  SELECT 
    unix_timestamp(
      current_timestamp()
    ) as current_timestamp, 
    unix_timestamp(producer_created_date) as unix_producer_created_date, 
    producer_created_date, 
    merchant_id, 
    (
      CASE WHEN magic_checkout_fact.summary_screen_continue_cta_clicked = 1 THEN 'Summary Screen CTA clicked' WHEN magic_checkout_fact.summary_screen_loaded = 0 THEN 'Summary Screen did not load' WHEN magic_checkout_fact.summary_screen_continue_cta_clicked = 0 
      AND (
        magic_checkout_fact.contact_email_entered = 1 
        OR magic_checkout_fact.contact_number_entered = 1 
        OR magic_checkout_fact.clicked_change_contact = 1 
        OR (
          magic_checkout_fact.contact_fill_began = 1 
          AND magic_checkout_fact.prefill_contact_number = 0
        ) 
        OR (
          magic_checkout_fact.email_fill_began = 1 
          AND magic_checkout_fact.prefill_email = 0
        )
      ) = TRUE 
      AND (
        magic_checkout_fact.have_coupon_clicked = 1 
        OR magic_checkout_fact.coupon_screen_loaded = 1
      ) = FALSE THEN 'Interacted w contact but not coupons' WHEN magic_checkout_fact.summary_screen_continue_cta_clicked = 0 
      AND (
        magic_checkout_fact.contact_email_entered = 1 
        OR magic_checkout_fact.contact_number_entered = 1 
        OR magic_checkout_fact.clicked_change_contact = 1 
        OR (
          magic_checkout_fact.contact_fill_began = 1 
          AND magic_checkout_fact.prefill_contact_number = 0
        ) 
        OR (
          magic_checkout_fact.email_fill_began = 1 
          AND magic_checkout_fact.prefill_email = 0
        )
      ) = FALSE 
      AND (
        magic_checkout_fact.have_coupon_clicked = 1 
        OR magic_checkout_fact.coupon_screen_loaded = 1
      ) = TRUE THEN 'Interacted w coupons but not contact' WHEN magic_checkout_fact.summary_screen_continue_cta_clicked = 0 
      AND (
        magic_checkout_fact.contact_email_entered = 1 
        OR magic_checkout_fact.contact_number_entered = 1 
        OR magic_checkout_fact.clicked_change_contact = 1 
        OR (
          magic_checkout_fact.contact_fill_began = 1 
          AND magic_checkout_fact.prefill_contact_number = 0
        ) 
        OR (
          magic_checkout_fact.email_fill_began = 1 
          AND magic_checkout_fact.prefill_email = 0
        )
      ) = TRUE 
      AND (
        magic_checkout_fact.have_coupon_clicked = 1 
        OR magic_checkout_fact.coupon_screen_loaded = 1
      ) = TRUE THEN 'Interacted w both coupons and contact' WHEN magic_checkout_fact.summary_screen_continue_cta_clicked = 0 
      AND (
        magic_checkout_fact.contact_email_entered = 1 
        OR magic_checkout_fact.contact_number_entered = 1 
        OR magic_checkout_fact.clicked_change_contact = 1 
        OR (
          magic_checkout_fact.contact_fill_began = 1 
          AND magic_checkout_fact.prefill_contact_number = 0
        ) 
        OR (
          magic_checkout_fact.email_fill_began = 1 
          AND magic_checkout_fact.prefill_email = 0
        )
      ) = FALSE 
      AND (
        magic_checkout_fact.have_coupon_clicked = 1 
        OR magic_checkout_fact.coupon_screen_loaded = 1
      ) = FALSE THEN 'Bounced w/o any interaction' WHEN magic_checkout_fact.summary_screen_continue_cta_clicked = 0 
      AND magic_checkout_fact.edit_address_clicked = 1 THEN 'Exited to Edit Address' ELSE 'Others' END
    ) AS summary_screen_dropoffs, 
    submit, 
    open, 
    auto_coupon_applied, 
    merchant_coupon_applied, 
    manual_coupon_applied, 
    validation_successful, 
    summary_screen_loaded 
  FROM 
    aggregate_pa.magic_checkout_fact 
  where 
    producer_created_date >= DATE('2023-02-01')
), 
base_3 as(
  SELECT 
    unix_producer_created_date, 
    producer_created_date, 
    merchant_id, 
    platform,
    max(current_timestamp) as current_timestamp, 
    sum(
      case when summary_screen_dropoffs = 'Bounced w/o any interaction' then 1 else 0 end
    ) as bounce_off, 
    sum(submit) as cnt_submit, 
    sum(open) as cnt_open, 
    sum(summary_screen_loaded) as cnt_summary_screen_loaded, 
    sum(
      auto_coupon_applied + merchant_coupon_applied + manual_coupon_applied
    ) as cnt_coupon_applied, 
    sum(validation_successful) as coupon_validation_successful, 
    count(*) as total 
  from 
    base 
  group by 
    1, 
    2, 
    3,
    4
) 
 
select b1.* 
from 
  base_3 b1 
    '''
df = spark.sql(query1)
db = df.toPandas()
db.head()

# COMMAND ----------

query_try='''
     WITH base as(
  SELECT 
    unix_timestamp(
      current_timestamp()
    ) as current_timestamp, 
    unix_timestamp(producer_created_date) as unix_producer_created_date, 
    producer_created_date, 
    merchant_id, 
    platform,
    (
      CASE WHEN magic_checkout_fact.summary_screen_continue_cta_clicked = 1 THEN 'Summary Screen CTA clicked' WHEN magic_checkout_fact.summary_screen_loaded = 0 THEN 'Summary Screen did not load' WHEN magic_checkout_fact.summary_screen_continue_cta_clicked = 0 
      AND (
        magic_checkout_fact.contact_email_entered = 1 
        OR magic_checkout_fact.contact_number_entered = 1 
        OR magic_checkout_fact.clicked_change_contact = 1 
        OR (
          magic_checkout_fact.contact_fill_began = 1 
          AND magic_checkout_fact.prefill_contact_number = 0
        ) 
        OR (
          magic_checkout_fact.email_fill_began = 1 
          AND magic_checkout_fact.prefill_email = 0
        )
      ) = TRUE 
      AND (
        magic_checkout_fact.have_coupon_clicked = 1 
        OR magic_checkout_fact.coupon_screen_loaded = 1
      ) = FALSE THEN 'Interacted w contact but not coupons' WHEN magic_checkout_fact.summary_screen_continue_cta_clicked = 0 
      AND (
        magic_checkout_fact.contact_email_entered = 1 
        OR magic_checkout_fact.contact_number_entered = 1 
        OR magic_checkout_fact.clicked_change_contact = 1 
        OR (
          magic_checkout_fact.contact_fill_began = 1 
          AND magic_checkout_fact.prefill_contact_number = 0
        ) 
        OR (
          magic_checkout_fact.email_fill_began = 1 
          AND magic_checkout_fact.prefill_email = 0
        )
      ) = FALSE 
      AND (
        magic_checkout_fact.have_coupon_clicked = 1 
        OR magic_checkout_fact.coupon_screen_loaded = 1
      ) = TRUE THEN 'Interacted w coupons but not contact' WHEN magic_checkout_fact.summary_screen_continue_cta_clicked = 0 
      AND (
        magic_checkout_fact.contact_email_entered = 1 
        OR magic_checkout_fact.contact_number_entered = 1 
        OR magic_checkout_fact.clicked_change_contact = 1 
        OR (
          magic_checkout_fact.contact_fill_began = 1 
          AND magic_checkout_fact.prefill_contact_number = 0
        ) 
        OR (
          magic_checkout_fact.email_fill_began = 1 
          AND magic_checkout_fact.prefill_email = 0
        )
      ) = TRUE 
      AND (
        magic_checkout_fact.have_coupon_clicked = 1 
        OR magic_checkout_fact.coupon_screen_loaded = 1
      ) = TRUE THEN 'Interacted w both coupons and contact' WHEN magic_checkout_fact.summary_screen_continue_cta_clicked = 0 
      AND (
        magic_checkout_fact.contact_email_entered = 1 
        OR magic_checkout_fact.contact_number_entered = 1 
        OR magic_checkout_fact.clicked_change_contact = 1 
        OR (
          magic_checkout_fact.contact_fill_began = 1 
          AND magic_checkout_fact.prefill_contact_number = 0
        ) 
        OR (
          magic_checkout_fact.email_fill_began = 1 
          AND magic_checkout_fact.prefill_email = 0
        )
      ) = FALSE 
      AND (
        magic_checkout_fact.have_coupon_clicked = 1 
        OR magic_checkout_fact.coupon_screen_loaded = 1
      ) = FALSE THEN 'Bounced w/o any interaction' WHEN magic_checkout_fact.summary_screen_continue_cta_clicked = 0 
      AND magic_checkout_fact.edit_address_clicked = 1 THEN 'Exited to Edit Address' ELSE 'Others' END
    ) AS summary_screen_dropoffs, 
    submit, 
    open, 
    auto_coupon_applied, 
    merchant_coupon_applied, 
    manual_coupon_applied, 
    validation_successful, 
    summary_screen_loaded 
  FROM 
    aggregate_pa.magic_checkout_fact 
  where 
    producer_created_date >= DATE('2023-02-01')
), 
base_3 as(
  SELECT 
    unix_producer_created_date, 
    producer_created_date, 
    merchant_id, 
    platform,
    max(current_timestamp) as current_timestamp, 
    sum(
      case when summary_screen_dropoffs = 'Bounced w/o any interaction' then 1 else 0 end
    ) as bounce_off, 
    sum(submit) as cnt_submit, 
    sum(open) as cnt_open, 
    sum(summary_screen_loaded) as cnt_summary_screen_loaded, 
    sum(
      auto_coupon_applied + merchant_coupon_applied + manual_coupon_applied
    ) as cnt_coupon_applied, 
    sum(validation_successful) as coupon_validation_successful, 
    count(*) as total 
  from 
    base 
  group by 
    1, 
    2, 
    3,
    4
) 
 
select b1.* 
from 
  base_3 b1 
''' 


# COMMAND ----------

df = spark.sql(query_try)
#df.createOrReplaceTempView("dfView")
#spark.sql("""select count(*) from dfView""").show(5)
db = df.toPandas()
db.head()

# COMMAND ----------

query_2='''
     WITH base as(
SELECT
  unix_timestamp(producer_created_date) as unix_producer_created_date,
  producer_created_date,
  merchant_id,
      CASE
        WHEN event_name = 'render:complete'
        AND CAST(get_json_object(context, '$.platform') AS string) = 'mobile_sdk' THEN 'mobile_sdk'
        WHEN event_name = 'render:complete'
        AND CAST(get_json_object (context, '$.platform') AS string) = 'browser'
        AND CAST(
          get_json_object (properties, '$.data.meta.is_mobile') AS string
        ) = 'true' THEN 'mweb'
        WHEN event_name = 'render:complete'
        AND CAST(get_json_object (context, '$.platform') AS string) = 'browser'
        AND (
          CAST(
            get_json_object (properties, '$.data.meta.is_mobile') AS string
          ) = 'false'
          OR get_json_object (properties, '$.data.meta.is_mobile') IS NULL
        ) THEN 'desktop_browser'
        ELSE 'NA'
      END AS platform,
  approx_percentile(
    CAST(
      get_json_object(properties, '$.data.meta["timeSince.open"]') AS bigint
    ),
    0.5
  ) AS median_time_since_open,
  approx_percentile(
    CAST(
      get_json_object(properties, '$.data.meta["timeSince.open"]') AS bigint
    ),
    0.9
  ) AS 90_perc_time_since_open,
   approx_percentile(
    CAST(
      get_json_object(properties, '$.data.meta["timeSince.open"]') AS bigint
    ),
    0.95
  ) AS 95_perc_time_since_open
FROM
  aggregate_pa.cx_1cc_events_dump_v1
WHERE
  producer_created_date BETWEEN date('2023-01-01')
  AND date('2023-05-31')
  AND ((UPPER(event_name) = UPPER('render:complete')))
GROUP BY
  1,
  2,
  3,
  4
)

  select * from base limit 10
''' 


# COMMAND ----------

df = spark.sql(query_2)
#df.createOrReplaceTempView("dfView")
#spark.sql("""select count(*) from dfView""").show(5)
db = df.toPandas()
db.head()

# COMMAND ----------

query_2_modified='''
       WITH base as(
SELECT
  unix_timestamp(producer_created_date) as unix_producer_created_date,
  producer_created_date,
  merchant_id,
   
      
     
  percentile(
    CAST(
      get_json_object(properties, '$.data.meta["timeSince.open"]') AS bigint
    ),
    0.5 
  ) OVER (partition by producer_created_date, merchant_id) AS median_time_merchant_level,
    percentile(
    CAST(
      get_json_object(properties, '$.data.meta["timeSince.open"]') AS bigint
    ),
    0.5 
  ) OVER (partition by producer_created_date) AS median_time_date_level,
  percentile(
    CAST(
      get_json_object(properties, '$.data.meta["timeSince.open"]') AS bigint
    ),
    0.9
  ) OVER (partition by producer_created_date, merchant_id) AS percentile_90_merchant_level,
    percentile(
    CAST(
      get_json_object(properties, '$.data.meta["timeSince.open"]') AS bigint
    ),
    0.9 
  ) OVER (partition by producer_created_date) AS percentile_90_date_level,
  percentile(
    CAST(
      get_json_object(properties, '$.data.meta["timeSince.open"]') AS bigint
    ),
    0.95
  ) OVER (partition by producer_created_date, merchant_id) AS percentile_95_merchant_level,
    percentile(
    CAST(
      get_json_object(properties, '$.data.meta["timeSince.open"]') AS bigint
    ),
    0.95 
  ) OVER (partition by producer_created_date) AS percentile_95_date_level

FROM
  aggregate_pa.cx_1cc_events_dump_v1
WHERE
  producer_created_date BETWEEN date('2023-03-01')
  AND date('2023-03-05')
  AND ((UPPER(event_name) = UPPER('render:complete')))
), base_3 as(
select 
unix_producer_created_date,
  producer_created_date,
  merchant_id,
  max(median_time_merchant_level) as median_time_merchant_level,
   max(median_time_date_level) as median_time_date_level,
   max(percentile_90_merchant_level) as percentile_90_merchant_level,
   max(percentile_90_date_level) as percentile_90_date_level, 
     max(percentile_95_merchant_level) as percentile_95_merchant_level,
   max(percentile_95_date_level) as percentile_95_date_level
   
   from base
   GROUP BY 1,2,3 
)

  select * from base_3
''' 


# COMMAND ----------

df = spark.sql(query_2_modified)
2
#df.createOrReplaceTempView("dfView")
3
#spark.sql("""select count(*) from dfView""").show(5)
4
db = df.toPandas()
5
db.head(500)

# COMMAND ----------

db.show(db.count())

# COMMAND ----------

query_3 = '''
     WITH base as(
SELECT 
order_created_date, 
merchant_id,
ml_model_id,
(CASE WHEN (UPPER(result_flag)) = UPPER('green') THEN 'safe'
WHEN ((UPPER(result_flag)) = UPPER('red')) AND (experimentation) THEN 'safe'
  WHEN (UPPER(result_flag)) = UPPER('red') AND (NOT (experimentation)) THEN 'risky'
  ELSE null
END)
 AS red_flag,
COUNT(DISTINCT order_id) AS count_of_order_id
FROM aggregate_pa.magic_rto_reimbursement_fact
WHERE order_created_date = DATE_ADD(CURRENT_DATE(),-1)
GROUP BY 1, 2, 3, 4
),
base_3 as(
SELECT
unix_timestamp(order_created_date) as unix_order_created_date,
order_created_date, merchant_id, ml_model_id, sum(case when red_flag='risky' then 1 else 0 end) as risky_order, count(*) as total_orders
FROM base

group by 1,2,3,4
)


  select * from base_3 limit 10
''' 


# COMMAND ----------

df = spark.sql(query_3)
#df.createOrReplaceTempView("dfView")
#spark.sql("""select count(*) from dfView""").show(5)
db = df.toPandas()
db.head(50)

# COMMAND ----------

query_4='''
      WITH base as(
SELECT created_date,
merchant_id,
method,
SUM(payment_attempt) as sum_payment_attempt,
SUM(payment_success) as sum_payment_success
FROM aggregate_pa.magic_payment_fact
WHERE created_date = '2023-03-09'
GROUP BY 1, 2, 3

), 
base_3 as(
select 
created_date,
merchant_id,
method,
sum_payment_attempt,
sum_payment_success
FROM base

)
 
select 
  b1.* 
from 
  base_3 b1 
''' 


# COMMAND ----------

df = spark.sql(query_4)
db = df.toPandas()
db.head(50)


# COMMAND ----------

query_5='''
     WITH Submits AS
(
SELECT producer_created_date, merchant_id, COUNT(*) as cnt_submit
FROM aggregate_pa.cx_1cc_events_dump_v1
WHERE LOWER(event_name) = 'submit'
  and producer_created_date = date('2023-03-07')
  ---DATE_ADD(CURRENT_DATE(),-1)
GROUP BY 1, 2
),

payment AS 
(
SELECT payments.created_date , merchant_id , COUNT(*) AS payment_attempts
FROM realtime_hudi_api.payments
LEFT JOIN realtime_hudi_api.order_meta  AS order_meta ON payments.order_id =order_meta.order_id
WHERE  order_meta.type = 'one_click_checkout' 
    and payments.created_date =  date('2023-03-07')
    --DATE_ADD(CURRENT_DATE(),-1)
GROUP BY 1, 2
), 
base_3 as(
SELECT 
unix_timestamp(submits.producer_created_date) as unix_producer_created_date,
submits.producer_created_date, Submits.merchant_id, payment_attempts, cnt_submit
FROM Submits
LEFT JOIN payment ON DATE(Submits.producer_created_date) = DATE(payment.created_date) AND Submits.merchant_id = payment.merchant_id
)



select 
  b1.* 
from 
  base_3 b1 
''' 


# COMMAND ----------

df = spark.sql(query_5)
db = df.toPandas()
db.head(50)


# COMMAND ----------


