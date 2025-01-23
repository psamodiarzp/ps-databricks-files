# Databricks notebook source
from difflib import SequenceMatcher,unified_diff

def compare_code(text1, text2):
    #text1 = f1.readlines()
    #text2 = f2.readlines()
    diff = unified_diff(text1, text2,)
    for line in diff:
        print(line)



# COMMAND ----------



# COMMAND ----------

text1 = """
# -------Importing pyspark modules
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
def get_spark():
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    return spark


spark = get_spark()



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
    read_data = spark.read.parquet(
        f's3://{s3_loc_for_Magic_Checkout}/{yesterdate}',
    )

    read_data.createOrReplaceTempView('existing_data_temp')

    # pull data from DWH to append; Pulls past day's data

    query1 = '''
    WITH base as(
  SELECT
    checkout_id,
    producer_created_date,
   case when team_owner = 'SME' then team_owner else magic_checkout_fact.merchant_id end as merchant_id,
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
    summary_screen_loaded,
    pincode_serviceability_initiated,
    pincode_serviceability_successful,
    access_address_otp_screen_loaded,
    access_address_otp_screen_skipped,
    access_address_otp_screen_submitted,
    mandatory_login_otp_screen_loaded,
    mandatory_login_otp_screen_skipped,
    mandatory_login_otp_screen_submitted,
    save_address_otp_screen_loaded,
    save_address_otp_screen_skipped,
    save_address_otp_screen_submitted
  FROM
    aggregate_pa.magic_checkout_fact
      left join aggregate_ba.final_team_tagging b on magic_checkout_fact.merchant_id = b.merchant_id
  where
    producer_created_date = DATE_ADD(CURRENT_DATE(),-1)
),
base_3 as(
  SELECT
 unix_timestamp(producer_created_date) as unix_producer_created_date,
    producer_created_date,
    merchant_id,
    platform,
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
   sum(pincode_serviceability_initiated) as cnt_pincode_serviceability_initiated,
    sum(pincode_serviceability_successful) as cnt_pincode_serviceability_successful,
    sum(case when access_address_otp_screen_skipped=1 or access_address_otp_screen_submitted=1 then 1 else 0 end) as access_address_otp_success,
    sum(access_address_otp_screen_loaded) as access_address_otp_screen_loaded,
    sum(case when mandatory_login_otp_screen_skipped=1 or mandatory_login_otp_screen_submitted=1 then 1 else 0 end) as mandatory_login_otp_success,
    sum(mandatory_login_otp_screen_loaded) as mandatory_login_otp_screen_loaded,
    sum(case when save_address_otp_screen_skipped=1 or save_address_otp_screen_submitted=1 then 1 else 0 end) as save_address_otp_success,
    sum(save_address_otp_screen_loaded) as save_address_otp_screen_loaded,
    count(*) as total
  from
    base
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
  left join existing_data_temp b2 on b1.producer_created_date = b2.producer_created_date and b1.merchant_id=b2.merchnat_id and b1.platform=b2.platform
  where b2.total is null
    '''
except:
    query1 = '''
      WITH base as(
  SELECT
    checkout_id,
    producer_created_date,
   case when team_owner = 'SME' then team_owner else magic_checkout_fact.merchant_id end as merchant_id,
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
    summary_screen_loaded,
    pincode_serviceability_initiated,
    pincode_serviceability_successful,
    access_address_otp_screen_loaded,
    access_address_otp_screen_skipped,
    access_address_otp_screen_submitted,
    mandatory_login_otp_screen_loaded,
    mandatory_login_otp_screen_skipped,
    mandatory_login_otp_screen_submitted,
    save_address_otp_screen_loaded,
    save_address_otp_screen_skipped,
    save_address_otp_screen_submitted
  FROM
    aggregate_pa.magic_checkout_fact
      left join aggregate_ba.final_team_tagging b on magic_checkout_fact.merchant_id = b.merchant_id
  where
    producer_created_date >= DATE('2023-07-01')
),
base_3 as(
  SELECT
 unix_timestamp(producer_created_date) as unix_producer_created_date,
    producer_created_date,
    merchant_id,
    platform,
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
   sum(pincode_serviceability_initiated) as cnt_pincode_serviceability_initiated,
    sum(pincode_serviceability_successful) as cnt_pincode_serviceability_successful,
    sum(case when access_address_otp_screen_skipped=1 or access_address_otp_screen_submitted=1 then 1 else 0 end) as access_address_otp_success,
    sum(access_address_otp_screen_loaded) as access_address_otp_screen_loaded,
    sum(case when mandatory_login_otp_screen_skipped=1 or mandatory_login_otp_screen_submitted=1 then 1 else 0 end) as mandatory_login_otp_success,
    sum(mandatory_login_otp_screen_loaded) as mandatory_login_otp_screen_loaded,
    sum(case when save_address_otp_screen_skipped=1 or save_address_otp_screen_submitted=1 then 1 else 0 end) as save_address_otp_success,
    sum(save_address_otp_screen_loaded) as save_address_otp_screen_loaded,
    count(*) as total
  from
    base
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
    '''
data_to_write_sdf = spark.sql(query1)  # define spark df
data_to_write_sdf.write.mode('append').partitionBy(
    'producer_created_date',
).parquet(
    final_filename,
)  # write data to s3 bucket
"""

# COMMAND ----------

text2 = """
# -------Importing pyspark modules
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
    read_data = spark.read.parquet(
        f's3://{s3_loc_for_Magic_Checkout}/{yesterdate}',
    )

    read_data.createOrReplaceTempView('existing_data_temp')

    # pull data from DWH to append; Pulls past day's data

    query1 = '''
    WITH base as(
  SELECT
    checkout_id,
    producer_created_date,
   case when team_owner = 'SME' then team_owner else magic_checkout_fact.merchant_id end as merchant_id,
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
    summary_screen_loaded,
    pincode_serviceability_initiated,
    pincode_serviceability_successful,
    access_address_otp_screen_loaded,
    access_address_otp_screen_skipped,
    access_address_otp_screen_submitted,
    mandatory_login_otp_screen_loaded,
    mandatory_login_otp_screen_skipped,
    mandatory_login_otp_screen_submitted,
    save_address_otp_screen_loaded,
    save_address_otp_screen_skipped,
    save_address_otp_screen_submitted
  FROM
    aggregate_pa.magic_checkout_fact
      left join aggregate_ba.final_team_tagging b on magic_checkout_fact.merchant_id = b.merchant_id
  where
    producer_created_date = DATE_ADD(CURRENT_DATE(),-1)
),
base_3 as(
  SELECT
 unix_timestamp(producer_created_date) as unix_producer_created_date,
    producer_created_date,
    merchant_id,
    platform,
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
   sum(pincode_serviceability_initiated) as cnt_pincode_serviceability_initiated,
    sum(pincode_serviceability_successful) as cnt_pincode_serviceability_successful,
    sum(case when access_address_otp_screen_skipped=1 or access_address_otp_screen_submitted=1 then 1 else 0 end) as access_address_otp_success,
    sum(access_address_otp_screen_loaded) as access_address_otp_screen_loaded,
    sum(case when mandatory_login_otp_screen_skipped=1 or mandatory_login_otp_screen_submitted=1 then 1 else 0 end) as mandatory_login_otp_success,
    sum(mandatory_login_otp_screen_loaded) as mandatory_login_otp_screen_loaded,
    sum(case when save_address_otp_screen_skipped=1 or save_address_otp_screen_submitted=1 then 1 else 0 end) as save_address_otp_success,
    sum(save_address_otp_screen_loaded) as save_address_otp_screen_loaded,
    count(*) as total
  from
    base
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
  left join existing_data_temp b2 on b1.producer_created_date = b2.producer_created_date and b1.merchant_id=b2.merchnat_id and b1.platform=b2.platform
  where b2.total is null
    '''
except:
    query1 = '''
      WITH base as(
  SELECT
    checkout_id,
    producer_created_date,
   case when team_owner = 'SME' then team_owner else magic_checkout_fact.merchant_id end as merchant_id,
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
    summary_screen_loaded,
    pincode_serviceability_initiated,
    pincode_serviceability_successful,
    access_address_otp_screen_loaded,
    access_address_otp_screen_skipped,
    access_address_otp_screen_submitted,
    mandatory_login_otp_screen_loaded,
    mandatory_login_otp_screen_skipped,
    mandatory_login_otp_screen_submitted,
    save_address_otp_screen_loaded,
    save_address_otp_screen_skipped,
    save_address_otp_screen_submitted
  FROM
    aggregate_pa.magic_checkout_fact
      left join aggregate_ba.final_team_tagging b on magic_checkout_fact.merchant_id = b.merchant_id
  where
    producer_created_date >= DATE('2023-07-01')
),
base_3 as(
  SELECT
 unix_timestamp(producer_created_date) as unix_producer_created_date,
    producer_created_date,
    merchant_id,
    platform,
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
   sum(pincode_serviceability_initiated) as cnt_pincode_serviceability_initiated,
    sum(pincode_serviceability_successful) as cnt_pincode_serviceability_successful,
    sum(case when access_address_otp_screen_skipped=1 or access_address_otp_screen_submitted=1 then 1 else 0 end) as access_address_otp_success,
    sum(access_address_otp_screen_loaded) as access_address_otp_screen_loaded,
    sum(case when mandatory_login_otp_screen_skipped=1 or mandatory_login_otp_screen_submitted=1 then 1 else 0 end) as mandatory_login_otp_success,
    sum(mandatory_login_otp_screen_loaded) as mandatory_login_otp_screen_loaded,
    sum(case when save_address_otp_screen_skipped=1 or save_address_otp_screen_submitted=1 then 1 else 0 end) as save_address_otp_success,
    sum(save_address_otp_screen_loaded) as save_address_otp_screen_loaded,
    count(*) as total
  from
    base
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
    '''
data_to_write_sdf = spark.sql(query1)  # define spark df
data_to_write_sdf.write.mode('append').partitionBy(
    'producer_created_date',
).parquet(
    final_filename,
)  # write data to s3 bucket
"""

# COMMAND ----------

# Example usage
diff_output = compare_code(text1, text2)
print(diff_output)

# COMMAND ----------


