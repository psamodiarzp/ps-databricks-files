# Databricks notebook source
from datetime import date, timedelta

# COMMAND ----------

import argparse
import json
import time
from datetime import datetime

import boto3
import pytz
import requests
from pyspark.sql import SparkSession

# COMMAND ----------

def get_spark():
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    return spark

spark = get_spark()

# COMMAND ----------

import pandas as pd

# COMMAND ----------

temp_table = '''
INSERT INTO
  hive.aggregate_pa.temp_cx_1cc_events_dump_v1
SELECT
  *
FROM(
    WITH raw AS(
      SELECT
        json_extract_scalar (context, '$.checkout_id') AS checkout_id,
        json_extract_scalar(context, '$.order_id') AS order_id,
        json_extract_scalar (properties, '$.options.key') AS merchant_key,
        k.merchant_id AS merchant_id,
        ---- making them null from 2023-01-04 on---
        CAST(NULL AS varchar) AS customer_id,
        CAST(NULL AS varchar) AS product_type,
        event_timestamp,
        l.event_name,
        'true' AS is_1cc_checkout,
        CAST(
          json_extract_scalar (properties, '$.data.meta.first_screen') AS varchar
        ) AS first_screen_name,
        CAST(
          json_extract_scalar (properties, '$.data.meta.is_mandatory_signup') AS varchar
        ) is_mandatory_signup,
        CAST(
          json_extract_scalar (properties, '$.data.meta.coupons_enabled') AS varchar
        ) is_coupons_enabled,
        CAST(
          json_extract_scalar (
            properties,
            '$.data.meta.available_coupons_count'
          ) AS varchar
        ) available_coupons_count,
        CAST(
          json_extract_scalar (properties, '$.data.meta.address_enabled') AS varchar
        ) is_address_enabled,
        json_extract_scalar (properties, '$.data.meta.address_screen_type') address_screen_type,
        CAST(
          json_extract_scalar (properties, '$.data.meta.saved_address_count') AS varchar
        ) AS saved_address_count,
        CAST(
          json_extract_scalar (properties, '$.data.meta["count.savedCards"]') AS varchar
        ) count_saved_cards,
        json_extract_scalar (properties, '$.data.meta.loggedIn') AS logged_in,
        event_timestamp_raw,
        LOWER(json_extract_scalar (context, '$.platform')) platform,
        json_extract_scalar(context, '$.user_agent_parsed.user_agent.family') AS browser_name,
        coalesce(
          LOWER(
            json_extract_scalar (properties, '$.data.data.method')
          ),
          LOWER(
            json_extract_scalar (properties, '$.data.method')
          )
        ) AS method,
        json_extract_scalar(properties, '$.data.meta.p13n') shown_p13n,
        json_extract_scalar(context, '$.mode') AS is_test_mode,
        /* live / test */
        IF(
          event_name = 'checkoutCODOptionShown'
          AND (
            json_extract_scalar (properties, '$.data.disabled') = 'false'
            OR json_extract_scalar (properties, '$.data.disabled') IS NULL
          ),
          'yes',
          'no'
        ) AS is_order_COD_eligible,
        CAST(
          json_extract_scalar (properties, '$.data.otp_reason') AS varchar
        ) AS rzp_OTP_reason,
        json_extract_scalar (properties, '$.data.opted_to_save_address') is_user_opted_to_save_address,
        json_extract_scalar (properties, '$.data.addressSaved') is_new_address_saved,
        json_extract_scalar (properties, '$.data.is_saved_address') AS is_saved_address,
        json_extract_scalar(properties, '$.data.address_id') AS address_id,
        properties,
        context,
        producer_timestamp,
        from_unixtime(producer_timestamp) AS producer_time,
        substr(
          CAST(
            json_extract(context, '$["device.id"]') AS varchar
          ),
          1,
          42
        ) AS device_id,
        CAST(producer_created_date AS date) AS producer_created_date,
      c.utm_source,
      c.utm_medium,
      c.utm_campaign
      FROM
        (
          SELECT
            event_name,
            properties,
            context,
            producer_created_date,
            event_timestamp,
            event_timestamp_raw,
            producer_timestamp
          FROM
            hive.events.lumberjack_intermediate
          WHERE
            source = 'checkoutjs'
            AND LOWER(json_extract_scalar(context, '$.library')) = 'checkoutjs'
            AND CAST(producer_created_date AS date) = DATE_ADD('day', -1, CURRENT_DATE)
            AND CAST(
              json_extract_scalar(
                properties,
                '$.data.meta.is_one_click_checkout_enabled_lite'
              ) AS boolean
            ) = TRUE
        ) AS l --- new code 01/04"
        LEFT JOIN realtime_hudi_api.keys k ON SUBSTR(
          (
            json_extract_scalar(l.properties, '$.options.key')
          ),
          10,
          14
        ) = k.id
      LEFT JOIN (
          SELECT
            order_id,
            type,
            json_extract_scalar(value, '$.utm_parameters.utm_source') AS utm_source,
            json_extract_scalar(value, '$.utm_parameters.utm_medium') AS utm_medium,
            json_extract_scalar(value, '$.utm_parameters.utm_campaign') AS utm_campaign
          FROM
            realtime_hudi_api.order_meta
          WHERE
            type = 'one_click_checkout'
        ) c ON substr(json_extract_scalar(context, '$.order_id'), 7) = c.order_id
    ),
    step1 AS(
      SELECT
        *,
        event_timestamp - (
          lead(event_timestamp) OVER (
            partition BY merchant_id,
            producer_created_date,
            device_id
            ORDER BY
              event_timestamp_raw DESC
          )
        ) AS lag_diff
      FROM
        raw
    ),
    step2 AS(
      SELECT
        *,
        CASE
          WHEN lag_diff >= 1800
          OR lag_diff IS NULL THEN 1
          ELSE 0
        END AS is_new_session
      FROM
        step1
    ),
    step3 AS(
      SELECT
        *,
        concat(
          CAST(
            sum(is_new_session) over(
              partition BY merchant_id,
              producer_created_date,
              device_id
              ORDER BY
                event_timestamp_raw
            ) AS varchar
          ),
          '-',
          CAST(producer_created_date AS varchar),
          '-',
          device_id
        ) AS session_id
      FROM
        step2
    )
    SELECT
      checkout_id,
      order_id,
      merchant_key,
      merchant_id,
      customer_id,
      product_type,
      event_timestamp,
      event_name,
      is_1cc_checkout,
      first_screen_name,
      is_mandatory_signup,
      is_coupons_enabled,
      available_coupons_count,
      is_address_enabled,
      address_screen_type,
      saved_address_count,
      count_saved_cards,
      logged_in,
      event_timestamp_raw,
      platform,
      browser_name,
      method,
      shown_p13n,
      is_test_mode,
      is_order_cod_eligible,
      rzp_otp_reason,
      is_user_opted_to_save_address,
      is_new_address_saved,
      is_saved_address,
      address_id,
      properties,
      context,
      device_id,
      session_id,
      producer_timestamp,
      CAST(producer_time AS varchar),
      utm_source,
      utm_medium,
      utm_campaign,
      producer_created_date
    FROM
      step3
  )

'''

# COMMAND ----------

temp_table_db = spark.sql(temp_table)


# COMMAND ----------

spark.sql("SELECT count(*) from temp_table_db")


# COMMAND ----------


