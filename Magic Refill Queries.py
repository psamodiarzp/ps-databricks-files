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

def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)

# COMMAND ----------


    df_master = spark.sql(
    f"""
    INSERT INTO
  aggregate_pa.temp_magic_rto_reimbursement_fact
SELECT
  *
FROM
  (
    SELECT
      order_id, fulfillment_id, shipping_provider, rto_charges, shipping_status, shipping_charges,
      source_origin, source, fulfillment_created_date, status, merchant_id, fulfillment_updated_date,
      fulfillment_updated_timestamp, experimentation, cod_intelligence_enabled, cod_eligible, IsPhoneWhitelisted, IsEmailWhitelisted, citytier, ml_flag,   rule_flag, is_rule_applied, ml_model_id,
    risk_tier, merchant_order_id, result_flag,
    order_status,
    order_created_date,
    order_updated_date,
    review_status, reviewed_at, reviewed_by, awb_number,
     row_number() over(
        partition BY order_id
        ORDER BY
          fulfillment_updated_timestamp DESC
      ) AS fulfillment_row_num
    FROM
      (
        SELECT
          a.order_id,
          c.id AS fulfillment_id,
          c.shipping_provider,
          get_json_object(c.shipping_provider, '$.rto_charges') AS rto_charges,
          get_json_object(c.shipping_provider, '$.shipping_status') AS shipping_status,
          get_json_object(c.shipping_provider, '$.shipping_charges') AS shipping_charges,
          get_json_object(c.source, '$.origin') AS source_origin,
          c.source,
          date(c.created_date) AS fulfillment_created_date,
          c.status,
          c.merchant_id AS merchant_id,
          date(from_unixtime(c.updated_at)) AS fulfillment_updated_date,
          CAST(from_unixtime(c.updated_at) AS timestamp) AS fulfillment_updated_timestamp,
          -- Order Meta
          CAST(
            get_json_object(a.value, '$.cod_intelligence.experimentation') AS boolean
          ) AS experimentation,
          CAST(
            get_json_object(a.value, '$.cod_intelligence.enabled') AS boolean
          ) AS cod_intelligence_enabled,
          CAST(
            get_json_object(a.value, '$.cod_intelligence.cod_eligible') AS boolean
          ) AS cod_eligible,
          CASE
            WHEN b.rules_evaluated LIKE '%IsPhoneWhitelisted%' THEN 1
            ELSE 0
          END AS IsPhoneWhitelisted,
          CASE
            WHEN b.rules_evaluated LIKE '%IsEmailWhitelisted%' THEN 1
            ELSE 0
          END AS IsEmailWhitelisted,
          --- Events table
          b.citytier,
          b.ml_flag,
          b.rule_flag,
          b.is_rule_applied,
          b.ml_model_id,
          CAST(
            get_json_object(a.value, '$.cod_intelligence.risk_tier') AS string
          ) AS risk_tier,
          c.merchant_order_id,
          (
            CASE
              WHEN (
                b.ml_flag = 'green'
                AND b.rule_flag = 'green'
              ) THEN 'green'
              ELSE 'red'
            END
          ) AS result_flag,
          --o.status order_status,
          null as order_status,
          date(from_unixtime(a.created_at + 19800)) AS order_created_date,
          date(from_unixtime(a.updated_at + 19800)) AS order_updated_date,
          get_json_object(a.value, '$.review_status') as review_status,
          get_json_object(a.value, '$.reviewed_at') as reviewed_at,
          get_json_object(a.value, '$.reviewed_by') as reviewed_by,
          get_json_object(c.shipping_provider, '$.awb_number') AS awb_number
        FROM
          realtime_hudi_api.order_meta a
         -- LEFT JOIN realtime_hudi_api.orders o on a.order_id=o.id
          LEFT JOIN realtime_prod_shipping_service.fulfillment_orders c ON c.order_id = a.order_id
          LEFT JOIN(
            SELECT *
            FROM (
            SELECT
              row_number() over(
                partition BY order_id
                ORDER BY
                  event_timestamp DESC
              ) AS row_num,
              *
            FROM
              events.events_1cc_tw_cod_score_v2
          ) WHERE row_num = 1
          ) b ON c.order_id = substr(b.order_id, 7)
      WHERE
          ( (
        date(a.created_date) = Date(date_sub(CURRENT_DATE,1))
      )
      OR (
        date(from_unixtime(a.updated_at + 19800)) = Date(date_sub(CURRENT_DATE,1))
      )
      OR  (
        date(from_unixtime(c.updated_at)) = Date(date_sub(CURRENT_DATE,1))
      )
      OR (
        date(from_unixtime(c.created_at)) = Date(date_sub(CURRENT_DATE,1))
      )
         )
          AND a.type = 'one_click_checkout'
        UNION ALL
        SELECT
          *
        FROM
          aggregate_pa.magic_rto_reimbursement_fact
      )
      
  )
    WHERE
  fulfillment_row_num = 1;
    """
    )
    

# COMMAND ----------

sql_list = [f"select * from survey_db.{table}" for table in table_array]
for sql in sql_list:
    df = spark.sql(sql)
    df.show()


# COMMAND ----------

start_date = date(2023, 1, 8)
end_date = date(2023, 1, 10)
for single_date in daterange(start_date, end_date):
  

# COMMAND ----------

#Deletion from cx events 
spark.sql(
    f"""
delete from aggregate_pa.cx_1cc_experiment_fact where producer_created_date = date('2023-11-30')
""")

# COMMAND ----------

##Backfilling cx events Part 2 VERIFIED

spark.sql(
    f"""
INSERT INTO
  aggregate_pa.cx_1cc_events_dump_v1
SELECT
  *
FROM(
    WITH raw AS(
      SELECT
        get_json_object (context, '$.checkout_id') AS checkout_id,
        get_json_object(context, '$.order_id') AS order_id,
        get_json_object (properties, '$.options.key') AS merchant_key,
        k.merchant_id AS merchant_id,
        ---- making them null from 2023-01-04 on---
        CAST(NULL AS string) AS customer_id,
        CAST(NULL AS string) AS product_type,
        event_timestamp,
        l.event_name,
        'true' AS is_1cc_checkout,
        CAST(
          get_json_object (properties, '$.data.meta.first_screen') AS string
        ) AS first_screen_name,
        CAST(
          get_json_object (properties, '$.data.meta.is_mandatory_signup') AS string
        ) is_mandatory_signup,
        CAST(
          get_json_object (properties, '$.data.meta.coupons_enabled') AS string
        ) is_coupons_enabled,
        CAST(
          get_json_object (
            properties,
            '$.data.meta.available_coupons_count'
          ) AS string
        ) available_coupons_count,
        CAST(
          get_json_object (properties, '$.data.meta.address_enabled') AS string
        ) is_address_enabled,
        get_json_object (properties, '$.data.meta.address_screen_type') address_screen_type,
        CAST(
          get_json_object (properties, '$.data.meta.saved_address_count') AS string
        ) AS saved_address_count,
        CAST(
          get_json_object (properties, '$.data.meta["count.savedCards"]') AS string
        ) count_saved_cards,
        get_json_object (properties, '$.data.meta.loggedIn') AS logged_in,
        event_timestamp_raw,
        LOWER(get_json_object (context, '$.platform')) platform,
        get_json_object(context, '$.user_agent_parsed.user_agent.family') AS browser_name,
        coalesce(
          LOWER(
            get_json_object (properties, '$.data.data.method')
          ),
          LOWER(
            get_json_object (properties, '$.data.method')
          )
        ) AS method,
        get_json_object(properties, '$.data.meta.p13n') shown_p13n,
        get_json_object(context, '$.mode') AS is_test_mode,
        /* live / test */
        IF(
          event_name = 'checkoutCODOptionShown'
          AND (
            get_json_object (properties, '$.data.disabled') = 'false'
            OR get_json_object (properties, '$.data.disabled') IS NULL
          ),
          'yes',
          'no'
        ) AS is_order_COD_eligible,
        CAST(
          get_json_object (properties, '$.data.otp_reason') AS string
        ) AS rzp_OTP_reason,
        get_json_object (properties, '$.data.opted_to_save_address') is_user_opted_to_save_address,
        get_json_object (properties, '$.data.addressSaved') is_new_address_saved,
        get_json_object (properties, '$.data.is_saved_address') AS is_saved_address,
        get_json_object(properties, '$.data.address_id') AS address_id,
        properties,
        context,
        producer_timestamp,
        from_unixtime(producer_timestamp) AS producer_time,
          CAST(
            get_json_object(context, '$["device.id"]') AS string
          )AS device_id,
        CAST(producer_created_date AS date) AS producer_created_date,
        c.utm_source,
        c.utm_medium,
        c.utm_campaign,
        'checkoutjs' as library,
        try_cast('-1' as integer) as is_magic_x
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
            events.lumberjack_intermediate
          WHERE
            source = 'checkoutjs'
            AND LOWER(get_json_object(context, '$.library')) = 'checkoutjs'
            AND CAST(producer_created_date AS date) between DATE('2024-08-01') and DATE('2024-08-15')
            AND ( try_cast(
              get_json_object(
                properties,
                '$.data.meta.is_one_click_checkout_enabled_lite'
              ) AS boolean
            ) = TRUE
           OR 
              get_json_object(properties, '$.options.one_click_checkout')  = 'true'
            )
        ) AS l --- new code 01/04"
        LEFT JOIN realtime_hudi_api.keys k ON SUBSTR(
          (
            get_json_object(l.properties, '$.options.key')
          ),
          10,
          14
        ) = k.id
        LEFT JOIN (
          SELECT
            order_id,
            type,
            get_json_object(value, '$.utm_parameters.utm_source') AS utm_source,
            get_json_object(value, '$.utm_parameters.utm_medium') AS utm_medium,
            get_json_object(value, '$.utm_parameters.utm_campaign') AS utm_campaign
          FROM
            realtime_hudi_api.order_meta
          WHERE
            type = 'one_click_checkout'
        ) c ON substr(get_json_object(context, '$.order_id'), 7) = c.order_id
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
            ) AS string
          ),
          '-',
          CAST(producer_created_date AS string),
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
      CAST(producer_time AS string),
      utm_source,
      utm_medium,
      utm_campaign,
      library,
      is_magic_x,
      producer_created_date
    FROM
      step3

     )
  """)

# COMMAND ----------

##Backfilling cx events Part 3 (MagicX) VERIFIED
spark.sql(
    f"""
INSERT INTO
  hive.aggregate_pa.cx_1cc_events_dump_v1
  --INSERT INTO hive.aggregate_pa.cx_1cc_events_dump_v1
  select * from(
    WITH raw as (
      SELECT
        coalesce(get_json_object (context, '$.checkout_id'), get_json_object (properties, '$.meta.checkout_id') ) AS checkout_id,
        coalesce( get_json_object (context, '$.analytics_order_id'), get_json_object (properties, '$.meta.analytics_order_id') ) AS order_id,
        substr(get_json_object (context, '$.key'),10) AS merchant_key,
        coalesce( get_json_object (context, '$.storefront_url'), get_json_object (properties, '$.meta.storefront_url') ) AS merchant_id,
        null as customer_id,
        'MagicX' as product_type,
        event_timestamp,
        l.event_name,
        'true' AS is_1cc_checkout,
        try_cast(
          get_json_object (properties, '$.data.meta.first_screen') AS varchar
        ) AS first_screen_name,
        try_cast(
          get_json_object (properties, '$.data.meta.is_mandatory_signup') AS varchar
        ) is_mandatory_signup,
        try_cast(
          get_json_object (properties, '$.data.meta.coupons_enabled') AS varchar
        ) is_coupons_enabled,
        try_cast(
          get_json_object (properties, '$.data.meta.available_coupons_count') AS varchar
        ) available_coupons_count,
        try_cast(
          get_json_object (properties, '$.data.meta.address_enabled') AS varchar
        ) is_address_enabled,
        get_json_object (properties, '$.data.meta.address_screen_type') address_screen_type,
        try_cast(
          get_json_object (properties, '$.data.meta.saved_address_count') AS varchar
        ) AS saved_address_count,
        try_cast(
          get_json_object (properties, '$.data.meta["count.savedCards"]') AS varchar
        ) count_saved_cards,
        get_json_object (properties, '$.data.meta.loggedIn') AS logged_in,
        event_timestamp_raw,
        LOWER(get_json_object (context, '$.platform')) platform,
        get_json_object(context, '$.user_agent_parsed.user_agent.family') AS browser_name,
        coalesce(
          LOWER(
            get_json_object (properties, '$.data.data.method')
          ),
          LOWER(get_json_object (properties, '$.data.method'))
        ) AS method,
        get_json_object(properties, '$.data.meta.p13n') shown_p13n,
        get_json_object(context, '$.mode') AS is_test_mode,
        /* live / test */
        IF(
          event_name = 'checkoutCODOptionShown'
          AND (
            get_json_object (properties, '$.data.disabled') = 'false'
            OR get_json_object (properties, '$.data.disabled') IS NULL
          ),
          'yes',
          'no'
        ) AS is_order_COD_eligible,
        try_cast(
          get_json_object (properties, '$.data.otp_reason') AS varchar
        ) AS rzp_OTP_reason,
        get_json_object (properties, '$.data.opted_to_save_address') is_user_opted_to_save_address,
        get_json_object (properties, '$.data.addressSaved') is_new_address_saved,
        get_json_object (properties, '$.data.is_saved_address') AS is_saved_address,
        get_json_object(properties, '$.data.address_id') AS address_id,
        properties,
        context,
        producer_timestamp,
        from_unixtime(producer_timestamp) AS producer_time,
          try_cast(
            json_extract(context, '$["device.id"]') AS varchar
          ) AS  device_id,
        try_cast(producer_created_date AS date) AS producer_created_date,
           null as utm_source,
      null as utm_medium,
      null as utm_campaign,
      'magic-x' as library,
      session_id,
      try_cast('1' as integer) as is_magic_x
      FROM
        (
          SELECT
            event_name,
            properties,
            context,
            producer_created_date,
            event_timestamp,
            event_timestamp_raw,
            producer_timestamp,
            coalesce(get_json_object (context, '$.shopify_session_token'), get_json_object (properties, '$.meta.shopify_session_token') ) as session_id
          FROM
            hive.events.lumberjack_intermediate
          WHERE
            get_json_object(context, '$.lib') = 'magic-x'

       AND producer_created_date between '2024-08-01' and '2024-08-31'
       --producer_created_date >=  cast(DATE_ADD('day', -70, CURRENT_DATE) as varchar)

        ) AS l
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
      try_cast(producer_time AS varchar),
      utm_source,
      utm_medium,
      utm_campaign,
      library,
      is_magic_x,
      producer_created_date
    FROM
      raw
     )
     """)

# COMMAND ----------

#Backfilling Magic RTO fact Part 2


spark.sql(
    f"""
    INSERT INTO
  aggregate_pa.temp_magic_rto_reimbursement_fact
SELECT
  *
FROM
  (
    SELECT
      order_id, fulfillment_id, shipping_provider, rto_charges, shipping_status, shipping_charges,
      source_origin, source, fulfillment_created_date, status, merchant_id, fulfillment_updated_date,
      fulfillment_updated_timestamp, experimentation, cod_intelligence_enabled, cod_eligible, IsPhoneWhitelisted, IsEmailWhitelisted, citytier, ml_flag, rule_flag, is_rule_applied, ml_model_id,
    risk_tier, merchant_order_id, result_flag,
    order_status,
    order_created_date,
    order_updated_date,
    review_status, reviewed_at, reviewed_by, awb_number, manual_control_cod_order,
     row_number() over(
        partition BY order_id
        ORDER BY
          fulfillment_updated_timestamp DESC
      ) AS fulfillment_row_num

    FROM
      (
          
          with min_date as(
              
              (
  SELECT (CASE WHEN min_date2<=min_date1 THEN min_date2
               WHEN min_date1<min_date2 THEN min_date1
               ELSE NULL END) min_date
  FROM (
          SELECT min((date(from_unixtime(a.created_at + 19800)))) min_date1, min((date(from_unixtime(b.created_at)))) min_date2
          FROM
          (SELECT order_id, created_date, created_at, updated_at from realtime_hudi_api.order_meta WHERE type = 'one_click_checkout') a
          LEFT JOIN (SELECT id, updated_at, created_at from realtime_prod_shipping_service.fulfillment_orders) b ON a.order_id = b.id
          WHERE
          ((
        date(a.created_date) IN (date('2023-06-02'), date('2023-07-08'), date('2023-07-12'), date('2023-07-14'), date('2023-07-15')
      ))
      OR (
        date(from_unixtime(a.updated_at + 19800)) >= (date('2023-06-02'), date('2023-07-08'), date('2023-07-12'), date('2023-07-14'), date('2023-07-15')
      )
      )
      OR (
        date(from_unixtime(b.updated_at)) >= (date('2023-06-02'), date('2023-07-08'), date('2023-07-12'), date('2023-07-14'), date('2023-07-15')
      )
      )
      OR (
        date(from_unixtime(b.created_at)) >= (date('2023-06-02'), date('2023-07-08'), date('2023-07-12'), date('2023-07-14'), date('2023-07-15')
      )
      )
      
)
)
          )
          )
          
          
          ,a AS (
               SELECT
          a.order_id,
           CAST(
            get_json_object(a.value, '$.cod_intelligence.experimentation') AS boolean
          ) AS experimentation,
          CAST(
            get_json_object(a.value, '$.cod_intelligence.enabled') AS boolean
          ) AS cod_intelligence_enabled,
          CAST(
            get_json_object(a.value, '$.cod_intelligence.cod_eligible') AS boolean
          ) AS cod_eligible,
             CAST(
            get_json_object(a.value, '$.cod_intelligence.risk_tier') AS string
          ) AS risk_tier,
              date(from_unixtime(a.created_at + 19800)) AS order_created_date,
          date(from_unixtime(a.updated_at + 19800)) AS order_updated_date,

          get_json_object(a.value, '$.review_status') as review_status,
          get_json_object(a.value, '$.reviewed_at') as reviewed_at,
          get_json_object(a.value, '$.reviewed_by') as reviewed_by,
            get_json_object(a.value, '$.cod_intelligence.manual_control_cod_order') AS manual_control_cod_order

    FROM
          realtime_hudi_api.order_meta a
          WHERE
          (
        date(a.created_date) in (date('2023-06-02'), date('2023-07-08'), date('2023-07-12'), date('2023-07-14'), date('2023-07-15')
      )
      OR 
        date(from_unixtime(a.updated_at + 19800)) in  (date('2023-06-02'), date('2023-07-08'), date('2023-07-12'), date('2023-07-14'), date('2023-07-15')
      ))
        AND a.type = 'one_click_checkout'

          ), 
          c AS(
            SELECT
    c.id AS fulfillment_id,
            c.shipping_provider,
            get_json_object(c.shipping_provider, '$.rto_charges') AS rto_charges,
            get_json_object(c.shipping_provider, '$.shipping_status') AS shipping_status,
            get_json_object(c.shipping_provider, '$.shipping_charges') AS shipping_charges,
            get_json_object(c.source, '$.origin') AS source_origin,
            c.source,
            date(c.created_date) AS fulfillment_created_date,
            c.status,
          --  c.merchant_id AS merchant_id,
              date(from_unixtime(c.updated_at)) AS fulfillment_updated_date,
          CAST(from_unixtime(c.updated_at) AS timestamp) AS fulfillment_updated_timestamp,
           c.merchant_order_id,
               get_json_object(c.shipping_provider, '$.awb_number') AS awb_number
               from 
               realtime_prod_shipping_service.fulfillment_orders c
                WHERE
          
       (
        date(from_unixtime(c.updated_at)) in  (date('2023-06-02'), date('2023-07-08'), date('2023-07-12'), date('2023-07-14'), date('2023-07-15')
      )
      OR (
        date(from_unixtime(c.created_at)) in  (date('2023-06-02'), date('2023-07-08'), date('2023-07-12'), date('2023-07-14'), date('2023-07-15')
      )
         )

          )),
          o AS (
           SELECT id, merchant_id, status
           FROM realtime_hudi_api.orders
           WHERE date(created_date) >= (Select * from min_date)
)

       
SELECT
a.*,
c.*,
o.*,
          CASE
            WHEN b.rules_evaluated LIKE '%IsPhoneWhitelisted%' THEN 1
            ELSE 0
          END AS IsPhoneWhitelisted,
          CASE
            WHEN b.rules_evaluated LIKE '%IsEmailWhitelisted%' THEN 1
            ELSE 0
          END AS IsEmailWhitelisted,
          --- Events table
          b.citytier,
          b.ml_flag,
          b.rule_flag,
          b.is_rule_applied,
          b.ml_model_id,
       
         
          (
            CASE
            WHEN b.ml_flag is null and b.rule_flag is null then null
            WHEN b.ml_flag is null or b.rule_flag is null then 'Either rule or ml flag is missing'
              WHEN (
                b.ml_flag = 'green'
                AND b.rule_flag = 'green'
              ) THEN 'green'
              ELSE 'red'
            END
          ) AS result_flag

          FROM a 
          left join c  ON a.order_id = c.order_id
           left join o on a.order_id = o.id   
          LEFT JOIN(
            SELECT *
            FROM (
            SELECT
              row_number() over(
                partition BY order_id
                ORDER BY
                  event_timestamp DESC
              ) AS row_num,
              *
            FROM
              events.events_1cc_tw_cod_score_v2
          ) WHERE row_num = 1
          ) b ON a.order_id = substr(b.order_id, 7)
       
        
        UNION ALL
        SELECT
          *
        FROM
          aggregate_pa.magic_rto_reimbursement_fact
      )
  )
WHERE
  fulfillment_row_num = 1
  """)

# COMMAND ----------

#Backfilling cx_1cc_events_dump
spark.sql(
    f"""
INSERT INTO
  aggregate_pa.cx_1cc_events_dump_v1
  select * from(
    WITH raw as (
      SELECT
        get_json_object (context, '$.checkout_id') AS checkout_id,
        get_json_object(context,'$.order_id') AS order_id,
        get_json_object (properties, '$.options.key') AS merchant_key,
        b.merchant_id AS merchant_id,
        b.customer_id,
        b.product_type,
        event_timestamp,
        l.event_name,
        'true' AS is_1cc_checkout,
        CAST(
          get_json_object (properties, '$.data.meta.first_screen') AS string
        ) AS first_screen_name,
        CAST(
          get_json_object (properties, '$.data.meta.is_mandatory_signup') AS string
        ) is_mandatory_signup,
        CAST(
          get_json_object (properties, '$.data.meta.coupons_enabled') AS string
        ) is_coupons_enabled,
        CAST(
          get_json_object (properties, '$.data.meta.available_coupons_count') AS string
        ) available_coupons_count,
        CAST(
          get_json_object (properties, '$.data.meta.address_enabled') AS string
        ) is_address_enabled,
        get_json_object (properties, '$.data.meta.address_screen_type') address_screen_type,
        CAST(
          get_json_object (properties, '$.data.meta.saved_address_count') AS string
        ) AS saved_address_count,
        CAST(
          get_json_object (properties, '$.data.meta["count.savedCards"]') AS string
        ) count_saved_cards,
        get_json_object (properties, '$.data.meta.loggedIn') AS logged_in,
        event_timestamp_raw,
        LOWER(get_json_object (context, '$.platform')) platform,
        get_json_object(context, '$.user_agent_parsed.user_agent.family') AS browser_name,
        coalesce(
          LOWER(
            get_json_object (properties, '$.data.data.method')
          ),
          LOWER(get_json_object (properties, '$.data.method'))
        ) AS method,
        get_json_object(properties, '$.data.meta.p13n') shown_p13n,
        get_json_object(context, '$.mode') AS is_test_mode,
        /* live / test */
        IF(
          event_name = 'checkoutCODOptionShown'
          AND (
            get_json_object (properties, '$.data.disabled') = 'false'
            OR get_json_object (properties, '$.data.disabled') IS NULL
          ),
          'yes',
          'no'
        ) AS is_order_COD_eligible,
        CAST(
          get_json_object (properties, '$.data.otp_reason') AS string
        ) AS rzp_OTP_reason,
        get_json_object (properties, '$.data.opted_to_save_address') is_user_opted_to_save_address,
        get_json_object (properties, '$.data.addressSaved') is_new_address_saved,
        get_json_object (properties, '$.data.is_saved_address') AS is_saved_address,
        get_json_object(properties, '$.data.address_id') AS address_id,
        properties,
        context,
        producer_timestamp,
        from_unixtime(producer_timestamp) AS producer_time,
        substr(
          CAST(
            get_json_object(context, '$["device.id"]') AS string
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
            events.lumberjack_intermediate
          WHERE
            source = 'checkoutjs'
            AND LOWER(get_json_object(context, '$.library')) = 'checkoutjs'
            AND CAST(producer_created_date AS date) = CURRENT_DATE
             AND CAST(
              get_json_object(
                properties,
                '$.data.meta.is_one_click_checkout_enabled_lite'
              ) AS boolean
            ) <> TRUE
        ) AS l
        LEFT JOIN (
          SELECT
            id AS order_id,
            merchant_id,
            customer_id,
            product_type
          FROM
            realtime_hudi_api.orders
          WHERE
         date(created_date) between DATE('2023-08-26') and DATE('2023-08-30')
        ) b ON substr(
          get_json_object(context,'$.order_id'),
          7
        ) = b.order_id
        INNER JOIN (
          SELECT
            order_id,
            type,
            get_json_object(value, '$.utm_parameters.utm_source') AS utm_source,
            get_json_object(value, '$.utm_parameters.utm_medium') AS utm_medium,
            get_json_object(value, '$.utm_parameters.utm_campaign') AS utm_campaign
          FROM
            realtime_hudi_api.order_meta
          WHERE
            type = 'one_click_checkout'
        ) c ON substr(
          get_json_object(context,'$.order_id'),
          7
        ) = c.order_id
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
            ) AS string
          ),
          '-',
          CAST(producer_created_date AS string),
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
      CAST(producer_time AS string),
      utm_source,
      utm_medium,
      utm_campaign,
      producer_created_date
    FROM
      step3
     )
      """)

# COMMAND ----------

#Backfilling cx_experiment_fact
spark.sql(
    f"""
    INSERT INTO aggregate_pa.cx_1cc_experiment_fact

 WITH base AS (
    SELECT
      checkout_id,
      get_json_object(properties, '$.magicExperiments') AS magic_experiments,
      producer_created_date
    FROM aggregate_pa.cx_1cc_events_dump_v1
    WHERE producer_created_date = date('2023-08-26')
  )
  
  SELECT
    DISTINCT b.checkout_id,
    CAST(key AS string) AS experiment_name,
    CAST(value AS string) AS experiment_value,
    producer_created_date
  FROM base AS b
  CROSS JOIN UNNEST(b.magic_experiments) AS unnested_exp_map(key, value)
      """)

# COMMAND ----------

#Backfilling RTO fact part 1
spark.sql(
    f"""
CREATE TABLE IF NOT EXISTS aggregate_pa.temp_magic_rto_reimbursement_fact(
order_id string,
fulfillment_id string,
shipping_provider string,
rto_charges string,
shipping_status string,
shipping_charges string,
source_origin string,
source string,
fulfillment_created_date date,
status string,
merchant_id string,
fulfillment_updated_date date,
fulfillment_updated_timestamp timestamp,
experimentation boolean,
cod_intelligence_enabled boolean,
cod_eligible boolean,
IsPhoneWhitelisted integer,
IsEmailWhitelisted integer,
citytier bigint,
ml_flag string,
rule_flag string,
is_rule_applied boolean,
ml_model_id string,
risk_tier string,
merchant_order_id string,
result_flag string,
order_status string,
order_created_date date,
order_updated_date date,
review_status string,
reviewed_at string,
reviewed_by string,
awb_number string,
manual_control_cod_order string,
fulfillment_row_num bigint
)
  """)


# COMMAND ----------

#Backfilling RTO fact part 2
#Backfilling Magic RTO fact


spark.sql(
f"""
INSERT INTO
  aggregate_pa.temp_magic_rto_reimbursement_fact
SELECT
  *
FROM
  (
    SELECT
      order_id,
      fulfillment_id,
      shipping_provider,
      rto_charges,
      shipping_status,
      shipping_charges,
      source_origin,
      source,
      fulfillment_created_date,
      status,
      merchant_id,
      fulfillment_updated_date,
      fulfillment_updated_timestamp,
      experimentation,
      cod_intelligence_enabled,
      cod_eligible,
      IsPhoneWhitelisted,
      IsEmailWhitelisted,
      citytier,
      ml_flag,
      rule_flag,
      is_rule_applied,
      ml_model_id,
      risk_tier,
      merchant_order_id,
      result_flag,
      order_status,
      order_created_date,
      order_updated_date,
      review_status,
      reviewed_at,
      reviewed_by,
      awb_number,
      manual_control_cod_order,
      row_number() over(
        partition BY order_id
        ORDER BY
          fulfillment_updated_timestamp DESC
      ) AS fulfillment_row_num
    FROM
      (
        with min_date as(
          (
            SELECT
              (
                CASE
                  WHEN min_date2 <= min_date1 THEN min_date2
                  WHEN min_date1 < min_date2 THEN min_date1
                  ELSE NULL
                END
              ) min_date
            FROM
              (
                SELECT
                  min(
                    (
                      cast(from_unixtime(a.created_at + 19800) as date)
                    )
                  ) as min_date1,
                  min((cast(from_unixtime(c.created_at) as date))) as min_date2
                FROM
                  (
                    SELECT
                      order_id,
                      created_date,
                      created_at,
                      updated_at
                    from
                      realtime_hudi_api.order_meta
                    WHERE
                      type = 'one_click_checkout'
                  ) a
                  LEFT JOIN (
                    SELECT
                      order_id,
                      updated_at,
                      created_at
                    from
                      realtime_prod_shipping_service.fulfillment_orders
                  ) c ON a.order_id = c.order_id
                WHERE
                  (
                    (
                      a.created_date IN (
                        '2023-08-22'
                      )
                    )
                    OR (
                      DATE_FORMAT(
                        FROM_UNIXTIME((a.updated_at + 19800)),
                        'yyyy-MM-dd'
                      ) IN (
                        '2023-08-22'
                      )
                    )
                    OR (
                      DATE_FORMAT(
                        FROM_UNIXTIME(c.updated_at + 19800),
                        'yyyy-MM-dd'
                      ) IN (
                        '2023-08-22'
                      )
                    )
                    OR (
                      DATE_FORMAT(
                        FROM_UNIXTIME(c.updated_at + 19800),
                        'yyyy-MM-dd'
                      ) IN (
                        '2023-08-22'
                      )
                    )
                  )
              )
          )
        ),
        a AS (
          SELECT
            a.order_id,
            CAST(
              get_json_object(a.value, '$.cod_intelligence.experimentation') AS boolean
            ) AS experimentation,
            CAST(
              get_json_object(a.value, '$.cod_intelligence.enabled') AS boolean
            ) AS cod_intelligence_enabled,
            CAST(
              get_json_object(a.value, '$.cod_intelligence.cod_eligible') AS boolean
            ) AS cod_eligible,
            CAST(
              get_json_object(a.value, '$.cod_intelligence.risk_tier') AS string
            ) AS risk_tier,
            cast(from_unixtime(a.created_at + 19800) as date) AS order_created_date,
            cast(from_unixtime(a.updated_at + 19800) as date) AS order_updated_date,
            get_json_object(a.value, '$.review_status') as review_status,
            get_json_object(a.value, '$.reviewed_at') as reviewed_at,
            get_json_object(a.value, '$.reviewed_by') as reviewed_by,
            get_json_object(
              a.value,
              '$.cod_intelligence.manual_control_cod_order'
            ) AS manual_control_cod_order
          FROM
            realtime_hudi_api.order_meta a
          WHERE
            (
              date(a.created_date) in (
                 date('2023-08-22')
              )
              OR date(from_unixtime(a.updated_at + 19800)) in (
                date('2023-08-22')
            
              )
            )
            AND a.type = 'one_click_checkout'
        ),
        c AS(
          SELECT
            c.order_id,
            c.id AS fulfillment_id,
            c.shipping_provider,
            get_json_object(c.shipping_provider, '$.rto_charges') AS rto_charges,
            get_json_object(c.shipping_provider, '$.shipping_status') AS shipping_status,
            get_json_object(c.shipping_provider, '$.shipping_charges') AS shipping_charges,
            get_json_object(c.source, '$.origin') AS source_origin,
            c.source,
            date(c.created_date) AS fulfillment_created_date,
            c.status,
            --  c.merchant_id AS merchant_id,
            date(from_unixtime(c.updated_at)) AS fulfillment_updated_date,
            CAST(from_unixtime(c.updated_at) AS timestamp) AS fulfillment_updated_timestamp,
            c.merchant_order_id,
            get_json_object(c.shipping_provider, '$.awb_number') AS awb_number
          from
            realtime_prod_shipping_service.fulfillment_orders c
          WHERE
            (
              date(from_unixtime(c.updated_at)) in (
                 date('2023-08-22')
              )
              OR date(from_unixtime(c.created_at)) in (
                 date('2023-08-22')
              )
            )
        ),
        o AS (
          SELECT
            id,
            merchant_id,
            status
          FROM
            realtime_hudi_api.orders
          WHERE
            date(created_date) >= (
              Select
                *
              from
                min_date
            )
        )
        SELECT
          a.order_id,
          fulfillment_id,
          shipping_provider,
          rto_charges,
          shipping_status,
          shipping_charges,
          source_origin,
          c.source,
          fulfillment_created_date,
          c.status,
          o.merchant_id,
          fulfillment_updated_date,
          fulfillment_updated_timestamp,
          a.experimentation,
          cod_intelligence_enabled,
          cod_eligible,
          CASE
            WHEN b.rules_evaluated LIKE '%IsPhoneWhitelisted%' THEN 1
            ELSE 0
          END AS IsPhoneWhitelisted,
          CASE
            WHEN b.rules_evaluated LIKE '%IsEmailWhitelisted%' THEN 1
            ELSE 0
          END AS IsEmailWhitelisted,
          b.citytier,
          b.ml_flag,
          b.rule_flag,
          b.is_rule_applied,
          b.ml_model_id,
          a.risk_tier,
          merchant_order_id,
          (
            CASE
              WHEN b.ml_flag is null
              and b.rule_flag is null then null
              WHEN b.ml_flag is null
              or b.rule_flag is null then 'Either rule or ml flag is missing'
              WHEN (
                b.ml_flag = 'green'
                AND b.rule_flag = 'green'
              ) THEN 'green'
              ELSE 'red'
            END
          ) AS result_flag,
          o.status as order_status,
          order_created_date,
          order_updated_date,
          review_status,
          reviewed_at,
          reviewed_by,
          awb_number,
          manual_control_cod_order --- Events table
        FROM
          a
          left join c ON a.order_id = c.order_id
          left join o on a.order_id = o.id
          LEFT JOIN(
            SELECT
              *
            FROM
              (
                SELECT
                  row_number() over(
                    partition BY order_id
                    ORDER BY
                      event_timestamp DESC
                  ) AS row_num,
                  *
                FROM
                  events.events_1cc_tw_cod_score_v2
              )
            WHERE
              row_num = 1
          ) b ON a.order_id = substr(b.order_id, 7)
        UNION ALL
        SELECT
          *
        FROM
          aggregate_pa.magic_rto_reimbursement_fact
      )
  )
WHERE
  fulfillment_row_num = 1

   

   
""")

# COMMAND ----------

#Backfilling Checkout fact part 1


spark.sql(
f"""
INSERT INTO
 aggregate_pa.temp_magic_checkout_fact
  WITH cte AS(
    SELECT
      order_id,
      checkout_id,
      merchant_id,
      producer_created_date,
      browser_name,
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
      CASE
        WHEN event_name = 'render:complete' THEN CAST(
          get_json_object(properties, '$.data.meta.initial_loggedIn') AS boolean
        )
      END AS initial_loggedin,
      CASE
        WHEN event_name = 'render:1cc_summary_screen_loaded_completed' THEN CAST(
          get_json_object(
            properties,
            '$.data.meta.initial_hasSavedAddress'
          ) AS boolean
        )
      END AS initial_hasSavedAddress,
      CASE
        WHEN event_name = 'open' THEN 1
        ELSE 0
      END AS open,
      CASE
        WHEN event_name = 'render:complete' THEN 1
        ELSE 0
      END AS render_complete,
      CASE
        WHEN event_name = 'render:1cc_summary_screen_loaded_completed' THEN 1
        ELSE 0
      END AS summary_screen_loaded,
      CASE
        WHEN event_name = 'behav:1cc_summary_screen_continue_cta_clicked' THEN 1
        ELSE 0
      END AS summary_screen_continue_cta_clicked,
      -- Contact screen events
      CASE
        WHEN event_name = 'behav:1cc_summary_screen_contact_email_entered' THEN 1
        ELSE 0
      END AS contact_email_entered,
      CASE
        WHEN event_name = 'behav:1cc_summary_screen_contact_number_entered' THEN 1
        ELSE 0
      END AS contact_number_entered,
      CASE
        WHEN event_name = 'behav:1cc_clicked_change_contact_summary_screen' THEN 1
        ELSE 0
      END AS clicked_change_contact,
      CASE
        WHEN event_name = 'render:1cc_change_contact_screen_loaded' THEN 1
        ELSE 0
      END AS edit_contact_screen_loaded,
      CASE
        WHEN event_name = 'behav:1cc_back_button_clicked'
        AND CAST(
          get_json_object(properties, '$.data.screen_name') AS string
        ) = 'details_screen' THEN 1
        ELSE 0
      END AS edit_contact_screen_back_button_clicked,
      CASE
        WHEN event_name = 'behav:1cc_clicked_change_contact_continue_cta' THEN 1
        ELSE 0
      END AS edit_contact_screen_submit_cta_clicked,
      CASE
        WHEN event_name = 'behav:1cc_summary_screen_edit_address_clicked' THEN 1
        ELSE 0
      END AS edit_address_clicked,
      CASE
        WHEN event_name = 'behav:1cc_account_screen_logout_clicked'
        OR event_name = 'behav:1cc_account_screen_logout_of_all_devices_clicked' THEN 1
        ELSE 0
      END AS logout_clicked,
      -- Coupon screen events
      CASE
        WHEN event_name = 'render:1cc_coupons_screen_loaded' THEN 1
        ELSE 0
      END AS coupon_screen_loaded,
      CASE
        WHEN event_name = 'behav:1cc_summary_screen_have_coupon_clicked' THEN 1
        ELSE 0
      END AS have_coupon_clicked,
      CASE
        WHEN event_name = 'behav:1cc_summary_screen_remove_coupon_clicked' THEN 1
        ELSE 0
      END AS remove_coupon_clicked,
      CASE
        WHEN event_name = 'behav:1cc_coupons_screen_custom_coupon_entered' THEN 1
        ELSE 0
      END AS custom_coupon_entered,
      CASE
        WHEN event_name = 'behav:1cc_coupons_screen_coupon_applied' THEN 1
        ELSE 0
      END AS coupon_applied,
      CASE
        WHEN event_name = 'behav:1cc_coupons_screen_coupon_applied'
        AND CAST(
          get_json_object(properties, '$.data.coupon_source') AS string
        ) = 'manual' THEN 1
        ELSE 0
      END AS manual_coupon_applied,
      CASE
        WHEN event_name = 'behav:1cc_coupons_screen_coupon_applied'
        AND CAST(
          get_json_object(properties, '$.data.coupon_source') AS string
        ) = 'merchant' THEN 1
        ELSE 0
      END AS merchant_coupon_applied,
      CASE
        WHEN event_name = 'behav:coupon_applied'
        AND CAST(
          get_json_object(properties, '$.data.input_source') AS string
        ) = 'auto' THEN 1
        ELSE 0
      END AS auto_coupon_applied,
      CASE
        WHEN event_name = 'metric:1cc_coupons_screen_coupon_validation_completed'
        AND CAST(
          get_json_object(properties, '$.data.is_coupon_valid') AS boolean
        ) THEN 1
        ELSE 0
      END AS validation_successful,
      CASE
        WHEN event_name = 'behav:1cc_coupons_screen_back_button_clicked' THEN 1
        ELSE 0
      END AS coupon_back_button_clicked,
      CASE
        WHEN coalesce(
          CAST(
            get_json_object(properties, '$.data.count_coupons_available') AS integer
          ),
          0
        ) > 0 THEN 1
        ELSE 0
      END AS coupons_available,
      --- Saved Address Flow Events
      CASE
        WHEN event_name = 'render:1cc_load_saved_address_bottom_sheet_shown' THEN 1
        ELSE 0
      END AS load_saved_address_bottom_sheet_shown,
      CASE
        WHEN event_name = 'behav:1cc_clicked_load_saved_address_bottom_sheet_cta' THEN 1
        ELSE 0
      END AS clicked_load_saved_address_bottom_sheet_cta,
      CASE
        WHEN event_name = 'behav:1cc_dismissed_load_saved_address_bottom_sheet' THEN 1
        ELSE 0
      END AS dismissed_load_saved_address_bottom_sheet,
      CASE
      WHEN event_name = 'submit' THEN 1
      ELSE 0
    END AS submit,
    CASE
      WHEN event_name = 'render:1cc_summary_screen_loaded_completed'
      AND (
        CAST(
          get_json_object(properties, '$.data.prefill_contact_number') AS string
        ) <> ''
      ) THEN 1
      ELSE 0
    END AS prefill_contact_number,
  CASE
      WHEN event_name = 'render:1cc_summary_screen_loaded_completed'
      AND (
        CAST(
          get_json_object(properties, '$.data.prefill_email') AS string
        ) <> ''
      ) THEN 1
      ELSE 0
    END AS prefill_email,
       CASE
      WHEN event_name = 'behav:contact:fill' AND get_json_object(properties,'$.data.value') <> ''
      THEN 1
      ELSE 0
    END AS contact_fill_began,
  CASE
      WHEN event_name = 'behav:email:fill' AND get_json_object(properties,'$.data.value') <> '' THEN 1
      ELSE 0
    END AS email_fill_began,
      CASE
        WHEN event_name = 'metric:checkoutshippinginfoapicallinitiated' THEN 1
        ELSE 0
      END AS pincode_serviceability_initiated,
      CASE
        WHEN event_name = 'metric:checkoutshippinginfoapicallcompleted' THEN 1
        ELSE 0
      END AS pincode_serviceability_successful,
      CASE
        WHEN event_name = 'metric:checkoutshippinginfoapicallcompleted' AND CAST(
          get_json_object(
            properties,
            '$.data.response.addresses[0].serviceable'
          ) AS boolean
        ) = true then 1
        ELSE 0
      END AS pincode_serviceability_true,
      CASE
        WHEN event_name = 'metric:checkoutshippinginfoapicallcompleted' THEN CAST(
          get_json_object(
            properties,
            '$.data.response.addresses[0].zipcode'
          ) AS string
        )
        ELSE '0'
      END AS pincode_serviceability_zipcode,
      CASE
        WHEN event_name = 'metric:checkoutshippinginfoapicallcompleted' THEN CAST(
          get_json_object(
            properties,
            '$.data.response.addresses[0].city'
          ) AS string
        )
        ELSE '0'
      END AS pincode_serviceability_city,
    CASE
        WHEN event_name = 'metric:checkoutshippinginfoapicallcompleted' THEN CAST(
          get_json_object(
            properties,
            '$.data.response.addresses[0].state'
          ) AS string
        )
        ELSE '0'
      END AS pincode_serviceability_state,

      case when ((event_name = 'render:1cc_rzp_otp_screen_loaded') AND (get_json_object(properties,'$.data.otp_reason') IN ('access_address', 'access_address_v2'))) then 1 else 0 end as access_address_otp_screen_loaded,

case when ((event_name = 'behav:1cc_rzp_otp_submitted') AND (get_json_object(properties,'$.data.otp_reason') IN ('access_address', 'access_address_v2' ))) then 1 else 0 end as access_address_otp_screen_submitted,

case when ((event_name = 'behav:1cc_rzp_otp_skip_clicked') AND (get_json_object(properties,'$.data.otp_reason') IN ('access_address', 'access_address_v2' ))) then 1 else 0 end as access_address_otp_screen_skipped,
case when ((event_name = 'behav:1cc_rzp_otp_resend_clicked') AND (get_json_object(properties,'$.data.otp_reason') IN ('access_address', 'access_address_v2' ))) then 1 else 0 end as access_address_otp_screen_resend,

case when ((event_name = 'behav:1cc_rzp_otp_back_clicked') AND (get_json_object(properties,'$.data.otp_reason') IN ('access_address', 'access_address_v2' ))) then 1 else 0 end as access_address_otp_screen_back_clicked,

case when ((event_name = 'render:1cc_rzp_otp_screen_loaded') AND (get_json_object(properties,'$.data.otp_reason') IN ( 'mandatory_login','mandatory_login_v2'))) then 1 else 0 end as mandatory_login_otp_screen_loaded,

case when ((event_name = 'behav:1cc_rzp_otp_submitted') AND (get_json_object(properties,'$.data.otp_reason') IN ('mandatory_login','mandatory_login_v2' ))) then 1 else 0 end as mandatory_login_otp_screen_submitted,

case when ((event_name = 'behav:1cc_rzp_otp_skip_clicked') AND (get_json_object(properties,'$.data.otp_reason') IN ('mandatory_login','mandatory_login_v2' ))) then 1 else 0 end as mandatory_login_otp_screen_skipped,

case when ((event_name = 'behav:1cc_rzp_otp_resend_clicked') AND (get_json_object(properties,'$.data.otp_reason') IN ('mandatory_login','mandatory_login_v2' ))) then 1 else 0 end as mandatory_login_otp_screen_resend,

case when ((event_name = 'behav:1cc_rzp_otp_back_clicked') AND (get_json_object(properties,'$.data.otp_reason') IN ('mandatory_login','mandatory_login_v2' ))) then 1 else 0 end as mandatory_login_otp_screen_back_clicked,

--- to add: modal close on all screens

case when event_name = 'render:1cc_saved_shipping_address_screen_loaded' then 1 else 0 end 
as saved_address_screen_loaded,

case when event_name = 'render:1cc_add_new_address_screen_loaded_completed' then 1 else 0 end 
as new_address_screen_loaded,

case when event_name = 'behav:1cc_saved_address_screen_continue_cta_clicked' then 1 else 0 end 
as saved_address_cta_clicked,

case when event_name = 'behav:1cc_add_new_address_screen_continue_cta_clicked' then 1 else 0 end 
as new_address_cta_clicked,

case when event_name = 'checkoutaddnewaddressctaclicked' then 1 else 0 end 
as new_address_cta_clicked_onsavedaddress,

case when event_name = 'behav:1cc_back_button_clicked' 
and get_json_object(properties, '$.data.screen_name') = 'new_shipping_address_screen'
then 1 else 0 end as new_address_screen_back_button_clicked,

case when event_name = 'behav:1cc_back_button_clicked' 
and get_json_object(properties, '$.data.screen_name') = 'saved_shipping_address_screen'
then 1 else 0 end as saved_address_screen_back_button_clicked,

case when event_name = 'behav:1cc_clicked_edit_saved_address'
then 1 else 0 end as edited_existing_saved_address,

case when event_name = 'checkoutsavenewaddressoptionunchecked'
then 1 else 0 end as save_address_option_unchecked,

case when event_name = 'checkoutsavenewaddressoptionchecked'
then 1 else 0 end as save_address_option_checked,

case when ((event_name = 'render:1cc_rzp_otp_screen_loaded') AND (get_json_object(properties,'$.data.otp_reason') IN ('save_address', 'save_address_v2'))) then 1 else 0 end as save_address_otp_screen_loaded,

case when ((event_name = 'behav:1cc_rzp_otp_submitted') AND (get_json_object(properties,'$.data.otp_reason') IN ('save_address', 'save_address_v2'))) then 1 else 0 end as save_address_otp_screen_submitted,

case when ((event_name = 'behav:1cc_rzp_otp_skip_clicked') AND (get_json_object(properties,'$.data.otp_reason') IN ('save_address', 'save_address_v2'))) then 1 else 0 end as save_address_otp_screen_skipped,

case when ((event_name = 'behav:1cc_rzp_otp_resend_clicked') AND (get_json_object(properties,'$.data.otp_reason') IN ('save_address', 'save_address_v2' ))) then 1 else 0 end as save_address_otp_screen_resend,

case when ((event_name = 'behav:1cc_rzp_otp_back_clicked') AND (get_json_object(properties,'$.data.otp_reason') IN ('save_address', 'save_address_v2' ))) then 1 else 0 end as save_address_otp_screen_back_clicked,



case when event_name = 'render:1cc_payment_home_screen_loaded'
then 1 else 0 end as payment_home_screen_loaded


    FROM
      aggregate_pa.cx_1cc_events_dump_v1
    WHERE
      producer_created_date >= DATE('2023-08-28')--Date(DATE_ADD('day', -1, CURRENT_DATE))
      AND merchant_id IS NOT NULL
      AND merchant_id <> 'Hb4PVe74lPmk0k'
  ),
  cte2 AS(
    SELECT
      a.merchant_id,
      a.checkout_id,
      MIN(producer_created_date) AS producer_created_date,
      max(browser_name) AS browser_name,
      MAX(DISTINCT platform) AS platform,
      MAX(DISTINCT open) AS open,
      max(render_complete) AS render_complete,
      MAX(DISTINCT summary_screen_loaded) AS summary_screen_loaded,
      MAX(DISTINCT contact_number_entered) AS contact_number_entered,
      MAX(DISTINCT contact_email_entered) AS contact_email_entered,
      max(edit_contact_screen_back_button_clicked) AS edit_contact_screen_back_button_clicked,
      MAX(edit_contact_screen_submit_cta_clicked) AS edit_contact_screen_submit_cta_clicked,
      MAX(have_coupon_clicked) AS have_coupon_clicked,
      MAX(remove_coupon_clicked) AS remove_coupon_clicked,
      MAX(DISTINCT coupon_screen_loaded) AS coupon_screen_loaded,
      MAX(DISTINCT custom_coupon_entered) AS custom_coupon_entered,
      MAX(DISTINCT coupon_applied) AS coupon_applied,
      max(manual_coupon_applied) AS manual_coupon_applied,
      max(merchant_coupon_applied) AS merchant_coupon_applied,
      max(auto_coupon_applied) AS auto_coupon_applied,
      MAX(DISTINCT validation_successful) AS validation_successful,
      MAX(DISTINCT coupon_back_button_clicked) AS coupon_back_button_clicked,
      MAX(DISTINCT summary_screen_continue_cta_clicked) AS summary_screen_continue_cta_clicked,
      MAX(DISTINCT coupons_available) AS coupons_available,
      max(edit_address_clicked) AS edit_address_clicked,
    max(logout_clicked) AS logout_clicked,
    max(initial_loggedin) AS initial_loggedin,
      max(initial_hasSavedAddress) AS initial_hasSavedAddress,
      max(clicked_change_contact) as clicked_change_contact,
      max(edit_contact_screen_loaded) as edit_contact_screen_loaded,
      max(load_saved_address_bottom_sheet_shown) as load_saved_address_bottom_sheet_shown,
     max(clicked_load_saved_address_bottom_sheet_cta) as  clicked_load_saved_address_bottom_sheet_cta,
      max(dismissed_load_saved_address_bottom_sheet) as dismissed_load_saved_address_bottom_sheet,
      max(submit) AS submit,
  max(prefill_contact_number) AS prefill_contact_number,
  max(prefill_email) AS prefill_email,
  max(contact_fill_began) AS contact_fill_began,
  max(email_fill_began) AS email_fill_began,
  max(pincode_serviceability_initiated) AS pincode_serviceability_initiated,
      max(pincode_serviceability_successful) AS pincode_serviceability_successful,
     max(pincode_serviceability_true) AS pincode_serviceability_true,
      max(pincode_serviceability_zipcode) AS pincode_serviceability_zipcode,
     max(pincode_serviceability_city) AS pincode_serviceability_city,
      max(pincode_serviceability_state) AS pincode_serviceability_state,
   max(access_address_otp_screen_loaded) as access_address_otp_screen_loaded,
max(access_address_otp_screen_submitted) as access_address_otp_screen_submitted,
max(access_address_otp_screen_skipped) as access_address_otp_screen_skipped,
max(access_address_otp_screen_resend) as access_address_otp_screen_resend,
max(access_address_otp_screen_back_clicked) as access_address_otp_screen_back_clicked,
max(mandatory_login_otp_screen_loaded) as mandatory_login_otp_screen_loaded,
max(mandatory_login_otp_screen_submitted) as mandatory_login_otp_screen_submitted,
max(mandatory_login_otp_screen_skipped) as mandatory_login_otp_screen_skipped,
max(mandatory_login_otp_screen_resend) as mandatory_login_otp_screen_resend,
max(mandatory_login_otp_screen_back_clicked) as mandatory_login_otp_screen_back_clicked,
max(saved_address_screen_loaded) as saved_address_screen_loaded,
max(new_address_screen_loaded) as new_address_screen_loaded,
max(saved_address_cta_clicked) as saved_address_cta_clicked,
max(new_address_cta_clicked) as new_address_cta_clicked,
max(new_address_cta_clicked_onsavedaddress) as new_address_cta_clicked_onsavedaddress,
max(new_address_screen_back_button_clicked) as new_address_screen_back_button_clicked,
max(saved_address_screen_back_button_clicked) as saved_address_screen_back_button_clicked,
max(edited_existing_saved_address) as edited_existing_saved_address,
max(save_address_option_unchecked) as save_address_option_unchecked,
max(save_address_option_checked) as save_address_option_checked,
max(save_address_otp_screen_loaded) as save_address_otp_screen_loaded,
max(save_address_otp_screen_submitted) as save_address_otp_screen_submitted,
max(save_address_otp_screen_skipped) as save_address_otp_screen_skipped,
max(save_address_otp_screen_resend) as save_address_otp_screen_resend,
max(save_address_otp_screen_back_clicked) as save_address_otp_screen_back_clicked,
max(payment_home_screen_loaded) as payment_home_screen_loaded
    FROM
      cte a
    GROUP BY
      1,
      2
  )
    SELECT
      *
    FROM
      cte2
    UNION all
    SELECT
      *
    FROM
      aggregate_pa.magic_checkout_fact;

""")

# COMMAND ----------

#Backfilling Magic Checkout Fact Part 2

spark.sql(
f"""
INSERT INTO
  aggregate_pa.magic_checkout_fact
SELECT
  a.merchant_id,
  a.checkout_id,
MIN(producer_created_date) AS producer_created_date,
max(browser_name) AS browser_name,
MAX(DISTINCT platform) AS platform,
MAX(DISTINCT open) AS open,
max(render_complete) AS render_complete,
MAX(DISTINCT summary_screen_loaded) AS summary_screen_loaded,
MAX(DISTINCT contact_number_entered) AS contact_number_entered,
MAX(DISTINCT contact_email_entered) AS contact_email_entered,
max(edit_contact_screen_back_button_clicked) AS edit_contact_screen_back_button_clicked,
MAX(edit_contact_screen_submit_cta_clicked) AS edit_contact_screen_submit_cta_clicked,
MAX(have_coupon_clicked) AS have_coupon_clicked,
MAX(remove_coupon_clicked) AS remove_coupon_clicked,
MAX(DISTINCT coupon_screen_loaded) AS coupon_screen_loaded,
MAX(DISTINCT custom_coupon_entered) AS custom_coupon_entered,
MAX(DISTINCT coupon_applied) AS coupon_applied,
max(manual_coupon_applied) AS manual_coupon_applied,
max(merchant_coupon_applied) AS merchant_coupon_applied,
max(auto_coupon_applied) AS auto_coupon_applied,
MAX(DISTINCT validation_successful) AS validation_successful,
MAX(DISTINCT coupon_back_button_clicked) AS coupon_back_button_clicked,
MAX(DISTINCT summary_screen_continue_cta_clicked) AS summary_screen_continue_cta_clicked,
MAX(DISTINCT coupons_available) AS coupons_available,
max(edit_address_clicked) AS edit_address_clicked,
max(logout_clicked) AS logout_clicked,
max(initial_loggedin) AS initial_loggedin,
max(initial_hasSavedAddress) AS initial_hasSavedAddress,
max(clicked_change_contact) as clicked_change_contact,
max(edit_contact_screen_loaded) as edit_contact_screen_loaded,
max(load_saved_address_bottom_sheet_shown) as load_saved_address_bottom_sheet_shown,
max(clicked_load_saved_address_bottom_sheet_cta) as  clicked_load_saved_address_bottom_sheet_cta,
max(dismissed_load_saved_address_bottom_sheet) as dismissed_load_saved_address_bottom_sheet,
max(submit) AS submit,
max(prefill_contact_number) AS prefill_contact_number,
max(prefill_email) AS prefill_email,
max(contact_fill_began) AS contact_fill_began,
max(email_fill_began) AS email_fill_began,
max(pincode_serviceability_initiated) AS pincode_serviceability_initiated,
max(pincode_serviceability_successful) AS pincode_serviceability_successful,
max(pincode_serviceability_true) AS pincode_serviceability_true,
max(pincode_serviceability_zipcode) AS pincode_serviceability_zipcode,
max(pincode_serviceability_city) AS pincode_serviceability_city,
max(pincode_serviceability_state) AS pincode_serviceability_state,
max(access_address_otp_screen_loaded) as access_address_otp_screen_loaded,
max(access_address_otp_screen_submitted) as access_address_otp_screen_submitted,
max(access_address_otp_screen_skipped) as access_address_otp_screen_skipped,
max(access_address_otp_screen_resend) as access_address_otp_screen_resend,
max(access_address_otp_screen_back_clicked) as access_address_otp_screen_back_clicked,
max(mandatory_login_otp_screen_loaded) as mandatory_login_otp_screen_loaded,
max(mandatory_login_otp_screen_submitted) as mandatory_login_otp_screen_submitted,
max(mandatory_login_otp_screen_skipped) as mandatory_login_otp_screen_skipped,
max(mandatory_login_otp_screen_resend) as mandatory_login_otp_screen_resend,
max(mandatory_login_otp_screen_back_clicked) as mandatory_login_otp_screen_back_clicked,
max(saved_address_screen_loaded) as saved_address_screen_loaded,
max(new_address_screen_loaded) as new_address_screen_loaded,
max(saved_address_cta_clicked) as saved_address_cta_clicked,
max(new_address_cta_clicked) as new_address_cta_clicked,
max(new_address_cta_clicked_onsavedaddress) as new_address_cta_clicked_onsavedaddress,
max(new_address_screen_back_button_clicked) as new_address_screen_back_button_clicked,
max(saved_address_screen_back_button_clicked) as saved_address_screen_back_button_clicked,
max(edited_existing_saved_address) as edited_existing_saved_address,
max(save_address_option_unchecked) as save_address_option_unchecked,
max(save_address_option_checked) as save_address_option_checked,
max(save_address_otp_screen_loaded) as save_address_otp_screen_loaded,
max(save_address_otp_screen_submitted) as save_address_otp_screen_submitted,
max(save_address_otp_screen_skipped) as save_address_otp_screen_skipped,
max(save_address_otp_screen_resend) as save_address_otp_screen_resend,
max(save_address_otp_screen_back_clicked) as save_address_otp_screen_back_clicked,
max(payment_home_screen_loaded) as payment_home_screen_loaded
FROM
  aggregate_pa.temp_magic_checkout_fact a
GROUP BY
  1,
  2
  ;
""")
spark.sql(
f"""
delete from aggregate_pa.temp_magic_checkout_fact ;
""")

# COMMAND ----------

for date in ('2023-08-01','2023-08-24'):
    spark.sql(
        """
        select * from table where created_date between '{0}' and '{1}'
        """.format(date, '2023-12-31')
    )

# COMMAND ----------

spark.sql(
        """
        select * from table where created_date between '{0}' and '{1}'
        """.format(date, '2023-12-31')
    )
