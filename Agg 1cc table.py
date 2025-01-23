# Databricks notebook source
from datetime import date, timedelta

# COMMAND ----------

from datetime import datetime, timedelta

# COMMAND ----------

def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)

# COMMAND ----------

start_date = date(2022, 11, 1)
end_date = date(2022, 11, 3)
for single_date in daterange(start_date, end_date):
    print("date('" + single_date.strftime("%Y-%m-%d")+"')")

# COMMAND ----------

start_date = date(2022, 11, 1)
end_date = date(2022, 11, 2)
for single_date in daterange(start_date, end_date):
    query='''
        INSERT INTO
      aggregate_pa.cx_1cc_events_dump_v1
    SELECT
      *
    FROM
      (
        WITH raw AS (
          SELECT
            get_json_object (context, '$.checkout_id') AS checkout_id,
            get_json_object(properties, '$.options.order_id') AS order_id,
            get_json_object (properties, '$.options.key') AS merchant_key,
            b.merchant_id AS merchant_id,
            b.customer_id,
            b.product_type,
            event_timestamp,
            l.event_name,
            get_json_object (properties, '$.options.one_click_checkout') AS is_1cc_checkout,
            CAST(
              get_json_object (properties, '$.data.meta.first_screen') AS varchar(10)
            ) AS first_screen_name,
            CAST(
              get_json_object (properties, '$.data.meta.is_mandatory_signup') AS varchar(10)
            ) is_mandatory_signup,
            CAST(
              get_json_object (properties, '$.data.meta.coupons_enabled') AS varchar(10)
            ) is_coupons_enabled,
            CAST(
              get_json_object (
                properties,
                '$.data.meta.available_coupons_count'
              ) AS varchar(10)
            ) available_coupons_count,
            CAST(
              get_json_object (properties, '$.data.meta.address_enabled') AS varchar(10)
            ) is_address_enabled,
            get_json_object (properties, '$.data.meta.address_screen_type') AS address_screen_type,
            CAST(
              get_json_object (properties, '$.data.meta.saved_address_count') AS varchar(10)
            ) AS saved_address_count,
            CAST(
              get_json_object (properties, '$.data.meta["count.savedCards"]') AS varchar(10)
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
              get_json_object (properties, '$.data.otp_reason') AS varchar(10)
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
                get_json_object(context, '$["device.id"]') AS varchar(100)
              ),
              1,
              42
            ) AS device_id,
            CAST(producer_created_date AS date) AS producer_created_date
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
                AND CAST(producer_created_date AS date) ='''+"date('" + single_date.strftime("%Y-%m-%d")+"')" +'''
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
                CAST(created_date AS date) = '''+"date('" + single_date.strftime("%Y-%m-%d")+"')" +'''
            ) b ON substr(
              get_json_object(properties, '$.options.order_id'),
              7
            ) = b.order_id
            INNER JOIN (
              SELECT
                order_id,
                type
              FROM
                realtime_hudi_api.order_meta
              WHERE
                type = 'one_click_checkout'
            ) c ON substr(
              get_json_object(properties, '$.options.order_id'),
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
                ) AS varchar(100)
              ),
              '-',
              CAST(producer_created_date AS varchar(10)),
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
          producer_time,
          producer_created_date
        FROM
          step3
      );
        '''
   # print(query)
    
    print("started",single_date.strftime("%Y-%m-%d"))
    sqlContext.sql(query)
    print("completed",single_date.strftime("%Y-%m-%d"))

# COMMAND ----------


