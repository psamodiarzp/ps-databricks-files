# Databricks notebook source
 pip install pyhive thrift

# COMMAND ----------

import pandas as pd


# COMMAND ----------

query = '''
WITH cte AS(
  SELECT
    order_id,
    checkout_id,
    merchant_id,
    CASE
      WHEN event_name = 'render:complete'
      AND LOWER(get_json_object (context, '$.platform')) = 'mobile_sdk' THEN '1. mobile_sdk'
      WHEN event_name = 'render:complete'
      AND LOWER(get_json_object (context, '$.platform')) = 'browser'
      AND get_json_object (properties, '$.data.meta.is_mobile') = 1 THEN '2. mweb'
      WHEN event_name = 'render:complete'
      AND LOWER(get_json_object (context, '$.platform')) = 'browser'
      AND (
        get_json_object (properties, '$.data.meta.is_mobile') = 0
        OR get_json_object (properties, '$.data.meta.is_mobile') IS NULL
      ) THEN '3. desktop_browser'
      ELSE '4. NA'
    END as platform,
  CASE
      WHEN event_name = 'render:complete' THEN get_json_object(properties, '$.data.meta.initial_loggedIn')
    END AS initial_loggedin,
    /*  CASE
              WHEN event_name = 'render:1cc_summary_screen_loaded_completed' THEN get_json_object(
                properties,
                '$.data.meta.initial_hasSavedAddress'
              )
            END AS initial_hasSavedAddress,
            CASE
              WHEN event_name =  'render:1cc_coupons_screen_loaded' THEN get_json_object(properties, '$.data.meta.loggedIn')
            END AS loginstatus_couponpage, */
    CASE
      WHEN event_name = 'open' THEN 1
      ELSE 0
    END AS open,
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
    -- Coupon screen events
    CASE
      WHEN event_name = 'render:1cc_coupons_screen_loaded' THEN 1
      ELSE 0
    END AS coupon_screen_loaded,
    CASE
      WHEN event_name = 'behav:1cc_coupons_screen_custom_coupon_entered' THEN 1
      ELSE 0
    END AS custom_coupon_entered,
    CASE
      WHEN event_name = 'behav:1cc_coupons_screen_coupon_applied' THEN 1
      ELSE 0
    END AS coupon_applied,
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
    END AS coupons_available
    /*  CASE
              WHEN COALESCE(
                get_json_object(properties, '$.data.contact'),
                get_json_object(properties, '$.data.data.contact')
              ) IS NOT NULL THEN COALESCE(
                get_json_object(properties, '$.data.contact'),
                get_json_object(properties, '$.data.data.contact')
              )
              ELSE NULL
            END AS contact, 
            CASE
              WHEN event_name = 'render:1cc_add_new_address_screen_loaded_completed' THEN 1
              ELSE 0
            END AS add_new_address_screen_loaded_completed,
            CASE
              WHEN event_name = 'behav:1cc_add_new_address_name_entered' THEN 1
              ELSE 0
            END AS add_new_address_name_entered
          */
  FROM
    aggregate_pa.cx_1cc_events_dump_v1
  WHERE
    producer_created_date >= date('2022-09-30')
    AND producer_created_date < date('2022-10-01')
    AND merchant_id IS NOT NULL
    AND merchant_id <> 'Hb4PVe74lPmk0k'
),
cte2 AS(
  SELECT
    a.merchant_id,
    platform,
    a.checkout_id,
    initial_loggedin,
    /*   (
              CASE
                WHEN (
                  CASE
                    WHEN payments.method = 'cod' THEN 'yes'
                    ELSE 'no'
                  END = 'yes'
                )
                OR (
                  (
                    CASE
                      WHEN payments.method = 'cod' THEN 'yes'
                      ELSE 'no'
                    END = 'no'
                  )
                  AND (NOT (payments.authorized_at IS NULL))
                ) THEN 1
                ELSE NULL
              END
            ) AS payment_successful,
          */
    sum(DISTINCT open) AS open,
    sum(DISTINCT summary_screen_loaded) AS summary_screen_loaded,
    sum(DISTINCT contact_number_entered) AS contact_number_entered,
    sum(DISTINCT contact_email_entered) AS contact_email_entered,
    sum(DISTINCT coupon_screen_loaded) AS coupon_screen_loaded,
    sum(DISTINCT custom_coupon_entered) AS custom_coupon_entered,
    sum(DISTINCT coupon_applied) AS coupon_applied,
    sum(DISTINCT validation_successful) AS validation_successful,
    sum(DISTINCT coupon_back_button_clicked) AS coupon_back_button_clicked,
    sum(DISTINCT coupons_available) AS coupons_available,
    sum(DISTINCT summary_screen_continue_cta_clicked) AS summary_screen_continue_cta_clicked
  FROM
    cte a
  GROUP BY
    1,
    2,
  3,4
)
SELECT
  ---  merchants.website,
  *
FROM
  cte2 -- LEFT JOIN realtime_hudi_api.merchants merchants ON cte2.merchant_id = merchants.id
'''

# COMMAND ----------


original_df = sqlContext.sql(query)
print((original_df.count(), len(original_df.columns)))
original_df = original_df.toPandas()

# COMMAND ----------

original_df.head(10)

# COMMAND ----------

original_df[original_df['summary_screen_loaded'] == 1].shape
df = original_df[original_df['summary_screen_loaded'] == 1]
df.shape

# COMMAND ----------

df.head(20)

# COMMAND ----------

#Breakdown by Looged in status (only available post render:complete)
df.groupby(['platform'], dropna=False)['checkout_id'].count()

# COMMAND ----------

df['initial_loggedin'].unique()

# COMMAND ----------

df.dtypes

# COMMAND ----------


