# Databricks notebook source

import pandas as pd

# COMMAND ----------

query='''
 WITH cte AS(
  SELECT
    order_id,
    checkout_id,
    merchant_id,
    CASE
      WHEN event_name = 'render:complete'
      AND LOWER(get_json_object (context, '$.platform')) = 'mobile_sdk' THEN 1
      WHEN event_name = 'render:complete'
      AND LOWER(get_json_object (context, '$.platform')) = 'browser'
      AND event_name = 'render:complete'
      AND get_json_object (properties, '$.data.meta.is_mobile') = 'true' THEN 2
      WHEN event_name = 'render:complete'
      AND LOWER(get_json_object (context, '$.platform')) = 'browser'
      AND (
        get_json_object (properties, '$.data.meta.is_mobile') = 'false'
        OR get_json_object (properties, '$.data.meta.is_mobile') IS NULL
      ) THEN 3
      ELSE 0
    END as platform,
  CASE
      WHEN event_name = 'render:complete' THEN get_json_object(properties, '$.data.meta.initial_loggedIn')
    END AS initial_loggedin,
    /*  CASE
              WHEN event_name = 'render:1cc_summary_screen_loaded_completed' THEN json_extract(
                properties,
                '$.data.meta.initial_hasSavedAddress'
              )
            END AS initial_hasSavedAddress,
            CASE
              WHEN event_name =  'render:1cc_coupons_screen_loaded' THEN json_extract(properties, '$.data.meta.loggedIn')
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
                json_extract_scalar(properties, '$.data.contact'),
                json_extract_scalar(properties, '$.data.data.contact')
              ) IS NOT NULL THEN COALESCE(
                json_extract_scalar(properties, '$.data.contact'),
                json_extract_scalar(properties, '$.data.data.contact')
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
    producer_created_date >= date('2022-09-21')
    AND producer_created_date < date('2022-10-01')
    AND merchant_id IS NOT NULL
    AND merchant_id <> 'Hb4PVe74lPmk0k'
),
cte2 AS(
  SELECT
    a.merchant_id,
    a.checkout_id,
   -- cast(initial_loggedin as BOOLEAN) as initial_loggedin,
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
    sum(DISTINCT platform) AS platform,
    sum(DISTINCT open) AS open,
    sum(DISTINCT summary_screen_loaded) AS summary_screen_loaded,
    sum(DISTINCT coupons_available) AS coupons_available,
    sum(DISTINCT contact_number_entered) AS contact_number_entered,
    sum(DISTINCT contact_email_entered) AS contact_email_entered,
    sum(DISTINCT coupon_screen_loaded) AS coupon_screen_loaded,
    sum(DISTINCT custom_coupon_entered) AS custom_coupon_entered,
    sum(DISTINCT coupon_applied) AS coupon_applied,
    sum(DISTINCT validation_successful) AS validation_successful,
    sum(DISTINCT coupon_back_button_clicked) AS coupon_back_button_clicked,
    
    sum(DISTINCT summary_screen_continue_cta_clicked) AS summary_screen_continue_cta_clicked
  FROM
    cte a
  GROUP BY
    1,
    2
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

original_df.head()

# COMMAND ----------

original_df.shape

# COMMAND ----------

original_df.groupby(['platform','open']).checkout_id.nunique()

# COMMAND ----------

original_df['lack_of_intent'] = original_df.iloc[:,6:].sum(axis=1)
original_df.head()

# COMMAND ----------

df = original_df[original_df['open'] == 1]
df.head()

# COMMAND ----------

platform = {0: 'NA', 1:'mobile_sdk', 2: 'mweb', 3: 'desktop_browser'}
opens = []
summary_screen_loaded = []
summary_screen_continue_cta_clicked = []
lack_of_intent = []
entered_phone_number_no_coupon=[]
entered_phone_number_and_email_no_coupon=[]
cta_clicked_after_contact_no_coupon=[]
dropped_after_contact_no_coupon=[]
entered_coupons_after_contact=[]
unsuccessful_coupons_after_contact=[]
successful_coupons_after_contact=[]
dropped_unsuccessful_coupons_after_contact=[]
dropped_successful_coupons_after_contact=[]
summary_screen_continue_cta_post_summary=[]
unsuccessful_coupons_no_contact=[]
successful_coupons_no_contact=[]
dropped_unsuccessful_coupons_no_contact=[]
dropped_successful_coupons_no_contact=[]
no_coupon_dropped_coupon_screen_no_action=[]
coupon_avail_dropped_coupon_screen_no_action=[]
dropped_coupon_screen_no_action=[]

for i in range(4):
  print(platform[i])
  opens.append(df[df['platform']==i].shape[0])
  summary_screen_loaded.append(df[(df['platform']==i) & (df['summary_screen_loaded']==1)].shape[0])
  summary_screen_continue_cta_clicked.append(df[(df['platform']==i) & (df['summary_screen_continue_cta_clicked']==1)].shape[0])
  summary_screen_continue_cta_post_summary.append(df[(df['platform']==i) & (df['summary_screen_loaded']==1) & (df['summary_screen_continue_cta_clicked']==1)].shape[0])
  lack_of_intent.append(df[(df['platform']==i) & (df['lack_of_intent']==0) & (df['summary_screen_loaded']==1)].shape[0])
  entered_phone_number_no_coupon.append(df[(df['platform']==i) & (df['summary_screen_loaded']==1) & (df['contact_number_entered']==1)
                                & (df['coupon_screen_loaded']==0)
                                ].shape[0])
  entered_phone_number_and_email_no_coupon.append(df[(df['platform']==i) & (df['summary_screen_loaded']==1) &
                                         (df['contact_number_entered']==1) & (df['contact_email_entered']==1)
                                          & (df['coupon_screen_loaded']==0)
                                          ].shape[0])
  cta_clicked_after_contact_no_coupon.append(df[(df['platform']==i) & (df['summary_screen_loaded']==1) &
                                         (df['contact_number_entered']==1) & (df['contact_email_entered']==1)
                                                & (df['coupon_screen_loaded']==0)
                                   & (df['summary_screen_continue_cta_clicked']==1) 
                                               ].shape[0])
  dropped_after_contact_no_coupon.append(df[(df['platform']==i) & (df['summary_screen_loaded']==1) &
                                         (df['contact_number_entered']==1) & (df['contact_email_entered']==1)
                                                & (df['coupon_screen_loaded']==0)
                                   & (df['summary_screen_continue_cta_clicked']==0)].shape[0]) 
  entered_coupons_after_contact.append(df[(df['platform']==i) & (df['summary_screen_loaded']==1) &
                                         (df['contact_number_entered']==1) & (df['contact_email_entered']==1)
                                          & (df['coupon_screen_loaded']==1)
                                          ].shape[0])
  unsuccessful_coupons_after_contact.append(df[(df['platform']==i) & (df['summary_screen_loaded']==1) &
                                         (df['contact_number_entered']==1) & (df['contact_email_entered']==1)
                                          & (df['coupon_screen_loaded']==1) & (df['validation_successful']==0)
                                               & (df['coupon_applied']==1)
                                          ].shape[0])
  successful_coupons_after_contact.append(df[(df['platform']==i) & (df['summary_screen_loaded']==1) &
                                         (df['contact_number_entered']==1) & (df['contact_email_entered']==1)
                                          & (df['coupon_screen_loaded']==1) & (df['validation_successful']==1)
                                             & (df['coupon_applied']==1)
                                          ].shape[0])
  unsuccessful_coupons_no_contact.append(df[(df['platform']==i) & (df['summary_screen_loaded']==1) &
                                         (df['contact_number_entered']==0) & (df['contact_email_entered']==0)
                                          & (df['coupon_screen_loaded']==1) & (df['coupon_applied']==1)
                                            & (df['validation_successful']==0)
                                          ].shape[0])
  successful_coupons_no_contact.append(df[(df['platform']==i) & (df['summary_screen_loaded']==1) &
                                         (df['contact_number_entered']==0) & (df['contact_email_entered']==0)
                                          & (df['coupon_screen_loaded']==1) & (df['validation_successful']==1)
                                          & (df['coupon_applied']==1)
                                          ].shape[0])
  dropped_unsuccessful_coupons_after_contact.append(df[(df['platform']==i) & (df['summary_screen_loaded']==1) &
                                         (df['contact_number_entered']==1) & (df['contact_email_entered']==1)
                                          & (df['coupon_screen_loaded']==1) & (df['validation_successful']==0)
                                           & (df['coupon_applied']==1)
                                           & (df['summary_screen_continue_cta_clicked']==0) ].shape[0])
  dropped_successful_coupons_after_contact.append(df[(df['platform']==i) & (df['summary_screen_loaded']==1) &
                                         (df['contact_number_entered']==1) & (df['contact_email_entered']==1)
                                          & (df['coupon_screen_loaded']==1) & (df['validation_successful']==1)
                                                     & (df['coupon_applied']==1)
                                          & (df['summary_screen_continue_cta_clicked']==0)].shape[0])
  dropped_unsuccessful_coupons_no_contact.append(df[(df['platform']==i) & (df['summary_screen_loaded']==1) &
                                         (df['contact_number_entered']==0) & (df['contact_email_entered']==0)
                                          & (df['coupon_screen_loaded']==1) & (df['validation_successful']==0)
                                                    & (df['coupon_applied']==1)
                                           & (df['summary_screen_continue_cta_clicked']==0) ].shape[0])
  dropped_successful_coupons_no_contact.append(df[(df['platform']==i) & (df['summary_screen_loaded']==1) &
                                         (df['contact_number_entered']==0) & (df['contact_email_entered']==0)
                                          & (df['coupon_screen_loaded']==1) & (df['validation_successful']==1)
                                                  & (df['coupon_applied']==1)
                                          & (df['summary_screen_continue_cta_clicked']==0)].shape[0])
  dropped_coupon_screen_no_action.append(df[(df['platform']==i) & (df['summary_screen_loaded']==1)
                                          & (df['coupon_screen_loaded']==1) & (df['coupon_applied']==0)
                                          & (df['coupon_back_button_clicked']==0) & (df['summary_screen_continue_cta_clicked']==0)].shape[0])   
  no_coupon_dropped_coupon_screen_no_action.append(df[(df['platform']==i) & (df['summary_screen_loaded']==1) & (df['coupons_available']==0)
                                          & (df['coupon_screen_loaded']==1) & (df['coupon_applied']==0)
                                          & (df['coupon_back_button_clicked']==0) & (df['summary_screen_continue_cta_clicked']==0)].shape[0])
  coupon_avail_dropped_coupon_screen_no_action.append(df[(df['platform']==i) & (df['summary_screen_loaded']==1)  & (df['coupons_available']==1)
                                          & (df['coupon_screen_loaded']==1) & (df['coupon_applied']==0) 
                                          & (df['coupon_back_button_clicked']==0) & (df['summary_screen_continue_cta_clicked']==0)].shape[0])                                               

  print('count of opens', opens[i])
  print('count of summary_screen_loaded', summary_screen_loaded[i])
 #print('count of summary_screen_continue_cta_clicked', summary_screen_continue_cta_clicked[i])

  print('\n')

# COMMAND ----------

results = pd.DataFrame({
    'platform':['NA','mobile_sdk', 'mweb','desktop_browser'],
    'opens':opens,
    'summary_screen_loaded':summary_screen_loaded,
    'summary_screen_continue_cta_clicked':summary_screen_continue_cta_clicked,
    'summary_screen_continue_cta_post_summary':summary_screen_continue_cta_post_summary,
    'lack_of_intent':lack_of_intent,
    'entered_phone_number_no_coupon':entered_phone_number_no_coupon,
    'entered_phone_number_and_email_no_coupon':entered_phone_number_and_email_no_coupon,
    'cta_clicked_after_contact_no_coupon': cta_clicked_after_contact_no_coupon,
    'dropped_after_contact_no_coupon':dropped_after_contact_no_coupon,
    'entered_coupons_after_contact': entered_coupons_after_contact,
    'successful_coupons_after_contact':successful_coupons_after_contact,
    'unsuccessful_coupons_after_contact':unsuccessful_coupons_after_contact,
    'dropped_successful_coupons_after_contact':dropped_successful_coupons_after_contact,
    'dropped_unsuccessful_coupons_after_contact':dropped_unsuccessful_coupons_after_contact,
    'unsuccessful_coupons_no_contact':unsuccessful_coupons_no_contact,
    'successful_coupons_no_contact':successful_coupons_no_contact,
    'dropped_unsuccessful_coupons_no_contact':dropped_unsuccessful_coupons_no_contact,
    'dropped_successful_coupons_no_contact':dropped_successful_coupons_no_contact,
    'no_coupon_dropped_coupon_screen_no_action':no_coupon_dropped_coupon_screen_no_action,
    'coupon_avail_dropped_coupon_screen_no_action':coupon_avail_dropped_coupon_screen_no_action,
    'dropped_coupon_screen_no_action':dropped_coupon_screen_no_action,

})
results.head()

# COMMAND ----------

original_df.to_csv('/dbfs/checkout_data.csv')
results.to_csv('/dbfs/results.csv')

# COMMAND ----------

display(original_df)

# COMMAND ----------

display(results)

# COMMAND ----------


