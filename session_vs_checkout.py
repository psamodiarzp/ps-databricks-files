# Databricks notebook source
import pandas as pd

# COMMAND ----------

old_query='''
SELECT
    CASE
      WHEN event_name='open' THEN '0. Open'
      WHEN event_name='render:1cc_summary_screen_loaded_completed' THEN '1.1 Summary Screen Loading Completed'

      ELSE 'NA' END
     AS "1cc_checkout_events.v1_5_funnel_events",
    COUNT(DISTINCT ("1cc_checkout_events".checkout_id)) AS "count_of_checkout_id_5",
    COUNT(DISTINCT ("1cc_checkout_events".session_id)) AS "count_of_session_id"
FROM hive.aggregate_pa.cx_1cc_events_dump_v1  AS "1cc_checkout_events"
LEFT JOIN realtime_hudi_api.merchants AS merchants ON ("1cc_checkout_events".merchant_id) = merchants.id
WHERE (FROM_UNIXTIME("1cc_checkout_events".event_timestamp + 19800) ) >= (TIMESTAMP '2022-04-20 16:00') AND ("1cc_checkout_events".producer_created_date ) = (DATE_TRUNC('DAY', TIMESTAMP '2022-11-01')) AND ((UPPER(( CASE
      WHEN event_name='open' THEN '0. Open'
      WHEN event_name='render:1cc_summary_screen_loaded_completed' THEN '1.1 Summary Screen Loading Completed'
      WHEN event_name='render:1cc_coupons_screen_loaded' THEN '2.1 Coupon Screen Loaded (Separate from flow)'
      WHEN event_name='behav:1cc_coupons_screen_coupon_applied' THEN '2.2 Coupon Applied'
      WHEN (event_name='behav:1cc_summary_screen_continue_cta_clicked' OR event_name='behav:1cc_add_new_address_screen_continue_cta_clicked'
      OR event_name='behav:1cc_saved_address_screen_continue_cta_clicked')
      THEN '5.2 Exit to Payment Method'
      WHEN event_name='render:1cc_add_new_address_screen_loaded_completed' OR event_name='render:1cc_saved_shipping_address_screen_loaded'
      THEN '4.1 new_or_saved_shipping_address_screen_loaded'
      --   when event_name='render:1cc_add_new_address_screen_loaded_completed' then '4.1 New Address Screen Loaded'
      --    when event_name='render:1cc_saved_shipping_address_screen_loaded' then '3.1 Saved Address Screen Loaded'
      WHEN event_name='behav:1cc_add_new_address_screen_continue_cta_clicked' AND (event_name!='render:1cc_summary_screen_loaded_completed')
      THEN '4.11 Add New Address Screen CTA Clicked'
      WHEN event_name='behav:1cc_summary_screen_edit_address_clicked' THEN '3.11 Edit (Saved) Address Selected '
      WHEN event_name='render:1cc_payment_home_screen_loaded' THEN '6.1 Payment Home Screen L0 Loaded'
      WHEN event_name='behav:1cc_payment_home_screen_method_selected' THEN '6.2 Payment Method selected on Payment L0 Screen'
      WHEN event_name='behav:1cc_confirm_order_summary_submitted' OR event_name='submit' THEN '6.3 Payment Submitted'

      ELSE 'NA' END
     )) = UPPER('0. Open') OR UPPER(( CASE
      WHEN event_name='open' THEN '0. Open'
      WHEN event_name='render:1cc_summary_screen_loaded_completed' THEN '1.1 Summary Screen Loading Completed'
      WHEN event_name='render:1cc_coupons_screen_loaded' THEN '2.1 Coupon Screen Loaded (Separate from flow)'
      WHEN event_name='behav:1cc_coupons_screen_coupon_applied' THEN '2.2 Coupon Applied'
      WHEN (event_name='behav:1cc_summary_screen_continue_cta_clicked' OR event_name='behav:1cc_add_new_address_screen_continue_cta_clicked'
      OR event_name='behav:1cc_saved_address_screen_continue_cta_clicked')
      THEN '5.2 Exit to Payment Method'
      WHEN event_name='render:1cc_add_new_address_screen_loaded_completed' OR event_name='render:1cc_saved_shipping_address_screen_loaded'
      THEN '4.1 new_or_saved_shipping_address_screen_loaded'
      --   when event_name='render:1cc_add_new_address_screen_loaded_completed' then '4.1 New Address Screen Loaded'
      --    when event_name='render:1cc_saved_shipping_address_screen_loaded' then '3.1 Saved Address Screen Loaded'
      WHEN event_name='behav:1cc_add_new_address_screen_continue_cta_clicked' AND (event_name!='render:1cc_summary_screen_loaded_completed')
      THEN '4.11 Add New Address Screen CTA Clicked'
      WHEN event_name='behav:1cc_summary_screen_edit_address_clicked' THEN '3.11 Edit (Saved) Address Selected '
      WHEN event_name='render:1cc_payment_home_screen_loaded' THEN '6.1 Payment Home Screen L0 Loaded'
      WHEN event_name='behav:1cc_payment_home_screen_method_selected' THEN '6.2 Payment Method selected on Payment L0 Screen'
      WHEN event_name='behav:1cc_confirm_order_summary_submitted' OR event_name='submit' THEN '6.3 Payment Submitted'

      ELSE 'NA' END
     )) = UPPER('1.1 Summary Screen Loading Completed'))) AND ((UPPER(( merchants.id  )) <> UPPER('Hb4PVe74lPmk0k') AND (( merchants.id  ) IS NOT NULL)))
GROUP BY
    1
ORDER BY
    1
LIMIT 50
'''

# COMMAND ----------

old_df = sqlContext.sql(old_query)
print((old_df.count(), len(old_df.columns)))
df = old_df.toPandas() 
