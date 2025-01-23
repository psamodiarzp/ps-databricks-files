# Databricks notebook source
sqlContext.sql(

    """
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
      producer_created_date in  (DATE('2023-05-29'),DATE('2023-06-29'))--Date(DATE_ADD('day', -1, CURRENT_DATE))

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


sqlContext.sql(

    """
delete from aggregate_pa.magic_checkout_fact 
""")


# COMMAND ----------


sqlContext.sql(

    """
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
  """)

sqlContext.sql(

    """
delete from aggregate_pa.temp_magic_checkout_fact 
    """
)

# COMMAND ----------

sqlContext.sql(

    """
    INSERT INTO aggregate_pa.magic_rto_reimbursement_fact
    select a.order_id,
a.fulfillment_id,
a.shipping_provider,
a.rto_charges,
a.shipping_status,
a.shipping_charges,
a.source_origin,
a.source,
a.fulfillment_created_date,
a.status,
a.merchant_id,
a.fulfillment_updated_date,
a.fulfillment_updated_timestamp,
a.experimentation,
a.cod_intelligence_enabled,
a.cod_eligible,
a.isphonewhitelisted,
a.isemailwhitelisted,
a.citytier,
a.ml_flag,
a.rule_flag,
a.is_rule_applied,
a.ml_model_id,
a.risk_tier,
a.merchant_order_id,
a.result_flag,
a.order_status,
a.order_created_date,
a.order_updated_date,
a.review_status,
a.reviewed_at,
a.reviewed_by,
a.awb_number,, get_json_object(b.value, '$.cod_intelligence.manual_control_cod_order') AS manual_control_cod_order
    from aggregate_pa.magic_rto_reimbursement_fact_revival a
    left join realtime_hudi_api.order_meta b limit 100


    """
)

# COMMAND ----------

sqlContext.sql(

    """
    INSERT INTO aggregate_pa.magic_rto_reimbursement_fact
select a.order_id,
a.fulfillment_id,
a.shipping_provider,
a.rto_charges,
a.shipping_status,
a.shipping_charges,
a.source_origin,
a.source,
a.fulfillment_created_date,
a.status,
a.merchant_id,
a.fulfillment_updated_date,
a.fulfillment_updated_timestamp,
a.experimentation,
a.cod_intelligence_enabled,
a.cod_eligible,
a.isphonewhitelisted,
a.isemailwhitelisted,
a.citytier,
a.ml_flag,
a.rule_flag,
a.is_rule_applied,
a.ml_model_id,
a.risk_tier,
a.merchant_order_id,
a.result_flag,
a.order_status,
a.order_created_date,
a.order_updated_date,
a.review_status,
a.reviewed_at,
a.reviewed_by,
a.awb_number, get_json_object(b.value, '$.cod_intelligence.manual_control_cod_order') AS manual_control_cod_order
    from aggregate_pa.magic_rto_reimbursement_fact_revival a
    left join realtime_hudi_api.order_meta b 
    WHERE b.created_date >= '2022-06-01'


    """
)

# COMMAND ----------


