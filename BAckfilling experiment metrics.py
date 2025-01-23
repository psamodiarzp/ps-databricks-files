# Databricks notebook source
# MAGIC %sql
# MAGIC create table if not exists aggregate_pa.experiment_metrics(
# MAGIC exp_name varchar,
# MAGIC exp_flag  varchar,
# MAGIC exp_eligibility_flag varchar,
# MAGIC exp_ineligibility_reason varchar,
# MAGIC merchant_id varchar,
# MAGIC platform_device varchar,
# MAGIC browser_name varchar,
# MAGIC device_os varchar,
# MAGIC device_brand varchar,
# MAGIC integration_type varchar,
# MAGIC environment varchar,
# MAGIC product_v2 varchar,
# MAGIC integration varchar,
# MAGIC build_id varchar,
# MAGIC renders bigint,
# MAGIC submits bigint,
# MAGIC payment_attempts bigint,
# MAGIC payment_success bigint,
# MAGIC CID_PA bigint,
# MAGIC CID_PS bigint,
# MAGIC producer_created_date varchar
# MAGIC )
# MAGIC WITH ( format = 'parquet', partitioned_by = ARRAY['producer_created_date'] )
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from aggregate_pa.experiment_metrics
# MAGIC limit 20

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into aggregate_pa.experiment_metrics
# MAGIC with lo_fact as (
# MAGIC select 
# MAGIC checkout_id,
# MAGIC is_submit,
# MAGIC merchant_id,
# MAGIC platform_device,
# MAGIC browser_name,
# MAGIC device_os,
# MAGIC device_brand,
# MAGIC integration_type,
# MAGIC environment,
# MAGIC product as product_v2,
# MAGIC integration,
# MAGIC checkout_build_number as build_id,
# MAGIC cast(producer_created_date as string) as producer_created_date
# MAGIC from whs_v.cx_edw_lo_checkout_fact
# MAGIC where producer_created_date between   DATE('2024-12-11') and DATE('2024-12-14')
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC ex.exp_name,
# MAGIC ex.exp_flag,
# MAGIC ex.exp_eligibility_flag,
# MAGIC ex.exp_ineligibility_reason,
# MAGIC lo_fact.merchant_id,
# MAGIC lo_fact.platform_device,
# MAGIC lo_fact.browser_name,
# MAGIC lo_fact.device_os,
# MAGIC lo_fact.device_brand,
# MAGIC lo_fact.integration_type,
# MAGIC lo_fact.environment,
# MAGIC lo_fact.product_v2,
# MAGIC lo_fact.integration,
# MAGIC lo_fact.build_id,
# MAGIC count(distinct lo_fact.checkout_id) as renders,
# MAGIC count(distinct case when lo_fact.is_submit = 1 then lo_fact.checkout_id else null end) as submits,
# MAGIC count(distinct p.id) as payment_attempts,
# MAGIC count(distinct case when p.authorized_at is not null  or method='cod' then p.id else null end) as payment_success,
# MAGIC count(distinct p.checkout_id) as CID_PA,
# MAGIC count(distinct case when p.authorized_at is not null then p.checkout_id else null end) as CID_PS,
# MAGIC lo_fact.producer_created_date
# MAGIC from lo_fact
# MAGIC left join (
# MAGIC   select 
# MAGIC   checkout_id,
# MAGIC   exp_name,
# MAGIC   exp_flag,
# MAGIC   exp_eligibility_flag,
# MAGIC   exp_ineligibility_reason
# MAGIC   from whs_v.cx_edw_checkout_experiment_fact
# MAGIC   where producer_created_date between   DATE('2024-12-11') and DATE('2024-12-14')
# MAGIC   and exp_name NOT IN ('preferred_methods_v3',
# MAGIC 'upi_turbo',
# MAGIC 'one_cc_shipping_info_decomp',
# MAGIC 'one_cc_upi_qr_v2',
# MAGIC 'otp_verify_v2',
# MAGIC 'upi_qr',
# MAGIC 'checkout_v2_custom_ramp',
# MAGIC 'eligibility_on_std_checkout',
# MAGIC 'recurring_upi_all_psp',
# MAGIC 'quick_buy_sdk',
# MAGIC 'email_less_checkout',
# MAGIC 'one_cc_shopify_merge_checkout_and_order_creation',
# MAGIC 'remove_default_tokenization_flag',
# MAGIC 'otp_unification_acs_page',
# MAGIC 'checkout_version',
# MAGIC 'show_merchant_trust_marker',
# MAGIC 'one_cc_global_experiments',
# MAGIC 'one_cc_customer_truecaller_verify_v2',
# MAGIC 'payment_method_grouping',
# MAGIC 'delegate_payment',
# MAGIC 'one_cc_enable_otp_auto_read_and_auto_submit',
# MAGIC 'gift_cards',
# MAGIC 'quick_buy_raas',
# MAGIC 'cvv_less',
# MAGIC 'emi_ux_revamp',
# MAGIC 'payment_create_ajax_api_bypass',
# MAGIC 'one_cc_offers_fix_exp',
# MAGIC 'one_cc_cod_disable_reasons_exp',
# MAGIC 'new_widget_design_enabled',
# MAGIC 'one_cc_dweb_uplift',
# MAGIC 'discount_whisperer_enabled',
# MAGIC 'enable_emi_without_card',
# MAGIC 'one_cc_shipping_using_checkout',
# MAGIC 'one_cc_methods_and_offer',
# MAGIC 'one_cc_show_coupon_callout_exp',
# MAGIC 'one_cc_auto_submit_otp',
# MAGIC 'one_cc_last_used_address',
# MAGIC 'one_cc_insta_fb_upi_intent_webview_enabled',
# MAGIC 'cb_redesign_v1_5',
# MAGIC 'dcc_vas_merchants',
# MAGIC 'cvv_less_visa_optimizer',
# MAGIC 'enable_otp_auto_read_and_auto_submit',
# MAGIC 'one_cc_otp_verify_v2',
# MAGIC 'one_cc_shopify_coupon_bugfix',
# MAGIC 'emi_ux_variant',
# MAGIC 'one_cc_unicommerce_address_ingestion',
# MAGIC 'checkout_redesign',
# MAGIC 'customer_truecaller_verify_v2',
# MAGIC 'low_discovery_paylater',
# MAGIC 'truecaller_standard_checkout_for_non_prefill',
# MAGIC 'emi_via_cards_revamp',
# MAGIC 'one_cc_address_line_optional',
# MAGIC 'core_payment_module',
# MAGIC 'no_cost_offers_visibility',
# MAGIC 'cardless_emi_convenience_fee',
# MAGIC 'enable_rudderstack_plugin',
# MAGIC 'icic_saved_card_offer',
# MAGIC 'cvv_less_amex_optimizer',
# MAGIC 'checkout_v2_recurring_ramp_new',
# MAGIC 'bajaj_l1_visibility',
# MAGIC 'hdfc_corporate_netbanking',
# MAGIC 'checkout_prefill_redrection',
# MAGIC 'one_cc_address_flow_exp',
# MAGIC 'checkout_downtime',
# MAGIC 'one_cc_conversion_address_improvements',
# MAGIC 'address_reorder',
# MAGIC 'show_offer_amount_l0',
# MAGIC 'one_cc_triple_consent',
# MAGIC 'one_cc_multiple_shipping',
# MAGIC 'autoread_otp_mweb',
# MAGIC 'upi_intent_on_whatsapp_webview',
# MAGIC 'no_instrument_error_message',
# MAGIC 'insta_fb_upi_intent_webview_enabled',
# MAGIC 'lc_emi_offers_visibility',
# MAGIC 'cvv_less_mastercard',
# MAGIC 'cred_logo_on_desktop',
# MAGIC 'recurring_intl_verify_phone',
# MAGIC 'one_cc_post_order_optimisation',
# MAGIC 'upi_ux')
# MAGIC           ) ex on ex.checkout_id = lo_fact.checkout_id
# MAGIC left join (SELECT 
# MAGIC             pa_checkout_id as checkout_id,
# MAGIC             p_id as id,
# MAGIC             p_authorized_at as authorized_at,
# MAGIC             p_method as method
# MAGIC         FROM whs_v.payments_flat_fact
# MAGIC         WHERE p_created_date between   ('2024-12-11') and ('2024-12-14')) p on p.checkout_id = lo_fact.checkout_id
# MAGIC group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,21

# COMMAND ----------

# MAGIC %md
# MAGIC Ye nhi chlega
# MAGIC
# MAGIC Yours truly,
# MAGIC Shubham

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from aggregate_pa.experiment_metrics
# MAGIC where producer_created_date='2024-12-17'

# COMMAND ----------


