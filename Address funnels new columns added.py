# Databricks notebook source
column_names =[
'access_address_otp_screen_loaded',
'access_address_otp_screen_submitted',
'access_address_otp_screen_skipped',
'access_address_otp_screen_resend',
'access_address_otp_screen_back_clicked',
'mandatory_login_otp_screen_loaded',
'mandatory_login_otp_screen_submitted',
'mandatory_login_otp_screen_skipped',
'mandatory_login_otp_screen_resend',
'mandatory_login_otp_screen_back_clicked',
'saved_address_screen_loaded',
'new_address_screen_loaded',
'saved_address_cta_clicked',
'new_address_cta_clicked',
'new_address_cta_clicked_onsavedaddress',
'new_address_screen_back_button_clicked',
'saved_address_screen_back_button_clicked',
'edited_existing_saved_address',
'save_address_option_unchecked',
'save_address_option_checked',
'save_address_otp_screen_loaded',
'save_address_otp_screen_submitted',
'save_address_otp_screen_skipped',
'save_address_otp_screen_resend',
'save_address_otp_screen_back_clicked',
'payment_home_screen_loaded',

]

# COMMAND ----------

for column_name in column_names:
    query = f"ALTER TABLE aggregate_pa.magic_checkout_fact ADD COLUMN {column_name} integer;"
    sqlContext.sql(query)

# COMMAND ----------

 chumbak_data =   sqlContext.sql(

    """
    with orders as(
select
--P.created_date,
  P.order_id
/*
( distinct 
          CASE
            WHEN P.authorized_at IS NOT NULL
            AND lower(P.method) <> 'cod' THEN P.order_id
            WHEN lower(P.method) = 'cod' THEN P.order_id
            ELSE null
          END
        ) AS payment_success
  */
        from realtime_hudi_api.payments P
        LEFT JOIN realtime_hudi_api.order_meta om ON om.order_id = P.order_id
WHERE
        P.merchant_id = 'Ir3TKZupRxSGw8'
        AND date(P.created_date) between  date('2023-06-01') and date('2023-06-21')
         AND om.type = 'one_click_checkout'
  AND (  method = 'cod' or authorized_at IS NOT NULL)
     ---    group by 1
)
select 

 order_id, producer_created_date, get_json_object(properties, '$.data.meta.page_shown') from aggregate_pa.cx_1cc_events_dump_v1
where producer_created_date >= date('2023-06-01')
and substr(order_id,7,14)  in (
  select * from orders
)
and get_json_object(properties, '$.data.meta.page_shown') is not null
group by 1,2,3

    """
    )

# COMMAND ----------


