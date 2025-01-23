# Databricks notebook source
from datetime import date, timedelta

# COMMAND ----------

"spark-shell --packages io.delta:delta-core_2.12:0.8.0 --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"

# COMMAND ----------

from datetime import datetime, timedelta

# COMMAND ----------

def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)

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

start_date = date(2022, 11, 1)
end_date = date(2022, 11, 2)
for single_date in daterange(start_date, end_date):
    query='''
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
        AND CAST(get_json_object(context, '$.platform') AS varchar(20)) = 'mobile_sdk' THEN 'mobile_sdk'
        WHEN event_name = 'render:complete'
        AND CAST(get_json_object (context, '$.platform') AS varchar(20)) = 'browser'
        AND CAST(
          get_json_object (properties, '$.data.meta.is_mobile') AS varchar(20)
        ) = 'true' THEN 'mweb'
        WHEN event_name = 'render:complete'
        AND CAST(get_json_object (context, '$.platform') AS varchar(20)) = 'browser'
        AND (
          CAST(
            get_json_object (properties, '$.data.meta.is_mobile') AS varchar(20)
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
          get_json_object(properties, '$.data.screen_name') AS varchar(20)
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
          get_json_object(properties, '$.data.coupon_source') AS varchar(20)
        ) = 'manual' THEN 1
        ELSE 0
      END AS manual_coupon_applied,
      CASE
        WHEN event_name = 'behav:1cc_coupons_screen_coupon_applied'
        AND CAST(
          get_json_object(properties, '$.data.coupon_source') AS varchar(20)
        ) = 'merchant' THEN 1
        ELSE 0
      END AS merchant_coupon_applied,
      CASE
        WHEN event_name = 'behav:1cc_coupons_screen_coupon_applied'
        AND CAST(
          get_json_object(properties, '$.data.coupon_source') AS varchar(20)
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
      END AS dismissed_load_saved_address_bottom_sheet
    FROM
      aggregate_pa.cx_1cc_events_dump_v1
    WHERE
      producer_created_date = '''+"date('" + single_date.strftime("%Y-%m-%d")+"')" +'''
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
      max(dismissed_load_saved_address_bottom_sheet) as dismissed_load_saved_address_bottom_sheet
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
      '''
    query_2 = '''delete from aggregate_pa.magic_checkout_fact ;'''
    query_3 = '''
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
          max(dismissed_load_saved_address_bottom_sheet) as dismissed_load_saved_address_bottom_sheet
    FROM
      aggregate_pa.temp_magic_checkout_fact a
    GROUP BY
      1,
      2
      ;

    '''


    query_4 = '''delete from aggregate_pa.temp_magic_checkout_fact ;'''
     # print(query)
    
    print("started",single_date.strftime("%Y-%m-%d"))
    #sqlContext.sql(query)
    print("query 1 finished")
    sqlContext.sql(query_2)
    print("query 2 finished")
    sqlContext.sql(query_3)
    print("query 3 finished")
    sqlContext.sql(query_4)
    print("query 4 finished")
    print("completed",single_date.strftime("%Y-%m-%d"))

# COMMAND ----------

sql1 = """
    DROP TABLE IF EXISTS hive.aggregate_pa.temp_magic_rto_reimbursement_fact_revival;
    CREATE TABLE hive.aggregate_pa.temp_magic_rto_reimbursement_fact_revival(
    order_id varchar,
    fulfillment_id varchar,
    shipping_provider varchar,
    rto_charges varchar,
    shipping_status varchar,
    shipping_charges varchar,
    source_origin varchar,
    source varchar,
    fulfillment_created_date date,
    status varchar,
    merchant_id varchar,
    fulfillment_updated_date date,
    fulfillment_updated_timestamp timestamp,
    experimentation boolean,
    cod_intelligence_enabled boolean,
    cod_eligible boolean,
    IsPhoneWhitelisted integer,
    IsEmailWhitelisted integer,
    citytier bigint,
    ml_flag varchar,
    rule_flag varchar,
    is_rule_applied boolean,
    ml_model_id varchar,
    risk_tier varchar,
    merchant_order_id varchar,
    result_flag varchar,
    order_status varchar,
    order_created_date date,
    order_updated_date date,
    review_status varchar,
    reviewed_at varchar,
    reviewed_by varchar,
    awb_number varchar,
    fulfillment_row_num bigint
    );
    INSERT INTO
      hive.aggregate_pa.temp_magic_rto_reimbursement_fact_revival
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
              json_extract_scalar(c.shipping_provider, '$.rto_charges') AS rto_charges,
              json_extract_scalar(c.shipping_provider, '$.shipping_status') AS shipping_status,
              json_extract_scalar(c.shipping_provider, '$.shipping_charges') AS shipping_charges,
              json_extract_scalar(c.source, '$.origin') AS source_origin,
              c.source,
              date(c.created_date) AS fulfillment_created_date,
              c.status,
              c.merchant_id AS merchant_id,
              date(from_unixtime(c.updated_at)) AS fulfillment_updated_date,
              CAST(from_unixtime(c.updated_at) AS timestamp) AS fulfillment_updated_timestamp,
              -- Order Meta
              CAST(
                json_extract_scalar(a.value, '$.cod_intelligence.experimentation') AS boolean
              ) AS experimentation,
              CAST(
                json_extract_scalar(a.value, '$.cod_intelligence.enabled') AS boolean
              ) AS cod_intelligence_enabled,
              CAST(
                json_extract_scalar(a.value, '$.cod_intelligence.cod_eligible') AS boolean
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
                json_extract(a.value, '$.cod_intelligence.risk_tier') AS varchar
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
              o.status order_status,
              date(from_unixtime(a.created_at + 19800)) AS order_created_date,
              date(from_unixtime(a.updated_at + 19800)) AS order_updated_date,
              json_extract_scalar(a.value, '$.review_status') as review_status,
              json_extract_scalar(a.value, '$.reviewed_at') as reviewed_at,
              json_extract_scalar(a.value, '$.reviewed_by') as reviewed_by,
              json_extract_scalar(c.shipping_provider, '$.awb_number') AS awb_number
            FROM
              realtime_hudi_api.order_meta a
              LEFT JOIN realtime_hudi_api.orders o on a.order_id=o.id
              LEFT JOIN hive.realtime_prod_shipping_service.fulfillment_orders c ON c.order_id = a.order_id
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
              ((

            date(a.created_date) = date(‘{single_date}’)

          )
          OR (
            date(from_unixtime(a.updated_at + 19800)) = date(‘{single_date}’)
          )
          OR (
            date(from_unixtime(c.updated_at)) = date(‘{single_date}’)
          )
          OR (
            date(from_unixtime(c.created_at)) = date(‘{single_date}’)


          )
             )
              AND a.type = 'one_click_checkout'
            UNION ALL
            SELECT
              *
            FROM
              hive.aggregate_pa.magic_rto_reimbursement_fact_revival
          )
      )
    WHERE
      fulfillment_row_num = 1;
    ALTER TABLE hive.aggregate_pa.temp_magic_rto_reimbursement_fact_revival
    DROP COLUMN fulfillment_row_num;
    """

# COMMAND ----------



def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)
        
start_date = date(2022, 10, 1)
end_date = date(2022, 10, 8)
for single_date in daterange(start_date, end_date):
    #print("date('"+single_date.strftime("%Y-%m-%d")+"')")
    print(sql1.format(single_date=single_date))

# COMMAND ----------


