# Databricks notebook source
sql0 = """
    CREATE TABLE analytics_selfserve.temp_magic_rto_reimbursement_fact_revival(
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
    fulfillment_row_num bigint
    )

"""
spark.sql(sql0)

# COMMAND ----------

sql1 = """
INSERT INTO
      analytics_selfserve.temp_magic_rto_reimbursement_fact_revival
SELECT *
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
          o.status order_status,
          date(from_unixtime(a.created_at + 19800)) AS order_created_date,
          date(from_unixtime(a.updated_at + 19800)) AS order_updated_date,
          get_json_object(a.value, '$.review_status') as review_status,
          get_json_object(a.value, '$.reviewed_at') as reviewed_at,
          get_json_object(a.value, '$.reviewed_by') as reviewed_by,
          get_json_object(c.shipping_provider, '$.awb_number') AS awb_number
        FROM
          realtime_hudi_api.order_meta a
          LEFT JOIN realtime_hudi_api.orders o on a.order_id=o.id
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
          ((
        date(a.created_date) = Date('2023-01-01')
      )
      OR (
        date(from_unixtime(a.updated_at + 19800)) = Date('2023-01-01')
      )
      OR (
        date(from_unixtime(c.updated_at)) = Date('2023-01-01')
      )
      OR (
        date(from_unixtime(c.created_at)) =Date('2023-01-01') 
      )
         )
          AND a.type = 'one_click_checkout'
        UNION ALL
        SELECT
          *
        FROM
          analytics_selfserve.magic_rto_reimbursement_fact_revival
      )
  )
WHERE
  fulfillment_row_num = 1 
"""
df = spark.sql(sql1)
df.createOrReplaceTempView("dfView")
spark.sql("""select count(*) from dfView""").show(5)

# COMMAND ----------

sql5 = """
INSERT INTO
      analytics_selfserve.temp_magic_rto_reimbursement_fact_revival
SELECT *
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
          o.status order_status,
          date(from_unixtime(a.created_at + 19800)) AS order_created_date,
          date(from_unixtime(a.updated_at + 19800)) AS order_updated_date,
          get_json_object(a.value, '$.review_status') as review_status,
          get_json_object(a.value, '$.reviewed_at') as reviewed_at,
          get_json_object(a.value, '$.reviewed_by') as reviewed_by,
          get_json_object(c.shipping_provider, '$.awb_number') AS awb_number
        FROM
          realtime_hudi_api.order_meta a
          LEFT JOIN realtime_hudi_api.orders o on a.order_id=o.id
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
          ((
        date(a.created_date) = Date('2023-01-01')
      )
      OR (
        date(from_unixtime(a.updated_at + 19800)) = Date('2023-01-01')
      )
      OR (
        date(from_unixtime(c.updated_at)) = Date('2023-01-01')
      )
      OR (
        date(from_unixtime(c.created_at)) =Date('2023-01-01') 
      )
         )
          AND a.type = 'one_click_checkout'
          AND o.created_date >= a.created_date
        UNION ALL
        SELECT
          *
        FROM
          analytics_selfserve.magic_rto_reimbursement_fact_revival
      )
  )
WHERE
  fulfillment_row_num = 1 
"""
df = spark.sql(sql5)
df.createOrReplaceTempView("dfView")
spark.sql("""select count(*) from dfView""").show(5)

# COMMAND ----------

sqlStmt = """
    INSERT INTO
      hive.aggregate_pa.temp_magic_rto_reimbursement_fact_revival
SELECT *
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
          o.status order_status,
          date(from_unixtime(a.created_at + 19800)) AS order_created_date,
          date(from_unixtime(a.updated_at + 19800)) AS order_updated_date,
          get_json_object(a.value, '$.review_status') as review_status,
          get_json_object(a.value, '$.reviewed_at') as reviewed_at,
          get_json_object(a.value, '$.reviewed_by') as reviewed_by,
          get_json_object(c.shipping_provider, '$.awb_number') AS awb_number
        FROM
          realtime_hudi_api.order_meta a
          (select * from realtime_hudi_api.order_meta where to_date(a.created_date) = DATE_ADD(CURRENT_DATE(), -1)) a
          LEFT JOIN realtime_hudi_api.orders o on a.order_id=o.id
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
          ((
        date(a.created_date) = DATE_ADD(CURRENT_DATE(), -1)
      )
      OR (
        date(from_unixtime(a.updated_at + 19800)) = DATE_ADD(CURRENT_DATE(), -1)
      )
      OR (
        date(from_unixtime(c.updated_at)) = DATE_ADD(CURRENT_DATE(), -1)
      )
      OR (
        date(from_unixtime(c.created_at)) = DATE_ADD(CURRENT_DATE(), -1)
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
  fulfillment_row_num = 1 
  
  """



df = spark.sql(sqlStmt)
df.createOrReplaceTempView("dfView")
spark.sql("""select * from dfView where fulfillment_created_date >date_add(current_date(),-2) and awb_number is not null """).show(5)

# COMMAND ----------

df1 = spark.sql("select * from realtime_hudi_api.order_meta where to_date(created_date) = DATE_ADD(CURRENT_DATE(), -1)")
df1.count()

# COMMAND ----------

#creating revival table
sql2="""
CREATE TABLE analytics_selfserve.magic_rto_reimbursement_fact_revival(
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
    awb_number string
    )
"""

spark.sql(sql2)

# COMMAND ----------

sql3="""
insert into analytics_selfserve.magic_rto_reimbursement_fact_revival
select 
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
awb_number

from aggregate_pa.magic_rto_reimbursement_fact_revival limit 1
"""


# COMMAND ----------

sql4 = """delete from analytics_selfserve.magic_rto_reimbursement_fact_revival where 1=1"""
df = spark.sql(sql4)
df.createOrReplaceTempView("dfView")
spark.sql("""select count(*) from dfView""").show()

# COMMAND ----------


