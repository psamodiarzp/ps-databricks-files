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
            get_json_object(a.value, '$.cod_intelligence.risk_tier') AS varchar(10)
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
          ( /*(
        date(a.created_date) = Date(date_sub(CURRENT_DATE,1))
      )
      OR (
        date(from_unixtime(a.updated_at + 19800)) = Date(date_sub(CURRENT_DATE,1))
      )
      OR */ (
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
  
