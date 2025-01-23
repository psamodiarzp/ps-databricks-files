# Databricks notebook source
# MAGIC %md
# MAGIC #Events base

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creating the table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS aggregate_ba.mhi_events_base (
# MAGIC   checkout_id STRING,
# MAGIC   merchant_key STRING,
# MAGIC   instrumentation_section STRING,
# MAGIC   block_code STRING,
# MAGIC   severity_tags STRING,
# MAGIC   event_name STRING
# MAGIC )
# MAGIC USING PARQUET
# MAGIC PARTITIONED BY (event_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Inserting the data into the table

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into aggregate_ba.mhi_events_base
# MAGIC (
# MAGIC select 
# MAGIC get_json_object(context,'$.checkout_id') as checkout_id,
# MAGIC get_json_object(properties,'$.options.key') as merchant_key,
# MAGIC get_json_object (properties,'$.data.instrument.section') instrumentation_section,
# MAGIC get_json_object (properties,'$.data.instrumentMeta.block.code') as block_code,
# MAGIC get_json_object(properties, '$.data.error.tags.severity') as severity_tags,
# MAGIC event_name
# MAGIC  FROM events.lumberjack_intermediate
# MAGIC
# MAGIC WHERE producer_created_date = '2024-09-01'
# MAGIC --and producer_created_date <= '2024-09-08'
# MAGIC and source = 'checkoutjs' 
# MAGIC AND get_json_object(context, '$.library') IN ('checkoutjs', 'hosted')
# MAGIC
# MAGIC and event_name in ('behav:contact_details:cta_click', 'behav:1cc_clicked_change_contact_continue_cta','render:complete', 'metric:p13n:instruments_shown', 'behav:instrument:select', 'metric:home:checkoutpaymentinstrumentselected', 'metric:checkoutPaymentInstrumentSelected', 'error_reported',
# MAGIC 'validation:failed',
# MAGIC 'unhandled_rejection',
# MAGIC 'js_error',
# MAGIC 'assist:and_s1_log')
# MAGIC
# MAGIC )
# MAGIC

# COMMAND ----------


