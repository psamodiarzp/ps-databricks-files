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

import pandas as pd

# COMMAND ----------

spark.conf.set("spark.sql.arrow.enabled", "false")

# COMMAND ----------

rto_orders = '''
select concat('order_',order_id), * from realtime_prod_shipping_service.fulfillment_orders
where status='rto'
'''

# COMMAND ----------

rto_orders_db = spark.sql(rto_orders)
rto_orders_df = rto_orders_db.toPandas()
rto_orders_df.head()

# COMMAND ----------

rto_orders_df.shape

# COMMAND ----------

min(rto_orders_df['created_date'])

# COMMAND ----------

events_query = '''
select event_name,  event_timestamp, producer_created_date, order_id, checkout_id, merchant_id
from aggregate_pa.cx_1cc_events_dump_v1
where order_id in (
select concat('order_',order_id) from realtime_prod_shipping_service.fulfillment_orders
where status='rto'
and created_date >= '2023-03-01'

)'''

# COMMAND ----------

events_query_db = spark.sql(events_query)
events_query_df = events_query_db.select("*").toPandas()
events_query_df.head()

# COMMAND ----------

#events_query_db = spark.sql(events_query)
final_filename = f's3://rzp-1642-payments/ppg_transient/magic_click_stream_data'
events_query_db.write.mode('append').partitionBy(
    'producer_created_date',
).parquet(
    final_filename,
)

# COMMAND ----------

sample_s3_data_df = pd.read_parquet(spark.read.parquet(final_filename), engine='pyarrow')
sample_s3_data_df.head()

# COMMAND ----------

events_query_df.head()

# COMMAND ----------

events_query_df['rank'] = events_query_df[events_query_df['event_name'].str.contains("behav")].groupby('checkout_id')['event_timestamp'].rank(method='first')
events_query_df['rank'] = pd.to_numeric(events_query_df['rank'], downcast='integer')
events_query_df.head()

# COMMAND ----------

print(', '.join(list((events_query_df[events_query_df['merchant_id']=='IH7E2OJQGEKKTN']['checkout_id']).unique())))

# COMMAND ----------

events_agg = events_query_df[events_query_df['event_name'].str.contains("behav")].sort_values(by='rank',ascending=False).groupby(['checkout_id'])['event_name'].transform(lambda x: ','.join(x)).reset_index()
events_agg.head()

# COMMAND ----------

events_agg

# COMMAND ----------



# COMMAND ----------

events_ranked_df = events_query_df.groupby(['event_name']).agg({'checkout_id':'nunique','rank':'mean'}).reset_index()
events_ranked_df

# COMMAND ----------

events_df = events_query_df.groupby(['event_name']).agg({'checkout_id':'nunique'}).reset_index()
events_df

# COMMAND ----------

pd.set_option('display.max_rows', 500)

# COMMAND ----------


events_df.sort_values(by='checkout_id',ascending=False)
events_df[events_df['event_name'].str.contains("behav")].sort_values(by='checkout_id',ascending=False)

# COMMAND ----------

spark.read.csv(“path”)
