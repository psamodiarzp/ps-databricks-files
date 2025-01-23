# Databricks notebook source
import pandas as pd

# COMMAND ----------

shopify_df = pd.read_csv("/dbfs/FileStore/shared_uploads/pallavi.samodia@razorpay.com/Magic_X_GMV_tracker___Feb_2024.csv")
shopify_df.head()

# COMMAND ----------

magic_sql = sqlContext.sql("""
                           select
                           day(producer_created_date) as Date,
                             substr (get_json_object (context, '$.key'), 10) as merchant_key,
 COALESCE(get_json_object (context, '$.storefront_url'),regexp_extract(get_json_object (context, '$.url'),'^https?://[^/]+/') ) as merchant_id,
 sum(case when event_timestamp < 1708008856 then cast(get_json_object (properties, '$.meta.amount') as double)
    when  event_timestamp >= 1708008856 then cast(get_json_object (properties, '$.meta.amount') as double)/100.00  end) as gmv,
    count(checkout_id) as orders 
    from  aggregate_pa.cx_1cc_events_dump_v1
  where producer_created_date between date('2024-02-01') and date('2024-02-29')
and event_name in ('magicx:order_create', 'magic_x:order_create')
  group by 1,2,3
                                               
                                               """)
magic_df = magic_sql.toPandas()
magic_df.head()

# COMMAND ----------

shopify_df

# COMMAND ----------


