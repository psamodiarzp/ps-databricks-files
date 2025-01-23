# Databricks notebook source
import pandas as pd
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.header import Header
import smtplib
import os

# COMMAND ----------

from datetime import datetime
import time

# COMMAND ----------

mx_details_db = sqlContext.sql("""
select * from batch_sheets.magic_checkout_rto_insurance
""")
mx_details_df = mx_details_db.toPandas()
mx_details_df.head()

# COMMAND ----------

if mx_details_df['mid'].count() != mx_details_df['mid'].nunique():
    print("There are duplicate merchants in the sheet")

# COMMAND ----------

tracker_details_db = sqlContext.sql("""
SELECT
  payments.created_date as payment_date,
  order_meta.created_date as order_created_date,
  CONCAT(
    MONTH(payments.created_date),
    '-',
    YEAR(payments.created_date)
  ) AS payment_month_year,
  payments.order_id,
  get_json_object(fulfillment.shipping_provider, '$.shipping_status') AS shipping_status,
  get_json_object(fulfillment.shipping_provider, '$.awb_number') AS awb_number,
  fulfillment.merchant_order_id,
  payments.merchant_id
FROM
  realtime_hudi_api.payments AS payments
  INNER JOIN realtime_hudi_api.order_meta AS order_meta on payments.order_id = order_meta.order_id
  inner join batch_sheets.magic_checkout_rto_insurance as mid_insurance on payments.merchant_id = mid_insurance.mid
  LEFT JOIN realtime_prod_shipping_service.fulfillment_orders As fulfillment on payments.order_id = fulfillment.order_id
where
  method = 'cod'
  and type = 'one_click_checkout'
  and payments.created_date >= rto_insurance_start_date
  and order_meta.created_date >= rto_insurance_start_date
  and fulfillment.created_date >= rto_insurance_start_date
  and insurance_opted_in = 'Yes'
""")
tracker_details_df = tracker_details_db.toPandas()
tracker_details_df.head()

# COMMAND ----------

tracker_details_db = sqlContext.sql("""
WITH payments as(
  SELECT
created_date as payment_date,
 CONCAT(
    MONTH(created_date),
    '-',
    YEAR(created_date)
  ) AS payment_month_year,
order_id,
merchant_id,
   COALESCE(SUM(( base_amount  *1.0/100) ), 0) as amount
   FROM
  realtime_hudi_api.payments 
  where payments.method = 'cod'
  GROUP BY 1,2,3,4
), fulfillment as (

SELECT
created_date,
 get_json_object(shipping_provider, '$.shipping_status') AS shipping_status,
  get_json_object(shipping_provider, '$.awb_number') AS awb_number,
merchant_order_id,
order_id
FROM
realtime_prod_shipping_service.fulfillment_orders 

), model_details as(
SELECT 
order_id,
risk_tier,
cod_eligible
from aggregate_pa.magic_rto_reimbursement_fact
where isemailwhitelisted=0
and isphonewhitelisted=0

)
select * from payments
left join fulfillment on payments.order_id = fulfillment.order_id
INNER JOIN batch_sheets.magic_checkout_rto_insurance as mid_insurance on payments.merchant_id = mid_insurance.mid
LEFT JOIN model_details on payments.order_id = model_details.order_id 
where   payments.payment_date >= rto_insurance_start_date
  and fulfillment.created_date >= rto_insurance_start_date
""")
tracker_details_df = tracker_details_db.toPandas()
tracker_details_df.head()

# COMMAND ----------


# Sample data
data = {
    'category': ['A', 'A', 'B', 'B', 'C'],
    'group': ['X', 'Y', 'X', 'X', 'Y'],
    'value': ['abc', 'def', 'abc', 'ghi', 'def']
}

# Create DataFrame
df = pd.DataFrame(data)

# List of values to check
values_to_check = ['abc', 'def']

# Group the data and count values in the list using agg and lambda
specific_counts = df.groupby(['category', 'group']).agg(
    specific_value_count=('value', lambda x: x[x.isin(values_to_check)].count())
)

# Print the results
print(specific_counts)


# COMMAND ----------

import pandas as pd

# Sample data
data = {
    'category': ['A', 'A', 'B', 'B', 'C'],
    'group': ['X', 'Y', 'X', 'X', 'Y'],
    'value1': ['abc', 'def', 'abc', 'ghi', 'def'],
    'value2': ['xyz', 'abc', 'xyz', 'lmn', 'abc']
}

# Create DataFrame
df = pd.DataFrame(data)

# Group the data and count unique values of 'value1' and 'value2'
unique_counts = df.groupby(['category', 'group']).agg(
    value1_count=('value1', 'nunique'),
    value2_count=('value2', 'nunique')
).reset_index()

# Print the results
print(unique_counts)


# COMMAND ----------

df.info()

# COMMAND ----------


