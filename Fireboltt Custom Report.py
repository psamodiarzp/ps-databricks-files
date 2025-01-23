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

p_id = ['Order Level Data',]

# COMMAND ----------

DF_list= list()

df = sqlContext.sql("""
with orders as (
select receipt as shopify_order_id, id
from realtime_hudi_api.orders
where date(orders.created_date) >= (date_add(date(now()), -1))
and merchant_id='IH7E2OJQGEKKTN'

),
order_meta_cte as (

select order_id, from_unixtime(order_meta.created_at + 19800) as order_created_time,
shopify_order_id,
get_json_object(value, '$.utm_parameters.utm_source') AS utm_source,
get_json_object(value, '$.utm_parameters.utm_medium') AS utm_medium,
get_json_object(value, '$.utm_parameters.utm_campaign') AS utm_campaign,
get_json_object(value, '$.utm_parameters.utm_term') AS utm_term,
get_json_object(value, '$.utm_parameters.utm_content') AS utm_content,
json_array_length(get_json_object(value, '$.line_items')) AS sku
  from realtime_hudi_api.order_meta as order_meta
  inner join orders on order_meta.order_id = orders.id
  where 1=1
 and type='one_click_checkout'
  and date(order_meta.created_date) >= (date_add(date(now()), -1))
     /*
     and date(order_meta.created_date) <= date('2023-09-28')

   and from_unixtime(order_meta.created_at) >= (current_timestamp() - INTERVAL '5' HOUR)
  and order_meta.created_date>='2023-05-01'
*/
  
 
  

  
), payments as(
select payments.order_id, 
   (CASE WHEN (CASE WHEN payments.method = 'cod' THEN 'yes' ELSE 'no' END
 = 'yes') OR ((CASE WHEN payments.method = 'cod' THEN 'yes' ELSE 'no' END
 = 'no') AND (NOT (payments.authorized_at IS NULL))) THEN 'payment_successful' ELSE NULL END) AS orders_placed,
   SUM(CASE WHEN (CASE WHEN payments.method = 'cod' THEN 'yes' ELSE 'no' END
 = 'yes') OR ((CASE WHEN payments.method = 'cod' THEN 'yes' ELSE 'no' END
 = 'no') AND (NOT (payments.authorized_at IS NULL))) THEN payments.base_amount  /100.00  ELSE NULL END) AS GMV
  from  realtime_hudi_api.payments as payments
  inner join realtime_hudi_api.order_meta on payments.order_id = order_meta.order_id
  where date(order_meta.created_date) >= (date_add(date(now()), -1))
  and date(payments.created_date) >= (date_add(date(now()), -1))

   /*
    and (from_unixtime(payments.created_at) >= (current_timestamp() - INTERVAL '5' HOUR)
  OR from_unixtime(payments.authorized_at) >= (current_timestamp() - INTERVAL '5' HOUR))
   
    where order_meta.created_date >= '2023-05-01'
  and payments.created_date >= '2023-05-01'
    
   
  */
  and payments.merchant_id = 'IH7E2OJQGEKKTN'
  and (method='cod' or authorized_at is not null)
  and type='one_click_checkout'
 group by 1,2
  
)
select order_meta_cte.order_id,
order_meta_cte.shopify_order_id,
order_created_time,
utm_source,
utm_medium,
utm_campaign,
utm_term,
utm_content,
sku,
COALESCE(GMV, 0) as Order_GMV,
COALESCE(orders_placed, 0) as orders_placed 
from order_meta_cte left join payments 
on order_meta_cte.order_id = payments.order_id

union all
select payments.order_id,
orders.shopify_order_id,
from_unixtime(order_meta.created_at + 19800) as order_created_time,
get_json_object(value, '$.utm_parameters.utm_source') AS utm_source,
get_json_object(value, '$.utm_parameters.utm_medium') AS utm_medium,
get_json_object(value, '$.utm_parameters.utm_campaign') AS utm_campaign,
get_json_object(value, '$.utm_parameters.utm_term') AS utm_term,
get_json_object(value, '$.utm_parameters.utm_content') AS utm_content,
json_array_length(get_json_object(value, '$.line_items')) AS sku,
COALESCE(GMV, 0) as Order_GMV,
COALESCE(orders_placed, 0) as orders_placed 
from payments 
left join order_meta_cte
on payments.order_id = order_meta_cte.order_id
left join realtime_hudi_api.order_meta as order_meta
on payments.order_id = order_meta.order_id
left join orders on payments.order_id = orders.id
where order_meta_cte.order_id is null
order by 2


""")
df = df.toPandas()
DF_list.append(df.loc[:,:])

# COMMAND ----------

df['order_id'].nunique()

# COMMAND ----------

df['Order_GMV'] = df['Order_GMV'].astype('float')
df.head()

# COMMAND ----------

df.to_csv('/dbfs/FileStore/fireboltt.csv', index=False)
"https://razorpay-dev.cloud.databricks.com/files/fireboltt.csv"

# COMMAND ----------

df_summarized = df.groupby(by=['utm_source','utm_medium','utm_campaign','utm_term','utm_content'], dropna=False).agg({'order_id':'count','orders_placed':'sum','Order_GMV':'sum'}).reset_index()
df_summarized.rename(columns={'order_id':'checkout_initiations'}, inplace=True)
df_summarized.head()

# COMMAND ----------

user = 'pallavi.samodia@razorpay.com'
app_password = 'cecqjrhdbklizthn' # Guide for app passwords: https://support.google.com/accounts/answer/185833?hl=en
host = 'smtp.gmail.com'
port = 465
server = smtplib.SMTP_SSL(host, port) 
server.login(user, app_password)

# COMMAND ----------


e_id= ['pallavi.samodia@razorpay.com']
att_name_label = ['FireBoltt_daily_report_']
subject_label = ['Daily Report from Razorpay Magic Checkout: FireBoltt',]

for i in range(len(p_id)):
    mailer = DF_list[i]
    a = '{0}.csv'.format(p_id[i])
    mailer.to_csv(a, index=False)
    to = e_id[0]
    subject = subject_label[i]
    attachment = a
    html_t = """
    <html>
    <body>
    </body>
    </html>
    """
    
    message = MIMEMultipart()
    # add From 
    message['From'] = Header(user)
    # add To
    message['To'] = Header(to)

   
    # add Subject
    message['Subject'] = Header(subject)
    # add content text
    message.attach(MIMEText(html_t, 'html', 'utf-8'))
    # add attachment
    att_name = att_name_label[i] + str(datetime.now())
    # time.strftime("%H:%M:%S", time.localtime())
    att1 = MIMEText(open(attachment, 'rb').read(), 'base64', 'utf-8')
    att1['Content-Type'] = 'application/octet-stream'
    att1['Content-Disposition'] = 'attachment; filename=' + att_name
    message.attach(att1)
   
    ### Send email ###
    server.sendmail(user, to, message.as_string()) 
    print('Sent email successfully for: ',p_id[i])

# COMMAND ----------

server.quit()

# COMMAND ----------


