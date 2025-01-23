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
import pytz

# COMMAND ----------

p_id = ['Order Level Data',]

# COMMAND ----------

DF_list= list()

df = sqlContext.sql("""

    with events_data as(
select 
producer_created_date,
merchant_id,
count(distinct case when event_name = 'render:1cc_summary_screen_loaded_completed' 
      then session_id else null end) as checkout_initiated,
count(distinct case when event_name = 'behav:1cc_saved_address_screen_continue_cta_clicked' 
      or event_name = 'behav:1cc_add_new_address_screen_continue_cta_clicked'
      then session_id 
      when event_name = 'behav:1cc_summary_screen_continue_cta_clicked'
      and get_json_object(properties, '$.data.address_id') is not null
      then session_id 
      else null end) as address_step_completed,
count(distinct case when event_name = 'behav:1cc_payment_home_screen_method_selected' 
      or event_name = 'behav:qr_cta_click'
      or event_name = 'behav:offer:apply'
      then session_id else null end) as payment_method_selected
      
from aggregate_pa.cx_1cc_events_dump_v1
where producer_created_date >= (date_add(date(now()), -1))
--where producer_created_date = date('2024-03-16')
and from_unixtime(producer_timestamp + 19800) >= (current_timestamp() - INTERVAL '5' HOUR)
and merchant_id='HEubznrgZIZsQz'
group by 1,2
  ), payments_data as(
  select
    merchant_id,
    count(distinct a.order_id) as payments_initiated,
    count(distinct case when authorized_at is not null or method='cod'
          then a.order_id else null end)  as orders_placed
    from realtime_hudi_api.payments a
   -- inner join dbt_databricks_prod_realtime_delta_api.order_meta b on a.order_id = b.order_id
   inner join realtime_hudi_api.order_meta b on a.order_id = b.order_id
  --  where a.created_date = '2024-03-16'
   where a.created_date >= (date_add(date(now()), -1))
  and (from_unixtime(a.created_at+19800) >= (current_timestamp() - INTERVAL '4' HOUR)
   OR from_unixtime(a.authorized_at+19800) >= (current_timestamp() - INTERVAL '4' HOUR))
    and a.merchant_id = 'HEubznrgZIZsQz'
     
 --   and b.created_date = '2024-03-16'
    and b.created_date >= (date_add(date(now()), -1))
   -- and from_unixtime(b.created_at + 19800) >= (current_timestamp() - INTERVAL '4' HOUR)
    and b.type = 'one_click_checkout'
  /* and order_id in (
      select distinct order_id
      from realtime_hudi_api.payments
    where created_date = '2023-06-11'
    
    ) */
    group by 1
  
  )
  select 
 -- producer_created_date,
 -- events_data.merchant_id,
  sum(checkout_initiated) as checkout_initiated,
  sum(address_step_completed) as address_step_completed,
 -- sum(payment_method_selected) as payment_method_selected ,
  sum(coalesce(payments_initiated,0)) as payments_initiated,
  sum(coalesce(orders_placed,0)) as orders_placed
from events_data
  left join payments_data on events_data.merchant_id = payments_data.merchant_id
 -- group by 1,2
   


       
   

 


""")
df = df.toPandas()
df.head()

# COMMAND ----------

# Get the current time in UTC
utc_time = datetime.utcnow()

# Convert UTC time to local time
local_timezone = pytz.timezone('Asia/Kolkata')  # Replace 'Your_Timezone' with your local time zone
local_time = utc_time.replace(tzinfo=pytz.utc).astimezone(local_timezone)
local_time

# COMMAND ----------


df['timestamp'] = local_time.strftime("%Y-%m-%d %H:%M:%S")
DF_list.append(df.loc[:,:])
df.head()

# COMMAND ----------

user = 'pallavi.samodia@razorpay.com'
app_password = 'rpoxvchgemoicygy' # Guide for app passwords: https://support.google.com/accounts/answer/185833?hl=en
host = 'smtp.gmail.com'
port = 465
server = smtplib.SMTP_SSL(host, port) 
server.login(user, app_password)

# COMMAND ----------


e_id= ['pallavi.samodia@razorpay.com']
att_name_label = ['Dermatouch_hourly_report_']
subject_label = ['Hourly Report from Razorpay Magic Checkout: Dermatouch',]

for i in range(len(p_id)):
    mailer = DF_list[i]
    a = '{0}.csv'.format(p_id[i])
    mailer.to_csv(a)
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


