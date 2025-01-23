# Databricks notebook source
import pandas as pd
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.header import Header
import smtplib
import os
from datetime import datetime, timedelta, date
import time

# COMMAND ----------

tracker_details_db = sqlContext.sql("""
                                    Select producer_created_date as date, 
                                    round(approx_percentile(cast(get_json_object(properties,'$.data.meta["timeSince.open"]') as integer)/1000.00,0.5),2) as median_load_time
from aggregate_pa.cx_1cc_events_dump_v1
where producer_created_date between DATE(NOW() - INTERVAL '7' DAY) and DATE(NOW() - INTERVAL '1' DAY)
and merchant_id='OFt4p3nAQX00hk'
and event_name='render:complete'
group by 1
order by 1

                                    """)
                                    


# COMMAND ----------

tracker_details_df = tracker_details_db.toPandas()
tracker_details_df['median_load_time'] = round(tracker_details_df['median_load_time'],2)
tracker_details_df.head(10)

# COMMAND ----------

if tracker_details_df.shape[0]==0:
    exit()

# COMMAND ----------

user = 'utkarsh.sarkar@razorpay.com'
app_password = 'brfgpxadqebpqgec' # Guide for app passwords: https://support.google.com/accounts/answer/185833?hl=en
host = 'smtp.gmail.com'
port = 465
server = smtplib.SMTP_SSL(host, port) 
server.login(user, app_password)

# COMMAND ----------

html_code = """
<html>
<head>
<title> Magic Checkout Load Time Report for Sleepyhead </title>
</head>
<body>
Here is your weekly report on the load time (in seconds) of Magic Checkout for Sleepyhead.
<p>
"""

# COMMAND ----------

html_code += tracker_details_df.to_html(index=False,escape=False,  )
html_code +="""</p></body></html>"""

# COMMAND ----------

def send_emails(email_id,html_t):
    today = datetime.now().strftime('%Y-%m-%d')
    subject = 'Weekly Magic Checkout Load Time Report for Sleepyhead | Date: {0}'.format( today)
    to = email_id
    message = MIMEMultipart()
    # add From 
    message['From'] = Header(user)
    # add To
    message['To'] = Header(to)
    # add Subject
    message['Subject'] = Header(subject)
    # add content text
    message.attach(MIMEText(html_t, 'html', 'utf-8'))
    ### Send email ###
    server.sendmail(user, to, message.as_string()) 
    print('Sent email successfully for: ',email_id)


# COMMAND ----------

email_id = ['pallavi.samodia@razorpay.com','akhilesh.kumar@razorpay.com','prasanth.polisetty@duroflexworld.com','deepak.chaudhary@mysleepyhead.com','jitenderkumar.y@mysleepyhead.com','jagmohan.gope@duroflexworld.com', 'aakash.bhattacharjee@razorpay.com']
for i in email_id:
    send_emails(i,html_code)

# COMMAND ----------

server.quit()
