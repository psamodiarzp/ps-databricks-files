# Databricks notebook source
import pandas as pd
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.header import Header
import smtplib
import os

# COMMAND ----------

mail_list = sqlContext.sql("""select producer_created_date, merchant_id,
get_json_object(properties,'$.data.meta.1cc_zero_coupon_move_experiment') as experiment_variant,
event_name,
count(distinct checkout_id) as cnt_cid
from
aggregate_pa.cx_1cc_events_dump_v1
where event_name in ('render:1cc_summary_screen_loaded_completed','render:1cc_coupons_screen_loaded',
                     'behav:1cc_coupons_screen_coupon_applied','behav:1cc_coupons_screen_back_button_clicked',
                    'behav:1cc_summary_screen_continue_cta_clicked')
and producer_created_date >= date('2022-10-20')
group by 1,2,3,4""")
mail_list = mail_list.toPandas()

# COMMAND ----------

mail_list

# COMMAND ----------

mail_list.groupby(['producer_created_date','experiment_variant','event_name']).sum()

# COMMAND ----------

p_id = mail_list['p_id']
s_date = mail_list['s_date']
e_date = mail_list['e_date']
e_id = mail_list['e_id']
filler = mail_list['filler']

# COMMAND ----------

DF_list= list()
for i in range(len(p_id)):
  df = sqlContext.sql("""SELECT * from aggregate_ba.partner_commissions where partner_id='{0}' AND Payment_date between '{1}' and '{2}'""".format(p_id[i],s_date[i],e_date[i]))
  df = df.toPandas()
  DF_list.append(df)

# COMMAND ----------

user = 'pallavi.samodia@razorpay.com'
app_password = 'cklyyjbkfsycxvqd' # Guide for app passwords: https://support.google.com/accounts/answer/185833?hl=en
host = 'smtp.gmail.com'
port = 465
server = smtplib.SMTP_SSL(host, port) 
server.login(user, app_password)

# COMMAND ----------

for i in range(len(p_id)):
    mailer = DF_list[i]
    pay = round(mailer['partner_commission'].sum(),2)
    mailer['partner_commission'] = mailer['partner_commission'].fillna('-')
    mailer['partner_pricing'] = mailer['partner_pricing'].fillna('-')
    a = '{0}.csv'.format(p_id[i])
    mailer.to_csv(a)
    to = e_id[i]
    cc = ['ateam-partnerships@razorpay.com']
    tocc = [e_id[i]] + cc
    subject = 'Commission Payout Report for Partner ID: ' + p_id[i] + ' | Period: {1} to {2}'.format(p_id[i],s_date[i],e_date[i]) + ' | Filler: ' + filler[i] 
    content_txt = 'Commission Payout Report for Parter ID: {0} \nPeriod: {1} to {2}'.format(p_id[i],s_date[i],e_date[i]) + '\n Total Commissions: {0}'.format(pay)
    attachment = a
    html_t = """
    <html>
    <body>
      <b>Partner ID: </b>{0}
      <br/>
      <b>Period: </b>{1} to {2}
      <br/>
      <b>Total Commissions: </b>{3}
    </body>
    </html>
    """.format(p_id[i],s_date[i],e_date[i],pay)
    
    message = MIMEMultipart()
    # add From 
    message['From'] = Header(user)
    # add To
    message['To'] = Header(to)
    #message['cc'] = Header(cc)
    message['reply-to'] = "ateam-partnerships@razorpay.com"
    # add Subject
    message['Subject'] = Header(subject)
    # add content text
    message.attach(MIMEText(html_t, 'html', 'utf-8'))
    # add attachment
    att_name = os.path.basename(attachment)
    att1 = MIMEText(open(attachment, 'rb').read(), 'base64', 'utf-8')
    att1['Content-Type'] = 'application/octet-stream'
    att1['Content-Disposition'] = 'attachment; filename=' + att_name
    message.attach(att1)
   
    ### Send email ###
    server.sendmail(user, tocc, message.as_string()) 
    print('Sent email successfully for: ',p_id[i])

# COMMAND ----------

server.quit()
