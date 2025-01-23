# Databricks notebook source
# MAGIC %md
# MAGIC #Initial Package downloads

# COMMAND ----------

import numpy as np
import pandas as pd

# COMMAND ----------

from datetime import datetime, timedelta
from datetime import date

# COMMAND ----------

from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.header import Header
import smtplib
import os

# COMMAND ----------

def percentage_conversion(value):
    return f'{value:.2%}'

# COMMAND ----------

# MAGIC %md
# MAGIC #Date Inputs
# MAGIC In YYYY-MM-DD format
# MAGIC

# COMMAND ----------


#start_date = '2024-07-20'
#end_date = '2024-07-29'

# COMMAND ----------

def get_start_end_dates(current_dt):
    current_year = pd.Timestamp.now().year
    end_date =  current_dt+timedelta(days=-1)
    start_date = current_dt+timedelta(days=-7)
    return start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')

# COMMAND ----------

today = date.today() #add another date to the past to get the last 7 day
start_date, end_date = get_start_end_dates(today)
print(start_date, end_date)

# COMMAND ----------

# MAGIC %md
# MAGIC ### HTML CODE Base for Email

# COMMAND ----------

user = 'pallavi.samodia@razorpay.com'
app_password = 'sibbzwuopwzqbzst' # Guide for app passwords: https://support.google.com/accounts/answer/185833?hl=en
host = 'smtp.gmail.com'
port = 465
server = smtplib.SMTP_SSL(host, port) 
server.login(user, app_password)

# COMMAND ----------

def send_emails(start_date, end_date,email_id,html_t):
    today = datetime.now().strftime('%Y-%m-%d')
    subject = 'Checkout V2 vs V1 report - {0} to {1}'.format(start_date, end_date,)
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

html_code = """
<html>
<head>
    <title>Checkout V2 vs V1 report - {0} to {1} </title>
    <style>
    td, th {{
  border: 1px solid #ddd;
  padding: 6px;
  text-align: left;  }}

    th {{
    background-color:  #0b5394;
    color: white; 
  }}
  </style>
</head>
<body>
""".format(start_date,end_date,)

print(html_code)

# COMMAND ----------

# MAGIC %md
# MAGIC #SQL code for data
# MAGIC

# COMMAND ----------

raw_data_db = sqlContext.sql("""
WITH cte AS(
  SELECT
    a.checkout_id,
    a.submit_checkout_id,
    a.merchant_id,
    producer_created_date,
  (ramp_up_date),
    (
      get_json_object(render_properties, '$.data.meta.v2_result')
    ) AS v2_result,
   (
      get_json_object(render_properties, '$.magicExperiments.checkout_redesign')
    ) AS experiment_result,
   (get_json_object(render_properties,'$.data.meta.v2_eligible')) as v2_eligible
  
  FROM
    aggregate_pa.cx_lo_fact_ism_v1 a
  inner join batch_sheets.checkout_v2_rampup_sheet b on a.merchant_id = b.merchant_id
  WHERE
    producer_created_date BETWEEN date('{0}') and date('{1}') 
  and producer_created_date > date(ramp_up_date)
  and rampup_100_percent_flag = 'false'
  and blacklisted_flag = 'false'

),


cte2 AS(
select 
  p.id,
  p.method, 
p.authorized_at,
p.gateway,

  checkout_id,
  error_code,
  error_description,
  internal_error_code,
      upi_app,
      upi_type,
      upi_meta_app,
      upi_provider
  from realtime_hudi_api.payments p
  inner join realtime_hudi_api.payment_analytics pa on p.id = pa.payment_id
  left join (
/* Updating the new UPI logic --- July 15 */
    select p_id, 
    lower(upi_payments_flat_fact. upi_meta_app ) as upi_meta_app,
      upi_payments_flat_fact.u_provider  as upi_provider,
CASE
WHEN lower(upi_payments_flat_fact.u_type ) = 'collect' AND upi_payments_flat_fact.u_provider  in ('ybl','ibl','axl') THEN 'Phonepe'
WHEN lower(upi_payments_flat_fact.u_type ) = 'collect' AND upi_payments_flat_fact.u_provider  in ('paytm','pthdfc','ptsbi','ptaxis','ptyes') THEN 'Paytm'
WHEN lower(upi_payments_flat_fact.u_type ) = 'collect' AND upi_payments_flat_fact.u_provider  = 'upi' THEN 'BHIM'
WHEN lower(upi_payments_flat_fact.u_type ) = 'collect' AND upi_payments_flat_fact.u_provider  = 'apl' THEN 'Amazon Pay'
WHEN lower(upi_payments_flat_fact.u_type ) = 'collect' AND upi_payments_flat_fact.u_provider  in ('oksbi','okhdfcbank','okicici','okaxis') THEN 'GooglePay'
WHEN lower(upi_payments_flat_fact.u_type ) = 'pay' AND upi_payments_flat_fact. upi_meta_app  = 'com.phonepe.app' THEN 'Phonepe'
WHEN lower(upi_payments_flat_fact.u_type ) = 'pay' AND lower(upi_payments_flat_fact. upi_meta_app ) like '%amazon%'  THEN 'Amazon Pay'
WHEN lower(upi_payments_flat_fact.u_type ) = 'pay' AND upi_payments_flat_fact. upi_meta_app  = 'net.one97.paytm' THEN 'Paytm'
WHEN lower(upi_payments_flat_fact.u_type ) = 'pay' AND upi_payments_flat_fact. upi_meta_app  = 'in.org.npci.upiapp' THEN 'BHIM'
WHEN lower(upi_payments_flat_fact.u_type ) = 'pay' AND upi_payments_flat_fact. upi_meta_app  = 'com.google.android.apps.nbu.paisa.user' THEN 'GooglePay'
when upi_payments_flat_fact.u_provider  in ('ybl','ibl','axl') THEN 'Phonepe'
when lower(upi_payments_flat_fact.u_type ) = 'pay' and upi_payments_flat_fact. upi_meta_app  like '%dreamplug%' then 'Cred'
when upi_payments_flat_fact.u_provider  in ('oksbi','okhdfcbank','okicici','okaxis') THEN 'GooglePay'
when upi_payments_flat_fact.u_provider  in ('Paytm','pthdfc','ptsbi','ptaxis','ptyes') THEN 'Paytm'
when upi_payments_flat_fact.u_provider  = 'axisb' then 'Cred'
when upi_payments_flat_fact.u_provider  in ('axisbank') then 'Axis Bank'
when upi_payments_flat_fact.u_provider  in ('upi') then 'BHIM'
when upi_payments_flat_fact.u_provider  in ('apl', 'yapl', 'rapl') then 'Amazon Pay'
WHEN lower(upi_payments_flat_fact.u_type ) = 'pay' AND pa_library <> 1 THEN 'NA'
ELSE 'Others'
END as upi_app,
CASE WHEN upi_payments_flat_fact.u_type = 'collect' THEN 'Collect'
    ELSE 'Intent' END AS upi_type

from whs_v.upi_payments_flat_fact 
where  upi_payments_flat_fact.p_created_date between ('{0}') and ('{1}') 

  )  AS upi_payments_flat_fact on p.id = upi_payments_flat_fact.p_id
  where p.created_date between ('{0}') and ('{1}') 
  AND pa.created_date between ('{0}') and ('{1}') 
    
)
select 
cte.producer_created_date,
cte.checkout_id,
cte.submit_checkout_id,

cte.merchant_id,
cte.experiment_result,
cte.v2_result,
cte.v2_eligible,
cte2.method as payment_method,
id as payment_attempts,
case when authorized_at is not null or cte2.method='cod' then id else null end as payment_success,
cte2.checkout_id as pa_checkout_id,
case when authorized_at is not null or cte2.method='cod' then cte2.checkout_id else null end as ps_checkout_id,
cte2.error_code,
  cte2.error_description,
  cte2.internal_error_code,
  gateway,
  upi_type,
   upi_meta_app,
      upi_provider,
  upi_app
from cte 
left join cte2 on cte.checkout_id = cte2.checkout_id
""".format(start_date, end_date))

raw_data_df = raw_data_db.toPandas()
raw_data_df.head()

# COMMAND ----------

raw_data_df.shape

# COMMAND ----------

# MAGIC %md
# MAGIC #Saving the file as csv for later
# MAGIC - If you need to use the data source but not run the above code again, please utilize below (especially using analytics cluster)

# COMMAND ----------

raw_data_df.to_csv('/dbfs/FileStore/std_raw_data_upi_{start_date}_{end_date}_df.csv', index=False)
"https://razorpay-dev.cloud.databricks.com/files/std_raw_data_upi_{0}_{1}_df.csv".format(start_date, end_date)

# COMMAND ----------

raw_data_df = pd.read_csv('/dbfs/FileStore/std_raw_data_upi_{start_date}_{end_date}_df.csv')
raw_data_df.shape

# COMMAND ----------

# MAGIC %md
# MAGIC #Overall Split Check
# MAGIC - to check here: V1 vs V2 split under eligible should be 95:5 or as expected
# MAGIC - % of NAs should be minimal

# COMMAND ----------

grouped_df = raw_data_df.groupby(by = ['v2_result','v2_eligible'], dropna=False).agg(
    { 'checkout_id':'nunique',
        'merchant_id':'nunique',
        'submit_checkout_id':'nunique',
     'pa_checkout_id':'nunique',
        'ps_checkout_id':'nunique',
        'payment_attempts':'nunique',
     'payment_success':'nunique',
    
     }
).reset_index()
grouped_df['split_percentage'] = grouped_df['checkout_id']*1.0 / grouped_df['checkout_id'].sum()
grouped_df['modal_cr'] = grouped_df['submit_checkout_id']*1.0 / grouped_df['checkout_id']
grouped_df['success_rate'] = grouped_df['payment_success']*1.0 / grouped_df['payment_attempts']
grouped_df['overall_cr'] = grouped_df['payment_success']*1.0 / grouped_df['checkout_id']
grouped_df[grouped_df.columns[-4:]] = grouped_df[grouped_df.columns[-4:]].applymap(percentage_conversion)
#grouped_df.pivot(  = 'v2_result', values = ['checkout_id','payment_attempts','payment_success','success_rate','overall_cr']).reset_index()
grouped_df

# COMMAND ----------

html_code += """<h3>Unfiltered data:</h3> <br/>"""
html_code += grouped_df.to_html(index=False,escape=False,  )

# COMMAND ----------

grouped_df.to_html(index=False, escape=False, )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Temporary code to close the email

# COMMAND ----------

html_code += """
</body>
</html>
"""

# COMMAND ----------

email_id = ['pallavi.samodia@razorpay.com',]
for i in email_id:
    send_emails(start_date,end_date,i,html_code)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Checking MID split % + Data Clean Up

# COMMAND ----------

grouped_df = raw_data_df[raw_data_df['v2_eligible']==True].groupby(by = ['v2_result','merchant_id'], dropna=False).agg(
    { 'checkout_id':'nunique',
    
     }
).reset_index()
#grouped_df['split_percentage'] = grouped_df['checkout_id']*1.0 / grouped_df['checkout_id'].sum()
grouped_df['checkout_id'] = grouped_df['checkout_id'].astype(int)
pivoted_df = grouped_df.pivot( index='merchant_id' ,columns= 'v2_result', values = ['checkout_id']).sort_values(by = 'merchant_id',).reset_index()
pivoted_df = pivoted_df.fillna(0)
pivoted_df['Total'] = pivoted_df[pivoted_df.columns[1:]].sum(axis=1)
percentage_df = pivoted_df[pivoted_df.columns[1:-1]].div(pivoted_df['Total'], axis=0) * 100
percentage_df = pd.concat([pivoted_df, percentage_df[:-1].rename(columns={'checkout_id': 'checkout_id_split'}), ], axis=1)
mid_df = percentage_df.sort_values(by = 'Total',ascending=False).reset_index()
mid_df

# COMMAND ----------

mid_df.head(20)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Filtering for merchants where test or control is 100%

# COMMAND ----------

final_mid_list = mid_df[(mid_df.loc[:, pd.IndexSlice['checkout_id_split', 'v1']]!=100.0) & (mid_df.loc[:, pd.IndexSlice['checkout_id_split', 'v1']]!=0.0)  ]['merchant_id'].tolist()
final_mid_list

# COMMAND ----------

# MAGIC %md
# MAGIC ### Merchants who will be excluded [Top 20]
# MAGIC

# COMMAND ----------

mid_df[(mid_df.loc[:, pd.IndexSlice['checkout_id_split', 'v1']]==100.0) | (mid_df.loc[:, pd.IndexSlice['checkout_id_split', 'v1']]==0.0)  ].head(20)

# COMMAND ----------

mid_df[(mid_df.loc[:, pd.IndexSlice['checkout_id_split', 'v1']]==100.0) | (mid_df.loc[:, pd.IndexSlice['checkout_id_split', 'v1']]==0.0) | (mid_df.loc[:, pd.IndexSlice['checkout_id_split', 'v2']]==0.0)  ].shape

# COMMAND ----------

final_df = raw_data_df[raw_data_df['merchant_id'].isin(final_mid_list)].reset_index(drop=True)
final_df

# COMMAND ----------

final_df['merchant_id'].nunique()

# COMMAND ----------


final_df.to_csv('/dbfs/FileStore/std_final_{start_date}_{end_date}_df.csv', index=False)
"https://razorpay-dev.cloud.databricks.com/files/std_final_df.csv"

# COMMAND ----------

saved_start_date = start_date #Change this if you want to extract file for another time
saved_end_date = end_date #Change this if you want to extract file for another time
final_df = pd.read_csv('/dbfs/FileStore/std_final_df.csv')
final_df.shape

# COMMAND ----------

# MAGIC %md
# MAGIC #L0 Metrics 
# MAGIC all analysis here on will be only for cases where V2 eligible = True

# COMMAND ----------

grouped_df = final_df[final_df['v2_eligible']==True].groupby(by = ['v2_result',], dropna=False).agg(
    { 'checkout_id':'nunique',
     'merchant_id':'nunique',
     'submit_checkout_id':'nunique',
        'payment_attempts':'nunique',
     'payment_success':'nunique',
         'pa_checkout_id':'nunique',
        'ps_checkout_id':'nunique',
     }
).reset_index()
grouped_df['split_percentage'] = grouped_df['checkout_id']*1.0 / grouped_df['checkout_id'].sum()
grouped_df['modal_cr'] = grouped_df['submit_checkout_id']*1.0 / grouped_df['checkout_id']
grouped_df['success_rate'] = grouped_df['payment_success']*1.0 / grouped_df['payment_attempts']
grouped_df['submit_to_pa'] = grouped_df['pa_checkout_id']*1.0 / grouped_df['submit_checkout_id']
grouped_df['overall_cr'] = grouped_df['payment_success']*1.0 / grouped_df['checkout_id']

#grouped_df.pivot(  = 'v2_result', values = ['checkout_id','payment_attempts','payment_success','success_rate','overall_cr']).reset_index()
grouped_df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Date-wise L0 metrics

# COMMAND ----------

grouped_df = final_df[final_df['v2_eligible']==True].groupby(by = ['v2_result','producer_created_date'], dropna=False).agg(
    { 'checkout_id':'nunique',
     'merchant_id':'nunique',
      'submit_checkout_id':'nunique',
        'payment_attempts':'nunique',
     'payment_success':'nunique',
      'pa_checkout_id':'nunique',
        'ps_checkout_id':'nunique',
     }
).reset_index()
grouped_df['split_percentage'] = grouped_df['checkout_id']*1.0 / grouped_df['checkout_id'].sum()
grouped_df['modal_cr'] = grouped_df['submit_checkout_id']*1.0 / grouped_df['checkout_id']
grouped_df['success_rate'] = grouped_df['payment_success']*1.0 / grouped_df['payment_attempts']
grouped_df['submit_to_pa'] = grouped_df['pa_checkout_id']*1.0 / grouped_df['submit_checkout_id']
grouped_df['overall_cr'] = grouped_df['payment_success']*1.0 / grouped_df['checkout_id']

pivoted_df = grouped_df.pivot( index='producer_created_date' ,columns= 'v2_result', values = ['checkout_id','payment_attempts','payment_success','modal_cr','success_rate','submit_to_pa','overall_cr']).sort_values(by = 'producer_created_date',).reset_index()
pivoted_df['modal_cr_diff'] = pivoted_df[('modal_cr', 'v2')] - pivoted_df[('modal_cr', 'v1')]
pivoted_df['success_rate_diff'] = pivoted_df[('success_rate', 'v2')] - pivoted_df[('success_rate', 'v1')]
pivoted_df['submit_to_pa_diff'] = pivoted_df[('submit_to_pa', 'v2')] - pivoted_df[('submit_to_pa', 'v1')]
pivoted_df['overall_cr_diff'] = pivoted_df[('overall_cr', 'v2')] - pivoted_df[('overall_cr', 'v1')]
pivoted_df[pivoted_df.columns[7:]] = pivoted_df[pivoted_df.columns[7:]].applymap(percentage_conversion)
pivoted_df

# COMMAND ----------

pivoted_df.to_csv('/dbfs/FileStore/pivoted_df.csv', index=False)
"https://razorpay-dev.cloud.databricks.com/files/pivoted_df.csv"

# COMMAND ----------

# MAGIC %md
# MAGIC ## MID-wise L0 metrics

# COMMAND ----------

grouped_df = final_df[final_df['v2_eligible']==True].groupby(by = ['v2_result','merchant_id'], dropna=False).agg(
    { 'checkout_id':'nunique',
      'submit_checkout_id':'nunique',
        'payment_attempts':'nunique',
     'payment_success':'nunique',
    'pa_checkout_id':'nunique',
        'ps_checkout_id':'nunique',
     }
).reset_index()
grouped_df['split_percentage'] = grouped_df['checkout_id']*1.0 / grouped_df['checkout_id'].sum()
grouped_df['modal_cr'] = grouped_df['submit_checkout_id']*1.0 / grouped_df['checkout_id']
grouped_df['submit_to_pa'] = grouped_df['pa_checkout_id']*1.0 / grouped_df['submit_checkout_id']
grouped_df['success_rate'] = grouped_df['payment_success']*1.0 / grouped_df['payment_attempts']
grouped_df['overall_cr'] = grouped_df['payment_success']*1.0 / grouped_df['checkout_id']


pivoted_df = grouped_df.pivot( index='merchant_id' ,columns= 'v2_result', values = ['checkout_id','modal_cr','success_rate','overall_cr']).sort_values(by = 'merchant_id',).reset_index()
pivoted_df['Total'] = pivoted_df[pivoted_df.columns[1:3]].sum(axis=1)
pivoted_df = pivoted_df.sort_values(by = 'Total', ascending=False).drop(columns=['Total'])
pivoted_df['modal_cr_diff'] = pivoted_df['modal_cr']['v2'] - pivoted_df['modal_cr']['v1']
pivoted_df['success_rate_diff'] = pivoted_df['success_rate']['v2'] - pivoted_df['success_rate']['v1']
pivoted_df['overall_cr_diff'] = pivoted_df['overall_cr']['v2'] - pivoted_df['overall_cr']['v1']
pivoted_df['v1 %'] = pivoted_df['checkout_id']['v1']*1.0 / pivoted_df['checkout_id']['v1'].sum()
pivoted_df['v2 %'] = pivoted_df['checkout_id']['v2']*1.0 / pivoted_df['checkout_id']['v2'].sum()
pivoted_df['vol_diff'] = pivoted_df['v2 %'] - pivoted_df['v1 %']
pivoted_df['vol_impact'] = pivoted_df['vol_diff'] * pivoted_df['modal_cr']['v1']
pivoted_df['cr_impact'] = pivoted_df['modal_cr_diff'] * pivoted_df['v2 %']
pivoted_df['total_impact'] = pivoted_df['cr_impact'] + pivoted_df['vol_impact']
print('Vol Impact ', pivoted_df['vol_impact'].sum())
pivoted_df[pivoted_df.columns[3:]] = pivoted_df[pivoted_df.columns[3:]].applymap(percentage_conversion)
pivoted_df.head(20)

# COMMAND ----------

pivoted_df.to_csv('/dbfs/FileStore/pivoted_df.csv', index=False)
"https://razorpay-dev.cloud.databricks.com/files/pivoted_df.csv"

# COMMAND ----------

# MAGIC %md
# MAGIC ## SR L1 : Payment Method

# COMMAND ----------

grouped_df = final_df[final_df['v2_eligible']==True].groupby(by = ['v2_result','payment_method'], dropna=False).agg(
    { 
     'merchant_id':'nunique',
        'payment_attempts':'nunique',
     'payment_success':'nunique',
    
     }
).reset_index()
grouped_df['split_percentage'] = grouped_df['payment_attempts']*1.0 / grouped_df['payment_attempts'].sum()
grouped_df['success_rate'] = grouped_df['payment_success']*1.0 / grouped_df['payment_attempts']
pivoted_df =grouped_df.pivot( index='payment_method' ,columns= 'v2_result', values = ['payment_attempts','success_rate',]).sort_values(by = 'payment_method',).reset_index()
pivoted_df['Total'] = pivoted_df[pivoted_df.columns[1:3]].sum(axis=1)
pivoted_df = pivoted_df.sort_values(by = 'Total', ascending=False).drop(columns=['Total'])
pivoted_df['success_rate_diff'] = pivoted_df['success_rate']['v2'] - pivoted_df['success_rate']['v1']
pivoted_df[pivoted_df.columns[3:]] = pivoted_df[pivoted_df.columns[3:]].applymap(percentage_conversion)
pivoted_df

# COMMAND ----------

# MAGIC %md
# MAGIC ##UPI SR: UPI Gateway

# COMMAND ----------

grouped_df = final_df[(final_df['v2_eligible']==True) & (final_df['payment_method']=='upi')].groupby(by = ['v2_result','gateway'], dropna=False).agg(
    { 
     'merchant_id':'nunique',
        'payment_attempts':'nunique',
     'payment_success':'nunique',
    
     }
).reset_index()
grouped_df['split_percentage'] = grouped_df['payment_attempts']*1.0 / grouped_df['payment_attempts'].sum()
grouped_df['success_rate'] = grouped_df['payment_success']*1.0 / grouped_df['payment_attempts']
pivoted_df =grouped_df.pivot( index='gateway' ,columns= 'v2_result', values = ['payment_attempts','success_rate',]).sort_values(by = 'gateway',).reset_index()
pivoted_df['Total'] = pivoted_df[pivoted_df.columns[1:3]].sum(axis=1)
pivoted_df = pivoted_df.sort_values(by = 'Total', ascending=False).drop(columns=['Total']).reset_index(drop=True)
pivoted_df['success_rate_diff'] = pivoted_df['success_rate']['v2'] - pivoted_df['success_rate']['v1']
pivoted_df[pivoted_df.columns[3:]] = pivoted_df[pivoted_df.columns[3:]].applymap(percentage_conversion)
pivoted_df

# COMMAND ----------

# MAGIC %md
# MAGIC ## UPI date-wise

# COMMAND ----------

group_cols = ['v2_result','producer_created_date']
grouped_df = final_df[(final_df['v2_eligible']==True) & (final_df['payment_method']=='upi')].groupby(by = group_cols, dropna=False).agg(
    { 
     'merchant_id':'nunique',
        'payment_attempts':'nunique',
     'payment_success':'nunique',
    
     }
).reset_index()
grouped_df['split_percentage'] = grouped_df['payment_attempts']*1.0 / grouped_df['payment_attempts'].sum()
grouped_df['success_rate'] = grouped_df['payment_success']*1.0 / grouped_df['payment_attempts']
pivoted_df =grouped_df.pivot( index=group_cols[1:] ,columns= 'v2_result', values = ['payment_attempts','success_rate',]).reset_index()
pivoted_df['Total'] = pivoted_df[pivoted_df.columns[1:3]].sum(axis=1)
pivoted_df = pivoted_df.sort_values(by = 'Total', ascending=False).drop(columns=['Total']).reset_index(drop=True)
pivoted_df['success_rate_diff'] = pivoted_df['success_rate']['v2'] - pivoted_df['success_rate']['v1']
pivoted_df[pivoted_df.columns[3:]] = pivoted_df[pivoted_df.columns[3:]].applymap(percentage_conversion)
pivoted_df

# COMMAND ----------

# MAGIC %md
# MAGIC ## UPI: Gaetway x Internal Error Code

# COMMAND ----------

final_df['upi_type'].unique()

# COMMAND ----------

group_cols = ['v2_result','gateway','internal_error_code']
grouped_df = final_df[(~final_df['internal_error_code'].isna()) 
                      & (final_df['upi_type']=='Intent')
                      & (final_df['gateway'].isin(['upi_icici','upi_axis','upi_rzpapb']))
                      ][(final_df['v2_eligible']==True) & (final_df['payment_method']=='upi')].groupby(by = group_cols, dropna=False).agg(
    { 
     'merchant_id':'nunique',
        'payment_attempts':'nunique',
     'payment_success':'nunique',
    
     }
).reset_index()
grouped_df['split_percentage'] = grouped_df['payment_attempts']*1.0 / grouped_df['payment_attempts'].sum()
#grouped_df['success_rate'] = grouped_df['payment_success']*1.0 / grouped_df['payment_attempts']
pivoted_df =grouped_df.pivot( index=group_cols[1:] ,columns= 'v2_result', values = ['payment_attempts',]).reset_index()
pivoted_df['Total'] = pivoted_df[pivoted_df.columns[2:4]].sum(axis=1)
pivoted_df['v1 %'] = pivoted_df['payment_attempts']['v1']*1.0 / pivoted_df['payment_attempts']['v1'].sum()
pivoted_df['v2 %'] = pivoted_df['payment_attempts']['v2']*1.0 / pivoted_df['payment_attempts']['v2'].sum()
pivoted_df['vol_diff'] = pivoted_df['v2 %'] - pivoted_df['v1 %']
pivoted_df = pivoted_df.sort_values(by = 'Total', ascending=False).drop(columns=['Total']).reset_index(drop=True)
pivoted_df[pivoted_df.columns[-3:]] = pivoted_df[pivoted_df.columns[-3:]].applymap(percentage_conversion)
pivoted_df.head(20)

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

pivoted_df

# COMMAND ----------

# MAGIC %md
# MAGIC ## UPI SR: UPI Type 

# COMMAND ----------

grouped_df = final_df[(final_df['v2_eligible']==True) & (final_df['payment_method']=='upi')].groupby(by = ['v2_result','upi_type'], dropna=False).agg(
    { 
     'merchant_id':'nunique',
        'payment_attempts':'nunique',
     'payment_success':'nunique',
    
     }
).reset_index()
grouped_df['split_percentage'] = grouped_df['payment_attempts']*1.0 / grouped_df['payment_attempts'].sum()
grouped_df['success_rate'] = grouped_df['payment_success']*1.0 / grouped_df['payment_attempts']
pivoted_df =grouped_df.pivot( index='upi_type' ,columns= 'v2_result', values = ['payment_attempts','success_rate',]).sort_values(by = 'upi_type',).reset_index()
pivoted_df['Total'] = pivoted_df[pivoted_df.columns[1:3]].sum(axis=1)
pivoted_df = pivoted_df.sort_values(by = 'Total', ascending=False).drop(columns=['Total']).reset_index(drop=True)
pivoted_df['success_rate_diff'] = pivoted_df['success_rate']['v2'] - pivoted_df['success_rate']['v1']
pivoted_df[pivoted_df.columns[3:]] = pivoted_df[pivoted_df.columns[3:]].applymap(percentage_conversion)
pivoted_df

# COMMAND ----------

# MAGIC %md
# MAGIC ## UPI SR: UPI App

# COMMAND ----------

grouped_df = final_df[(final_df['v2_eligible']==True) & (final_df['payment_method']=='upi')].groupby(by = ['v2_result','upi_app'], dropna=False).agg(
    { 
     'merchant_id':'nunique',
        'payment_attempts':'nunique',
     'payment_success':'nunique',
    
     }
).reset_index()
grouped_df['split_percentage'] = grouped_df['payment_attempts']*1.0 / grouped_df['payment_attempts'].sum()
grouped_df['success_rate'] = grouped_df['payment_success']*1.0 / grouped_df['payment_attempts']
pivoted_df =grouped_df.pivot( index='upi_app' ,columns= 'v2_result', values = ['payment_attempts','success_rate',]).sort_values(by = 'upi_app',).reset_index()
pivoted_df['Total'] = pivoted_df[pivoted_df.columns[1:3]].sum(axis=1)
pivoted_df = pivoted_df.sort_values(by = 'Total', ascending=False).drop(columns=['Total']).reset_index(drop=True)
pivoted_df['success_rate_diff'] = pivoted_df['success_rate']['v2'] - pivoted_df['success_rate']['v1']
pivoted_df[pivoted_df.columns[3:]] = pivoted_df[pivoted_df.columns[3:]].applymap(percentage_conversion)
pivoted_df

# COMMAND ----------

# MAGIC %md
# MAGIC ## UPI SR: App, Type, Gateway Information

# COMMAND ----------

grouped_df = final_df[(final_df['v2_eligible']==True) & (final_df['payment_method']=='upi')].groupby(
    by = ['v2_result','gateway','upi_type','upi_app', 'upi_provider',], dropna=False).agg(
    { 
     'merchant_id':'nunique',
        'payment_attempts':'nunique',
     'payment_success':'nunique',
    
     }
).reset_index()
grouped_df['split_percentage'] = grouped_df['payment_attempts']*1.0 / grouped_df['payment_attempts'].sum()
grouped_df['success_rate'] = grouped_df['payment_success']*1.0 / grouped_df['payment_attempts']
pivoted_df =grouped_df.pivot( index=['gateway','upi_type','upi_app', 'upi_provider',] ,columns= 'v2_result', values = ['payment_attempts','success_rate',]).sort_values(by = 'gateway',).reset_index()
pivoted_df['Total'] = pivoted_df[pivoted_df.columns[5:7]].sum(axis=1)
pivoted_df = pivoted_df.sort_values(by = 'Total', ascending=False).drop(columns=['Total']).reset_index(drop=True)
pivoted_df['success_rate_diff'] = pivoted_df['success_rate']['v2'] - pivoted_df['success_rate']['v1']
pivoted_df[pivoted_df.columns[6:]] = pivoted_df[pivoted_df.columns[6:]].applymap(percentage_conversion)
pivoted_df.head(20)

# COMMAND ----------

# MAGIC %md
# MAGIC # Funnel Analysis

# COMMAND ----------

# Distinct checkout ID list
merchant_id_list = final_df['merchant_id'].unique()
merchant_id_list = "', '".join(merchant_id_list)
merchant_id_list = f"('{merchant_id_list}')"

# COMMAND ----------

funnel_query_db = sqlContext.sql(
"""
SELECT
    COUNT(DISTINCT CASE WHEN (checkout_fact_emr.open  = 1) THEN checkout_fact_emr.checkout_id ELSE NULL END) AS checkout_open,
    COUNT(DISTINCT CASE WHEN (checkout_fact_emr.checkout_render_complete  = 1) THEN checkout_fact_emr.checkout_id ELSE NULL END) AS checkout_render_complete,
    COUNT(DISTINCT CASE WHEN (checkout_fact_emr.contact_page_rendered_initial  = 1) THEN checkout_fact_emr.checkout_id ELSE NULL END) AS initial_contact_page_rendered,
    COUNT(DISTINCT CASE WHEN (checkout_fact_emr.initial_contact_filled  = 1) THEN checkout_fact_emr.checkout_id ELSE NULL END) AS initial_contact_filled,
    COUNT(DISTINCT CASE WHEN (checkout_fact_emr.contact_page_cta_clicked_initial  = 1) THEN checkout_fact_emr.checkout_id ELSE NULL END) AS initial_contact_page_cta_clicked,
    COUNT(DISTINCT CASE WHEN (checkout_fact_emr.payment_screen_loaded  = 1) THEN checkout_fact_emr.checkout_id ELSE NULL END) AS payment_screen_loaded,
    COUNT(DISTINCT CASE WHEN (checkout_fact_emr.instrument_selected = 1) OR (checkout_fact_emr.method_selected = 1) THEN checkout_fact_emr.checkout_id ELSE NULL END) AS instrument_method_selected,
    COUNT(DISTINCT CASE WHEN (checkout_fact_emr.submit  = 1) THEN checkout_fact_emr.checkout_id ELSE NULL END) AS checkout_submit,
    COUNT(DISTINCT CASE WHEN (checkout_fact_emr.payment_initiated  = 1) THEN checkout_fact_emr.checkout_id ELSE NULL END) AS payment_initiated,
    COUNT(DISTINCT CASE WHEN (checkout_fact_emr.payment_completed  = 1) THEN checkout_fact_emr.checkout_id ELSE NULL END) AS payment_completed
FROM analytics_selfserve.checkout_fact_emr   AS checkout_fact_emr
LEFT JOIN aggregate_pa.cx_lo_fact_ism_v1   AS cx_checkout_l0_fact ON checkout_fact_emr.checkout_id = cx_checkout_l0_fact.checkout_id
LEFT JOIN batch_sheets.checkout_v2_rampup_sheet  AS checkout_v2_rampup_sheet ON cx_checkout_l0_fact.merchant_id = checkout_v2_rampup_sheet.merchant_id
          AND cx_checkout_l0_fact.producer_created_date  > checkout_v2_rampup_sheet.ramp_up_date

WHERE checkout_fact_emr.producer_created_date between '{0}' and '{1}'
 AND cx_checkout_l0_fact.producer_created_date between '{0}' and '{1}'
  And get_json_object(render_properties,'$.data.meta.v2_result')='v2'
   AND cx_checkout_l0_fact.merchant_id in {2}
   """.format(start_date, end_date,merchant_id_list)
)
funnel_query_df = funnel_query_db.toPandas()
funnel_query_df.head()

# COMMAND ----------

funnel_v1_db = sqlContext.sql(
"""
SELECT
    COUNT(DISTINCT CASE WHEN cx_high_level_funnel.integration_checkout_invoked_event  THEN cx_high_level_funnel.checkout_id ELSE NULL END) AS checkout_open,
    COUNT(DISTINCT CASE WHEN cx_high_level_funnel.render_checkout_open_event  THEN cx_high_level_funnel.checkout_id ELSE NULL END) AS checkout_render_complete,
    COUNT(DISTINCT CASE WHEN cx_high_level_funnel.render_contact_details_event  THEN cx_high_level_funnel.checkout_id ELSE NULL END) AS initial_contact_page_rendered,
    COUNT(DISTINCT CASE WHEN cx_high_level_funnel.behav_contact_number_filled_event  THEN cx_high_level_funnel.checkout_id ELSE NULL END) AS initial_contact_filled,
    COUNT(DISTINCT CASE WHEN cx_high_level_funnel.behav_contact_details_proceed_clicked_event  THEN cx_high_level_funnel.checkout_id ELSE NULL END) AS initial_contact_page_cta_clicked,
    COUNT(DISTINCT CASE WHEN cx_high_level_funnel.render_method_selection_screen_event  THEN cx_high_level_funnel.checkout_id ELSE NULL END) AS payment_screen_loaded,
    COUNT(DISTINCT CASE WHEN cx_high_level_funnel.behav_method_selected_event OR cx_high_level_funnel.behav_instrument_selected_event THEN cx_high_level_funnel.checkout_id ELSE NULL END) AS instrument_method_selected,
    COUNT(DISTINCT CASE WHEN cx_high_level_funnel.behav_submit_event  THEN cx_high_level_funnel.checkout_id ELSE NULL END) AS checkout_submit
FROM analytics_selfserve.hlf_v1   AS cx_high_level_funnel

LEFT JOIN batch_sheets.checkout_v2_rampup_sheet  AS checkout_v2_rampup_sheet ON cx_high_level_funnel.merchant_id = checkout_v2_rampup_sheet.merchant_id
          AND date(from_unixtime((cx_high_level_funnel.producer_timestamp+19800000)/1000.0))  > date(checkout_v2_rampup_sheet.ramp_up_date)
          AND checkout_v2_rampup_sheet.blacklisted_flag='false'
          AND checkout_v2_rampup_sheet.rampup_100_percent_flag='false'
LEFT JOIN aggregate_pa.cx_lo_fact_ism_v1   AS cx_checkout_l0_fact 
ON cx_high_level_funnel.checkout_id = cx_checkout_l0_fact.checkout_id
WHERE date(from_unixtime((cx_high_level_funnel.producer_timestamp+19800000)/1000.0))  between date('{0}') and date('{1}')
AND cx_checkout_l0_fact.producer_created_date between '{0}' and '{1}'
AND cx_checkout_l0_fact.merchant_id in {2}
AND get_json_object(render_properties,'$.data.meta.v2_result')  = 'v1' 
AND get_json_object(render_properties,'$.data.meta.v2_eligible')  = 'true' 
AND (checkout_v2_rampup_sheet.merchant_id ) IS NOT NULL

""".format(start_date, end_date,merchant_id_list)
)
funnel_v1_df = funnel_v1_db.toPandas()
funnel_v1_df.head()

# COMMAND ----------

# MAGIC %sql
# MAGIC select id, website from realtime_hudi_api.merchants
# MAGIC where id in ('8ZCnkVYR3WG7PY','KqsQD5pCQWZvtE','IgnuOmEhIbJMJM','IDVkQFXRWGybl9')

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC checkout_id,
# MAGIC json_extract_scalar(properties,'$.data.code') as coupon_code,
# MAGIC json_extract_scalar(properties,'$.data.coupon_code') as coupon_code2,
# MAGIC json_extract_scalar(properties,'$.data.is_coupon_valid') as coupon_valid
# MAGIC from aggregate_pa.cx_1cc_events_dump_v1 a
# MAGIC where event_name='metric:1cc_coupons_screen_coupon_validation_completed'
# MAGIC and merchant_id= 'JUHfXse0FDfnru'
# MAGIC and a.producer_created_date between date('2024-07-01') and date('2024-07-07')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Historic Coverage Data

# COMMAND ----------

#Getting data from Rampup sheet
rampup_master_db =  sqlContext.sql( """
select * from batch_sheets.checkout_v2_rampup_sheet
""")
rampup_master_df = rampup_master_db.toPandas()
print(rampup_master_df.shape)
rampup_master_df.head()

# COMMAND ----------

rampup_pivot_df = rampup_master_df[
  (rampup_master_df['blacklisted_flag']=='false')
    ].groupby(by='ramp_up_date').agg({'merchant_id':'nunique'}).reset_index()
rampup_pivot_df.sort_values(by='ramp_up_date',ascending=True,inplace=True)

# COMMAND ----------

rampup_pivot_df['merchants_ramped_up'] = rampup_pivot_df['merchant_id'].cumsum()
rampup_pivot_df

# COMMAND ----------


