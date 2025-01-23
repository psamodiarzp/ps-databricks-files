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

# MAGIC %md
# MAGIC ## Helper Functions

# COMMAND ----------

def percentage_conversion(value):
    return f'{value:.2%}'

# COMMAND ----------

def formatINR(number):
    s, *d = str(number).partition(".")
    r = ",".join([s[x-2:x] for x in range(-3, -len(s), -2)][::-1] + [s[-3:]])
    return r

# COMMAND ----------

# MAGIC %md
# MAGIC #Date Inputs
# MAGIC In YYYY-MM-DD format
# MAGIC

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


start_date = '2024-12-25'
end_date = '2025-01-01'

# COMMAND ----------

# MAGIC %md
# MAGIC ### HTML CODE Base for Email

# COMMAND ----------

def send_emails(start_date, end_date,email_id_list,html_t):
    today = datetime.now().strftime('%Y-%m-%d')
    subject = 'additional_mweb_intent_apps report - {0} to {1}'.format(start_date, end_date,)
    to = ", ".join(email_id_list)
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
    for i in email_id_list:
        server.sendmail(user, i, message.as_string()) 
    print('Sent email successfully for: ',email_id)

# COMMAND ----------

html_code = """
<html>
<head>
    <title>additional_mweb_intent_apps report (Only Android x Mweb) - {0} to {1} </title>
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
<h2> Folks, this report is a rendition of the V2 report so v2_result : additional_mweb_intent_appst</h2>
""".format(start_date,end_date,)

print(html_code)

# COMMAND ----------

# MAGIC %md
# MAGIC #SQL code for data
# MAGIC

# COMMAND ----------



# Define a function to run the query for each date range and aggregate the results
def fetch_and_aggregate_data(start_date_1, end_date_1, start_date_2, end_date_2):
    # Function to run the SQL query for a given date range
    def run_query(start_date, end_date):
        query = """
        WITH cte AS(
  SELECT
    a.checkout_id,
    a.submit_checkout_id,
    a.merchant_id,
    a.producer_created_date,


   cx_experiment_fact.flag  AS v2_result,
  platform,
  browser_name,
 select_method,
 select_section,
 get_json_object(submit_properties, '$.data.instrument_name') as instrument_name

  
  FROM
    aggregate_pa.cx_lo_fact_ism_v1 a
     Left join (
       select * from
       aggregate_pa.cx_experiment_fact 
       where producer_created_date BETWEEN date('{0}') and date('{1}') 
    and exp_name = 'additional_mweb_intent_apps' )AS cx_experiment_fact on a.checkout_id = cx_experiment_fact.checkout_id
  WHERE
    a.producer_created_date BETWEEN date('{0}') and date('{1}') 

and a.checkout_id <> '00000000000000'
and get_json_object(a.render_properties, '$.data.meta.v2_result') = 'v2'
and platform = '2. mweb'
and device_os = 'Android'

),

cte2 AS(
with payments_cte as(
select 
id,
method, 
authorized_at,
gateway,
error_code,
  error_description,
  internal_error_code

from realtime_hudi_api.payments 
where created_date between ('{0}') and ('{1}') 

  )
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
      from payments_cte as p
 inner join (
    select payment_id, checkout_id from realtime_hudi_api.payment_analytics 
    WHERE created_date between ('{0}') and ('{1}') 
  )pa on p.id = pa.payment_id
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
when lower(upi_payments_flat_fact.u_type ) = 'pay' then upi_payments_flat_fact.upi_meta_app
ELSE 'Others'
END as upi_app,
CASE WHEN upi_payments_flat_fact.u_type = 'collect' THEN 'Collect'
    ELSE 'Intent' END AS upi_type

from whs_v.upi_payments_flat_fact 
where  upi_payments_flat_fact.p_created_date between ('{0}') and ('{1}') 

  )  AS upi_payments_flat_fact on p.id = upi_payments_flat_fact.p_id
  
    
)
select 
cte.producer_created_date,
cte.checkout_id,
cte.submit_checkout_id,
cte.platform,
cte.select_method,
cte.select_section,
cte.instrument_name,
cte.browser_name,
cte.merchant_id,
cte.v2_result,
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
        """.format(start_date, end_date)
        
        # Run the query and return the result as a pandas DataFrame
        raw_data_db = sqlContext.sql(query)
        raw_data_df = raw_data_db.toPandas()
        return raw_data_df

    # Run the query for both date ranges
    df1 = run_query(start_date_1, end_date_1)
    df2 = run_query(start_date_2, end_date_2)
    
    # Aggregate the results by concatenating the two DataFrames
    aggregated_df = pd.concat([df1, df2], ignore_index=True)
    
    return aggregated_df

# Call the function with the two date ranges
start_date_1 = '2024-12-25'
end_date_1 = '2024-12-26'

start_date_2 = '2024-12-28'
end_date_2 = '2025-01-01'

# Fetch and aggregate the data
raw_data_df = fetch_and_aggregate_data(start_date_1, end_date_1, start_date_2, end_date_2)

# Display the aggregated DataFrame
display(raw_data_df.head())

# COMMAND ----------

merchants_db = sqlContext.sql("""
                             select id as merchant_id, website
                             from realtime_hudi_api.merchants
                           
                             """)
merchants_df = merchants_db.toPandas()
merchants_df.head()

# COMMAND ----------

raw_data_df.shape

# COMMAND ----------

# MAGIC %md
# MAGIC #Saving the file as csv for later
# MAGIC - If you need to use the data source but not run the above code again, please utilize below (especially using analytics cluster)

# COMMAND ----------

#raw_data_df.to_csv('/dbfs/FileStore/std_raw_data_upi_{start_date}_{end_date}_df.csv', index=False)
"https://razorpay-dev.cloud.databricks.com/files/std_raw_data_upi_{0}_{1}_df.csv".format(start_date, end_date)

# COMMAND ----------

#raw_data_df = pd.read_csv('/dbfs/FileStore/std_raw_data_upi_{start_date}_{end_date}_df.csv')
raw_data_df.shape

# COMMAND ----------

# MAGIC %md
# MAGIC #Overall Split Check
# MAGIC - to check here: V1 vs V2 split under eligible should be 95:5 or as expected
# MAGIC - % of NAs should be minimal

# COMMAND ----------

overall_df = raw_data_df.groupby(by = ['v2_result'], dropna=False).agg(
    { 'checkout_id':'nunique',
        'merchant_id':'nunique',
        'submit_checkout_id':'nunique',
     'pa_checkout_id':'nunique',
        'ps_checkout_id':'nunique',
        'payment_attempts':'nunique',
     'payment_success':'nunique',
    
     }
).reset_index()
overall_df['split_percentage'] = overall_df['checkout_id']*1.0 / overall_df['checkout_id'].sum()
overall_df['modal_cr'] = overall_df['submit_checkout_id']*1.0 / overall_df['checkout_id']
overall_df['success_rate'] = overall_df['payment_success']*1.0 / overall_df['payment_attempts']
overall_df['overall_cr'] = overall_df['payment_success']*1.0 / overall_df['checkout_id']
overall_df[overall_df.columns[2:-4]] = overall_df[overall_df.columns[2:-4]].applymap(formatINR)
overall_df[overall_df.columns[-4:]] = overall_df[overall_df.columns[-4:]].applymap(percentage_conversion)
#grouped_df.pivot(  = 'v2_result', values = ['checkout_id','payment_attempts','payment_success','success_rate','overall_cr']).reset_index()
overall_df

# COMMAND ----------

# MAGIC %md
# MAGIC ### Checking MID split % + Data Clean Up

# COMMAND ----------

eligibility_condition = (
    raw_data_df['v2_result'].isin(['control','variant_on'])
)

# COMMAND ----------

grouped_df = raw_data_df[eligibility_condition].groupby(by = ['v2_result','merchant_id'], dropna=True).agg(
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

final_mid_list = mid_df[(mid_df.loc[:, pd.IndexSlice['checkout_id_split', 'control']]!=100.0) & (mid_df.loc[:, pd.IndexSlice['checkout_id_split', 'control']]!=0.0)  ]['merchant_id'].tolist()
final_mid_list

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ### Merchants who will be excluded [Top 20]
# MAGIC

# COMMAND ----------

mid_df[(mid_df.loc[:, pd.IndexSlice['checkout_id_split', 'control']]==100.0) | (mid_df.loc[:, pd.IndexSlice['checkout_id_split', 'control']]==0.0)  ].head(20)

# COMMAND ----------

final_df = raw_data_df[raw_data_df['merchant_id'].isin(final_mid_list)].reset_index(drop=True)
final_df

# COMMAND ----------

final_df['merchant_id'].nunique()

# COMMAND ----------


final_df.to_csv('/dbfs/FileStore/std_final_df.csv', index=False)
"https://razorpay-dev.cloud.databricks.com/files/std_final_df.csv".format(start_date, end_date)

# COMMAND ----------

saved_start_date = start_date #Change this if you want to extract file for another time
saved_end_date = end_date #Change this if you want to extract file for another time
#final_df = pd.read_csv('/dbfs/FileStore/std_final_{saved_start_date}_{saved_end_date}_df.csv')
final_df.shape

# COMMAND ----------

# MAGIC %md
# MAGIC #L0 Metrics 
# MAGIC all analysis here on will be only for cases where V2 eligible = True

# COMMAND ----------

conditions = (final_df['v2_result'].isin(['control','variant_on']))

# COMMAND ----------

final_l0_df = final_df[conditions].groupby(by = ['v2_result',], dropna=False).agg(
    { 'checkout_id':'nunique',
     'merchant_id':'nunique',
     'submit_checkout_id':'nunique',
        'payment_attempts':'nunique',
     'payment_success':'nunique',
         'pa_checkout_id':'nunique',
        'ps_checkout_id':'nunique',
     }
).reset_index()
final_l0_df['split_percentage'] = final_l0_df['checkout_id']*1.0 / final_l0_df['checkout_id'].sum()
final_l0_df['modal_cr'] = final_l0_df['submit_checkout_id']*1.0 / final_l0_df['checkout_id']
final_l0_df['success_rate'] = final_l0_df['payment_success']*1.0 / final_l0_df['payment_attempts']
final_l0_df['submit_to_pa'] = final_l0_df['payment_attempts']*1.0 / final_l0_df['submit_checkout_id']
final_l0_df['overall_cr'] = final_l0_df['payment_success']*1.0 / final_l0_df['checkout_id']
final_l0_copy_df = final_l0_df.copy()
final_l0_df[final_l0_df.columns[1:-5]] = final_l0_df[final_l0_df.columns[1:-5]].applymap(formatINR)
final_l0_df[final_l0_df.columns[-5:]] = final_l0_df[final_l0_df.columns[-5:]].applymap(percentage_conversion)
#grouped_df.pivot(  = 'v2_result', values = ['checkout_id','payment_attempts','payment_success','success_rate','overall_cr']).reset_index()
final_l0_df

# COMMAND ----------

html_code += """<p><h3>L0 Metrics for only Merchants who are on experiment:</h3> 

<br/>"""
html_code += final_l0_df.to_html(index=False,escape=False,  )
html_code += """<h4> Modal CR: Submits / Renders | Success Rate : Payment Success / Payment Attempts | Overall CR: Payments Success / Renders | Submit to PA: Payment Attempts / Submits </h4></p>"""

# COMMAND ----------

# MAGIC %md
# MAGIC ## Date-wise L0 metrics

# COMMAND ----------

grouped_df = final_df[conditions].groupby(by = ['v2_result','producer_created_date'], dropna=False).agg(
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
grouped_df['submit_to_pa'] = grouped_df['payment_attempts']*1.0 / grouped_df['submit_checkout_id']
grouped_df['overall_cr'] = grouped_df['payment_success']*1.0 / grouped_df['checkout_id']

pivoted_df = grouped_df.pivot( index='producer_created_date' ,columns= 'v2_result', values = ['checkout_id','payment_attempts','payment_success','modal_cr','success_rate','submit_to_pa','overall_cr']).sort_values(by = 'producer_created_date',).reset_index()
pivoted_df['modal_cr_diff'] = pivoted_df[('modal_cr', 'variant_on')] - pivoted_df[('modal_cr', 'control')]
pivoted_df['success_rate_diff'] = pivoted_df[('success_rate', 'variant_on')] - pivoted_df[('success_rate', 'control')]
pivoted_df['submit_to_pa_diff'] = pivoted_df[('submit_to_pa', 'variant_on')] - pivoted_df[('submit_to_pa', 'control')]
pivoted_df['overall_cr_diff'] = pivoted_df[('overall_cr', 'variant_on')] - pivoted_df[('overall_cr', 'control')]
pivoted_df[pivoted_df.columns[1:7]] = pivoted_df[pivoted_df.columns[1:7]].applymap(formatINR)
pivoted_df[pivoted_df.columns[7:]] = pivoted_df[pivoted_df.columns[7:]].applymap(percentage_conversion)
pivoted_df

# COMMAND ----------

html_code += """<p><h3>L0 Metrics - Date Wise:</h3> <br/>"""
html_code += pivoted_df.to_html(index=False,escape=False,  )
html_code += """</p>"""

# COMMAND ----------

#pivoted_df.to_csv('/dbfs/FileStore/v_imp_data_df.csv', index=False)
"https://razorpay-dev.cloud.databricks.com/files/v_imp_data_df.csv".format(start_date, end_date)

# COMMAND ----------

# MAGIC %md
# MAGIC ## MID-wise L0 metrics

# COMMAND ----------

grouped_df = final_df[conditions].groupby(by = ['v2_result','merchant_id'], dropna=False).agg(
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
grouped_df['submit_to_pa'] = grouped_df['payment_attempts']*1.0 / grouped_df['submit_checkout_id']
grouped_df['success_rate'] = grouped_df['payment_success']*1.0 / grouped_df['payment_attempts']
grouped_df['overall_cr'] = grouped_df['payment_success']*1.0 / grouped_df['checkout_id']
grouped_df = grouped_df.merge(merchants_df, on='merchant_id', how='left')

pivoted_df = grouped_df.pivot( index=['merchant_id' ,'website'],columns= 'v2_result', values = ['checkout_id','modal_cr','submit_to_pa','success_rate','overall_cr']).sort_values(by = 'merchant_id',).reset_index()
pivoted_df['Total'] = pivoted_df[pivoted_df.columns[2:4]].sum(axis=1)
pivoted_df = pivoted_df.sort_values(by = 'Total', ascending=False).drop(columns=['Total'])
pivoted_df['modal_cr_diff'] = pivoted_df['modal_cr']['variant_on'] - pivoted_df['modal_cr']['control']
pivoted_df['success_rate_diff'] = pivoted_df['success_rate']['variant_on'] - pivoted_df['success_rate']['control']
pivoted_df['submit_to_pa_diff'] = pivoted_df['submit_to_pa']['variant_on'] - pivoted_df['submit_to_pa']['control']
pivoted_df['overall_cr_diff'] = pivoted_df['overall_cr']['variant_on'] - pivoted_df['overall_cr']['control']
#pivoted_df['control %'] = pivoted_df['checkout_id']['control']*1.0 / pivoted_df['checkout_id']['control'].sum()
#pivoted_df['v2 %'] = pivoted_df['checkout_id']['v2']*1.0 / pivoted_df['checkout_id']['v2'].sum()
#pivoted_df['vol_diff'] = pivoted_df['v2 %'] - pivoted_df['control %']
#pivoted_df['vol_impact'] = pivoted_df['vol_diff'] * pivoted_df['modal_cr']['v1']
#pivoted_df['cr_impact'] = pivoted_df['modal_cr_diff'] * pivoted_df['v2 %']
#pivoted_df['total_impact'] = pivoted_df['cr_impact'] + pivoted_df['vol_impact']
#print('Vol Impact ', pivoted_df['vol_impact'].sum())
pivoted_df[pivoted_df.columns[2:4]] = pivoted_df[pivoted_df.columns[2:4]].applymap(formatINR)
pivoted_df[pivoted_df.columns[4:]] = pivoted_df[pivoted_df.columns[4:]].applymap(percentage_conversion)
pivoted_df.head(20)

# COMMAND ----------

pivoted_df_filtered = pivoted_df[pivoted_df['overall_cr_diff'] > '100.00%']
pivoted_df_filtered.head(20)

# COMMAND ----------

html_code += """<p><h3>L0 Metrics - Merchant wise [Top 20 by volume]:</h3> <br/>"""
html_code += (pivoted_df.head(20)).to_html(index=False,escape=False,  )
html_code += """</p>"""

# COMMAND ----------

# MAGIC %md
# MAGIC ## PSM Metrics

# COMMAND ----------

grouped_df = final_df[conditions].groupby(by = ['v2_result','platform','select_method','select_section',], dropna=False).agg(
    { 'checkout_id':'nunique',
      'submit_checkout_id':'nunique',
        'payment_attempts':'nunique',
     'payment_success':'nunique',
    'pa_checkout_id':'nunique',
        'ps_checkout_id':'nunique',
     }
).reset_index()
grouped_df['modal_cr'] = grouped_df['submit_checkout_id']*1.0 / grouped_df['checkout_id']
grouped_df['submit_to_pa'] = grouped_df['payment_attempts']*1.0 / grouped_df['submit_checkout_id']
grouped_df['success_rate'] = grouped_df['payment_success']*1.0 / grouped_df['payment_attempts']
grouped_df['overall_cr'] = grouped_df['payment_success']*1.0 / grouped_df['checkout_id']
psm_df = grouped_df.pivot( index=['platform','select_method','select_section',],columns= 'v2_result', values = ['checkout_id','modal_cr','submit_to_pa','success_rate','overall_cr']).reset_index()
psm_df['Total'] = psm_df[psm_df.columns[3:5]].sum(axis=1)
psm_df = psm_df.sort_values(by = 'Total', ascending=False).drop(columns=['Total'])
psm_df['modal_cr_diff'] = psm_df['modal_cr']['variant_on'] - psm_df['modal_cr']['control']
psm_df['success_rate_diff'] = psm_df['success_rate']['variant_on'] - psm_df['success_rate']['control']
psm_df['submit_to_pa_diff'] = psm_df['submit_to_pa']['variant_on'] - psm_df['submit_to_pa']['control']
psm_df['overall_cr_diff'] = psm_df['overall_cr']['variant_on'] - psm_df['overall_cr']['control']
psm_df[psm_df.columns[3:5]] = psm_df[psm_df.columns[3:5]].applymap(formatINR)
psm_df[psm_df.columns[5:]] = psm_df[psm_df.columns[5:]].applymap(percentage_conversion)
psm_df.head(20)

# COMMAND ----------

html_code += """<p><h3>Platform x Section x Method RCA</h3> <br/>"""
html_code += (psm_df.head(10)).to_html(index=False,escape=False,  )
html_code += """</p>"""

# COMMAND ----------

# MAGIC %md
# MAGIC #Browser-wise L0 metrics
# MAGIC

# COMMAND ----------

grouped_df = final_df[conditions].groupby(by = ['v2_result','browser_name'], dropna=False).agg(
    { 'checkout_id':'nunique',
      'submit_checkout_id':'nunique',
        'payment_attempts':'nunique',
     'payment_success':'nunique',
    
     }
).reset_index()
grouped_df['split_percentage'] = grouped_df['checkout_id']*1.0 / grouped_df['checkout_id'].sum()
grouped_df['modal_cr'] = grouped_df['submit_checkout_id']*1.0 / grouped_df['checkout_id']
grouped_df['overall_cr'] = grouped_df['payment_success']*1.0 / grouped_df['checkout_id']
grouped_df['success_rate'] = grouped_df['payment_success']*1.0 / grouped_df['payment_attempts']

pivoted_df = grouped_df.pivot( index='browser_name' ,columns= 'v2_result', values = ['checkout_id','modal_cr','success_rate','overall_cr']).sort_values(by = 'browser_name',).reset_index()
pivoted_df['Total'] = pivoted_df[pivoted_df.columns[1:3]].sum(axis=1)
pivoted_df = pivoted_df.sort_values(by = 'Total', ascending=False).drop(columns=['Total'])
pivoted_df['modal_cr_diff'] = pivoted_df['modal_cr']['variant_on'] - pivoted_df['modal_cr']['control']
pivoted_df['success_rate_diff'] = pivoted_df['success_rate']['variant_on'] - pivoted_df['success_rate']['control']
pivoted_df['overall_cr_diff'] = pivoted_df['overall_cr']['variant_on'] - pivoted_df['overall_cr']['control']
pivoted_df[pivoted_df.columns[1:3]] = pivoted_df[pivoted_df.columns[1:3]].applymap(formatINR)
pivoted_df[pivoted_df.columns[3:]] = pivoted_df[pivoted_df.columns[3:]].applymap(percentage_conversion)
pivoted_df.head(20)

# COMMAND ----------

html_code += """<p><h3>L0 Metrics - Browser wise :</h3> <br/>
"""
html_code += (pivoted_df.head(20)).to_html(index=False,escape=False,  )
html_code += """</p>"""

# COMMAND ----------

# Distinct checkout ID list
merchant_id_list = final_df['merchant_id'].unique()
merchant_id_list = "', '".join(merchant_id_list)
merchant_id_list = f"('{merchant_id_list}')"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Submit to Payment Initiated

# COMMAND ----------

cols = ['v2_result','payment_method']
submit_to_pa_df = final_df[conditions].groupby(by = cols, dropna=False).agg(
    { 
     'submit_checkout_id':'nunique',
        'payment_attempts':'nunique',
     'payment_success':'nunique',
         'pa_checkout_id':'nunique',
        'ps_checkout_id':'nunique',
     }
).reset_index()

submit_to_pa_df['success_rate'] = submit_to_pa_df['payment_success']*1.0 / submit_to_pa_df['payment_attempts']
submit_to_pa_df['submit_to_pa'] = submit_to_pa_df['payment_attempts']*1.0 / submit_to_pa_df['submit_checkout_id']
submit_to_pa_df['submit_to_pa_checkout_id'] = submit_to_pa_df['pa_checkout_id']*1.0 / submit_to_pa_df['submit_checkout_id']


#final_l0_df[final_l0_df.columns[1:-5]] = final_l0_df[final_l0_df.columns[1:-5]].applymap(formatINR)
#submit_to_pa_df[submit_to_pa_df.columns[-3:]] = submit_to_pa_df[submit_to_pa_df.columns[-3:]].applymap(percentage_conversion)

submit_to_pa_df

# COMMAND ----------



# COMMAND ----------


submit_to_pa_pivot_df = submit_to_pa_df.pivot( columns='v2_result',index = cols[1:], values = ['submit_checkout_id','payment_attempts','pa_checkout_id','success_rate','submit_to_pa','submit_to_pa_checkout_id']).reset_index()
submit_to_pa_pivot_df['Total'] = submit_to_pa_pivot_df[submit_to_pa_pivot_df.columns[1:3]].sum(axis=1)
submit_to_pa_pivot_df = submit_to_pa_pivot_df.sort_values(by = 'Total', ascending=False).drop(columns=['Total'])
submit_to_pa_pivot_df['submit_to_pa_diff'] = submit_to_pa_pivot_df['submit_to_pa']['variant_on'] - submit_to_pa_pivot_df['submit_to_pa']['control']
submit_to_pa_pivot_df['submit_to_pa_checkout_id_diff'] = submit_to_pa_pivot_df['submit_to_pa_checkout_id']['variant_on'] - submit_to_pa_pivot_df['submit_to_pa_checkout_id']['control']
submit_to_pa_pivot_df[submit_to_pa_pivot_df.columns[1:10]] = submit_to_pa_pivot_df[submit_to_pa_pivot_df.columns[1:10]].applymap(formatINR)
submit_to_pa_pivot_df[submit_to_pa_pivot_df.columns[10:]] = submit_to_pa_pivot_df[submit_to_pa_pivot_df.columns[10:]].applymap(percentage_conversion)
submit_to_pa_pivot_df

# COMMAND ----------

# MAGIC %md
# MAGIC ## SR L1 : Payment Method

# COMMAND ----------

html_code += """<h2>SR Deep-dive</h2> <br/>"""

# COMMAND ----------

grouped_df = final_df[conditions].groupby(by = ['v2_result','payment_method'], dropna=False).agg(
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
pivoted_df['success_rate_diff'] = pivoted_df['success_rate']['variant_on'] - pivoted_df['success_rate']['control']
pivoted_df[pivoted_df.columns[1:3]] = pivoted_df[pivoted_df.columns[1:3]].applymap(formatINR)
pivoted_df[pivoted_df.columns[3:]] = pivoted_df[pivoted_df.columns[3:]].applymap(percentage_conversion)
pivoted_df

# COMMAND ----------

html_code += """<p><h3>SR - Payment Method:</h3> <br/>"""
html_code += pivoted_df.to_html(index=False,escape=False,  )
html_code += """</p>"""

# COMMAND ----------

# MAGIC %md
# MAGIC ##UPI SR: UPI Gateway

# COMMAND ----------

grouped_df = final_df[conditions & (final_df['payment_method']=='upi')].groupby(by = ['v2_result','gateway'], dropna=False).agg(
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
pivoted_df['success_rate_diff'] = pivoted_df['success_rate']['variant_on'] - pivoted_df['success_rate']['control']
pivoted_df[pivoted_df.columns[1:3]] = pivoted_df[pivoted_df.columns[1:3]].applymap(formatINR)
pivoted_df[pivoted_df.columns[3:]] = pivoted_df[pivoted_df.columns[3:]].applymap(percentage_conversion)
pivoted_df

# COMMAND ----------

html_code += """<p><h3>UPI SR - Gateway:</h3> <br/>"""
html_code += pivoted_df.to_html(index=False,escape=False,  )
html_code += """</p>"""

# COMMAND ----------

# MAGIC %md
# MAGIC ## UPI date-wise

# COMMAND ----------

group_cols = ['v2_result','producer_created_date']
grouped_df = final_df[conditions & (final_df['payment_method']=='upi')].groupby(by = group_cols, dropna=False).agg(
    { 
     'merchant_id':'nunique',
        'payment_attempts':'nunique',
     'payment_success':'nunique',
    
     }
).reset_index()
grouped_df['split_percentage'] = grouped_df['payment_attempts']*1.0 / grouped_df['payment_attempts'].sum()
grouped_df['success_rate'] = grouped_df['payment_success']*1.0 / grouped_df['payment_attempts']
pivoted_df =grouped_df.pivot( index=group_cols[1:] ,columns= 'v2_result', values = ['payment_attempts','success_rate',]).reset_index()
#pivoted_df['Total'] = pivoted_df[pivoted_df.columns[1:3]].sum(axis=1)
#pivoted_df = pivoted_df.sort_values(by = 'Total', ascending=False).drop(columns=['Total']).reset_index(drop=True)
pivoted_df['success_rate_diff'] = pivoted_df['success_rate']['variant_on'] - pivoted_df['success_rate']['control']
pivoted_df[pivoted_df.columns[1:3]] = pivoted_df[pivoted_df.columns[1:3]].applymap(formatINR)
pivoted_df[pivoted_df.columns[3:]] = pivoted_df[pivoted_df.columns[3:]].applymap(percentage_conversion)
pivoted_df

# COMMAND ----------

html_code += """<p><h3>UPI SR - Date-wise:</h3> <br/>"""
html_code += pivoted_df.to_html(index=False,escape=False,  )
html_code += """</p>"""

# COMMAND ----------

# MAGIC %md
# MAGIC ## UPI SR: UPI Type 

# COMMAND ----------

grouped_df = final_df[conditions & (final_df['payment_method']=='upi')].groupby(by = ['v2_result','upi_type'], dropna=False).agg(
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
pivoted_df['success_rate_diff'] = pivoted_df['success_rate']['variant_on'] - pivoted_df['success_rate']['control']
pivoted_df[pivoted_df.columns[1:3]] = pivoted_df[pivoted_df.columns[1:3]].applymap(formatINR)
pivoted_df[pivoted_df.columns[3:]] = pivoted_df[pivoted_df.columns[3:]].applymap(percentage_conversion)
pivoted_df

# COMMAND ----------

html_code += """<p><h3>UPI SR - Type:</h3> <br/>"""
html_code += pivoted_df.to_html(index=False,escape=False,  )
html_code += """</p>"""

# COMMAND ----------

# MAGIC %md
# MAGIC ## UPI: Gateway x Internal Error Code

# COMMAND ----------

group_cols = ['v2_result','gateway','internal_error_code']
grouped_df = final_df[(~final_df['internal_error_code'].isna()) 
                      & (final_df['upi_type']=='Intent')
                      #& (final_df['gateway'].isin(['upi_icici','upi_axis','upi_rzpapb']))
                      ][conditions & (final_df['payment_method']=='upi')].groupby(by = group_cols, dropna=False).agg(
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
pivoted_df['control %'] = pivoted_df['payment_attempts']['control']*1.0 / pivoted_df['payment_attempts']['control'].sum()
pivoted_df['variant_on %'] = pivoted_df['payment_attempts']['variant_on']*1.0 / pivoted_df['payment_attempts']['variant_on'].sum()
pivoted_df['vol_diff'] = pivoted_df['variant_on %'] - pivoted_df['control %']
pivoted_df = pivoted_df.sort_values(by = 'Total', ascending=False).drop(columns=['Total']).reset_index(drop=True)
pivoted_df[pivoted_df.columns[2:-3]] = pivoted_df[pivoted_df.columns[2:-3]].applymap(formatINR)
pivoted_df[pivoted_df.columns[-3:]] = pivoted_df[pivoted_df.columns[-3:]].applymap(percentage_conversion)
pivoted_df.head(20)

# COMMAND ----------

html_code += """<p><h3>UPI Intent SR - Gateway x Error Code:</h3> <br/>"""
html_code += (pivoted_df.head(10)).to_html(index=False,escape=False,  )
html_code += """</p>"""

# COMMAND ----------

# MAGIC %md
# MAGIC ## UPI SR: UPI App

# COMMAND ----------

grouped_df = final_df[conditions & (final_df['payment_method']=='upi')].groupby(by = ['v2_result','upi_app'], dropna=False).agg(
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
pivoted_df['success_rate_diff'] = pivoted_df['success_rate']['variant_on'] - pivoted_df['success_rate']['control']
pivoted_df[pivoted_df.columns[1:3]] = pivoted_df[pivoted_df.columns[1:3]].applymap(formatINR)
pivoted_df[pivoted_df.columns[3:]] = pivoted_df[pivoted_df.columns[3:]].applymap(percentage_conversion)
pivoted_df

# COMMAND ----------

html_code += """<p><h3>UPI SR - App:</h3> <br/>"""
html_code += (pivoted_df.head(60)).to_html(index=False,escape=False,  )
html_code += """</p>"""

# COMMAND ----------

# MAGIC %md
# MAGIC ## UPI SR: App, Type, Gateway Information

# COMMAND ----------

grouped_df = final_df[conditions & (final_df['payment_method']=='upi')].groupby(
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
pivoted_df['success_rate_diff'] = pivoted_df['success_rate']['variant_on'] - pivoted_df['success_rate']['control']
pivoted_df[pivoted_df.columns[6:]] = pivoted_df[pivoted_df.columns[6:]].applymap(percentage_conversion)
pivoted_df = pivoted_df.head(20)
pivoted_df

# COMMAND ----------

html_code += """<p><h3>UPI SR - App x Type x Gateway:</h3> <br/>"""
html_code += (pivoted_df.head(20)).to_html(index=False,escape=False,  )
html_code += """</p>"""

# COMMAND ----------

# MAGIC %md
# MAGIC ## Instagram SR Deep-dive

# COMMAND ----------

html_code += """<p><h2>Instagram SR breakdown</h2> <br/>"""

# COMMAND ----------

grouped_df = final_df[conditions & (final_df['browser_name']=='Instagram')].groupby(by = ['v2_result','payment_method'], dropna=False).agg(
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
pivoted_df['success_rate_diff'] = pivoted_df['success_rate']['variant_on'] - pivoted_df['success_rate']['control']
pivoted_df[pivoted_df.columns[1:3]] = pivoted_df[pivoted_df.columns[1:3]].applymap(formatINR)
pivoted_df[pivoted_df.columns[3:]] = pivoted_df[pivoted_df.columns[3:]].applymap(percentage_conversion)
pivoted_df

# COMMAND ----------

html_code += """<p><h3>SR Instagram breakdown:</h3> <br/>"""
html_code += pivoted_df.to_html(index=False,escape=False,  )
html_code += """</p>"""

# COMMAND ----------

grouped_df = final_df[conditions & (final_df['payment_method']=='upi') & (final_df['browser_name']=='Instagram')].groupby(by = ['v2_result','instrument_name','upi_type',], dropna=False).agg(
    { 
     'merchant_id':'nunique',
        'payment_attempts':'nunique',
     'payment_success':'nunique',
    
     }
).reset_index()
grouped_df['split_percentage'] = grouped_df['payment_attempts']*1.0 / grouped_df['payment_attempts'].sum()
grouped_df['success_rate'] = grouped_df['payment_success']*1.0 / grouped_df['payment_attempts']
pivoted_df =grouped_df.pivot( index=['instrument_name','upi_type',] ,columns= 'v2_result', values = ['payment_attempts','merchant_id','success_rate',]).reset_index()
pivoted_df['Total'] = pivoted_df[pivoted_df.columns[4:6]].sum(axis=1)
pivoted_df = pivoted_df.sort_values(by = 'Total', ascending=False).drop(columns=['Total']).reset_index(drop=True)
pivoted_df['success_rate_diff'] = pivoted_df['success_rate']['variant_on'] - pivoted_df['success_rate']['control']
pivoted_df[pivoted_df.columns[3:7]] = pivoted_df[pivoted_df.columns[3:7]].applymap(formatINR)
pivoted_df[pivoted_df.columns[7:]] = pivoted_df[pivoted_df.columns[7:]].applymap(percentage_conversion)
pivoted_df

# COMMAND ----------

html_code += """<p><h3>Instagram UPI SR - Front-end Instrument-wise:</h3> <br/>"""
html_code += (pivoted_df.head(20)).to_html(index=False,escape=False,  )
html_code += """</p>"""

# COMMAND ----------

# MAGIC %md
# MAGIC # Funnel Analysis

# COMMAND ----------

# Distinct checkout ID list
merchant_id_list = final_df['merchant_id'].unique()
merchant_id_list = "', '".join(merchant_id_list)
merchant_id_list = f"('{merchant_id_list}')"

# COMMAND ----------

merchant_id_list

# COMMAND ----------

final_df['merchant_id'].to_csv('/dbfs/FileStore/merchant_id_list.csv', index=False)
"https://razorpay-dev.cloud.databricks.com/files/merchant_id_list.csv"

# COMMAND ----------

funnel_query_db = sqlContext.sql(
"""


SELECT
cx_experiment_fact.flag as exp_flag,
 --is_quickbuy_flow,
  ---  COUNT(DISTINCT CASE WHEN (checkout_fact_emr.open  = 1) THEN checkout_fact_emr.checkout_id ELSE NULL END) AS checkout_open,
    COUNT(DISTINCT CASE WHEN (checkout_fact_emr.checkout_render_complete  = 1) THEN checkout_fact_emr.checkout_id ELSE NULL END) AS checkout_render_complete,
    COUNT(DISTINCT CASE WHEN (checkout_fact_emr.contact_page_rendered_initial  = 1) THEN checkout_fact_emr.checkout_id ELSE NULL END) AS initial_contact_page_rendered,
    COUNT(DISTINCT CASE WHEN (checkout_fact_emr.initial_contact_filled  = 1) THEN checkout_fact_emr.checkout_id ELSE NULL END) AS initial_contact_filled,
    COUNT(DISTINCT CASE WHEN (checkout_fact_emr.contact_page_cta_clicked_initial  = 1) THEN checkout_fact_emr.checkout_id ELSE NULL END) AS initial_contact_page_cta_clicked,
    COUNT(DISTINCT CASE WHEN (checkout_fact_emr.payment_screen_loaded  = 1) THEN checkout_fact_emr.checkout_id ELSE NULL END) AS payment_screen_loaded,
    COUNT(DISTINCT CASE WHEN (checkout_fact_emr.instrument_selected = 1) OR (checkout_fact_emr.method_selected = 1) THEN checkout_fact_emr.checkout_id ELSE NULL END) AS instrument_method_selected,
    COUNT(DISTINCT CASE WHEN (checkout_fact_emr.submit  = 1) THEN checkout_fact_emr.checkout_id ELSE NULL END) AS checkout_submit
   -- COUNT(DISTINCT CASE WHEN (checkout_fact_emr.payment_initiated  = 1) THEN checkout_fact_emr.checkout_id ELSE NULL END) AS payment_initiated,
  --  COUNT(DISTINCT CASE WHEN (checkout_fact_emr.payment_completed  = 1) THEN checkout_fact_emr.checkout_id ELSE NULL END) AS payment_completed
FROM analytics_selfserve.checkout_fact_emr   AS checkout_fact_emr
--LEFT JOIN (
--select *, get_json_object(render_properties,'$.data.meta.is_quickbuy_flow') as is_quickbuy_flow
-- from aggregate_pa.cx_lo_fact_ism_v1  
--where producer_created_date between '{0}' and '{1}'
 -- And get_json_object(render_properties,'$.data.meta.quickbuy_eligible')='true' 
 --  AND merchant_id in {2}
--)  AS cx_checkout_l0_fact ON checkout_fact_emr.checkout_id = cx_checkout_l0_fact.checkout_id
Left join (
       select * from
       aggregate_pa.cx_experiment_fact 
       where producer_created_date BETWEEN '{0}' and '{1}'
    and exp_name = 'additional_mweb_intent_apps' )AS cx_experiment_fact on checkout_fact_emr.checkout_id = cx_experiment_fact.checkout_id
inner join (
  select checkout_id
  FROM
    aggregate_pa.cx_lo_fact_ism_v1 a
 
  WHERE
    a.producer_created_date BETWEEN date('{0}') and date('{1}') 
and a.checkout_id <> '00000000000000'
-- and get_json_object(render_properties, '$.data.meta.v2_result') = "v2"

)
WHERE checkout_fact_emr.producer_created_date BETWEEN date('{0}') and date('{1}') 
AND cx_experiment_fact.flag <> 'None'
  group by 1
   """.format(start_date, end_date,merchant_id_list)
)
funnel_query_df = funnel_query_db.toPandas()
funnel_query_df.head()

# COMMAND ----------

funnel_transposed = funnel_query_df.transpose().reset_index()
funnel_transposed

# COMMAND ----------

funnel_transposed = funnel_query_df.transpose().reset_index()

funnel_transposed.columns = list(funnel_transposed.loc[0,:])
funnel_transposed = funnel_transposed[2:]
funnel_transposed

# COMMAND ----------

# Calculate percentages based on the maximum values in each column
funnel_transposed['variant %'] = funnel_transposed['variant_on'] / funnel_transposed['variant_on'].max()


funnel_transposed['control %'] = funnel_transposed['control'] / funnel_transposed['control'].max()
funnel_transposed['diff %'] = funnel_transposed['variant %'] - funnel_transposed['control %']
funnel_transposed['variant %'] = funnel_transposed[['variant %']].applymap(percentage_conversion)
funnel_transposed['control %'] = funnel_transposed[['control %']].applymap(percentage_conversion)
funnel_transposed['diff %'] = funnel_transposed[['diff %']].applymap(percentage_conversion)

funnel_transposed

# COMMAND ----------

html_code += """<p><h3> High Level Funnel:</h3> 

"""
html_code += funnel_transposed.to_html(index=False,escape=False,  )
html_code += """</p>"""

# COMMAND ----------

# MAGIC %md
# MAGIC ## UPI Funnel

# COMMAND ----------

upi_funnel_query_db = sqlContext.sql(
"""

SELECT
    cx_experiment_fact.flag as exp_flag,
   
    COUNT(DISTINCT CASE WHEN (checkout_fact_emr.payment_screen_loaded  = 1) THEN checkout_fact_emr.checkout_id ELSE NULL END) AS payment_screen_loaded,
    COUNT(DISTINCT CASE WHEN (checkout_fact_emr.upi_shown  = 1) THEN checkout_fact_emr.checkout_id ELSE NULL END) AS upi_shown,
    COUNT(DISTINCT CASE WHEN (checkout_fact_emr.upi_selected  = 1) THEN checkout_fact_emr.checkout_id ELSE NULL END) AS upi_selected,
    COUNT(DISTINCT CASE WHEN (checkout_fact_emr.upi_submit  = 1) THEN checkout_fact_emr.checkout_id ELSE NULL END) AS upi_submit,
    COUNT(DISTINCT CASE WHEN (checkout_fact_emr.upi_payment_init  = 1) THEN checkout_fact_emr.checkout_id ELSE NULL END) AS upi_payment_initiated,
    COUNT(DISTINCT CASE WHEN (checkout_fact_emr.upi_payment_complete  = 1) THEN checkout_fact_emr.checkout_id ELSE NULL END) AS upi_payment_completed
  FROM analytics_selfserve.checkout_fact_emr   AS checkout_fact_emr
INNER JOIN (
select * from aggregate_pa.cx_lo_fact_ism_v1  
where producer_created_date between '{0}' and '{1}'
  --And get_json_object(render_properties,'$.data.meta.quickbuy_eligible')='true' 
   AND merchant_id in {2}
)  AS cx_checkout_l0_fact ON checkout_fact_emr.checkout_id = cx_checkout_l0_fact.checkout_id
Left join (
       select * from
       aggregate_pa.cx_experiment_fact 
       where producer_created_date BETWEEN date('{0}') and date('{1}') 
    and exp_name = 'bhim_intent_mweb' )AS cx_experiment_fact on checkout_fact_emr.checkout_id = cx_experiment_fact.checkout_id

WHERE checkout_fact_emr.producer_created_date between '{0}' and '{1}'
AND cx_experiment_fact.flag <> 'None'
  group by 1
   """.format(start_date, end_date,merchant_id_list)
)
upi_funnel_query_df = upi_funnel_query_db.toPandas()
upi_funnel_query_df.head()

# COMMAND ----------

upi_funnel_transposed = upi_funnel_query_df.transpose().reset_index()
upi_funnel_transposed

# COMMAND ----------

upi_funnel_transposed = upi_funnel_query_df.transpose().reset_index()

upi_funnel_transposed.columns = list(upi_funnel_transposed.loc[0,:])
upi_funnel_transposed = upi_funnel_transposed[2:]
upi_funnel_transposed = upi_funnel_transposed[['exp_flag','variant_on','control']]
upi_funnel_transposed

# COMMAND ----------

# Calculate percentages based on the maximum values in each column
upi_funnel_transposed['variant %'] = upi_funnel_transposed['variant_on'] / upi_funnel_transposed['variant_on'].max()
upi_funnel_transposed['control %'] = upi_funnel_transposed['control'] / upi_funnel_transposed['control'].max()

upi_funnel_transposed['variant %'] = upi_funnel_transposed[['variant %']].applymap(percentage_conversion)
upi_funnel_transposed['control %'] = upi_funnel_transposed[['control %']].applymap(percentage_conversion)

upi_funnel_transposed

# COMMAND ----------

# upi_funnel_final_df['percentage of render'] = upi_funnel_final_df['Checkout ID'].div(upi_funnel_final_df['Checkout ID'].iloc[0])
# #funnel_final_df['Checkout ID'] = funnel_final_df[['Checkout ID']].applymap(formatINR)
# upi_funnel_final_df['percentage of render'] = upi_funnel_final_df[['percentage of render']].applymap(percentage_conversion)
# upi_funnel_final_df

# COMMAND ----------

html_code += """<p><h3>UPI Funnel:</h3> 

"""
html_code += upi_funnel_transposed.to_html(index=False,escape=False,  )
html_code += """</p>"""

# COMMAND ----------

# MAGIC %md
# MAGIC ## Card Funnel

# COMMAND ----------

card_funnel_query_db = sqlContext.sql(
"""
SELECT
cx_experiment_fact.flag as exp_flag,
COUNT(DISTINCT CASE WHEN (checkout_fact_emr.payment_screen_loaded  = 1) THEN checkout_fact_emr.checkout_id ELSE NULL END) AS payment_screen_loaded,
    COUNT(DISTINCT CASE WHEN (checkout_fact_emr.cards_shown  = 1) THEN checkout_fact_emr.checkout_id ELSE NULL END) AS cards_shown,
    COUNT(DISTINCT CASE WHEN (checkout_fact_emr.cards_selected  = 1) THEN checkout_fact_emr.checkout_id ELSE NULL END) AS cards_selected,
    COUNT(DISTINCT CASE WHEN (checkout_fact_emr.cards_submit  = 1) THEN checkout_fact_emr.checkout_id ELSE NULL END) AS cards_submit,
      COUNT(DISTINCT CASE WHEN (checkout_fact_emr.cards_payment_init  = 1) THEN checkout_fact_emr.checkout_id ELSE NULL END) AS cards_payment_init,
    COUNT(DISTINCT CASE WHEN (checkout_fact_emr.cards_payment_complete  = 1) THEN checkout_fact_emr.checkout_id ELSE NULL END) AS cards_payment_completed
  FROM analytics_selfserve.checkout_fact_emr   AS checkout_fact_emr
LEFT JOIN aggregate_pa.cx_lo_fact_ism_v1   AS cx_checkout_l0_fact ON checkout_fact_emr.checkout_id = cx_checkout_l0_fact.checkout_id
Left join (
       select * from
       aggregate_pa.cx_experiment_fact 
       where producer_created_date BETWEEN date('{0}') and date('{1}') 
    and exp_name = 'bhim_intent_mweb' )AS cx_experiment_fact on checkout_fact_emr.checkout_id = cx_experiment_fact.checkout_id
WHERE checkout_fact_emr.producer_created_date between '{0}' and '{1}'
 AND cx_checkout_l0_fact.producer_created_date between '{0}' and '{1}'
  --And get_json_object(render_properties,'$.data.meta.v2_result')='v2'
   AND cx_checkout_l0_fact.merchant_id in {2}
   AND cx_experiment_fact.flag <> 'None'
   group by 1
   """.format(start_date, end_date,merchant_id_list)
)
card_funnel_query_df = card_funnel_query_db.toPandas()
card_funnel_query_df.head()

# COMMAND ----------

card_funnel_transposed = card_funnel_query_df.transpose().reset_index()
card_funnel_transposed

# COMMAND ----------

card_funnel_transposed = card_funnel_query_df.transpose().reset_index()

card_funnel_transposed.columns = list(card_funnel_transposed.loc[0,:])
card_funnel_transposed = card_funnel_transposed[2:]
card_funnel_transposed

# COMMAND ----------

# Calculate percentages based on the maximum values in each column
card_funnel_transposed['variant %'] = card_funnel_transposed['variant_on'] / card_funnel_transposed['variant_on'].max()
card_funnel_transposed['variant %'] = card_funnel_transposed[['variant %']].applymap(percentage_conversion)

card_funnel_transposed['control %'] = card_funnel_transposed['control'] / card_funnel_transposed['control'].max()
card_funnel_transposed['control %'] = card_funnel_transposed[['control %']].applymap(percentage_conversion)

card_funnel_transposed

# COMMAND ----------

html_code += """<p><h3>Card Funnel:</h3> 

"""
html_code += card_funnel_transposed.to_html(index=False,escape=False,  )
html_code += """</p>"""

# COMMAND ----------

# MAGIC %md
# MAGIC ## Netbanking funnel

# COMMAND ----------

nb_funnel_query_db = sqlContext.sql(
"""
SELECT
cx_experiment_fact.flag as exp_flag,
COUNT(DISTINCT CASE WHEN (checkout_fact_emr.payment_screen_loaded  = 1) THEN checkout_fact_emr.checkout_id ELSE NULL END) AS payment_screen_loaded,
 COUNT(DISTINCT CASE WHEN (checkout_fact_emr.netbanking_shown  = 1) THEN checkout_fact_emr.checkout_id ELSE NULL END) AS netbanking_shown,
    COUNT(DISTINCT CASE WHEN (checkout_fact_emr.netbanking_selected  = 1) THEN checkout_fact_emr.checkout_id ELSE NULL END) AS netbanking_selected,
    COUNT(DISTINCT CASE WHEN (checkout_fact_emr.netbanking_submit  = 1) THEN checkout_fact_emr.checkout_id ELSE NULL END) AS netbanking_submit,
    COUNT(DISTINCT CASE WHEN (checkout_fact_emr.netbanking_payment_init  = 1) THEN checkout_fact_emr.checkout_id ELSE NULL END) AS netbanking_payment_init,
    COUNT(DISTINCT CASE WHEN (checkout_fact_emr.netbanking_payment_complete  = 1) THEN checkout_fact_emr.checkout_id ELSE NULL END) AS netbanking_payment_complete
  FROM analytics_selfserve.checkout_fact_emr   AS checkout_fact_emr
LEFT JOIN aggregate_pa.cx_lo_fact_ism_v1   AS cx_checkout_l0_fact ON checkout_fact_emr.checkout_id = cx_checkout_l0_fact.checkout_id
Left join (
       select * from
       aggregate_pa.cx_experiment_fact 
       where producer_created_date BETWEEN date('{0}') and date('{1}') 
    and exp_name = 'bhim_intent_mweb' )AS cx_experiment_fact on checkout_fact_emr.checkout_id = cx_experiment_fact.checkout_id
WHERE checkout_fact_emr.producer_created_date between '{0}' and '{1}'
 AND cx_checkout_l0_fact.producer_created_date between '{0}' and '{1}'
  --And get_json_object(render_properties,'$.data.meta.v2_result')='v2'
   AND cx_checkout_l0_fact.merchant_id in {2}
   AND cx_experiment_fact.flag <> 'None'
   group by 1
   """.format(start_date, end_date,merchant_id_list)
)
nb_funnel_query_df = nb_funnel_query_db.toPandas()
nb_funnel_query_df.head()

# COMMAND ----------

nb_funnel_transposed = nb_funnel_query_df.transpose().reset_index()
nb_funnel_transposed

# COMMAND ----------

nb_funnel_transposed = nb_funnel_query_df.transpose().reset_index()

nb_funnel_transposed.columns = list(nb_funnel_transposed.loc[0,:])
nb_funnel_transposed = nb_funnel_transposed[2:]
nb_funnel_transposed

# COMMAND ----------

# Calculate percentages based on the maximum values in each column
nb_funnel_transposed['variant %'] = nb_funnel_transposed['variant_on'] / nb_funnel_transposed['variant_on'].max()
nb_funnel_transposed['variant %'] = nb_funnel_transposed[['variant %']].applymap(percentage_conversion)

nb_funnel_transposed['control %'] = nb_funnel_transposed['control'] / nb_funnel_transposed['control'].max()
nb_funnel_transposed['control %'] = nb_funnel_transposed[['control %']].applymap(percentage_conversion)

nb_funnel_transposed

# COMMAND ----------

html_code += """<p><h3>Netbanking Funnel:</h3> 
"""
html_code += nb_funnel_transposed.to_html(index=False,escape=False,  )
html_code += """</p>"""

# COMMAND ----------

# MAGIC %md
# MAGIC ## Appendix
# MAGIC

# COMMAND ----------

html_code += """
<h2> Appendix </h2>
<h3>Unfiltered Complete data:</h3>
<h4> This table displays coverage at a transaction level. The data from this table is not filtered for eligibility or whether the merchant is on experimentation or not. The data above accounts for those factors. </h4>
 <br/>"""
html_code += overall_df.to_html(index=False,escape=False,  )

# COMMAND ----------

html_code += """<p><h3>CR to SR: </h3> 
<h4> Payment Initiated Checkout IDs to Submit [submit_to_pa_checkout_id] should ideally be 100%. If >100% then we have instrumentation gaps where submits are not being registered and our Modal CR is underestimated. If < 100% then either the payments are not getting initiated or we have a bug in our instrumentation. </h4>

<br/>"""
html_code += (submit_to_pa_pivot_df).to_html(index=False,escape=False,  )
html_code += """</p>"""

# COMMAND ----------

# MAGIC %md
# MAGIC #RCA

# COMMAND ----------

# MAGIC %md
# MAGIC ## Email Code

# COMMAND ----------

user = 'aakash.bhattacharjee@razorpay.com'
app_password = 'fupdltkwunoytfdg' # Guide for app passwords: https://support.google.com/accounts/answer/185833?hl=en
host = 'smtp.gmail.com'
port = 465
server = smtplib.SMTP_SSL(host, port) 
server.login(user, app_password)

# COMMAND ----------

html_code += """
</body>
</html>
"""
email_id = ['aravinth.pk@razorpay.com','pallavi.samodia@razorpay.com','revanth.m@razorpay.com','pranav.gupta@razorpay.com','kunal.vishnoi@razorpay.com', 'aakash.bhattacharjee@razorpay.com', 'chandresh.gupta@razorpay.com','manjeet.singh@razorpay.com']

#email_id = ['aakash.bhattacharjee@razorpay.com','pallavi.samodia@razorpay.com']
# 
#for i in email_id:
#email_id=['pallavi.samodia@razorpay.com','chandresh.gupta@razorpay.com','aakash.bhattacharjee@razorpay.com','manjeet.singh@razorpay.com',]
send_emails(start_date,end_date,email_id,html_code)

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
# MAGIC select properties
# MAGIC FROM
# MAGIC   events.lumberjack_intermediate
# MAGIC WHERE
# MAGIC   event_name = 'metric:exit_intent'
# MAGIC   AND producer_created_date = '2024-08-26'
# MAGIC   limit 10

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

# MAGIC %sql
# MAGIC select producer_created_date,
# MAGIC --checkout_render_complete,
# MAGIC count(distinct checkout_id)
# MAGIC from analytics_selfserve.checkout_fact_emr 
# MAGIC where producer_created_date >= date('2024-09-12')
# MAGIC group by 1

# COMMAND ----------

# MAGIC %sql
# MAGIC  SELECT
# MAGIC   ---  a.checkout_id,
# MAGIC   ---  a.submit_checkout_id,
# MAGIC  ---   a.merchant_id,
# MAGIC   ---  producer_created_date,
# MAGIC   ---(ramp_up_date),
# MAGIC     (
# MAGIC       get_json_object(render_properties, '$.data.meta.v2_result')
# MAGIC     ) AS v2_result,
# MAGIC    (get_json_object(render_properties,'$.data.meta.v2_eligible')) as v2_eligible,
# MAGIC   platform,
# MAGIC  select_method,
# MAGIC  select_section,
# MAGIC  count(distinct checkout_id) as renders,
# MAGIC   count(distinct submit_checkout_id) as submits
# MAGIC
# MAGIC   
# MAGIC   FROM
# MAGIC     aggregate_pa.cx_lo_fact_ism_v1 a
# MAGIC  --- inner join batch_sheets.checkout_v2_rampup_sheet b on a.merchant_id = b.merchant_id
# MAGIC   WHERE
# MAGIC     producer_created_date BETWEEN ('2024-08-01') and ('2024-08-31') 
# MAGIC     group by 1,2,3,4,5

# COMMAND ----------

# MAGIC %sql
# MAGIC with payments as (
# MAGIC select p.id, p.method, p.authorized_at, checkout_id from realtime_hudi_api.payments p
# MAGIC   inner join (select checkout_id, payment_id
# MAGIC               from realtime_hudi_api.payment_analytics 
# MAGIC               where created_date between '2024-08-01' and '2024-08-31'
# MAGIC               )pa on p.id = pa.payment_id
# MAGIC   where p.created_date between '2024-08-01' and '2024-08-31'
# MAGIC
# MAGIC )
# MAGIC SELECT
# MAGIC   ---  a.checkout_id,
# MAGIC   ---  a.submit_checkout_id,
# MAGIC  ---   a.merchant_id,
# MAGIC   ---  producer_created_date,
# MAGIC   ---(ramp_up_date),
# MAGIC     (
# MAGIC       get_json_object(render_properties, '$.data.meta.v2_result')
# MAGIC     ) AS v2_result,
# MAGIC    (get_json_object(render_properties,'$.data.meta.v2_eligible')) as v2_eligible,
# MAGIC   platform,
# MAGIC
# MAGIC  select_section,
# MAGIC    select_method,
# MAGIC --browser_name,
# MAGIC --integration_type,
# MAGIC --library,
# MAGIC  count(distinct a.checkout_id) as renders,
# MAGIC count(distinct payments.checkout_id) as attempts,
# MAGIC  
# MAGIC count(distinct case when method='cod' or authorized_at is not null then payments.checkout_id else null end) as success
# MAGIC   
# MAGIC   FROM
# MAGIC     aggregate_pa.cx_lo_fact_ism_v1 a
# MAGIC     left join payments on a.checkout_id = payments.checkout_id
# MAGIC  --- inner join batch_sheets.checkout_v2_rampup_sheet b on a.merchant_id = b.merchant_id
# MAGIC   WHERE
# MAGIC     producer_created_date BETWEEN '2024-08-01' and '2024-08-31'
# MAGIC ---    and merchant_id = 'IwykjRFidgt5Ok'
# MAGIC     group by 1,2,3,4,5

# COMMAND ----------

# MAGIC %sql
# MAGIC select submit_properties,
# MAGIC render_properties,
# MAGIC get_json_object(submit_properties, '$.data.data.upi_app') as upi_app,
# MAGIC get_json_object(submit_properties, '$.data.instrument_name') as upi_app2,
# MAGIC  get_json_object(submit_properties, '$.data.meta.v2_result') as v2_result
# MAGIC
# MAGIC from aggregate_pa.cx_lo_fact_ism_v1
# MAGIC where producer_created_date >= '2024-10-01'
# MAGIC and submit_method='upi'
# MAGIC ---and  get_json_object(render_properties, '$.data.meta.v2_result') ='v2'
# MAGIC limit 10

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dbt_prod_success_factors.employees
# MAGIC where 
# MAGIC date(date_of_exit)> CURRENT_DATE
# MAGIC --and department = 'Product Org'
# MAGIC and (lower(sub_function) like '%analyti%'
# MAGIC or lower(sub_function) like '%product%'
# MAGIC or lower(sub_function) like '%data solutions%'
# MAGIC )
# MAGIC limit 500

# COMMAND ----------


