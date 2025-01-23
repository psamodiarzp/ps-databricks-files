# Databricks notebook source
# MAGIC %md
# MAGIC #Initial Package downloads

# COMMAND ----------

import numpy as np
import pandas as pd

# COMMAND ----------

spark.sql("REFRESH TABLE aggregate_pa.magic_checkout_fact")


# COMMAND ----------

def percentage_conversion(value):
    return f'{value:.2%}'

# COMMAND ----------

def formatINR(number):
    s, *d = str(number).partition(".")
    r = ",".join([s[x-2:x] for x in range(-3, -len(s), -2)][::-1] + [s[-3:]])
    return r

# COMMAND ----------

from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.header import Header
import smtplib
import os

# COMMAND ----------

from datetime import datetime, timedelta
from datetime import date

# COMMAND ----------

# MAGIC %md
# MAGIC #Date & MID Inputs
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

start_date = '2024-11-01'
end_date = '2024-11-06'
mid = "('JUHfXse0FDfnru')"

# COMMAND ----------

# MAGIC %md
# MAGIC ## HTML Code base for Email

# COMMAND ----------

def send_emails(start_date, end_date,email_id_list,html_t, merchant_id):
    today = datetime.now().strftime('%Y-%m-%d')
    subject = 'Magic Checkout V2 vs V1 report for MID {2} - {0} to {1}'.format(start_date, end_date,merchant_id)
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
    <title>Magic Checkout V2 vs V1 report for MID: {2} - {0} to {1} </title>
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
<h2> All the data in this sheet is SESSION-LEVEL </h2>
""".format(start_date,end_date,mid)

print(html_code)

# COMMAND ----------

# MAGIC %md
# MAGIC #SQL code for data
# MAGIC

# COMMAND ----------

raw_data_db = sqlContext.sql(
    """
WITH cte AS(
  SELECT
    a.session_id,
    a.checkout_id,
 --- max(ramp_up_date),
    max(
      get_json_object(properties, '$.data.meta.v2_result')
    ) AS v2_result,
   max(
      get_json_object(properties, '$.magicExperiments.checkout_redesign')
    ) AS experiment_result,
   max(get_json_object(properties,'$.data.meta.v2_eligible')) as v2_eligible
  
  FROM
    aggregate_pa.cx_1cc_events_dump_v1 a
 --- inner join batch_sheets.checkout_v2_rampup_sheet b on a.merchant_id = b.merchant_id
  WHERE
    producer_created_date BETWEEN date('{0}') and date('{1}') 
 and merchant_id in {2}                                                           --- TEMPORARY DELETE
 -- and producer_created_date > date(ramp_up_date)
 -- and rampup_100_percent_flag = 'false'
 --- and blacklisted_flag = 'false'
  GROUP BY
    1,2
),

submits as(

select 
distinct
   checkout_id
   /*
   coalesce(get_json_object (properties,'$.data.data.upi_app'),
                split_part(try_cast(get_json_object(properties,'$.data.data.vpa')as string),'@',2), /* collect VPA handle */
                get_json_object (properties,'$.data.provider'),
                get_json_object (properties,'$.data.bank'),
                get_json_object (properties,'$.data.wallet')) as submit_instrument,
  
  coalesce(
  get_json_object(properties,'$.data.method'),
    get_json_object(properties,'$.data.data.method')
  ) as method
  */
    FROM
    aggregate_pa.cx_1cc_events_dump_v1 a
  WHERE
    ((producer_created_date BETWEEN date('{0}')  and date('{1}')) 
    )
    --- temporary code 
  and event_name = 'submit'
 and merchant_id in {2}                                                            --- TEMPORARY DELETE



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
  internal_error_code,
  order_id

from realtime_hudi_api.payments 
where created_date between ('{0}') and ('{1}') 
 and merchant_id in {2}                                                             --- TEMPORARY DELETE
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
  inner join (
   select order_id from realtime_hudi_api.order_meta
    WHERE created_date between ('{0}') and ('{1}') 
  )om on p.order_id = om.order_id
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
  
    
)
select 
cte.checkout_id as cid,
cte.session_id as checkout_id,
submits.checkout_id as submit_checkout_id,
merchant_id,
open,
summary_screen_loaded,
summary_screen_continue_cta_clicked,
payment_home_screen_loaded,
submit, 
browser_major_version,
browser_name,
os_brand_family,
os_major_version,
mcf.producer_created_date,
mcf.platform,
cte.experiment_result,
cte.v2_result,
cte.v2_eligible,
--submit_instrument,
--submits.method as checkout_method,
cte2.method as payment_method,
id as payment_attempts,
case when authorized_at is not null or cte2.method='cod' then id else null end as payment_success,
case when id is not null then cte.session_id else null end as pa_session_id,
case when authorized_at is not null or cte2.method='cod' then cte.session_id else null end as ps_session_id,
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
left join submits on cte.checkout_id = submits.checkout_id
left join aggregate_pa.magic_checkout_fact mcf on cte.checkout_id = mcf.checkout_id
where mcf.producer_created_date between date('{0}') and date('{1}') 
and mcf.library <> 'magic-x' or mcf.library is null
and mcf.checkout_integration_type <> 'x' or mcf.checkout_integration_type is null
""".format(
        start_date, end_date, mid
    )
)

raw_data_df = raw_data_db.toPandas()
raw_data_df.head()

# COMMAND ----------

merchants_db = sqlContext.sql("""
                             select id as merchant_id, website
                             from realtime_hudi_api.merchants
                             where id in {0}
                             """.format(mid))
merchants_df = merchants_db.toPandas()
merchants_df.head()

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC #Saving the file as csv for later
# MAGIC - If you need to use the data source but not run the above code again, please utilize below (especially using analytics cluster)

# COMMAND ----------

#raw_data_df.to_csv('/dbfs/FileStore/magic_raw_data_upi_{start_date}_{end_date}_df.csv', index=False)
"https://razorpay-dev.cloud.databricks.com/files/magic_raw_data_upi_{0}_{1}_df.csv".format(start_date, end_date)

# COMMAND ----------

#raw_data_df = pd.read_csv('/dbfs/FileStore/magic_raw_data_upi_{start_date}_{end_date}_df.csv')
raw_data_df.shape

# COMMAND ----------

# MAGIC %md
# MAGIC #Overall Split Check
# MAGIC - to check here: V1 vs V2 split under eligible should be 95:5 or as expected
# MAGIC - % of NAs should be minimal

# COMMAND ----------



# COMMAND ----------




overall_df = raw_data_df.groupby(by = ['v2_result','v2_eligible'], dropna=False).agg(
    { 'checkout_id':'nunique',
        'merchant_id':'nunique',
        'submit_checkout_id':'nunique',
        'payment_attempts':'nunique',
     'payment_success':'nunique',
    
     }
).reset_index()
overall_df['split_percentage'] = overall_df['checkout_id']*1.0 / overall_df['checkout_id'].sum()
overall_df['modal_cr'] = overall_df['submit_checkout_id']*1.0 / overall_df['checkout_id']
overall_df['success_rate'] = overall_df['payment_success']*1.0 / overall_df['payment_attempts']
overall_df['overall_cr'] = overall_df['payment_success']*1.0 / overall_df['checkout_id']
overall_df[overall_df.columns[2:7]] = overall_df[overall_df.columns[2:7]].applymap(formatINR)
overall_df[overall_df.columns[7:]] = overall_df[overall_df.columns[7:]].applymap(percentage_conversion)
#grouped_df.pivot(  = 'v2_result', values = ['checkout_id','payment_attempts','payment_success','success_rate','overall_cr']).reset_index()
overall_df

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ### Checking MID split % + Data Clean Up

# COMMAND ----------



# COMMAND ----------

eligibility_condition = (
    (raw_data_df['v2_eligible'] == True) | 
    (raw_data_df['v2_eligible'] == 'true')
) 

# COMMAND ----------

grouped_df = raw_data_df[eligibility_condition].groupby(by = ['v2_result','merchant_id'], dropna=False).agg(
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

excluded_mids = mid_df[(mid_df.loc[:, pd.IndexSlice['checkout_id_split', 'v1']]==100.0) | (mid_df.loc[:, pd.IndexSlice['checkout_id_split', 'v1']]==0.0)  ].reset_index()
excluded_mids[['merchant_id','checkout_id','Total']].head(20)

# COMMAND ----------

final_df = raw_data_df[raw_data_df['merchant_id'].isin(final_mid_list)].reset_index(drop=True)
final_df

# COMMAND ----------

#final_df.to_csv('/dbfs/FileStore/magic_final_df.csv', index=False)
"https://razorpay-dev.cloud.databricks.com/files/magic_final_df.csv".format(start_date, end_date)

# COMMAND ----------

saved_start_date = start_date #Change this if you want to extract file for another time
saved_end_date = end_date #Change this if you want to extract file for another time
#final_df = pd.read_csv('/dbfs/FileStore/magic_final_{start_date}_{end_date}_df.csv')
final_df.shape

# COMMAND ----------

#final_df = pd.read_csv('/dbfs/FileStore/magic_final_df.csv')
final_df.shape

# COMMAND ----------

final_df.head(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ### temp (open to summary load)

# COMMAND ----------

open_to_summary_df = final_df[(final_df['open']==1) & (final_df['summary_screen_loaded']==0)]
open_to_summary_mid = open_to_summary_df.groupby(by='merchant_id')['submit_checkout_id'].nunique()
open_to_summary_mid.sort_values(ascending=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conditions
# MAGIC all analysis here on will be only for cases where V2 eligible = True + any other conditions specified hereby
# MAGIC

# COMMAND ----------

#write an either/or
conditions = ((final_df['v2_eligible']=='true') |(final_df['v2_eligible']==True)) 

# COMMAND ----------

final_df.head()

# COMMAND ----------

# MAGIC %md
# MAGIC #L0 Metrics 
# MAGIC

# COMMAND ----------

final_l0_df = final_df[conditions].groupby(by = ['v2_result',], dropna=False).agg(
    { 'checkout_id':'nunique',
     'merchant_id':'nunique',
     'submit_checkout_id':'nunique',
        'payment_attempts':'nunique',
     'payment_success':'nunique',
    'pa_session_id':'nunique',
    'ps_session_id':'nunique',
     }
).reset_index()
final_l0_df['split_percentage'] = final_l0_df['checkout_id']*1.0 / final_l0_df['checkout_id'].sum()
final_l0_df['modal_cr'] = final_l0_df['submit_checkout_id']*1.0 / final_l0_df['checkout_id']
final_l0_df['success_rate'] = final_l0_df['payment_success']*1.0 / final_l0_df['payment_attempts']
final_l0_df['overall_cr'] = final_l0_df['payment_success']*1.0 / final_l0_df['checkout_id']
final_l0_df_copy = final_l0_df.copy()
final_l0_df[final_l0_df.columns[1:8]] = final_l0_df[final_l0_df.columns[1:8]].applymap(formatINR)
final_l0_df[final_l0_df.columns[8:]] = final_l0_df[final_l0_df.columns[8:]].applymap(percentage_conversion)
#grouped_df.pivot(  = 'v2_result', values = ['checkout_id','payment_attempts','payment_success','success_rate','overall_cr']).reset_index()

final_l0_df

# COMMAND ----------

html_code += """<p><h3>L0 Metrics for only Merchants who are on experiment:</h3> <br/>"""
html_code += final_l0_df.to_html(index=False,escape=False,  )
html_code += """</p>"""

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
    
     }
).reset_index()
grouped_df['split_percentage'] = grouped_df['checkout_id']*1.0 / grouped_df['checkout_id'].sum()
grouped_df['modal_cr'] = grouped_df['submit_checkout_id']*1.0 / grouped_df['checkout_id']
grouped_df['overall_cr'] = grouped_df['payment_success']*1.0 / grouped_df['checkout_id']
grouped_df['success_rate'] = grouped_df['payment_success']*1.0 / grouped_df['payment_attempts']
pivoted_df = grouped_df.pivot( index='producer_created_date' ,columns= 'v2_result', values = ['checkout_id','payment_attempts','payment_success','modal_cr','success_rate','overall_cr']).sort_values(by = 'producer_created_date',).reset_index()
pivoted_df['modal_cr_diff'] = pivoted_df['modal_cr']['v2'] - pivoted_df['modal_cr']['v1']
pivoted_df['success_rate_diff'] = pivoted_df['success_rate']['v2'] - pivoted_df['success_rate']['v1']
pivoted_df['overall_cr_diff'] = pivoted_df['overall_cr']['v2'] - pivoted_df['overall_cr']['v1']
pivoted_df[pivoted_df.columns[1:7]] = pivoted_df[pivoted_df.columns[1:7]].applymap(formatINR)
pivoted_df[pivoted_df.columns[7:]] = pivoted_df[pivoted_df.columns[7:]].applymap(percentage_conversion)
pivoted_df

# COMMAND ----------

html_code += """<p><h3>L0 Metrics - Date Wise:</h3> <br/>"""
html_code += pivoted_df.to_html(index=False,escape=False,  )
html_code += """</p>"""

# COMMAND ----------

pivoted_df.to_csv('/dbfs/FileStore/v_imp_data_magic_df.csv', index=False)
"https://razorpay-dev.cloud.databricks.com/files/v_imp_data_magic_df.csv".format(start_date, end_date)

# COMMAND ----------

# MAGIC %md
# MAGIC ## MID-wise L0 metrics

# COMMAND ----------

grouped_df = final_df[conditions].groupby(by = ['v2_result','merchant_id'], dropna=False).agg(
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
grouped_df = grouped_df.merge(merchants_df, on='merchant_id', how='left')
pivoted_df = grouped_df.pivot( index=['merchant_id','website'] ,columns= 'v2_result', values = ['checkout_id','modal_cr','success_rate','overall_cr']).sort_values(by = 'merchant_id',).reset_index()

pivoted_df['Total'] = pivoted_df[pivoted_df.columns[2:4]].sum(axis=1)
pivoted_df = pivoted_df.sort_values(by = 'Total', ascending=False).drop(columns=['Total'])
pivoted_df['modal_cr_diff'] = pivoted_df['modal_cr']['v2'] - pivoted_df['modal_cr']['v1']
pivoted_df['success_rate_diff'] = pivoted_df['success_rate']['v2'] - pivoted_df['success_rate']['v1']
pivoted_df['overall_cr_diff'] = pivoted_df['overall_cr']['v2'] - pivoted_df['overall_cr']['v1']
pivoted_df[pivoted_df.columns[2:4]] = pivoted_df[pivoted_df.columns[2:4]].applymap(formatINR)
pivoted_df[pivoted_df.columns[4:]] = pivoted_df[pivoted_df.columns[4:]].applymap(percentage_conversion)
pivoted_df.head(20)

# COMMAND ----------

html_code += """<p><h3>L0 Metrics - Merchant wise :</h3> <br/>
<h4>Top 20 merchants on experiment by volume :</h4> <br/>"""
html_code += (pivoted_df.head(20)).to_html(index=False,escape=False,  )
html_code += """</p>"""

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #Browser-wise L0 metrics

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
pivoted_df['modal_cr_diff'] = pivoted_df['modal_cr']['v2'] - pivoted_df['modal_cr']['v1']
pivoted_df['success_rate_diff'] = pivoted_df['success_rate']['v2'] - pivoted_df['success_rate']['v1']
pivoted_df['overall_cr_diff'] = pivoted_df['overall_cr']['v2'] - pivoted_df['overall_cr']['v1']
pivoted_df[pivoted_df.columns[1:3]] = pivoted_df[pivoted_df.columns[1:3]].applymap(formatINR)
pivoted_df[pivoted_df.columns[3:]] = pivoted_df[pivoted_df.columns[3:]].applymap(percentage_conversion)
pivoted_df.head(20)

# COMMAND ----------

html_code += """<p><h3>L0 Metrics - Browser wise :</h3> <br/>
"""
html_code += (pivoted_df.head(20)).to_html(index=False,escape=False,  )
html_code += """</p>"""

# COMMAND ----------

# MAGIC %md
# MAGIC # Funnel
# MAGIC This will present a funnel comparison only for merchants who are selected after cleanup above

# COMMAND ----------

# MAGIC %md
# MAGIC ### High Level Funnel

# COMMAND ----------

# Distinct checkout ID list
merchant_id_list = final_df[conditions]['merchant_id'].unique()
merchant_id_list = "', '".join(merchant_id_list)
merchant_id_list = f"('{merchant_id_list}')"


# COMMAND ----------

funnel_query_db = sqlContext.sql(
    """
    Select

      get_json_object(properties, '$.data.meta.v2_result')
AS v2_result,
   (get_json_object(properties,'$.data.meta.v2_eligible')) as v2_eligible,
(CASE
     -- WHEN event_name='open' THEN '0. Open'
     WHEN event_name='render:1cc_summary_screen_loading_initiated' THEN '1.0 Summary Screen Loading Initiated (Screen shown to user)'
     WHEN event_name='render:complete' THEN '1.01 Render Complete'
      WHEN event_name='render:1cc_summary_screen_loaded_completed' THEN '1.1 Summary Screen Loading Completed'
      WHEN event_name='behav:1cc_summary_screen_edit_address_clicked' THEN '1.2 Edit Saved Address from Summary Screen'
      WHEN event_name='behav:1cc_summary_screen_continue_cta_clicked' THEN '1.3 Summary Screen Continue CTA Clicked'
      WHEN ((event_name = 'render:1cc_rzp_otp_screen_loaded') AND ((get_json_object(properties,'$.data.otp_reason')) LIKE ('%access_address%') OR (get_json_object(properties,'$.data.otp_reason')) LIKE ('%mandatory_login%'))) THEN '2.1 Access Address OTP Load'
      WHEN (((event_name='behav:1cc_rzp_otp_submitted')   OR (event_name='behav:1cc_rzp_otp_skip_clicked')) AND ((get_json_object(properties,'$.data.otp_reason')) like ('%access_address%') OR (get_json_object(properties,'$.data.otp_reason')) like ('%mandatory_login%'))) THEN '2.2 Access Address OTP Submit/Skipped'
      WHEN event_name='render:1cc_saved_shipping_address_screen_loaded' THEN '3.1 Saved Address Screen Loaded'
      WHEN event_name='checkoutaddnewaddressctaclicked' THEN '3.2 Add New Address from Saved Screen'
      WHEN event_name='behav:1cc_saved_address_screen_continue_cta_clicked' THEN '3.3 Saved Address Screen Continue CTA Clicked'
      WHEN event_name='render:1cc_add_new_address_screen_loaded_completed' THEN '4.1 Add New Address Screen Loaded'
      WHEN event_name='behav:1cc_add_new_address_screen_continue_cta_clicked' THEN '4.2 Add New Address Continue CTA Clicked'
      WHEN ((event_name = 'render:1cc_rzp_otp_screen_loaded') AND ((get_json_object(properties,'$.data.otp_reason')) LIKE ('%save_address%'))) THEN '5.1 Save Address OTP Load'
      WHEN (((event_name='behav:1cc_rzp_otp_submitted') OR (event_name='behav:1cc_rzp_otp_skip_clicked')) AND ((get_json_object(properties,'$.data.otp_reason')) LIKE ('%save_address%'))) THEN '5.2 Save Address OTP Submit/Skipped'
      WHEN event_name='render:1cc_payment_home_screen_loaded' THEN '6.1 Payment Home Screen L0 Loaded'
      WHEN event_name='behav:1cc_payment_home_screen_method_selected' or event_name = 'method:selected' THEN '6.2 Payment Method selected on Payment L0 Screen'
      WHEN event_name='behav:1cc_confirm_order_summary_submitted' OR event_name='submit' THEN '6.3 Payment Submitted'
      ELSE 'NA' END) as funnel_steps,
      count(distinct session_id) as cnt_checkout_id
      from aggregate_pa.cx_1cc_events_dump_v1 a
  ---    inner join batch_sheets.checkout_v2_rampup_sheet b on a.merchant_id = b.merchant_id
  WHERE
    producer_created_date BETWEEN date('{0}') and date('{1}') 
 --- and producer_created_date > date(ramp_up_date)
  and a.merchant_id in {2}
      group by 1,2,3
      order by 1
    """.format(start_date, end_date, merchant_id_list)
)
funnel_query_df = funnel_query_db.toPandas()
funnel_query_df.head()

# COMMAND ----------

#funnel_query_df = pd.read_csv('/dbfs/FileStore/magic_funnel.csv')
funnel_query_df

# COMMAND ----------

final_l0_df_copy

# COMMAND ----------

funnel_query_df = funnel_query_df[~funnel_query_df['funnel_steps'].isin(['0. Open','NA'])]
funnel_query_df
open_rows = pd.DataFrame({'v2_result': final_l0_df_copy['v2_result'],
                         'v2_eligible': True,
                         'funnel_steps': '0. Open',
                         'cnt_checkout_id': final_l0_df_copy['checkout_id']})
pa_rows = pd.DataFrame({'v2_result': final_l0_df_copy['v2_result'],
                         'v2_eligible': True,
                         'funnel_steps': '7.1 payment_attempts',
                         'cnt_checkout_id': final_l0_df_copy['pa_session_id']})    
ps_rows = pd.DataFrame({'v2_result': final_l0_df_copy['v2_result'],
                         'v2_eligible': True,
                         'funnel_steps': '7.2 payment_success',
                         'cnt_checkout_id': final_l0_df_copy['ps_session_id']})                                                  
funnel_final_df = pd.concat([funnel_query_df, open_rows, pa_rows, ps_rows], ignore_index=True)
funnel_final_df

# COMMAND ----------

funnel_pivot_df = funnel_final_df[((funnel_final_df['v2_eligible']==True)| (funnel_final_df['v2_eligible']=='true')) & (~funnel_final_df['funnel_steps'].isnull())][~funnel_final_df['funnel_steps'].isin(['NA'])].pivot(index='funnel_steps', columns=['v2_result'], values='cnt_checkout_id').reset_index()
funnel_pivot_df['v1 %'] = funnel_pivot_df['v1'].div(funnel_pivot_df['v1'].iloc[0])
funnel_pivot_df['v2 %'] = funnel_pivot_df['v2'].div(funnel_pivot_df['v2'].iloc[0])
funnel_pivot_df['diff'] = funnel_pivot_df['v2 %'] - funnel_pivot_df['v1 %']
funnel_pivot_df.iloc[:,-3:] = funnel_pivot_df.iloc[:,-3:].applymap(percentage_conversion)
funnel_pivot_df

# COMMAND ----------

funnel_final_df[((funnel_final_df['v2_eligible']==True)| (funnel_final_df['v2_eligible']=='true'))]

# COMMAND ----------

html_code += """<p>
<h2> Funnels </h2>
<h3>[Corrected] V2 High Level Funnel:</h3> <br/>"""
html_code += funnel_pivot_df.to_html(index=False,escape=False,  )
html_code += """</p>"""

# COMMAND ----------

# MAGIC %md
# MAGIC ## New Address Screen funnel comparison

# COMMAND ----------

new_address_funnel_query_db = sqlContext.sql(
    """
    Select

      get_json_object(properties, '$.data.meta.v2_result') AS v2_result,
   get_json_object(properties,'$.data.meta.v2_eligible') as v2_eligible,
concat(event_name,' ',coalesce(
  get_json_object(properties,'$.data.serviceable'),
  get_json_object(properties,'$.data.response.addresses[0].serviceable'),
  ''
  )
) as event_name,
      count(distinct session_id) as cnt_checkout_id
      from aggregate_pa.cx_1cc_events_dump_v1 a
  ---    inner join batch_sheets.checkout_v2_rampup_sheet b on a.merchant_id = b.merchant_id
  WHERE
    producer_created_date BETWEEN date('{0}') and date('{1}') 
 --- and producer_created_date > date(ramp_up_date)
  and a.merchant_id in {2}
  and event_name in (
      'render:1cc_add_new_address_screen_loaded_completed',
      'behav:1cc_add_new_address_country_entered',
      'behav:1cc_add_new_address_pincode_entered',
     -- 'metric:checkoutshippinginfoapicallinitiated',
     --- 'metric:checkoutshippinginfoapicallcompleted',
      'metric:1cc_checkout_suggestions_api_initiated',
'metric:1cc_checkout_suggestions_api_completed',
'behav:1cc_add_new_address_state_entered',
'behav:1cc_add_new_address_name_entered',
'behav:1cc_add_new_address_city_entered',

'behav:1cc_add_new_address_line1_entered',
'behav:1cc_add_new_address_line2_entered',

'behav:1cc_add_new_address_screen_continue_cta_clicked'

  )
      group by 1,2,3
      order by 1
    """.format(start_date, end_date, merchant_id_list)
)
new_address_funnel_query_df = new_address_funnel_query_db.toPandas()
new_address_funnel_query_df

# COMMAND ----------

new_address_funnel_query_df['event_name'].unique()

# COMMAND ----------

address_mapping = {
    'render:1cc_add_new_address_screen_loaded_completed ': '1.0 render:1cc_add_new_address_screen_loaded_completed',
     'behav:1cc_add_new_address_country_entered ': '1.01 behav:1cc_add_new_address_country_entered', 
     'behav:1cc_add_new_address_pincode_entered ': '1.02 behav:1cc_add_new_address_pincode_entered',
      'behav:1cc_add_new_address_state_entered ': '1.03 behav:1cc_add_new_address_state_entered',
      'behav:1cc_add_new_address_city_entered ': '1.04 behav:1cc_add_new_address_city_entered', 
      'behav:1cc_add_new_address_name_entered ': '1.05 behav:1cc_add_new_address_name_entered',
      'behav:1cc_add_new_address_line1_entered ': '1.06 behav:1cc_add_new_address_line1_entered',
'behav:1cc_add_new_address_line2_entered ':'1.07 behav:1cc_add_new_address_line2_entered',
'metric:1cc_checkout_suggestions_api_initiated ': '1.08 metric:1cc_checkout_suggestions_api_initiated',
     'metric:1cc_checkout_suggestions_api_completed ':'1.09 metric:1cc_checkout_suggestions_api_completed',
'behav:1cc_add_new_address_screen_continue_cta_clicked ': '1.1 behav:1cc_add_new_address_screen_continue_cta_clicked'
                   }
new_address_funnel_query_df['address_events'] = new_address_funnel_query_df['event_name'].map(address_mapping)

# COMMAND ----------

new_address_funnel_query_df

# COMMAND ----------

new_address_funnel_pivot_df = new_address_funnel_query_df[(new_address_funnel_query_df['v2_eligible']=='true') & (~new_address_funnel_query_df['address_events'].isnull())].pivot(index='address_events', columns=['v2_result'], values='cnt_checkout_id').reset_index()
new_address_funnel_pivot_df.sort_values(by='address_events', inplace=True)
#new_address_funnel_pivot_df['v1 %'] = new_address_funnel_pivot_df['v1'].div(new_address_funnel_pivot_df['v1'].iloc[0])
new_address_funnel_pivot_df['v2 %'] = new_address_funnel_pivot_df['v2'].div(new_address_funnel_pivot_df['v2'].iloc[0])
#new_address_funnel_pivot_df['diff'] = new_address_funnel_pivot_df['v2 %'] - new_address_funnel_pivot_df['v1 %']
#new_address_funnel_pivot_df.iloc[:,-3:] = new_address_funnel_pivot_df.iloc[:,-3:].applymap(percentage_conversion)
new_address_funnel_pivot_df

# COMMAND ----------

new_address_funnel_pivot_df

# COMMAND ----------

html_code += """<p><h3>[Correct one] New Address Funnel:</h3> <br/>"""
html_code += new_address_funnel_pivot_df.to_html(index=False,escape=False,  )
html_code += """</p>"""

# COMMAND ----------

# MAGIC %md
# MAGIC ## V2 Payments Screen Funnel

# COMMAND ----------

payment_screen_funnel_query_db = sqlContext.sql(
    """  
WITH events_data AS (
    SELECT
   --     get_json_object(properties, '$.data.meta.v2_result') AS v2_result,
 --  get_json_object(properties,'$.data.meta.v2_eligible') as v2_eligible,
        event_name,
        coalesce(
            get_json_object(properties, '$.data.payment_method'), 
            get_json_object(properties, '$.data.method'), 
            get_json_object(properties, '$.data.data.method'), 
            get_json_object(properties, '$.data.method_name')
        ) AS payment_method,
        get_json_object(properties, '$.data.meta.v2_result') as v2_result,
      
      ---  CAST(json_extract(properties, '$.data.methods_shown') AS array(string)) AS methods_shown,
       session_id 
    FROM aggregate_pa.cx_1cc_events_dump_v1 a
    WHERE
        producer_created_date BETWEEN date('{0}') and date('{1}') 
        and a.merchant_id in {2}
     AND get_json_object(properties, '$.data.meta.v2_eligible') = 'true'
)
   ,
event_combinations AS (
    SELECT
        session_id,
    
     max(v2_result) as v2_result,
      --   max(v2_eligible) as v2_eligible,
        MAX(IF(event_name = 'render:1cc_payment_home_screen_loaded', 1, 0)) AS payment_screen_loaded,
        MAX(IF(event_name = 'render:1cc_cod_block' or lower(event_name)='checkoutcodoptionshown', 1, 0)) AS cod_shown,
        MAX(IF((event_name = 'method:selected' or event_name = 'behav:1cc_payment_home_screen_method_selected') AND payment_method = 'cod', 1, 0)) AS cod_selected,
      ---  MAX(IF(event_name = 'render:1cc_payment_cod_l1_screen_loads', 1, 0)) AS cod_L1_screen_shown,
        MAX(IF(event_name = 'submit' AND payment_method = 'cod', 1, 0)) AS cod_submit,

     ---   MAX(IF(contains(methods_shown, 'upi'), 1, 0)) AS upi_shown,
        MAX(IF((event_name = 'method:selected' or event_name = 'behav:1cc_payment_home_screen_method_selected')  AND payment_method = 'upi', 1, 0)) AS upi_selected,
      ---  MAX(IF(event_name = 'render:1cc_payment_l1_screen_loaded' AND payment_method = 'upi', 1, 0)) AS upi_l1_screen_shown,
        MAX(IF(event_name = 'submit' AND payment_method = 'upi', 1, 0)) AS upi_submit,

     ---   MAX(IF(contains(methods_shown, 'card'), 1, 0)) AS card_shown,
        MAX(IF((event_name = 'method:selected' or event_name = 'behav:1cc_payment_home_screen_method_selected')  AND payment_method = 'card', 1, 0)) AS card_selected,
      ---  MAX(IF(event_name = 'render:1cc_payment_l1_screen_loaded' AND payment_method = 'card', 1, 0)) AS card_l1_screen_shown,
        MAX(IF(event_name = 'submit' AND payment_method = 'card', 1, 0)) AS card_submit,

     ---   MAX(IF(contains(methods_shown, 'emi'), 1, 0)) AS emi_shown,
        MAX(IF((event_name = 'method:selected' or event_name = 'behav:1cc_payment_home_screen_method_selected')  AND payment_method = 'emi', 1, 0)) AS emi_selected,
      ---  MAX(IF(event_name = 'render:1cc_payment_l1_screen_loaded' AND payment_method = 'emi', 1, 0)) AS emi_l1_screen_shown,
        MAX(IF(event_name = 'submit' AND payment_method = 'emi', 1, 0)) AS emi_submit,

     ---   MAX(IF(contains(methods_shown, 'netbanking'), 1, 0)) AS netbanking_shown,
        MAX(IF((event_name = 'method:selected' or event_name = 'behav:1cc_payment_home_screen_method_selected')  AND payment_method = 'netbanking', 1, 0)) AS netbanking_selected,
      --  MAX(IF(event_name = 'render:1cc_payment_l1_screen_loaded' AND payment_method = 'netbanking', 1, 0)) AS netbanking_l1_screen_shown,
        MAX(IF(event_name = 'submit' AND payment_method = 'netbanking', 1, 0)) AS netbanking_submit,

        MAX(IF((event_name = 'method:selected' or event_name = 'behav:1cc_payment_home_screen_method_selected')  AND payment_method NOT IN ('cod', 'upi', 'card', 'emi', 'netbanking'), 1, 0)) AS other_prepaid_selected,
       --- MAX(IF(event_name = 'render:1cc_payment_l1_screen_loaded' AND payment_method NOT IN ('cod', 'upi', 'card', 'emi', 'netbanking'), 1, 0)) AS other_prepaid_l1_screen_shown,
        MAX(IF(event_name = 'submit' AND payment_method NOT IN ('cod', 'upi', 'card', 'emi', 'netbanking'), 1, 0)) AS other_prepaid_submit,
         MAX(IF(event_name = 'submit', 1, 0)) AS submit
    FROM events_data
    GROUP BY 1
)
SELECT
    v2_result,
 
   -- v2_eligible,
    COUNT(IF(payment_screen_loaded = 1, session_id, NULL)) AS payment_screen_loaded,
    
    COUNT(IF(payment_screen_loaded = 1 AND cod_shown = 1, session_id, NULL)) AS cod_shown,
    COUNT(IF(payment_screen_loaded = 1 AND cod_selected = 1, session_id, NULL)) AS cod_selected,
 ---   COUNT(IF(cod_L1_screen_shown = 1, checkout_id, NULL)) AS cod_L1_screen_shown,
    COUNT(IF(cod_submit = 1, session_id, NULL)) AS cod_submit,

  ---  COUNT(IF(payment_screen_loaded = 1 AND upi_shown = 1, checkout_id, NULL)) AS upi_shown,
    COUNT(IF(payment_screen_loaded = 1  AND upi_selected = 1, session_id, NULL)) AS upi_selected,
 ---   COUNT(IF(upi_l1_screen_shown = 1, checkout_id, NULL)) AS upi_l1_screen_shown,
    COUNT(IF(upi_submit = 1, session_id, NULL)) AS upi_submit,

  ---  COUNT(IF(payment_screen_loaded = 1 AND card_shown = 1, checkout_id, NULL)) AS card_shown,
    COUNT(IF(payment_screen_loaded = 1 AND card_selected = 1, session_id, NULL)) AS card_selected,
  ---  COUNT(IF(card_l1_screen_shown = 1, checkout_id, NULL)) AS card_l1_screen_shown,
    COUNT(IF(card_submit = 1, session_id, NULL)) AS card_submit,

  ---  COUNT(IF(payment_screen_loaded = 1 AND emi_shown = 1, checkout_id, NULL)) AS emi_shown,
    COUNT(IF(payment_screen_loaded = 1  AND emi_selected = 1, session_id, NULL)) AS emi_selected,
  ---  COUNT(IF(emi_l1_screen_shown = 1, checkout_id, NULL)) AS emi_l1_screen_shown,
    COUNT(IF(emi_submit = 1, session_id, NULL)) AS emi_submit,

  --  COUNT(IF(payment_screen_loaded = 1 AND netbanking_shown = 1, checkout_id, NULL)) AS netbanking_shown,
    COUNT(IF(payment_screen_loaded = 1 AND netbanking_selected = 1, session_id, NULL)) AS netbanking_selected,
 ---   COUNT(IF(netbanking_l1_screen_shown = 1, checkout_id, NULL)) AS netbanking_l1_screen_shown,
    COUNT(IF(netbanking_submit = 1, session_id, NULL)) AS netbanking_submit,

    COUNT(IF(other_prepaid_selected = 1, session_id, NULL)) AS other_prepaid_selected,
 ---   COUNT(IF(other_prepaid_l1_screen_shown = 1, checkout_id, NULL)) AS other_prepaid_l1_screen_shown,
    COUNT(IF(other_prepaid_submit = 1, session_id, NULL)) AS other_prepaid_submit,
    COUNT(IF(submit = 1, session_id, NULL)) AS submit
FROM event_combinations
  GROUP BY 1
    """.format(start_date, end_date, merchant_id_list)
)
payment_screen_funnel_query_df = (payment_screen_funnel_query_db.toPandas()).transpose()
payment_screen_funnel_query_df.reset_index(inplace=True)
payment_screen_funnel_query_df 

# COMMAND ----------

payment_screen_funnel_query_df.columns = payment_screen_funnel_query_df.iloc[0].to_list()
payment_screen_funnel_query_df = payment_screen_funnel_query_df[1:]
payment_screen_funnel_query_df

# COMMAND ----------

payment_screen_funnel_query_df.iloc[:,-2:]

# COMMAND ----------


payment_screen_funnel_query_df['v2%'] =payment_screen_funnel_query_df['v2']*1.0/ payment_screen_funnel_query_df.iloc[0]['v2']
#payment_screen_funnel_query_df['v1%'] =payment_screen_funnel_query_df['v1']*1.0/ payment_screen_funnel_query_df.iloc[0]['v1']
#payment_screen_funnel_query_df['diff'] = payment_screen_funnel_query_df['v2%'] - payment_screen_funnel_query_df['v1%']
#payment_screen_funnel_query_df.iloc[:,-3:] = payment_screen_funnel_query_df.iloc[:,-3:].applymap(percentage_conversion)
payment_screen_funnel_query_df

# COMMAND ----------

html_code += """<p><h3>[Correct one] Payment Screen Funnel:</h3> <br/>"""
html_code += payment_screen_funnel_query_df.to_html(index=False,escape=False,  )
html_code += """</p>"""

# COMMAND ----------

# MAGIC %md
# MAGIC ## SR L1 : Payment Method

# COMMAND ----------

html_code += """
<h2>SR Deep Dive:</h2> <br/>"""


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
pivoted_df['v1 %'] = pivoted_df['payment_attempts']['v1']*1.0 / pivoted_df['payment_attempts']['v1'].sum()
pivoted_df['v2 %'] = pivoted_df['payment_attempts']['v2']*1.0 / pivoted_df['payment_attempts']['v2'].sum()
pivoted_df['vol_diff'] = pivoted_df['v2 %'] - pivoted_df['v1 %']
pivoted_df['success_rate_diff'] = pivoted_df['success_rate']['v2'] - pivoted_df['success_rate']['v1']
pivoted_df['vol_impact'] = pivoted_df['vol_diff'] * pivoted_df['success_rate']['v1']
pivoted_df['cr_impact'] = pivoted_df['success_rate_diff'] * pivoted_df['v2 %']
vol_impact_sum = pivoted_df['vol_impact'].sum()
#pivoted_df['total_impact'] = pivoted_df['cr_impact'] + pivoted_df['vol_impact']
pivoted_df[pivoted_df.columns[1:3]] = pivoted_df[pivoted_df.columns[1:3]].applymap(formatINR)
pivoted_df[pivoted_df.columns[3:]] = pivoted_df[pivoted_df.columns[3:]].applymap(percentage_conversion)
pivoted_df

# COMMAND ----------

html_code += """<p><h3>SR - Payment Method:</h3> 
<p4> Total volume mix impact is {0}
<br/>""".format(vol_impact_sum)
html_code += pivoted_df.to_html(index=False,escape=False,  )
html_code += """</p>"""

# COMMAND ----------

# MAGIC %md
# MAGIC ### without fireboltt

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
pivoted_df['v1 %'] = pivoted_df['payment_attempts']['v1']*1.0 / pivoted_df['payment_attempts']['v1'].sum()
pivoted_df['v2 %'] = pivoted_df['payment_attempts']['v2']*1.0 / pivoted_df['payment_attempts']['v2'].sum()
pivoted_df['vol_diff'] = pivoted_df['v2 %'] - pivoted_df['v1 %']
pivoted_df['success_rate_diff'] = pivoted_df['success_rate']['v2'] - pivoted_df['success_rate']['v1']
pivoted_df['vol_impact'] = pivoted_df['vol_diff'] * pivoted_df['success_rate']['v1']
pivoted_df['cr_impact'] = pivoted_df['success_rate_diff'] * pivoted_df['v2 %']
pivoted_df['total_impact'] = pivoted_df['cr_impact'] + pivoted_df['vol_impact']
pivoted_df[pivoted_df.columns[3:]] = pivoted_df[pivoted_df.columns[3:]].applymap(percentage_conversion)
pivoted_df

# COMMAND ----------

# MAGIC %md
# MAGIC ## SR : date-wise

# COMMAND ----------

grouped_df = final_df[conditions].groupby(by = ['v2_result','producer_created_date'], dropna=False).agg(
    { 
     'merchant_id':'nunique',
        'payment_attempts':'nunique',
     'payment_success':'nunique',
    
     }
).reset_index()
grouped_df['split_percentage'] = grouped_df['payment_attempts']*1.0 / grouped_df['payment_attempts'].sum()
grouped_df['success_rate'] = grouped_df['payment_success']*1.0 / grouped_df['payment_attempts']
pivoted_df =grouped_df.pivot( index='producer_created_date' ,columns= 'v2_result', values = ['payment_attempts','success_rate',]).sort_values(by = 'producer_created_date',).reset_index()
pivoted_df['Total'] = pivoted_df[pivoted_df.columns[1:3]].sum(axis=1)
pivoted_df = pivoted_df.sort_values(by = 'producer_created_date', ascending=True).drop(columns=['Total'])
pivoted_df['success_rate_diff'] = pivoted_df['success_rate']['v2'] - pivoted_df['success_rate']['v1']
pivoted_df[pivoted_df.columns[1:3]] = pivoted_df[pivoted_df.columns[1:3]].applymap(formatINR)
pivoted_df[pivoted_df.columns[3:]] = pivoted_df[pivoted_df.columns[3:]].applymap(percentage_conversion)
pivoted_df

# COMMAND ----------

# MAGIC %md
# MAGIC ## UPI SR datewise trend

# COMMAND ----------

grouped_df = final_df[conditions & (final_df['payment_method']=='upi')].groupby(by = ['v2_result','producer_created_date'], dropna=False).agg(
    { 
     'merchant_id':'nunique',
        'payment_attempts':'nunique',
     'payment_success':'nunique',
    
     }
).reset_index()
grouped_df['split_percentage'] = grouped_df['payment_attempts']*1.0 / grouped_df['payment_attempts'].sum()
grouped_df['success_rate'] = grouped_df['payment_success']*1.0 / grouped_df['payment_attempts']
pivoted_df =grouped_df.pivot( index='producer_created_date' ,columns= 'v2_result', values = ['payment_attempts','success_rate',]).sort_values(by = 'producer_created_date',).reset_index()
pivoted_df['Total'] = pivoted_df[pivoted_df.columns[1:3]].sum(axis=1)
pivoted_df = pivoted_df.sort_values(by = 'producer_created_date', ascending=True).drop(columns=['Total'])
#pivoted_df['success_rate_diff'] = pivoted_df['success_rate']['v2'] - pivoted_df['success_rate']['v1']
#pivoted_df[pivoted_df.columns[1:3]] = pivoted_df[pivoted_df.columns[1:3]].applymap(formatINR)
#pivoted_df[pivoted_df.columns[3:]] = pivoted_df[pivoted_df.columns[3:]].applymap(percentage_conversion)
pivoted_df

# COMMAND ----------

html_code += """<p><h3>UPI SR - Date-wise:</h3> <br/>"""
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
pivoted_df['v1 %'] = pivoted_df['payment_attempts']['v1']*1.0 / pivoted_df['payment_attempts']['v1'].sum()
pivoted_df['v2 %'] = pivoted_df['payment_attempts']['v2']*1.0 / pivoted_df['payment_attempts']['v2'].sum()
pivoted_df['vol_diff'] = pivoted_df['v2 %'] - pivoted_df['v1 %']
pivoted_df['success_rate_diff'] = pivoted_df['success_rate']['v2'] - pivoted_df['success_rate']['v1']
pivoted_df['vol_impact'] = pivoted_df['vol_diff'] * pivoted_df['success_rate']['v1']
pivoted_df['cr_impact'] = pivoted_df['success_rate_diff'] * pivoted_df['v2 %']
pivoted_df[pivoted_df.columns[1:3]] = pivoted_df[pivoted_df.columns[1:3]].applymap(formatINR)
pivoted_df[pivoted_df.columns[3:]] = pivoted_df[pivoted_df.columns[3:]].applymap(percentage_conversion)
pivoted_df

# COMMAND ----------

html_code += """<p><h3>UPI SR - Gateway:</h3> <br/>"""
html_code += pivoted_df.to_html(index=False,escape=False,  )
html_code += """</p>"""

# COMMAND ----------

# MAGIC %md
# MAGIC ## UPI SR: Gateway x Date

# COMMAND ----------

gateway_list = ['upi_rzpapb','upi_axis','upi_icici','upi_airtel']
grouped_df = final_df[conditions
                      & (final_df['payment_method']=='upi')
                      & (final_df['gateway'].isin(gateway_list))
                      ].groupby(by = ['gateway','v2_result','producer_created_date'], dropna=False).agg(
    { 
     'merchant_id':'nunique',
        'payment_attempts':'nunique',
     'payment_success':'nunique',
    
     }
).reset_index()
grouped_df['split_percentage'] = grouped_df['payment_attempts']*1.0 / grouped_df['payment_attempts'].sum()
grouped_df['success_rate'] = grouped_df['payment_success']*1.0 / grouped_df['payment_attempts']
pivoted_df =grouped_df.pivot( index='producer_created_date' ,columns= ['gateway','v2_result',], values = ['payment_attempts','success_rate',]).sort_values(by = 'producer_created_date',).reset_index()



#pivoted_df['vol_impact'] = pivoted_df['vol_diff'] * pivoted_df['success_rate']['v1']
#pivoted_df['cr_impact'] = pivoted_df['success_rate_diff'] * pivoted_df['v2 %']
#pivoted_df['total_impact'] = pivoted_df['cr_impact'] + pivoted_df['vol_impact']
pivoted_df[pivoted_df.columns[1:9]] = pivoted_df[pivoted_df.columns[1:9]].applymap(formatINR)
pivoted_df[pivoted_df.columns[9:]] = pivoted_df[pivoted_df.columns[9:]].applymap(percentage_conversion)
pivoted_df

# COMMAND ----------

html_code += """<p>
<h2> UPI deep dive </h2>
<h3>UPI SR - Top 4 gateways:</h3> <br/>"""
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
#pivoted_df['success_rate_diff'] = pivoted_df['success_rate']['v2'] - pivoted_df['success_rate']['v1']
#pivoted_df[pivoted_df.columns[1:3]] = pivoted_df[pivoted_df.columns[1:3]].applymap(formatINR)
#pivoted_df[pivoted_df.columns[3:]] = pivoted_df[pivoted_df.columns[3:]].applymap(percentage_conversion)
pivoted_df

# COMMAND ----------

html_code += """<p><h3>UPI SR - Type:</h3> <br/>"""
html_code += pivoted_df.to_html(index=False,escape=False,  )
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
#pivoted_df['success_rate_diff'] = pivoted_df['success_rate']['v2'] - pivoted_df['success_rate']['v1']
#pivoted_df[pivoted_df.columns[1:3]] = pivoted_df[pivoted_df.columns[1:3]].applymap(formatINR)
#pivoted_df[pivoted_df.columns[3:]] = pivoted_df[pivoted_df.columns[3:]].applymap(percentage_conversion)
pivoted_df

# COMMAND ----------

html_code += """<p><h3>UPI SR - App:</h3> <br/>"""
html_code += pivoted_df.to_html(index=False,escape=False,  )
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
#pivoted_df['success_rate_diff'] = pivoted_df['success_rate']['v2'] - pivoted_df['success_rate']['v1']
#pivoted_df[pivoted_df.columns[6:]] = pivoted_df[pivoted_df.columns[6:]].applymap(percentage_conversion)
pivoted_df.head(20)

# COMMAND ----------

#funnel_query_df.to_csv('/dbfs/FileStore/magic_funnel.csv', index=False)
"https://razorpay-dev.cloud.databricks.com/files/magic_funnel.csv"

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ## Email Code

# COMMAND ----------

user = 'pallavi.samodia@razorpay.com'
app_password = 'ijlchahkwyhervxv' # Guide for app passwords: https://support.google.com/accounts/answer/185833?hl=en
host = 'smtp.gmail.com'
port = 465
server = smtplib.SMTP_SSL(host, port) 
server.login(user, app_password)

# COMMAND ----------

# MAGIC %md
# MAGIC <h3> To be modified manually </h3>

# COMMAND ----------

html_code += """<h2>Observations / Call outs </h2>"""
html_code += """ <ol> <li>  OTP Submit rate is most likely low because the logic for trigger does not align with the V1 logic. Fix pending with Engineering since August 9. Slack thread: https://razorpay.slack.com/archives/C04N2HQCXQV/p1724828168587129?thread_ts=1722234598.643499&cid=C04N2HQCXQV </li>
</ol>
"""

# COMMAND ----------

html_code += """<h2>Appendix</h2>"""
html_code += """<h3>Unfiltered Complete data:</h3> <br/>"""
html_code += overall_df.to_html(index=False,escape=False,  )

# COMMAND ----------

html_code += """
</body>
</html>
"""
#email_id = ['pallavi.samodia@razorpay.com','sanjay.garg@razorpay.com','aakash.bhattacharjee@razorpay.com','manjeet.singh@razorpay.com','chirag.patel@razorpay.com','aravinth.pk@razorpay.com','kruthika.m@razorpay.com','revanth.m@razorpay.com','pranav.gupta@razorpay.com','ankit.punia@razorpay.com','gaurav.dadhich@razorpay.com','shivam.bansal@razorpay.com','vishal.gupta@razorpay.com','aravinth.pk@razorpay.com','chetna.handa@razorpay.com']
# 
#for i in email_id:
email_id=['pallavi.samodia@razorpay.com','sanjay.garg@razorpay.com','aakash.bhattacharjee@razorpay.com','manjeet.singh@razorpay.com','aravinth.pk@razorpay.com','chetna.handa@razorpay.com','yamini.dadhich@razorpay.com','kanika.kaushik@razorpay.com']
send_emails(start_date,end_date,email_id,html_code,mid)

# COMMAND ----------

# MAGIC %md
# MAGIC #Specific Deep dives [Case by Case basis]
# MAGIC ### 1. Fireboltt SR RCA [Aug 9]

# COMMAND ----------

final_df.columns

# COMMAND ----------

grouped_df = final_df[(final_df['v2_eligible']==True) & (final_df['merchant_id'] == 'IH7E2OJQGEKKTN') & (final_df['payment_method']=='upi')].groupby(by = ['v2_result','producer_created_date'], dropna=False).agg(
    { 
     'merchant_id':'nunique',
        'payment_attempts':'nunique',
     'payment_success':'nunique',
    
     }
).reset_index()
grouped_df['split_percentage'] = grouped_df['payment_attempts']*1.0 / grouped_df['payment_attempts'].sum()
grouped_df['success_rate'] = grouped_df['payment_success']*1.0 / grouped_df['payment_attempts']
pivoted_df =grouped_df.pivot( index='producer_created_date' ,columns= 'v2_result', values = ['payment_attempts','success_rate',]).sort_values(by = 'producer_created_date',ascending=True).reset_index()
#pivoted_df['Total'] = pivoted_df[pivoted_df.columns[1:3]].sum(axis=1)
#pivoted_df = pivoted_df.sort_values(by = 'Total', ascending=False).drop(columns=['Total']).reset_index(drop=True)
pivoted_df['success_rate_diff'] = pivoted_df['success_rate']['v2'] - pivoted_df['success_rate']['v1']
pivoted_df[pivoted_df.columns[3:]] = pivoted_df[pivoted_df.columns[3:]].applymap(percentage_conversion)
pivoted_df

# COMMAND ----------

#Defining fireboltt conditions
fireboltt_condition = (final_df['v2_eligible']==True) & (final_df['merchant_id'] == 'IH7E2OJQGEKKTN') & (final_df['payment_method']=='upi') & (final_df['producer_created_date']>='2024-08-01')

# COMMAND ----------

groupby_cols = ['v2_result','upi_app','upi_type','gateway']
grouped_df = final_df[fireboltt_condition].groupby(by = groupby_cols, dropna=False).agg(
    { 
     'merchant_id':'nunique',
        'payment_attempts':'nunique',
     'payment_success':'nunique',
    
     }
).reset_index()
grouped_df['split_percentage'] = grouped_df['payment_attempts']*1.0 / grouped_df['payment_attempts'].sum()
grouped_df['success_rate'] = grouped_df['payment_success']*1.0 / grouped_df['payment_attempts']
pivoted_df =grouped_df.pivot( index=['upi_app','upi_type','gateway'] ,columns= 'v2_result', values = ['payment_attempts','success_rate',]).reset_index()
pivoted_df['Total'] = pivoted_df[pivoted_df.columns[-4:-3]].sum(axis=1)
pivoted_df = pivoted_df.sort_values(by = 'Total', ascending=False).drop(columns=['Total']).reset_index(drop=True)
pivoted_df['success_rate_diff'] = pivoted_df['success_rate']['v2'] - pivoted_df['success_rate']['v1']
pivoted_df[pivoted_df.columns[-3:]] = pivoted_df[pivoted_df.columns[-3:]].applymap(percentage_conversion)
pivoted_df

# COMMAND ----------

groupby_cols = ['v2_result','upi_type',]
grouped_df = final_df[fireboltt_condition].groupby(by = groupby_cols, dropna=False).agg(
    { 
     'merchant_id':'nunique',
        'payment_attempts':'nunique',
     'payment_success':'nunique',
    
     }
).reset_index()
grouped_df['split_percentage'] = grouped_df['payment_attempts']*1.0 / grouped_df['payment_attempts'].sum()
grouped_df['success_rate'] = grouped_df['payment_success']*1.0 / grouped_df['payment_attempts']
pivoted_df =grouped_df.pivot( index=['upi_type'] ,columns= 'v2_result', values = ['payment_attempts','success_rate',]).reset_index()
pivoted_df['Total'] = pivoted_df[pivoted_df.columns[-4:-3]].sum(axis=1)
pivoted_df = pivoted_df.sort_values(by = 'Total', ascending=False).drop(columns=['Total']).reset_index(drop=True)
pivoted_df['success_rate_diff'] = pivoted_df['success_rate']['v2'] - pivoted_df['success_rate']['v1']
pivoted_df[pivoted_df.columns[-3:]] = pivoted_df[pivoted_df.columns[-3:]].applymap(percentage_conversion)
pivoted_df

# COMMAND ----------


groupby_cols = ['v2_result','upi_app',]
grouped_df = final_df[fireboltt_condition].groupby(by = groupby_cols, dropna=False).agg(
    { 
     'merchant_id':'nunique',
        'payment_attempts':'nunique',
     'payment_success':'nunique',
    
     }
).reset_index()
grouped_df['split_percentage'] = grouped_df['payment_attempts']*1.0 / grouped_df['payment_attempts'].sum()

grouped_df['success_rate'] = grouped_df['payment_success']*1.0 / grouped_df['payment_attempts']
pivoted_df =grouped_df.pivot( index=groupby_cols[1:] ,columns= 'v2_result', values = ['payment_attempts','success_rate',]).reset_index()
pivoted_df['v1 %'] = pivoted_df['payment_attempts']['v1']*1.0 / pivoted_df['payment_attempts']['v1'].sum()
pivoted_df['v2 %'] = pivoted_df['payment_attempts']['v2']*1.0 / pivoted_df['payment_attempts']['v2'].sum()
pivoted_df['Total'] = pivoted_df[pivoted_df.columns[-6:-5]].sum(axis=1)
pivoted_df = pivoted_df.sort_values(by = 'Total', ascending=False).drop(columns=['Total']).reset_index(drop=True)
pivoted_df['success_rate_diff'] = pivoted_df['success_rate']['v2'] - pivoted_df['success_rate']['v1']
pivoted_df[pivoted_df.columns[-5:]] = pivoted_df[pivoted_df.columns[-5:]].applymap(percentage_conversion)
pivoted_df

# COMMAND ----------


groupby_cols = ['v2_result','upi_app','upi_meta_app', 'upi_provider',]
grouped_df = final_df[fireboltt_condition & (final_df['upi_app']=='Others')].groupby(by = groupby_cols, dropna=False).agg(
    { 
     'merchant_id':'nunique',
        'payment_attempts':'nunique',
     'payment_success':'nunique',
    
     }
).reset_index()
grouped_df['split_percentage'] = grouped_df['payment_attempts']*1.0 / grouped_df['payment_attempts'].sum()

grouped_df['success_rate'] = grouped_df['payment_success']*1.0 / grouped_df['payment_attempts']
pivoted_df =grouped_df.pivot( index=groupby_cols[1:] ,columns= 'v2_result', values = ['payment_attempts','success_rate',]).reset_index()
pivoted_df['v1 %'] = pivoted_df['payment_attempts']['v1']*1.0 / pivoted_df['payment_attempts']['v1'].sum()
pivoted_df['v2 %'] = pivoted_df['payment_attempts']['v2']*1.0 / pivoted_df['payment_attempts']['v2'].sum()
pivoted_df['Total'] = pivoted_df[pivoted_df.columns[-6:-5]].sum(axis=1)
pivoted_df = pivoted_df.sort_values(by = 'Total', ascending=False).drop(columns=['Total']).reset_index(drop=True)
pivoted_df['success_rate_diff'] = pivoted_df['success_rate']['v2'] - pivoted_df['success_rate']['v1']
pivoted_df[pivoted_df.columns[-5:]] = pivoted_df[pivoted_df.columns[-5:]].applymap(percentage_conversion)
pivoted_df

# COMMAND ----------


groupby_cols = ['v2_result','producer_created_date',]
grouped_df = final_df[fireboltt_condition & (final_df['internal_error_code']=='BAD_REQUEST_PAYMENT_TIMED_OUT') ].groupby(by = groupby_cols, dropna=False).agg(
    { 
     'merchant_id':'nunique',
        'payment_attempts':'nunique',
     'payment_success':'nunique',
    
     }
).reset_index()
grouped_df['split_percentage'] = grouped_df['payment_attempts']*1.0 / grouped_df['payment_attempts'].sum()

grouped_df['success_rate'] = grouped_df['payment_success']*1.0 / grouped_df['payment_attempts']
pivoted_df =grouped_df.pivot( index=groupby_cols[1:] ,columns= 'v2_result', values = ['payment_attempts','success_rate',]).reset_index()
pivoted_df['v1 %'] = pivoted_df['payment_attempts']['v1']*1.0 / pivoted_df['payment_attempts']['v1'].sum()
pivoted_df['v2 %'] = pivoted_df['payment_attempts']['v2']*1.0 / pivoted_df['payment_attempts']['v2'].sum()
pivoted_df['Total'] = pivoted_df[pivoted_df.columns[-6:-5]].sum(axis=1)
pivoted_df = pivoted_df.sort_values(by = 'Total', ascending=False).drop(columns=['Total']).reset_index(drop=True)
pivoted_df['success_rate_diff'] = pivoted_df['success_rate']['v2'] - pivoted_df['success_rate']['v1']
pivoted_df[pivoted_df.columns[-5:]] = pivoted_df[pivoted_df.columns[-5:]].applymap(percentage_conversion)
pivoted_df

# COMMAND ----------

# MAGIC %sql
# MAGIC select platform,
# MAGIC get_json_object(render_context,'$.user_agent_parsed.os.family' ) as os,
# MAGIC get_json_object(render_context,'$.platform' ) as context_patform,
# MAGIC get_json_object(render_properties,'$.data.meta.is_mobile' ) as is_mobile,
# MAGIC
# MAGIC case when get_json_object(render_context,'$.platform' ) = 'mobile_sdk' then 'mobile_sdk'
# MAGIC  when get_json_object(render_context,'$.platform' ) = 'browser' and get_json_object(render_context,'$.user_agent_parsed.os.family' ) in ('Mac OS X','Windows','Ubuntu') then 'desktop'
# MAGIC  when get_json_object(render_context,'$.platform' ) = 'browser' and get_json_object(render_context,'$.user_agent_parsed.os.family' ) in ('Android','iOS') then 'mweb'
# MAGIC  else 'others' end as new_platform,
# MAGIC count(*)
# MAGIC from aggregate_pa.cx_lo_fact_ism_v1
# MAGIC where producer_created_date='2024-11-01'
# MAGIC group by 1,2,3,4,5

# COMMAND ----------

# MAGIC %sql
# MAGIC
