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

start_date = '2024-11-21'
end_date = '2024-11-27'

# COMMAND ----------

# MAGIC %md
# MAGIC ## HTML Code base for Email

# COMMAND ----------

def send_emails(start_date, end_date,email_id_list,html_t):
    today = datetime.now().strftime('%Y-%m-%d')
    subject = '[Session ID] Quickbuy report Magic - {0} to {1}'.format(start_date, end_date,)
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
    <title>Magic Checkout quickbuy report - {0} to {1} </title>
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

raw_data_db = sqlContext.sql(
    """
WITH cte AS(
  SELECT
    a.session_id,
    a.checkout_id,
 --- max(ramp_up_date),
    max(case when cx_experiment_fact.experiment_value = 'control' then 'control'
    when cx_experiment_fact.experiment_value = 'variant_on' and get_json_object(properties,'$.data.meta.is_quickbuy_flow') = 'true' then 'test' else 'others' end) as v2_result,
   max(get_json_object(properties,'$.data.meta.quickbuy_eligible')) as v2_eligible,

   max(get_json_object(properties,'$.data.meta.quickbuy_ineligible_reason')) as quickbuy_ineligible_reason,
   max(case when event_name = 'submit' then get_json_object(properties, '$.data.instrument_name') end) as instrument_name

  
  FROM
    aggregate_pa.cx_1cc_events_dump_v1 a
    Left join (
       select * from
       aggregate_pa.cx_1cc_experiment_fact 
       where producer_created_date BETWEEN date('{0}') and date('{1}') 
    and experiment_name = 'one_cc_quick_buy' )AS cx_experiment_fact on a.checkout_id = cx_experiment_fact.checkout_id
 --- inner join batch_sheets.checkout_v2_rampup_sheet b on a.merchant_id = b.merchant_id
  WHERE
    a.producer_created_date BETWEEN date('{0}') and date('{1}') 
   --- and get_json_object(properties,'$.data.meta.v2_result') = 'v2'
   --and merchant_id <> 'ElKk2sBD1gFHvw'    -- deodap                                    --- TEMPORARY DELETE
 -- and producer_created_date > date(ramp_up_date)
 -- and rampup_100_percent_flag = 'false'
 --- and blacklisted_flag = 'false'
 --and merchant_id = 'F1gzQllfIsbqZS'
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
-- and merchant_id = 'JUHfXse0FDfnru'                                                       --- TEMPORARY DELETE

--and merchant_id <> 'ElKk2sBD1gFHvw'    -- deodap 
 --and merchant_id = 'F1gzQllfIsbqZS'

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
--- and merchant_id = 'JUHfXse0FDfnru'                                                       --- TEMPORARY DELETE
  --and merchant_id <> 'ElKk2sBD1gFHvw'    -- deodap 
 --and merchant_id = 'F1gzQllfIsbqZS'

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
instrument_name,
browser_major_version,
browser_name,
os_brand_family,
os_major_version,
mcf.producer_created_date,
mcf.platform,
mcf.original_amount,
cte.v2_result,
cte.v2_eligible,
cte.quickbuy_ineligible_reason,
--submit_instrument,
--submits.method as checkout_method,
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
left join submits on cte.checkout_id = submits.checkout_id
left join aggregate_pa.magic_checkout_fact mcf on cte.checkout_id = mcf.checkout_id
where mcf.producer_created_date between date('{0}') and date('{1}') 
and (mcf.library <> 'magic-x' or mcf.library is null)
and (mcf.is_magic_x <> 1 or mcf.is_magic_x is NULL)
and (mcf.checkout_integration_type <> 'x' or mcf.checkout_integration_type is null)
and mcf.v2_result = 'v2'
--and merchant_id <> 'ElKk2sBD1gFHvw'    -- deodap 
--and merchant_id = 'F1gzQllfIsbqZS'


""".format(
        start_date, end_date
    )
)

raw_data_df = raw_data_db.toPandas()
raw_data_df.head()

# COMMAND ----------



# COMMAND ----------

len(raw_data_df)

# COMMAND ----------

merchants_db = sqlContext.sql("""
                             select id as merchant_id, website
                             from realtime_hudi_api.merchants
                             """)
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

overall_df1 = raw_data_df[(raw_data_df['v2_eligible'] == 'false') | 
    (raw_data_df['v2_eligible'].isna())].groupby(by = ['quickbuy_ineligible_reason'], dropna=False).agg(
    { 'checkout_id':'nunique'
        # 'merchant_id':'nunique'
     }
).reset_index()
overall_df1 = overall_df1.sort_values('checkout_id', ascending=False)
overall_df1['split_percentage'] = overall_df1['checkout_id']*1.0 / overall_df1['checkout_id'].sum()

overall_df1[overall_df1.columns[1:2]] = overall_df1[overall_df1.columns[1:2]].applymap(formatINR)
overall_df1[overall_df1.columns[2:]] = overall_df1[overall_df1.columns[2:]].applymap(percentage_conversion)
#grouped_df.pivot(  = 'v2_result', values = ['checkout_id','payment_attempts','payment_success','success_rate','overall_cr']).reset_index()

overall_df1

# COMMAND ----------

# MAGIC %md
# MAGIC ### Checking MID split % + Data Clean Up

# COMMAND ----------



# COMMAND ----------

eligibility_condition = (
    ((raw_data_df['v2_eligible'] == True) | 
    (raw_data_df['v2_eligible'] == 'true')) & raw_data_df['v2_result'].isin(['control','test'])
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

final_mid_list = mid_df[(mid_df.loc[:, pd.IndexSlice['checkout_id_split', 'control']]!=100.0) & (mid_df.loc[:, pd.IndexSlice['checkout_id_split', 'control']]!=0.0)  ]['merchant_id'].tolist()
final_mid_list

# COMMAND ----------

# MAGIC %md
# MAGIC ### Merchants who will be excluded [Top 20]
# MAGIC

# COMMAND ----------

excluded_mids = mid_df[(mid_df.loc[:, pd.IndexSlice['checkout_id_split', 'control']]==100.0) | (mid_df.loc[:, pd.IndexSlice['checkout_id_split', 'control']]==0.0)  ].reset_index()
excluded_mids[['merchant_id','checkout_id','Total']].head(20)

# COMMAND ----------


final_df = raw_data_df[raw_data_df['merchant_id'].isin(final_mid_list)].reset_index(drop=True)
final_df

# COMMAND ----------

final_df['merchant_id'].nunique()

# COMMAND ----------

excluded_mids.to_csv('/dbfs/FileStore/magic_final_excluded_mids.csv', index=False)
"https://razorpay-dev.cloud.databricks.com/files/magic_final_excluded_mids.csv".format(start_date, end_date)

# COMMAND ----------

pd.DataFrame(final_mid_list).to_csv('/dbfs/FileStore/magic_final_mid_list.csv', index=False)
"https://razorpay-dev.cloud.databricks.com/files/magic_final_mid_list.csv".format(start_date, end_date)

# COMMAND ----------

saved_start_date = start_date #Change this if you want to extract file for another time
saved_end_date = end_date #Change this if you want to extract file for another time
#final_df = pd.read_csv('/dbfs/FileStore/magic_final_{start_date}_{end_date}_df.csv')
final_df.shape

# COMMAND ----------

#final_df = pd.read_csv('/dbfs/FileStore/magic_final_df.csv')
final_df.shape

# COMMAND ----------


final_df = final_df.rename(columns={'submit_checkout_id': 'submit_cid'})
final_df['submit_checkout_id'] = final_df.apply(lambda row: row['checkout_id'] if pd.notnull(row['submit_cid']) else None, axis=1)
display(final_df.head(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conditions
# MAGIC all analysis here on will be only for cases where V2 eligible = True + any other conditions specified hereby
# MAGIC

# COMMAND ----------

#write an either/or
conditions = ((final_df['v2_eligible']=='true') |(final_df['v2_eligible']==True))  & (final_df['v2_result'].isin(['control','test']))

# COMMAND ----------

final_df['v2_result'].unique()

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
     'cid':'nunique',
    
     }
).reset_index()
final_l0_df['split_percentage_session_id'] = final_l0_df['checkout_id']*1.0 / final_l0_df['checkout_id'].sum()
final_l0_df['modal_cr_session_id'] = final_l0_df['submit_checkout_id']*1.0 / final_l0_df['checkout_id']
final_l0_df['submit_to_pa_session_id'] = final_l0_df['payment_attempts']*1.0 / final_l0_df['submit_checkout_id']
final_l0_df['success_rate'] = final_l0_df['payment_success']*1.0 / final_l0_df['payment_attempts']
final_l0_df['overall_cr_session_id'] = final_l0_df['payment_success']*1.0 / final_l0_df['checkout_id']
#final_l0_df['split_percentage_session_id'] = final_l0_df['checkout_id']*1.0 / final_l0_df['checkout_id'].sum()
#final_l0_df['modal_cr_session_id'] = final_l0_df['submit_checkout_id']*1.0 / final_l0_df['checkout_id']
#final_l0_df['submit_to_pa_session_id'] = final_l0_df['payment_attempts']*1.0 / final_l0_df['submit_checkout_id']
final_l0_df_copy = final_l0_df.copy()
final_l0_df[final_l0_df.columns[1:6]] = final_l0_df[final_l0_df.columns[1:6]].applymap(formatINR)
final_l0_df[final_l0_df.columns[6:]] = final_l0_df[final_l0_df.columns[6:]].applymap(percentage_conversion)
#grouped_df.pivot(  = 'v2_result', values = ['checkout_id','payment_attempts','payment_success','success_rate','overall_cr']).reset_index()

final_l0_df

# COMMAND ----------

html_code += """<p><h3>L0 Metrics for only Merchants who are on experiment:</h3> <br/>"""
html_code += final_l0_df.to_html(index=False,escape=False,  )
html_code += """</p>"""

# COMMAND ----------

html_code += """<p><h3>L0 Metrics for AOV bucket:</h3> <br/>"""
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
grouped_df['submit_to_pa'] = grouped_df['payment_attempts']*1.0 / grouped_df['submit_checkout_id']
grouped_df['overall_cr'] = grouped_df['payment_success']*1.0 / grouped_df['checkout_id']
grouped_df['success_rate'] = grouped_df['payment_success']*1.0 / grouped_df['payment_attempts']
pivoted_df = grouped_df.pivot( index='producer_created_date' ,columns= 'v2_result', values = ['checkout_id','payment_attempts','payment_success','modal_cr','success_rate','overall_cr']).sort_values(by = 'producer_created_date',).reset_index()
pivoted_df['modal_cr_diff'] = pivoted_df['modal_cr']['test'] - pivoted_df['modal_cr']['control']
pivoted_df['success_rate_diff'] = pivoted_df['success_rate']['test'] - pivoted_df['success_rate']['control']
pivoted_df['overall_cr_diff'] = pivoted_df['overall_cr']['test'] - pivoted_df['overall_cr']['control']
pivoted_df[pivoted_df.columns[1:7]] = pivoted_df[pivoted_df.columns[1:7]].applymap(formatINR)
pivoted_df[pivoted_df.columns[7:]] = pivoted_df[pivoted_df.columns[7:]].applymap(percentage_conversion)
pivoted_df

# COMMAND ----------



# COMMAND ----------

html_code += """<p><h3>L0 Metrics - Date Wise:</h3> <br/>"""
html_code += pivoted_df.to_html(index=False,escape=False,  )
html_code += """</p>"""

# COMMAND ----------

html_code += """<p><h3>Quickbuy ineligible reason, checkout id split:</h3> <br/>"""
html_code += (overall_df1.head(20)).to_html(index=False,escape=False,)
html_code += """</p>"""

# COMMAND ----------

pivoted_df.to_csv('/dbfs/FileStore/v_imp_data_magic_df.csv', index=False)
"https://razorpay-dev.cloud.databricks.com/files/v_imp_data_magic_df.csv".format(start_date, end_date)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Session ID Reports

# COMMAND ----------

final_df.head()

# COMMAND ----------

grouped_df = final_df[conditions].groupby(by = ['v2_result','merchant_id'], dropna=False).agg(
    { 'checkout_id':'nunique',
     'cid':'nunique',
      'submit_checkout_id':'nunique',
      'submit_cid':'nunique',
        'payment_attempts':'nunique',
     'payment_success':'nunique',
    
     }
).reset_index()

grouped_df['session_to_checkout_id'] = grouped_df['checkout_id']*1.0 / grouped_df['cid']
grouped_df['submit_session_to_checkout_id'] = grouped_df['submit_checkout_id']*1.0 / grouped_df['submit_cid']
grouped_df = grouped_df.merge(merchants_df, on='merchant_id', how='left')
pivoted_df = grouped_df.pivot( index=['merchant_id','website'] ,columns= 'v2_result', values = ['checkout_id',       'submit_checkout_id','payment_attempts', 'payment_success','session_to_checkout_id','submit_session_to_checkout_id']).sort_values(by = 'merchant_id',).reset_index()
pivoted_df['Total'] = pivoted_df[pivoted_df.columns[2:4]].sum(axis=1)
pivoted_df = pivoted_df.sort_values(by = 'Total', ascending=False).drop(columns=['Total'])
pivoted_df[pivoted_df.columns[2:10]] = pivoted_df[pivoted_df.columns[2:10]].applymap(formatINR)
pivoted_df[pivoted_df.columns[10:]] = pivoted_df[pivoted_df.columns[10:]].applymap(percentage_conversion)
pivoted_df.head(20)

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
grouped_df['submit_to_pa'] = grouped_df['payment_attempts']*1.0 / grouped_df['submit_checkout_id']
grouped_df['overall_cr'] = grouped_df['payment_success']*1.0 / grouped_df['checkout_id']
grouped_df['success_rate'] = grouped_df['payment_success']*1.0 / grouped_df['payment_attempts']
grouped_df = grouped_df.merge(merchants_df, on='merchant_id', how='left')
pivoted_df = grouped_df.pivot( index=['merchant_id','website'] ,columns= 'v2_result', values = ['checkout_id',       'submit_checkout_id','payment_attempts', 'payment_success','modal_cr','success_rate','overall_cr']).sort_values(by = 'merchant_id',).reset_index()

pivoted_df['Total'] = pivoted_df[pivoted_df.columns[2:4]].sum(axis=1)
pivoted_df = pivoted_df.sort_values(by = 'Total', ascending=False).drop(columns=['Total'])
pivoted_df['modal_cr_diff'] = pivoted_df['modal_cr']['test'] - pivoted_df['modal_cr']['control']
pivoted_df['success_rate_diff'] = pivoted_df['success_rate']['test'] - pivoted_df['success_rate']['control']
pivoted_df['overall_cr_diff'] = pivoted_df['overall_cr']['test'] - pivoted_df['overall_cr']['control']
pivoted_df[pivoted_df.columns[2:10]] = pivoted_df[pivoted_df.columns[2:10]].applymap(formatINR)
pivoted_df[pivoted_df.columns[10:]] = pivoted_df[pivoted_df.columns[10:]].applymap(percentage_conversion)
pivoted_df.head(20)

# COMMAND ----------

pivoted_df.to_csv('/dbfs/FileStore/v_imp_data_magic_df.csv', index=False)
"https://razorpay-dev.cloud.databricks.com/files/v_imp_data_magic_df.csv".format(start_date, end_date)

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
pivoted_df['modal_cr_diff'] = pivoted_df['modal_cr']['test'] - pivoted_df['modal_cr']['control']
pivoted_df['success_rate_diff'] = pivoted_df['success_rate']['test'] - pivoted_df['success_rate']['control']
pivoted_df['overall_cr_diff'] = pivoted_df['overall_cr']['test'] - pivoted_df['overall_cr']['control']
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
# MAGIC #Browser x OS
# MAGIC

# COMMAND ----------


grouped_df = final_df[conditions].groupby(by = ['v2_result','browser_name','os_brand_family'], dropna=False).agg(
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

pivoted_df = grouped_df.pivot( index=['browser_name','os_brand_family'] ,columns= 'v2_result', values = ['checkout_id','modal_cr','success_rate','overall_cr']).sort_values(by = 'browser_name',).reset_index()
pivoted_df['Total'] = pivoted_df[pivoted_df.columns[2:4]].sum(axis=1)
pivoted_df = pivoted_df.sort_values(by = 'Total', ascending=False).drop(columns=['Total'])
pivoted_df['modal_cr_diff'] = pivoted_df['modal_cr']['test'] - pivoted_df['modal_cr']['control']
pivoted_df['success_rate_diff'] = pivoted_df['success_rate']['test'] - pivoted_df['success_rate']['control']
pivoted_df['overall_cr_diff'] = pivoted_df['overall_cr']['test'] - pivoted_df['overall_cr']['control']
pivoted_df[pivoted_df.columns[2:4]] = pivoted_df[pivoted_df.columns[2:4]].applymap(formatINR)
pivoted_df[pivoted_df.columns[4:]] = pivoted_df[pivoted_df.columns[4:]].applymap(percentage_conversion)
pivoted_df.head(20)

# COMMAND ----------

html_code += """<p><h3>L0 Metrics - Browser x OS wise :</h3> <br/>
"""

html_code += (pivoted_df.head(20)).to_html(index=False,escape=False,  )
html_code += """</p>"""

# COMMAND ----------

grouped_df = final_df[conditions & (final_df['payment_method']=='upi') ].groupby(by = ['v2_result','instrument_name','upi_type','merchant_id'], dropna=False).agg(
    { 
        'payment_attempts':'nunique',
     'payment_success':'nunique',
    
     }
).reset_index()
grouped_df['split_percentage'] = grouped_df['payment_attempts']*1.0 / grouped_df['payment_attempts'].sum()
grouped_df['success_rate'] = grouped_df['payment_success']*1.0 / grouped_df['payment_attempts']
pivoted_df =grouped_df.pivot( index=['instrument_name','upi_type','merchant_id'] ,columns= 'v2_result', values = ['payment_attempts','success_rate',]).reset_index()
pivoted_df['Total'] = pivoted_df[pivoted_df.columns[3:5]].sum(axis=1)
pivoted_df = pivoted_df.sort_values(by = 'Total', ascending=False).drop(columns=['Total']).reset_index(drop=True)
pivoted_df['success_rate_diff'] = pivoted_df['success_rate']['test'] - pivoted_df['success_rate']['control']
pivoted_df[pivoted_df.columns[3:5]] = pivoted_df[pivoted_df.columns[3:5]].applymap(formatINR)
pivoted_df[pivoted_df.columns[5:]] = pivoted_df[pivoted_df.columns[5:]].applymap(percentage_conversion)
pivoted_df.head(20)

# COMMAND ----------

html_code += """<p><h3>UPI SR - Front-end Instrument-wise:</h3> <br/>"""
html_code += (pivoted_df.head(20)).to_html(index=False,escape=False,  )
html_code += """</p>"""

# COMMAND ----------

# MAGIC %md
# MAGIC ## Instrument name x UPI type

# COMMAND ----------

grouped_df = final_df[conditions & (final_df['payment_method']=='upi') ].groupby(by = ['v2_result','instrument_name','upi_type'], dropna=False).agg(
    { 
        'payment_attempts':'nunique',
     'payment_success':'nunique',
    
     }
).reset_index()
grouped_df['split_percentage'] = grouped_df['payment_attempts']*1.0 / grouped_df['payment_attempts'].sum()
grouped_df['success_rate'] = grouped_df['payment_success']*1.0 / grouped_df['payment_attempts']
pivoted_df =grouped_df.pivot( index=['instrument_name','upi_type'] ,columns= 'v2_result', values = ['payment_attempts','success_rate',]).reset_index()
pivoted_df['Total'] = pivoted_df[pivoted_df.columns[2:4]].sum(axis=1)
pivoted_df = pivoted_df.sort_values(by = 'Total', ascending=False).drop(columns=['Total']).reset_index(drop=True)
pivoted_df['success_rate_diff'] = pivoted_df['success_rate']['test'] - pivoted_df['success_rate']['control']
pivoted_df[pivoted_df.columns[2:4]] = pivoted_df[pivoted_df.columns[2:4]].applymap(formatINR)
pivoted_df[pivoted_df.columns[4:]] = pivoted_df[pivoted_df.columns[4:]].applymap(percentage_conversion)
pivoted_df

# COMMAND ----------

html_code += """<p><h3>UPI SR - Front-end Instrument-wise (Instrument X UPI type):</h3> <br/>"""
html_code += (pivoted_df.head(20)).to_html(index=False,escape=False,  )
html_code += """</p>"""

# COMMAND ----------

grouped_df = final_df[conditions & (final_df['payment_method']=='upi') ].groupby(by = ['v2_result','instrument_name','upi_type', 'browser_name', 'os_brand_family'], dropna=False).agg(
    { 
        'payment_attempts':'nunique',
     'payment_success':'nunique',
    
     }
).reset_index()

grouped_df['split_percentage'] = grouped_df['payment_attempts']*1.0 / grouped_df['payment_attempts'].sum()
grouped_df['success_rate'] = grouped_df['payment_success']*1.0 / grouped_df['payment_attempts']
pivoted_df =grouped_df.pivot( index=['instrument_name','upi_type','browser_name', 'os_brand_family'] ,columns= 'v2_result', values = ['payment_attempts','success_rate',]).reset_index()
pivoted_df['Total'] = pivoted_df[pivoted_df.columns[4:6]].sum(axis=1)
pivoted_df = pivoted_df.sort_values(by = 'Total', ascending=False).drop(columns=['Total']).reset_index(drop=True)
pivoted_df['success_rate_diff'] = pivoted_df['success_rate']['test'] - pivoted_df['success_rate']['control']
pivoted_df[pivoted_df.columns[4:6]] = pivoted_df[pivoted_df.columns[4:6]].applymap(formatINR)
pivoted_df[pivoted_df.columns[6:]] = pivoted_df[pivoted_df.columns[6:]].applymap(percentage_conversion)
pivoted_df.head(20)

# COMMAND ----------

html_code += """<p><h3>UPI SR - Front-end Instrument-wise (Instrument X UPI type X Browser X OS):</h3> <br/>"""
html_code += (pivoted_df.head(20)).to_html(index=False,escape=False,  )
html_code += """</p>"""

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC ## Submit to Payment Initiated

# COMMAND ----------

# cols = ['v2_result','payment_method']
# submit_to_pa_df = final_df[conditions].groupby(by = cols, dropna=False).agg(
#     { 
#      'submit_checkout_id':'nunique',
#         'payment_attempts':'nunique',
#      'payment_success':'nunique',
#          'pa_checkout_id':'nunique',
#         'ps_checkout_id':'nunique',
#      }
# ).reset_index()

# submit_to_pa_df['success_rate'] = submit_to_pa_df['payment_success']*1.0 / submit_to_pa_df['payment_attempts']
# submit_to_pa_df['submit_to_pa'] = submit_to_pa_df['payment_attempts']*1.0 / submit_to_pa_df['submit_checkout_id']
# submit_to_pa_df['submit_to_pa_checkout_id'] = submit_to_pa_df['pa_checkout_id']*1.0 / submit_to_pa_df['submit_checkout_id']

# submit_to_pa_df =submit_to_pa_df.pivot( index=['payment_method'] ,columns= 'v2_result', values = ['submit_checkout_id','payment_attempts', 'payment_success', 'pa_checkout_id', 'ps_checkout_id', 'submit_to_pa', 'submit_to_pa_checkout_id',    'success_rate',]).reset_index()

# submit_to_pa_df['Total'] = submit_to_pa_df[submit_to_pa_df.columns[2:4]].sum(axis=1)
# submit_to_pa_df = submit_to_pa_df.sort_values(by = 'Total', ascending=False).drop(columns=['Total']).reset_index(drop=True)

# submit_to_pa_df['success_rate_diff'] = submit_to_pa_df['success_rate']['test'] - submit_to_pa_df['success_rate']['control']
# submit_to_pa_df[submit_to_pa_df.columns[1:11]] = submit_to_pa_df[submit_to_pa_df.columns[1:11]].applymap(formatINR)
# submit_to_pa_df[submit_to_pa_df.columns[11:]] = submit_to_pa_df[submit_to_pa_df.columns[11:]].applymap(percentage_conversion)

# submit_to_pa_df

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

submit_to_pa_df.head(10)

# COMMAND ----------

submit_to_pa_df.sort_values(by='submit_to_pa', ascending=False)

# COMMAND ----------


submit_to_pa_pivot_df = submit_to_pa_df.pivot( columns='v2_result',index = cols[1:], values = ['submit_checkout_id','payment_attempts','pa_checkout_id','success_rate','submit_to_pa','submit_to_pa_checkout_id']).reset_index()
submit_to_pa_pivot_df['Total'] = submit_to_pa_pivot_df[submit_to_pa_pivot_df.columns[1:3]].sum(axis=1)
submit_to_pa_pivot_df = submit_to_pa_pivot_df.sort_values(by = 'Total', ascending=False).drop(columns=['Total'])
submit_to_pa_pivot_df['submit_to_pa_diff'] = submit_to_pa_pivot_df['submit_to_pa']['test'] - submit_to_pa_pivot_df['submit_to_pa']['control']
submit_to_pa_pivot_df['submit_to_pa_checkout_id_diff'] = submit_to_pa_pivot_df['submit_to_pa_checkout_id']['test'] - submit_to_pa_pivot_df['submit_to_pa_checkout_id']['control']
submit_to_pa_pivot_df[submit_to_pa_pivot_df.columns[1:7]] = submit_to_pa_pivot_df[submit_to_pa_pivot_df.columns[1:7]].applymap(formatINR)
submit_to_pa_pivot_df[submit_to_pa_pivot_df.columns[7:]] = submit_to_pa_pivot_df[submit_to_pa_pivot_df.columns[7:]].applymap(percentage_conversion)
submit_to_pa_pivot_df

# COMMAND ----------

html_code += """<p><h3> Submit to Payment Initiated (payment method level):</h3> <br/>"""
html_code += (submit_to_pa_pivot_df.head(20)).to_html(index=False,escape=False,  )
html_code += """</p>"""

# COMMAND ----------

# MAGIC %md
# MAGIC #FE Instrument level

# COMMAND ----------

cols = ['v2_result','instrument_name']
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

submit_to_pa_pivot_df = submit_to_pa_df.pivot( columns='v2_result',index = cols[1:], values = ['submit_checkout_id','payment_attempts','pa_checkout_id','success_rate','submit_to_pa','submit_to_pa_checkout_id']).reset_index()
submit_to_pa_pivot_df['Total'] = submit_to_pa_pivot_df[submit_to_pa_pivot_df.columns[1:3]].sum(axis=1)
submit_to_pa_pivot_df = submit_to_pa_pivot_df.sort_values(by = 'Total', ascending=False).drop(columns=['Total'])
submit_to_pa_pivot_df['submit_to_pa_diff'] = submit_to_pa_pivot_df['submit_to_pa']['test'] - submit_to_pa_pivot_df['submit_to_pa']['control']
submit_to_pa_pivot_df['submit_to_pa_checkout_id_diff'] = submit_to_pa_pivot_df['submit_to_pa_checkout_id']['test'] - submit_to_pa_pivot_df['submit_to_pa_checkout_id']['control']
submit_to_pa_pivot_df[submit_to_pa_pivot_df.columns[1:7]] = submit_to_pa_pivot_df[submit_to_pa_pivot_df.columns[1:7]].applymap(formatINR)
submit_to_pa_pivot_df[submit_to_pa_pivot_df.columns[7:]] = submit_to_pa_pivot_df[submit_to_pa_pivot_df.columns[7:]].applymap(percentage_conversion)
submit_to_pa_pivot_df

# COMMAND ----------

final_df.head(5)

# COMMAND ----------

cols = ['v2_result', 'merchant_id', 'payment_method']
submit_to_pa_df_cod = final_df[conditions & (final_df['payment_method'].isin(['cod', 'COD']))].groupby(by = cols, dropna=False).agg(
    { 
     'submit_checkout_id':'nunique',
        'payment_attempts':'nunique',
     'payment_success':'nunique',
         'pa_checkout_id':'nunique',
        'ps_checkout_id':'nunique',
     }
).reset_index()
submit_to_pa_df_cod = final_df[conditions].groupby(by = cols, dropna=False).agg(
    { 
     'submit_checkout_id':'nunique',
        'payment_attempts':'nunique',
     'payment_success':'nunique',
         'pa_checkout_id':'nunique',
        'ps_checkout_id':'nunique',
     }
).reset_index()

submit_to_pa_df_cod['success_rate'] = submit_to_pa_df_cod['payment_success']*1.0 / submit_to_pa_df_cod['payment_attempts']
submit_to_pa_df_cod['submit_to_pa'] = submit_to_pa_df_cod['payment_attempts']*1.0 / submit_to_pa_df_cod['submit_checkout_id']
submit_to_pa_df_cod['submit_to_pa_checkout_id'] = submit_to_pa_df_cod['pa_checkout_id']*1.0 / submit_to_pa_df_cod['submit_checkout_id']

# COMMAND ----------

import pandas as pd

# Assuming final_df is your DataFrame and has a 'website' column
# Filter to only include 'control' and 'test' in v2_result
filtered_df = final_df[conditions & (final_df['v2_result'].isin(['control', 'test']))]

# Count total payment_attempts per merchant_id and v2_result
total_attempts = filtered_df.groupby(['merchant_id', 'v2_result'])['payment_attempts'].count().reset_index()
total_attempts.rename(columns={'payment_attempts': 'total_attempts'}, inplace=True)

# Count payment_attempts for COD per merchant_id, v2_result
cod_attempts = filtered_df[filtered_df['payment_method'] == 'cod'].groupby(['merchant_id', 'v2_result'])['payment_attempts'].count().reset_index()
cod_attempts.rename(columns={'payment_attempts': 'cod_attempts'}, inplace=True)

# Merge COD attempts with total attempts
merged = pd.merge(cod_attempts, total_attempts, on=['merchant_id', 'v2_result'], how='left')

# Calculate COD share as a percentage
merged['cod_share_percent'] = (merged['cod_attempts'] / merged['total_attempts']) * 100

# Pivot the data on v2_result to create columns for 'control' and 'test' cod_share_percent
pivot_table = merged.pivot(index=['merchant_id'], columns='v2_result', values=['cod_share_percent', 'total_attempts', 'cod_attempts']).fillna(0)

# Flatten the MultiIndex columns
pivot_table.columns = ['_'.join(col).strip() for col in pivot_table.columns.values]

# Rename columns for clarity
pivot_table.rename(columns={
    'cod_share_percent_control': 'cod_share_control',
    'cod_share_percent_test': 'cod_share_test',
    'total_attempts_control': 'total_attempts_control',
    'total_attempts_test': 'total_attempts_test',
    'cod_attempts_control': 'cod_attempts_control',
    'cod_attempts_test': 'cod_attempts_test'
}, inplace=True)

# Add a difference column: Test COD share - Control COD share
pivot_table['difference_cod_share'] = pivot_table['cod_share_test'] - pivot_table['cod_share_control']

# Add a column for total payment attempts (control + test)
pivot_table['total_payment_attempts'] = pivot_table['total_attempts_control'] + pivot_table['total_attempts_test']

# Sort the data by total payment attempts in descending order
pivot_table = pivot_table.sort_values(by='total_payment_attempts', ascending=False)
pivot_table = pivot_table.drop(columns=['total_payment_attempts'])
# Reset the index to make merchant_id a column
pivot_table = pivot_table.reset_index()

# Convert COD share to percentage format for display (optional)
pivot_table['cod_share_control'] = pivot_table['cod_share_control'].round(2)
pivot_table['cod_share_test'] = pivot_table['cod_share_test'].round(2)
pivot_table['difference_cod_share'] = pivot_table['difference_cod_share'].round(2)

pivot_table = pivot_table.merge(merchants_df, on='merchant_id', how='left')

# Display the results using display (assuming Jupyter Notebook or similar environment)
display(pivot_table)





# COMMAND ----------

pivot_table.head(20)

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
pivoted_df['control %'] = pivoted_df['payment_attempts']['control']*1.0 / pivoted_df['payment_attempts']['control'].sum()
pivoted_df['test %'] = pivoted_df['payment_attempts']['test']*1.0 / pivoted_df['payment_attempts']['test'].sum()
pivoted_df['vol_diff'] = pivoted_df['test %'] - pivoted_df['control %']
pivoted_df['success_rate_diff'] = pivoted_df['success_rate']['test'] - pivoted_df['success_rate']['control']
pivoted_df['vol_impact'] = pivoted_df['vol_diff'] * pivoted_df['success_rate']['control']
pivoted_df['cr_impact'] = pivoted_df['success_rate_diff'] * pivoted_df['test %']
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
pivoted_df['control %'] = pivoted_df['payment_attempts']['control']*1.0 / pivoted_df['payment_attempts']['control'].sum()
pivoted_df['test %'] = pivoted_df['payment_attempts']['test']*1.0 / pivoted_df['payment_attempts']['test'].sum()
pivoted_df['vol_diff'] = pivoted_df['test %'] - pivoted_df['control %']
pivoted_df['success_rate_diff'] = pivoted_df['success_rate']['test'] - pivoted_df['success_rate']['control']
pivoted_df['vol_impact'] = pivoted_df['vol_diff'] * pivoted_df['success_rate']['control']
pivoted_df['cr_impact'] = pivoted_df['success_rate_diff'] * pivoted_df['test %']
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
pivoted_df['success_rate_diff'] = pivoted_df['success_rate']['test'] - pivoted_df['success_rate']['control']
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
pivoted_df['success_rate_diff'] = pivoted_df['success_rate']['test'] - pivoted_df['success_rate']['control']
pivoted_df[pivoted_df.columns[1:3]] = pivoted_df[pivoted_df.columns[1:3]].applymap(formatINR)
pivoted_df[pivoted_df.columns[3:]] = pivoted_df[pivoted_df.columns[3:]].applymap(percentage_conversion)
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
pivoted_df['control %'] = pivoted_df['payment_attempts']['control']*1.0 / pivoted_df['payment_attempts']['control'].sum()
pivoted_df['test %'] = pivoted_df['payment_attempts']['test']*1.0 / pivoted_df['payment_attempts']['test'].sum()
pivoted_df['vol_diff'] = pivoted_df['test %'] - pivoted_df['control %']
pivoted_df['success_rate_diff'] = pivoted_df['success_rate']['test'] - pivoted_df['success_rate']['control']
pivoted_df['vol_impact'] = pivoted_df['vol_diff'] * pivoted_df['success_rate']['control']
pivoted_df['cr_impact'] = pivoted_df['success_rate_diff'] * pivoted_df['test %']
pivoted_df[pivoted_df.columns[1:3]] = pivoted_df[pivoted_df.columns[1:3]].applymap(formatINR)
pivoted_df[pivoted_df.columns[3:]] = pivoted_df[pivoted_df.columns[3:]].applymap(percentage_conversion)
pivoted_df

# COMMAND ----------



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
pivoted_df['success_rate_diff'] = pivoted_df['success_rate']['test'] - pivoted_df['success_rate']['control']
pivoted_df[pivoted_df.columns[1:3]] = pivoted_df[pivoted_df.columns[1:3]].applymap(formatINR)
pivoted_df[pivoted_df.columns[3:]] = pivoted_df[pivoted_df.columns[3:]].applymap(percentage_conversion)
pivoted_df

# COMMAND ----------

html_code += """<p><h3>UPI SR - Type:</h3> <br/>"""
html_code += pivoted_df.to_html(index=False,escape=False,  )
html_code += """</p>"""

# COMMAND ----------

# MAGIC %md
# MAGIC ## UPI Intent SR - Gateway x Error Code:

# COMMAND ----------

group_cols = ['v2_result','gateway','internal_error_code']
#[(~final_df['internal_error_code'].isna()) & (final_df['upi_type']=='Intent')
                      #& (final_df['gateway'].isin(['upi_icici','upi_axis','upi_rzpapb']))]
grouped_df = final_df[conditions & (final_df['payment_method']=='upi')].groupby(by = group_cols, dropna=False).agg(
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
pivoted_df['test %'] = pivoted_df['payment_attempts']['test']*1.0 / pivoted_df['payment_attempts']['test'].sum()
pivoted_df['vol_diff'] = pivoted_df['test %'] - pivoted_df['control %']
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
pivoted_df['success_rate_diff'] = pivoted_df['success_rate']['test'] - pivoted_df['success_rate']['control']
pivoted_df[pivoted_df.columns[1:3]] = pivoted_df[pivoted_df.columns[1:3]].applymap(formatINR)
pivoted_df[pivoted_df.columns[3:]] = pivoted_df[pivoted_df.columns[3:]].applymap(percentage_conversion)
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
pivoted_df['success_rate_diff'] = pivoted_df['success_rate']['test'] - pivoted_df['success_rate']['control']
pivoted_df[pivoted_df.columns[6:]] = pivoted_df[pivoted_df.columns[6:]].applymap(percentage_conversion)
pivoted_df.head(20)

# COMMAND ----------

#funnel_query_df.to_csv('/dbfs/FileStore/magic_funnel.csv', index=False)
"https://razorpay-dev.cloud.databricks.com/files/magic_funnel.csv"

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC <h3> To be modified manually </h3>

# COMMAND ----------

# MAGIC %md
# MAGIC ##High Level Funnel
# MAGIC

# COMMAND ----------

# Distinct checkout ID list
merchant_id_list = final_df[conditions]['merchant_id'].unique()
merchant_id_list = "', '".join(merchant_id_list)
merchant_id_list = f"('{merchant_id_list}')"

# COMMAND ----------

merchant_id_list

# COMMAND ----------

funnel_query_db = sqlContext.sql(
    """
  WITH events_data AS (
    SELECT
  
        event_name,
        a.checkout_id,
        a.sssion_id,
        cx_1cc_experiment_fact.experiment_value as experiment_value,
a.producer_created_date,
  case when event_name = 'submit' then   coalesce( 
          get_json_object(properties,'$.data.data.method'), 
            get_json_object(properties, '$.data.payment_method'), 
            get_json_object(properties, '$.data.method'), 
            get_json_object(properties, '$.data.method_name')
        ) end AS payment_method,
        

       case when (event_name = 'method:selected' or event_name = 'behav:1cc_payment_home_screen_method_selected')  then coalesce( 
            get_json_object(properties, '$.data.payment_method'), 
            get_json_object(properties, '$.data.method_name')
        ) end AS selected_method,
 
        

        
--case when event_name = 'render:retry_modal' then 1 else 0 end  as retry_modal_loaded,
  
  case when event_name = 'behav:pay_clicked' and get_json_object(properties, '$.data.cod_visible') = 'true' then 1 else NULL end  as is_COD_visible,

        
  case when event_name = 'redirected_to_normal_flow' then 1 else 0 end  as redirected,
  
  case when event_name = 'render:quickbuy_instrument' then get_json_object(properties, '$.data.method') end  as initial_method,
  case when event_name = 'render:quickbuy_instrument' then get_json_object(properties, '$.data.instrument') end  as initial_instrument,
  
case when event_name = 'submit' and get_json_object(properties, '$.data.section') = 'p13n' and get_json_object(properties, '$.data.method') = 'cod' then 1 else NULL end  as is_quickbuy_cod_submit,
  
  
  
  get_json_object(properties,'$.data.meta.quickbuy_eligible') as quickbuy_eligible_flag,
  get_json_object(properties, '$.data.meta.v2_result') as v2_result,
  get_json_object(properties, '$.data.meta.v2_eligible') as v2_eligible,
  get_json_object(properties,'$.data.meta.is_quickbuy_flow') as is_quickbuy_flow
    
  FROM aggregate_pa.cx_1cc_events_dump_v1 a
    Left join aggregate_pa.cx_1cc_experiment_fact AS cx_1cc_experiment_fact 
    ON a.checkout_id = cx_1cc_experiment_fact.checkout_id
    WHERE
        a.producer_created_date BETWEEN date('{0}') and date('{1}') 

  and experiment_name = 'one_cc_quick_buy'
and a.merchant_id in {2}
--and a.merchant_id = 'JUHfXse0FDfnru'





),
  base as ( 
    SELECT
        session_id as checkout_id,
        experiment_value,
  producer_created_date,
  max(quickbuy_eligible_flag) as quickbuy_eligible_flag,
  max(redirected) as redirected,
       max(v2_result) as v2_result,
        max(v2_eligible) as v2_eligible,
        max(is_quickbuy_flow) as is_quickbuy_flow,
    max(initial_method) as initial_method,
    max(initial_instrument) as initial_instrument,
    max(is_quickbuy_cod_submit) as is_quickbuy_cod_submit,
    max(is_COD_visible) as is_COD_visible,
    max(payment_method) as submit_method,
    MAX(IF(event_name = 'open', 1, 0)) AS open,
  
  MAX(IF(event_name = 'render:1cc_summary_screen_loaded_completed', 1, 0)) AS summary_screen_loaded,
  
  MAX(IF(event_name = 'render:1cc_account_screen_bottom_sheet_loaded', 1, 0)) AS account_screen_loaded,

  MAX(IF(event_name = 'behav:1cc_summary_screen_continue_cta_clicked', 1, 0)) AS summary_screen_continue_cta_clicked,
  
  MAX(IF(event_name = 'change_method_clicked', 1, 0)) AS change_method_clicked,
    
    
  MAX(IF(event_name = 'behav:1cc_change_address_clicked', 1, 0)) AS change_address_clicked,
  
   
  MAX(IF(event_name = 'render:1cc_saved_shipping_address_screen_loaded', 1, 0)) AS saved_address_screen_loaded,
  
  MAX(IF(event_name = 'behav:1cc_saved_address_screen_continue_cta_clicked', 1, 0)) AS saved_address_cta_clicked,

  
  MAX(IF(event_name = 'render:1cc_payment_home_screen_loaded', 1, 0)) AS payment_screen_loaded,
        
  MAX(IF(lower(event_name) in ('render:1cc_cod_block', 'checkoutcodoptionshown' ) , 1, 0)) AS cod_shown,
        
  MAX(IF
       (
           (  
             (event_name = 'method:selected' or event_name =     
                 'behav:1cc_payment_home_screen_method_selected') 
          
               AND payment_method = 'cod'
           ) 
           OR lower(event_name) = 'behav:checkoutcodoptionselected'
           , 1, 0
       ) 
           ) AS cod_selected,
   
        MAX(IF(event_name = 'submit' AND payment_method = 'cod', 1, 0)) AS cod_submit,


        MAX(IF((event_name = 'method:selected' or event_name = 'behav:1cc_payment_home_screen_method_selected')  AND selected_method = 'upi', 1, 0)) AS upi_selected,

        MAX(IF(event_name = 'submit' AND payment_method = 'upi', 1, 0)) AS upi_submit,

   
        MAX(IF((event_name = 'method:selected' or event_name = 'behav:1cc_payment_home_screen_method_selected')  AND selected_method = 'card', 1, 0)) AS card_selected,
     
        MAX(IF(event_name = 'submit' AND payment_method = 'card', 1, 0)) AS card_submit,

     
        MAX(IF((event_name = 'method:selected' or event_name = 'behav:1cc_payment_home_screen_method_selected')  AND selected_method = 'emi', 1, 0)) AS emi_selected,
     
        MAX(IF(event_name = 'submit' AND payment_method = 'emi', 1, 0)) AS emi_submit,

     
        MAX(IF((event_name = 'method:selected' or event_name = 'behav:1cc_payment_home_screen_method_selected')  AND selected_method = 'netbanking', 1, 0)) AS netbanking_selected,
      
        MAX(IF(event_name = 'submit' AND payment_method = 'netbanking', 1, 0)) AS netbanking_submit,

        MAX(IF((event_name = 'method:selected' or event_name = 'behav:1cc_payment_home_screen_method_selected')  AND selected_method NOT IN ('cod', 'upi', 'card', 'emi', 'netbanking'), 1, 0)) AS other_prepaid_selected,
       
        
  MAX(IF(event_name = 'submit' AND selected_method NOT IN ('cod', 'upi', 'card', 'emi', 'netbanking'), 1, 0)) AS other_prepaid_submit,
  
  MAX(IF(event_name = 'submit', 1, 0)) AS submit,
  
    
    
  MAX(IF(event_name = 'behav:1cc_change_coupon_clicked', 1, 0)) AS change_coupon_clicked,
    
  MAX(IF(event_name = 'render:1cc_coupons_screen_loaded', 1, 0)) AS coupons_screen_loaded,
  
  MAX(IF(event_name = 'behav:1cc_delivery_option_selected', 1, 0)) AS delivery_option_selected,

  MAX(IF(event_name = 'render:1cc_account_screen_bottom_sheet_loaded', 1, 0)) AS account_screen_bottom_sheet_loaded,
  

  MAX(IF(event_name = 'render:retry_modal', 1, 0)) AS retry_modal_loaded
  
    
  FROM events_data
  
    GROUP BY 1,2,3
    
    )
    
    select experiment_value,
    base.producer_created_date,
    initial_method,
    initial_instrument,
    submit_method,
    change_method_clicked,

    sum(base.open) as open,
    
    sum(change_coupon_clicked) as change_coupon_clicked,
    sum(coupons_screen_loaded) as coupons_screen_loaded,
    sum(delivery_option_selected) as delivery_option_selected,
    sum(base.account_screen_bottom_sheet_loaded) as account_screen_bottom_sheet_loaded,
    sum(base.change_address_clicked) as change_address_clicked,
    
    
    sum(base.summary_screen_loaded) as summary_screen_loaded,
    sum(base.summary_screen_continue_cta_clicked) as summary_screen_continue_cta_clicked,
    sum(base.account_screen_loaded) as account_screen_loaded,
    sum(base.saved_address_screen_loaded) as saved_address_screen_loaded,
    sum(base.saved_address_cta_clicked) as saved_address_cta_clicked,
    sum(base.payment_screen_loaded) as payment_screen_loaded,
    sum(base.cod_shown) as cod_shown,
    sum(base.cod_selected) as cod_selected,
    sum(base.cod_submit) as cod_submit,
    sum(base.upi_selected) as upi_selected,
    sum(base.upi_submit) as upi_submit,
    sum(base.card_selected) as card_selected,
    sum(base.card_submit) as card_submit,
    sum(base.emi_selected) as emi_selected,
    sum(emi_submit) as emi_submit,
    sum(netbanking_selected) as netbanking_selected,
    sum(netbanking_submit) as netbanking_submit,
    sum(other_prepaid_selected) as other_prepaid_selected,
    sum(other_prepaid_submit) as other_prepaid_submit,
    sum(base.submit) as submit,
   Count(Distinct payments.id) AS payment_attempt,
 
 count(DISTINCT case when payments.authorized_at IS NOT NULL 
            OR LOWER(payments.method) = 'cod' then payments.id end) as payment_success,

    sum(base.retry_modal_loaded) as retry_modal_loaded,
    is_quickbuy_cod_submit,
    is_quickbuy_flow,
    base.is_COD_visible
            
    
    
    from base 
    LEFT JOIN 
(select * from realtime_hudi_api.payment_analytics  where created_date BETWEEN '{0}' and date'{1}'
--and merchant_id = 'JUHfXse0FDfnru'
and merchant_id in {2}



) as pa
    on base.checkout_id = pa.checkout_id
    
left join (select * from realtime_hudi_api.payments where payments.created_date BETWEEN '{0}' and '{1}'
           and merchant_id in {2}
            --and merchant_id = 'JUHfXse0FDfnru'
           
          
          
          ) AS payments
    on payments.id = pa.payment_id
    left join (select * from aggregate_pa.magic_checkout_fact where 
              producer_created_date BETWEEN date('{0}') and date('{1}') 
              and merchant_id in {2}
              --and merchant_id = 'JUHfXse0FDfnru'
              )as mcf
    on base.checkout_id = mcf.checkout_id
    where 
    quickbuy_eligible_flag = 'true'
    
and (mcf.library <> 'magic-x' or mcf.library is null)
and (mcf.is_magic_x <> 1 or mcf.is_magic_x is NULL)
and (mcf.checkout_integration_type <> 'x' or mcf.checkout_integration_type is null)
and base.v2_result = 'v2'
--and base.merchant_id = '4bnk7yysqr5Wx5'

    group by 1,2,3,4,5,6,36,37,38
    """.format(start_date, end_date,merchant_id_list)
)
funnel_query_df = funnel_query_db.toPandas()
funnel_query_df.head()

# COMMAND ----------

funnel_query_df['actual_flag'] = funnel_query_df.apply(
    lambda row: 'test_change_method_clicked' if row['experiment_value'] == 'variant_on' and row['is_quickbuy_flow'] == 'true' and row['change_method_clicked'] == 1 else 
                ('test' if row['experiment_value'] == 'variant_on' and row['is_quickbuy_flow'] == 'true' else 
                 ('control_change_method_clicked' if row['experiment_value'] == 'variant_on' and row['is_quickbuy_flow'] == 'false' and row['change_method_clicked'] == 1 else 
                  ('control' if row['experiment_value'] == 'variant_on' and row['is_quickbuy_flow'] == 'false' else row['experiment_value']))), axis=1
)
funnel_query_df.head()

# COMMAND ----------

{col: 'sum' for col in funnel_query_df.columns}

# COMMAND ----------

funnel_grouped_df = funnel_query_df.groupby(by=['actual_flag',], dropna=False).agg(
    {
        'open':'sum',
         'summary_screen_loaded': 'sum',
         'summary_screen_continue_cta_clicked': 'sum',
         'change_coupon_clicked': 'sum',
 'coupons_screen_loaded': 'sum',
 'delivery_option_selected': 'sum',
 'account_screen_bottom_sheet_loaded': 'sum',
 'change_address_clicked': 'sum',

 'account_screen_loaded': 'sum',
 'saved_address_screen_loaded': 'sum',
 'saved_address_cta_clicked': 'sum',
 'payment_screen_loaded': 'sum',
 'cod_shown': 'sum',
 'cod_selected': 'sum',
 'cod_submit': 'sum',
 'upi_selected': 'sum',
 'upi_submit': 'sum',
 'card_selected': 'sum',
 'card_submit': 'sum',
 'emi_selected': 'sum',
 'emi_submit': 'sum',
 'netbanking_selected': 'sum',
 'netbanking_submit': 'sum',
 'other_prepaid_selected': 'sum',
 'other_prepaid_submit': 'sum',
 'submit': 'sum',
 'payment_attempt': 'sum',
 'payment_success': 'sum',
 'retry_modal_loaded': 'sum',
 'is_COD_visible': 'sum',

    }
).reset_index()

# funnel_grouped_df['split_percentage'] = funnel_grouped_df['open']*1.0 / funnel_grouped_df['open'].sum()
# funnel_grouped_df['modal_cr'] = funnel_grouped_df['submit']*1.0 / funnel_grouped_df['open']
# funnel_grouped_df['success_rate'] = funnel_grouped_df['payment_success']*1.0 / funnel_grouped_df['payment_attempt']
# funnel_grouped_df['overall_cr'] = funnel_grouped_df['payment_success']*1.0 / funnel_grouped_df['open']

# pivoted_df =funnel_grouped_df.pivot( index=['actual_flag',] ,columns= 'change_method_clicked', 
#                                  values = ['open', 'summary_screen_loaded',
#       'payment_screen_loaded', 'submit', 'cod_shown', 'cod_submit',
#       'upi_submit', 'payment_attempt', 'payment_success','modal_cr','success_rate','overall_cr']).reset_index()
funnel_grouped_df

# COMMAND ----------

funnel_transposed_df = funnel_grouped_df.transpose()
funnel_transposed_df.reset_index(inplace=True)
funnel_transposed_df.columns = funnel_transposed_df.iloc[0].to_list()
funnel_transposed_df = funnel_transposed_df[1:]

funnel_transposed_df['control%'] =funnel_transposed_df['control']*1.0/ funnel_transposed_df.iloc[0]['control']
funnel_transposed_df['test%'] =funnel_transposed_df['test']*1.0/ funnel_transposed_df.iloc[0]['test']
funnel_transposed_df['test_change_method_clicked%'] =funnel_transposed_df['test_change_method_clicked']*1.0/ funnel_transposed_df.iloc[0]['test_change_method_clicked']
funnel_transposed_df['test_diff%'] = funnel_transposed_df['test%'] - funnel_transposed_df['control%']
funnel_transposed_df['test_change_method_clicked_diff%'] = funnel_transposed_df['test_change_method_clicked%'] - funnel_transposed_df['control%']
funnel_transposed_df.iloc[:,4:] = funnel_transposed_df.iloc[:,4:].applymap(percentage_conversion)
funnel_transposed_df


# COMMAND ----------

html_code += """<p>
<h2> Funnels </h2>
<h3>V2 High Level Funnel:</h3> <br/>"""
html_code += funnel_transposed_df.to_html(index=False,escape=False,  )
html_code += """</p>"""

# COMMAND ----------

html_code += """<h2>Appendix</h2>"""
html_code += """<h3>Unfiltered Complete data:</h3> <br/>"""
html_code += overall_df.to_html(index=False,escape=False,  )

# COMMAND ----------

# html_code += """<h2>Observations / Call outs </h2>"""
# html_code += """ <ol> <li>  OTP Submit rate is most likely low because the logic for trigger does not align with the V1 logic. Fix pending with Engineering since August 9. Slack thread: https://razorpay.slack.com/archives/C04N2HQCXQV/p1724828168587129?thread_ts=1722234598.643499&cid=C04N2HQCXQV </li>
# </ol>
# """

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

html_code += """
</body>
</html>
"""
# email_id = ['aravinth.pk@razorpay.com','pallavi.samodia@razorpay.com','revanth.m@razorpay.com','pranav.gupta@razorpay.com','kunal.vishnoi@razorpay.com', 'aakash.bhattacharjee@razorpay.com', 'sanjay.garg@razorpay.com', 'manjeet.singh@razorpay.com', 'kruthika.m@razorpay.com']
# 
#for i in email_id:
email_id=['manjeet.singh@razorpay.com']
          
          
send_emails(start_date,end_date,email_id,html_code)

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


