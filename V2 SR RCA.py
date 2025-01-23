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

# MAGIC %md
# MAGIC #SQL code for data
# MAGIC

# COMMAND ----------

raw_data_db = sqlContext.sql("""
WITH cte AS(
  SELECT
    a.checkout_id,
  max(ramp_up_date),
    max(
      get_json_object(properties, '$.data.meta.v2_result')
    ) AS v2_result,
   max(
      get_json_object(properties, '$.magicExperiments.checkout_redesign')
    ) AS experiment_result,
   max(get_json_object(properties,'$.data.meta.v2_eligible')) as v2_eligible
  
  FROM
    aggregate_pa.cx_1cc_events_dump_v1 a
  inner join batch_sheets.checkout_v2_rampup_sheet b on a.merchant_id = b.merchant_id
  WHERE
    producer_created_date BETWEEN date('2024-08-01') and date('2024-08-04') 
  and producer_created_date > date(ramp_up_date)
  and rampup_100_percent_flag = 'false'
  and blacklisted_flag = 'false'
  GROUP BY
    1
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
    producer_created_date BETWEEN date('2024-08-01') and date('2024-08-04') 
  and event_name = 'submit'



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
  inner join realtime_hudi_api.order_meta om on p.order_id = om.order_id
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
where  upi_payments_flat_fact.p_created_date between ('2024-08-01') and ('2024-08-04') 

  )  AS upi_payments_flat_fact on p.id = upi_payments_flat_fact.p_id
  where p.created_date between ('2024-08-01') and ('2024-08-04') 
  AND pa.created_date between ('2024-08-01') and ('2024-08-04') 
      AND om.created_date between ('2024-08-01') and ('2024-08-04') 
    
)
select 
cte.checkout_id,
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
where mcf.producer_created_date between date('2024-08-01') and date('2024-08-04') 
""")

raw_data_df = raw_data_db.toPandas()
raw_data_df.head()

# COMMAND ----------

# MAGIC %md
# MAGIC #Saving the file as csv for later
# MAGIC - If you need to use the data source but not run the above code again, please utilize below (especially using analytics cluster)

# COMMAND ----------

raw_data_df.to_csv('/dbfs/FileStore/raw_data_upi_df.csv', index=False)
"https://razorpay-dev.cloud.databricks.com/files/raw_data_upi_df.csv"

# COMMAND ----------

#raw_data_df = pd.read_csv('/dbfs/FileStore/raw_data_upi_df.csv')
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
        'payment_attempts':'nunique',
     'payment_success':'nunique',
    
     }
).reset_index()
grouped_df['split_percentage'] = grouped_df['checkout_id']*1.0 / grouped_df['checkout_id'].sum()
grouped_df['modal_cr'] = grouped_df['submit_checkout_id']*1.0 / grouped_df['checkout_id']
grouped_df['success_rate'] = grouped_df['payment_success']*1.0 / grouped_df['payment_attempts']
grouped_df['overall_cr'] = grouped_df['payment_success']*1.0 / grouped_df['checkout_id']

#grouped_df.pivot(  = 'v2_result', values = ['checkout_id','payment_attempts','payment_success','success_rate','overall_cr']).reset_index()
grouped_df

# COMMAND ----------

# MAGIC %md
# MAGIC ### Checking MID split % + Data Clean Up

# COMMAND ----------

grouped_df = raw_data_df[raw_data_df['v2_eligible']=='true'].groupby(by = ['v2_result','merchant_id'], dropna=False).agg(
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

mid_df[(mid_df.loc[:, pd.IndexSlice['checkout_id_split', 'v1']]==100.0) | (mid_df.loc[:, pd.IndexSlice['checkout_id_split', 'v1']]==0.0)  ].head(20)

# COMMAND ----------

final_df = raw_data_df[raw_data_df['merchant_id'].isin(final_mid_list)].reset_index(drop=True)
final_df

# COMMAND ----------

final_df.to_csv('/dbfs/FileStore/final_df.csv', index=False)
"https://razorpay-dev.cloud.databricks.com/files/final_df.csv"

# COMMAND ----------

final_df = pd.read_csv('/dbfs/FileStore/final_df.csv')
final_df.head()

# COMMAND ----------

# MAGIC %md
# MAGIC #L0 Metrics 
# MAGIC all analysis here on will be only for cases where V2 eligible = True

# COMMAND ----------

grouped_df = final_df[final_df['v2_eligible']=='true'].groupby(by = ['v2_result',], dropna=False).agg(
    { 'checkout_id':'nunique',
     'merchant_id':'nunique',
     'submit_checkout_id':'nunique',
        'payment_attempts':'nunique',
     'payment_success':'nunique',
    
     }
).reset_index()
grouped_df['split_percentage'] = grouped_df['checkout_id']*1.0 / grouped_df['checkout_id'].sum()
grouped_df['modal_cr'] = grouped_df['submit_checkout_id']*1.0 / grouped_df['checkout_id']
grouped_df['success_rate'] = grouped_df['payment_success']*1.0 / grouped_df['payment_attempts']
grouped_df['overall_cr'] = grouped_df['payment_success']*1.0 / grouped_df['checkout_id']

#grouped_df.pivot(  = 'v2_result', values = ['checkout_id','payment_attempts','payment_success','success_rate','overall_cr']).reset_index()
grouped_df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Date-wise L0 metrics

# COMMAND ----------

grouped_df = final_df[final_df['v2_eligible']=='true'].groupby(by = ['v2_result','producer_created_date'], dropna=False).agg(
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
grouped_df.pivot( index='producer_created_date' ,columns= 'v2_result', values = ['checkout_id','payment_attempts','payment_success','modal_cr','success_rate','overall_cr']).sort_values(by = 'producer_created_date',).reset_index()


# COMMAND ----------

# MAGIC %md
# MAGIC ## MID-wise L0 metrics

# COMMAND ----------

grouped_df = final_df[final_df['v2_eligible']=='true'].groupby(by = ['v2_result','merchant_id'], dropna=False).agg(
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

pivoted_df = grouped_df.pivot( index='merchant_id' ,columns= 'v2_result', values = ['checkout_id','modal_cr','success_rate','overall_cr']).sort_values(by = 'merchant_id',).reset_index()
pivoted_df['Total'] = pivoted_df[pivoted_df.columns[1:3]].sum(axis=1)
pivoted_df = pivoted_df.sort_values(by = 'Total', ascending=False).drop(columns=['Total'])
pivoted_df['modal_cr_diff'] = pivoted_df['modal_cr']['v2'] - pivoted_df['modal_cr']['v1']
pivoted_df['success_rate_diff'] = pivoted_df['success_rate']['v2'] - pivoted_df['success_rate']['v1']
pivoted_df['overall_cr_diff'] = pivoted_df['overall_cr']['v2'] - pivoted_df['overall_cr']['v1']
pivoted_df[pivoted_df.columns[3:]] = pivoted_df[pivoted_df.columns[3:]].applymap(percentage_conversion)
pivoted_df.head(20)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Browser Name

# COMMAND ----------

grouped_df = final_df[final_df['v2_eligible']=='true'].groupby(by = ['v2_result','browser_name'], dropna=False).agg(
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
pivoted_df[pivoted_df.columns[3:]] = pivoted_df[pivoted_df.columns[3:]].applymap(percentage_conversion)
pivoted_df.head(20)

# COMMAND ----------

# MAGIC %md
# MAGIC ## SR L1 : Payment Method

# COMMAND ----------

grouped_df = final_df[final_df['v2_eligible']=='true'].groupby(by = ['v2_result','payment_method'], dropna=False).agg(
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

grouped_df = final_df[(final_df['v2_eligible']=='true') & (final_df['payment_method']=='upi')].groupby(by = ['v2_result','gateway'], dropna=False).agg(
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
# MAGIC ## UPI SR: UPI Type 

# COMMAND ----------

grouped_df = final_df[(final_df['v2_eligible']=='true') & (final_df['payment_method']=='upi')].groupby(by = ['v2_result','upi_type'], dropna=False).agg(
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
# MAGIC ## UPI SR: App, Type, Gateway Information

# COMMAND ----------

grouped_df = final_df[(final_df['v2_eligible']=='true') & (final_df['payment_method']=='upi')].groupby(
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
# MAGIC #The End

# COMMAND ----------

grouped_df = raw_data_df[raw_data_df['v2_eligible']==True].groupby(by = ['v2_result',]).agg(
    {'checkout_id':'nunique',
        'payment_attempts':'nunique',
     'payment_success':'nunique',
   
     }
).reset_index()
grouped_df['success_rate'] = grouped_df['payment_success']*1.0 / grouped_df['payment_attempts']
grouped_df['overall_cr'] = grouped_df['payment_success']*1.0 / grouped_df['checkout_id']
grouped_df

# COMMAND ----------

raw_data_df.columns

# COMMAND ----------

method_df = raw_data_df[raw_data_df['v2_eligible']=='true'].groupby(by = ['v2_result','payment_method']).agg(
    {'checkout_id':'nunique',
        'payment_attempts':'nunique',
     'payment_success':'nunique',
   
     }
).reset_index()
method_df['success_rate'] = method_df['payment_success']*1.0 / method_df['payment_attempts']
method_df['overall_cr'] = method_df['payment_success']*1.0 / method_df['checkout_id']
method_df.pivot(index =['payment_method'] , columns = 'v2_result', values = ['checkout_id','payment_attempts','payment_success','success_rate','overall_cr']).reset_index()

# COMMAND ----------

upi_df = raw_data_df[(raw_data_df['v2_eligible']=='true') & (raw_data_df['payment_method']=='upi')].groupby(by = ['v2_result','producer_created_date']).agg(
    {'checkout_id':'nunique',
        'payment_attempts':'nunique',
     'payment_success':'nunique',
   
     }
).reset_index()
upi_df['success_rate'] = upi_df['payment_success']*1.0 / upi_df['payment_attempts']
upi_df['overall_cr'] = upi_df['payment_success']*1.0 / upi_df['checkout_id']
upi_df.pivot(index =['producer_created_date'] , columns = 'v2_result', values = ['payment_attempts','payment_success','success_rate',]).sort_values(by = 'producer_created_date',).reset_index()

# COMMAND ----------

upi_axis_df = raw_data_df[(raw_data_df['v2_eligible']==True) & (raw_data_df['payment_method']=='upi')
                          & (raw_data_df['gateway']=='upi_axis')
                          ].groupby(by = ['v2_result','producer_created_date']).agg(
    {'checkout_id':'nunique',
        'payment_attempts':'nunique',
     'payment_success':'nunique',
   
     }
).reset_index()
upi_axis_df['success_rate'] = upi_axis_df['payment_success']*1.0 / upi_axis_df['payment_attempts']
upi_axis_df['overall_cr'] = upi_axis_df['payment_success']*1.0 / upi_axis_df['checkout_id']
upi_axis_df.pivot(index =['producer_created_date'] , columns = 'v2_result', values = ['payment_attempts','payment_success','success_rate',]).sort_values(by = 'producer_created_date',).reset_index()

# COMMAND ----------

upi_airtel_df = raw_data_df[(raw_data_df['v2_eligible']==True) & (raw_data_df['payment_method']=='upi')
                          & (raw_data_df['gateway']=='upi_airtel')
                          ].groupby(by = ['v2_result','producer_created_date']).agg(
    {'checkout_id':'nunique',
        'payment_attempts':'nunique',
     'payment_success':'nunique',
   
     }
).reset_index()
upi_airtel_df['success_rate'] = upi_airtel_df['payment_success']*1.0 / upi_airtel_df['payment_attempts']
upi_airtel_df['overall_cr'] = upi_airtel_df['payment_success']*1.0 / upi_airtel_df['checkout_id']
upi_airtel_df.pivot(index =['producer_created_date'] , columns = 'v2_result', values = ['payment_attempts','payment_success','success_rate',]).sort_values(by = 'producer_created_date',).reset_index()

# COMMAND ----------

pupi_gateway_df = raw_data_df[(raw_data_df['v2_eligible']=='true') & (raw_data_df['payment_method']=='upi')].groupby(by = ['v2_result','gateway']).agg(
    {'checkout_id':'nunique',
        'payment_attempts':'nunique',
     'payment_success':'nunique',
   
     }
).reset_index()
upi_gateway_df['success_rate'] = upi_gateway_df['payment_success']*1.0 / upi_gateway_df['payment_attempts']
upi_gateway_df['overall_cr'] = upi_gateway_df['payment_success']*1.0 / upi_gateway_df['checkout_id']
upi_gateway_df.pivot(index =['gateway'] , columns = 'v2_result', values = ['payment_attempts','payment_success','success_rate',]).sort_values(by = 'gateway',).reset_index()

# COMMAND ----------

upi_gateway_app_df = raw_data_df[(raw_data_df['v2_eligible']==True) & (raw_data_df['payment_method']=='upi')].groupby(by = ['v2_result','gateway','upi_app']).agg(
    {'checkout_id':'nunique',
        'payment_attempts':'nunique',
     'payment_success':'nunique',
   
     }
).reset_index()
upi_gateway_app_df['success_rate'] = upi_gateway_app_df['payment_success']*1.0 / upi_gateway_app_df['payment_attempts']
upi_gateway_app_df.pivot(index =['gateway','upi_app'] , columns = 'v2_result', values = ['payment_attempts','payment_success','success_rate',]).sort_values(by = 'gateway',).reset_index()

# COMMAND ----------

raw_data_df.columns

# COMMAND ----------

upi_rzpapb_others_df = raw_data_df[(raw_data_df['v2_eligible']=='true') 
                                 & (raw_data_df['payment_method']=='upi')
                                 & (raw_data_df['gateway']=='upi_rzpapb')
                                 & (raw_data_df['upi_app']=='Others')
                                 ].groupby(by = ['v2_result','upi_meta_app','upi_provider']).agg(
    {'checkout_id':'nunique',
        'payment_attempts':'nunique',
     'payment_success':'nunique',
   
     }
).reset_index()
upi_rzpapb_others_df['success_rate'] = upi_rzpapb_others_df['payment_success']*1.0 / upi_rzpapb_others_df['payment_attempts']
upi_rzpapb_others_df.pivot(index =['upi_meta_app','upi_provider'] , columns = 'v2_result', values = ['payment_attempts','payment_success','success_rate',]).sort_values(by = 'upi_meta_app',).reset_index()

# COMMAND ----------

upi_axis_error_df = raw_data_df[(raw_data_df['v2_eligible']==True) 
                            & (raw_data_df['payment_method']=='upi')
                            & (raw_data_df['gateway']=='upi_axis')
                            & (raw_data_df['merchant_id']=='IH7E2OJQGEKKTN')
                            ].groupby(by = ['v2_result','internal_error_code']).agg(
    {'checkout_id':'nunique',
        'payment_attempts':'nunique',
     'payment_success':'nunique',
   
     }
).reset_index()
upi_axis_error_df['success_rate'] = upi_axis_error_df['payment_success']*1.0 / upi_axis_error_df['payment_attempts']
upi_axis_error_df['overall_cr'] = upi_axis_error_df['payment_success']*1.0 / upi_axis_error_df['checkout_id']
upi_axis_error_df.pivot(index =['internal_error_code'] , columns = 'v2_result', values = ['payment_attempts','payment_success','success_rate',]).sort_values(by = 'internal_error_code',).reset_index()

# COMMAND ----------

upi_rzpapb_df = raw_data_df[(raw_data_df['v2_eligible']=='true') 
                            & (raw_data_df['payment_method']=='upi')
                            & (raw_data_df['gateway']=='upi_rzpapb')
                            ].groupby(by = ['v2_result','internal_error_code']).agg(
    {'checkout_id':'nunique',
        'payment_attempts':'nunique',
     'payment_success':'nunique',
   
     }
).reset_index()
upi_rzpapb_df['success_rate'] = upi_rzpapb_df['payment_success']*1.0 / upi_rzpapb_df['payment_attempts']
upi_rzpapb_df['overall_cr'] = upi_rzpapb_df['payment_success']*1.0 / upi_rzpapb_df['checkout_id']
upi_rzpapb_df.pivot(index =['internal_error_code'] , columns = 'v2_result', values = ['payment_attempts','payment_success','success_rate',]).sort_values(by = 'internal_error_code',).reset_index()

# COMMAND ----------

card_df = raw_data_df[(raw_data_df['v2_eligible']=='true') & (raw_data_df['payment_method']=='card')].groupby(by = ['v2_result','producer_created_date',]).agg(
    {'checkout_id':'nunique',
        'payment_attempts':'nunique',
     'payment_success':'nunique',
   
     }
).reset_index()
card_df['success_rate'] = card_df['payment_success']*1.0 / card_df['payment_attempts']
card_df['overall_cr'] = card_df['payment_success']*1.0 / card_df['checkout_id']
card_df.pivot(index =['producer_created_date',] , columns = 'v2_result', values = ['checkout_id','payment_attempts','payment_success','success_rate','overall_cr']).sort_values(by = 'producer_created_date',ascending=False).reset_index()

# COMMAND ----------

card_gateway_df = raw_data_df[(raw_data_df['v2_eligible']=='true') & (raw_data_df['payment_method']=='card')].groupby(by = ['v2_result','gateway']).agg(
    {'checkout_id':'nunique',
        'payment_attempts':'nunique',
     'payment_success':'nunique',
   
     }
).reset_index()
card_gateway_df['success_rate'] = card_gateway_df['payment_success']*1.0 / card_gateway_df['payment_attempts']
card_gateway_df['overall_cr'] = card_gateway_df['payment_success']*1.0 / card_gateway_df['checkout_id']
card_gateway_df.pivot(index =['gateway'] , columns = 'v2_result', values = ['checkout_id','payment_attempts','payment_success','success_rate','overall_cr']).sort_values(by = 'gateway',ascending=False).reset_index()

# COMMAND ----------

card_cybersource_df = raw_data_df[(raw_data_df['v2_eligible']=='true')
                                   & (raw_data_df['payment_method']=='card')
                                   & (raw_data_df['gateway']=='cybersource')
                                   ].groupby(by = ['v2_result','internal_error_code']).agg(
    {'checkout_id':'nunique',
        'payment_attempts':'nunique',
     'payment_success':'nunique',
   
     }
).reset_index()
card_cybersource_df['success_rate'] = card_cybersource_df['payment_success']*1.0 / card_cybersource_df['payment_attempts']
card_cybersource_df['overall_cr'] = card_cybersource_df['payment_success']*1.0 / card_cybersource_df['checkout_id']
card_cybersource_df.pivot(index =['internal_error_code'] , columns = 'v2_result', values = ['payment_attempts','payment_success','success_rate',]).sort_values(by = 'internal_error_code',ascending=False).reset_index()

# COMMAND ----------

raw_data_df.dtypes

# COMMAND ----------

card_df = raw_data_df[(raw_data_df['v2_eligible']=='true') & (raw_data_df['payment_method']=='card') ].groupby(by = ['v2_result','producer_created_date','merchant_id']).agg(
    {'checkout_id':'nunique',
        'payment_attempts':'nunique',
     'payment_success':'nunique',
   
     }
).reset_index()
card_df = card_df.sort_values(by='checkout_id',ascending=False)
card_df['success_rate'] = card_df['payment_success']*1.0 / card_df['payment_attempts']
card_df['overall_cr'] = card_df['payment_success']*1.0 / card_df['checkout_id']
card_pivot = card_df.pivot(index =['producer_created_date','merchant_id'] , columns = 'v2_result', values = ['checkout_id','payment_attempts','payment_success','success_rate','overall_cr']).reset_index()
card_pivot.head()

# COMMAND ----------

card_pivot.to_csv('/dbfs/FileStore/card_pivot.csv', index=False)
"https://razorpay-dev.cloud.databricks.com/files/card_pivot.csv"

# COMMAND ----------

upi_df = raw_data_df[(raw_data_df['v2_eligible']=='true') & (raw_data_df['payment_method']=='upi')].groupby(by = ['v2_result','producer_created_date']).agg(
    {'checkout_id':'nunique',
        'payment_attempts':'nunique',
     'payment_success':'nunique',
   
     }
).reset_index()
upi_df['success_rate'] = upi_df['payment_success']*1.0 / upi_df['payment_attempts']
upi_df['overall_cr'] = upi_df['payment_success']*1.0 / upi_df['checkout_id']
upi_df.pivot(index =['producer_created_date'] , columns = 'v2_result', values = ['checkout_id','payment_attempts','payment_success','success_rate','overall_cr']).sort_values(by = 'producer_created_date',).reset_index()

# COMMAND ----------

# MAGIC %md
# MAGIC #Checking for UPI Switch

# COMMAND ----------

upi_switch_merchants_db = sqlContext.sql("""
                                         select * from aggregate_pa.upi_switch_merchants
                                         """)
upi_switch_merchants_df = upi_switch_merchants_db.toPandas()
upi_switch_merchants_df.head()

# COMMAND ----------

upi_switch_merchants_df['upi_switch_active'] = 'yes'
upi_switch_merchants_df.rename(columns={'switch_mid':'merchant_id'}, inplace=True)
upi_switch_merchants_df.head()

# COMMAND ----------

upi_mid_df = raw_data_df[(raw_data_df['v2_eligible']==True) & (raw_data_df['payment_method']=='upi')].groupby(by = ['v2_result','merchant_id','gateway']).agg(
    {'checkout_id':'nunique',
        'payment_attempts':'nunique',
     'payment_success':'nunique',
   
     }
).reset_index()
upi_mid_df['success_rate'] = upi_mid_df['payment_success']*1.0 / upi_mid_df['payment_attempts']
upi_mid_df['overall_cr'] = upi_mid_df['payment_success']*1.0 / upi_mid_df['checkout_id']
upi_mid_final_df = upi_mid_df.pivot(index =['merchant_id','gateway'] , columns = 'v2_result', values = ['payment_attempts','payment_success','success_rate',]).sort_values(by = 'merchant_id',).reset_index()
upi_mid_final_df.head(20)

# COMMAND ----------

upi_mid_final_df.columns = upi_mid_final_df.columns.map('{0[0]}_{0[1]}'.format)
upi_mid_final_df.rename(columns={'merchant_id_':'merchant_id','gateway_':'gateway'},inplace=True)
upi_mid_final_df.head()

# COMMAND ----------

upi_mid_df = upi_mid_final_df.merge(upi_switch_merchants_df, how='left',on='merchant_id')
upi_mid_df.head()

# COMMAND ----------

upi_mid_df.pivot(index=['merchant_id',], columns = 'gateway', values = ['payment_attempts_v1','payment_attempts_v2','payment_success_v1','payment_success_v2','success_rate_v1','success_rate_v2',])

# COMMAND ----------

upi_mid_df[(upi_mid_df['gateway']=='upi_rzpapb') |(upi_mid_df['gateway']=='upi_rzpapb')  ]['merchant_id']

# COMMAND ----------

api_apb_mid_df = upi_mid_df[(upi_mid_df['gateway']=='upi_rzpapb') |(upi_mid_df['gateway']=='upi_axis') ]
api_apb_mid_df['sr_delta'] = api_apb_mid_df['success_rate_v2'] - api_apb_mid_df['success_rate_v1']
api_apb_mid_df.head()

# COMMAND ----------

fireboltt_errors.to_csv('/dbfs/FileStore/fireboltt_errors.csv', index=False)
"https://razorpay-dev.cloud.databricks.com/files/fireboltt_errors.csv"

# COMMAND ----------

fireboltt_errors = raw_data_df[(raw_data_df['v2_eligible']==True) 
            & (raw_data_df['payment_method']=='upi')
            & (raw_data_df['gateway']=='upi_axis')
            & (raw_data_df['merchant_id']=='IH7E2OJQGEKKTN')
            ].groupby(by = ['v2_result','internal_error_code']).agg(
    {'checkout_id':'nunique',
        'payment_attempts':'nunique',
     'payment_success':'nunique',
   
     }, dropna=False
).reset_index()
fireboltt_errors['success_rate'] = fireboltt_errors['payment_success']*1.0 / fireboltt_errors['payment_attempts']

fireboltt_errors = fireboltt_errors.pivot(index =['internal_error_code'] , columns = 'v2_result', values = ['payment_attempts','payment_success','success_rate',]).sort_values(by = 'internal_error_code',).reset_index()
fireboltt_errors.head(20)

# COMMAND ----------

raw_data_df[raw_data_df['payment_method']=='upi'].groupby(by=['upi_type','upi_app'], dropna=False).agg({'payment_attempts': 'nunique', 'payment_success': 'nunique'}).reset_index()

# COMMAND ----------

raw_data_df['payment_attempt_flag'] =  np.where(raw_data_df['payment_attempts'].isna(), 0, 1)
raw_data_df['payment_success_flag'] =  np.where(raw_data_df['payment_success'].isna(), 0, 1)

# COMMAND ----------



# COMMAND ----------

raw_data_df.to_csv('/dbfs/FileStore/raw_data_df.csv', index=False)
"https://razorpay-dev.cloud.databricks.com/files/raw_data_df.csv"

# COMMAND ----------

raw_data_df.groupby(by=['payment_method','v2_result']).agg({'payment_attempts':'nunique'})

# COMMAND ----------

raw_data_df.columns

# COMMAND ----------

overall_df = raw_data_df.groupby(by=['v2_result','v2_eligible','platform','browser_major_version', 'browser_name', 'os_brand_family',
       'os_major_version','open', 'summary_screen_loaded',
       'summary_screen_continue_cta_clicked','payment_home_screen_loaded','submit'], dropna=False).agg(
  {'checkout_id':'nunique','payment_attempts':'nunique','payment_success':'nunique'}
).reset_index()
overall_df.head()

# COMMAND ----------

print('Opens')
raw_data_df[(raw_data_df['v2_eligible']==True) & (raw_data_df['open']==1)].groupby(by='v2_result').agg({'checkout_id':'nunique'}).reset_index()

# COMMAND ----------

print('summary_screen_loaded')
raw_data_df[(raw_data_df['v2_eligible']=='true') & (raw_data_df['summary_screen_loaded']==1)].groupby(by='v2_result').agg({'checkout_id':'nunique'}).reset_index()

# COMMAND ----------

print('summary_screen_continue_cta_clicked')
raw_data_df[(raw_data_df['v2_eligible']=='true') & (raw_data_df['summary_screen_continue_cta_clicked']==1)].groupby(by='v2_result').agg({'checkout_id':'nunique'}).reset_index()

# COMMAND ----------

print('payment_home_screen_loaded')
raw_data_df[(raw_data_df['v2_eligible']=='true') & (raw_data_df['payment_home_screen_loaded']==1)].groupby(by='v2_result').agg({'checkout_id':'nunique'}).reset_index()

# COMMAND ----------

print('submit')
raw_data_df[(raw_data_df['v2_eligible']=='true') & (raw_data_df['submit']==1)].groupby(by='v2_result').agg({'checkout_id':'nunique'}).reset_index()

# COMMAND ----------

print('payment attempts')
raw_data_df[(raw_data_df['v2_eligible']=='true')].groupby(by='v2_result').agg({'payment_attempts':'nunique'}).reset_index()

# COMMAND ----------

print('payment_success')
raw_data_df[(raw_data_df['v2_eligible']=='true')].groupby(by='v2_result').agg({'payment_success':'nunique'}).reset_index()

# COMMAND ----------

print('method-wise SR RCA')
raw_data_df[(raw_data_df['v2_eligible']=='true')].groupby(by='v2_result').agg({'payment_attempts':'nunique', 'payment_success':'nunique'}).reset_index()

# COMMAND ----------

 'payment_method','submit','upi_type','upi_app'

# COMMAND ----------

grouped_df = raw_data_df[raw_data_df['merchant_id']!='IH7E2OJQGEKKTN'].groupby(by=['v2_result','v2_eligible','platform','browser_major_version', 'browser_name', 'os_brand_family',
       'os_major_version','payment_home_screen_loaded', 'payment_method','submit','upi_type','upi_app'], dropna=False).agg(
  {'checkout_id':'nunique','payment_attempts':'nunique','payment_success':'nunique'}
).reset_index()
grouped_df.head()

# COMMAND ----------

raw_data_df.groupby(by=['v2_result','v2_eligible','platform']).agg(
  {'checkout_id':'nunique'}
).reset_index()

# COMMAND ----------

grouped_df.to_csv('/dbfs/FileStore/grouped_df.csv', index=False)
"https://razorpay-dev.cloud.databricks.com/files/grouped_df.csv"

# COMMAND ----------

grouped3_df = raw_data_df.groupby(by=['payment_method'], dropna=False).agg(
  {'checkout_id':'nunique','payment_attempts':'nunique','payment_success':'nunique'}
).reset_index()
grouped3_df.head()

# COMMAND ----------

grouped4_df = raw_data_df.groupby(by=['v2_result','v2_eligible','platform','browser_major_version', 'browser_name', 'os_brand_family','merchant_id',
       'os_major_version','payment_home_screen_loaded', 'payment_method','submit','upi_type','upi_app'], dropna=False).agg(
  {'checkout_id':'nunique','payment_attempts':'nunique','payment_success':'nunique'}
).reset_index()
grouped4_df.head()

# COMMAND ----------

pivot = raw_data_df[(raw_data_df['v2_eligible']==True) 
                   # & (raw_data_df['merchant_id']=='IH7E2OJQGEKKTN')
                    ].groupby(by=['v2_result','payment_method','merchant_id','upi_type','upi_app'],dropna=False).agg({'checkout_id':'nunique','payment_attempts':'nunique','payment_success':'nunique'}).reset_index()


# COMMAND ----------

pivot['overall_cr'] = pivot['payment_success'] * 1.0 / pivot['payment_attempts']
pivot.head(100)

# COMMAND ----------

0.251659 - 0.249918


# COMMAND ----------

upi_pivot = grouped4_df[(grouped4_df['payment_method']=='upi') & (grouped4_df['v2_eligible']==True) 
                        #& (grouped4_df['merchant_id']!='IH7E2OJQGEKKTN')
                        
                        ].pivot_table(
values=['payment_attempts','payment_success'],
index=['upi_type','upi_app']
, columns=['v2_result'],
aggfunc='sum',
dropna=False
).reset_index()
upi_pivot.columns = ['_'.join(col).strip() for col in upi_pivot.columns.values]
upi_pivot['payment_attempts_total'] = upi_pivot['payment_attempts_v1']+upi_pivot['payment_attempts_v2']
upi_pivot['payment_success_total'] = upi_pivot['payment_success_v1']+upi_pivot['payment_success_v2']

upi_pivot = upi_pivot.sort_values(by='payment_attempts_total', ascending=False)
upi_pivot.head()

# COMMAND ----------

grouped4_df

# COMMAND ----------


upi_pivot['sr_v1'] = upi_pivot['payment_success_v1']*1.0/upi_pivot['payment_attempts_v1']
upi_pivot['sr_v2'] = upi_pivot['payment_success_v2']*1.0/upi_pivot['payment_attempts_v2']
upi_pivot['sr_total'] = upi_pivot['payment_success_total']*1.0/upi_pivot['payment_attempts_total']
upi_pivot['vol_v1'] = upi_pivot['payment_attempts_v1']*1.0/upi_pivot['payment_attempts_v1'].sum()
upi_pivot['vol_v2'] = upi_pivot['payment_attempts_v2']*1.0/upi_pivot['payment_attempts_v2'].sum()
upi_pivot['vol_impact'] = (upi_pivot['vol_v2']-upi_pivot['vol_v1'])*upi_pivot['sr_v1']
upi_pivot['sr_impact'] = (upi_pivot['sr_v2']-upi_pivot['sr_v1'])*upi_pivot['vol_v2']
upi_pivot['total_impact'] = (upi_pivot['sr_v2']*upi_pivot['vol_v2']) - (upi_pivot['sr_v1']*upi_pivot['vol_v1'])
upi_pivot['sr_delta'] = upi_pivot['sr_v2']-upi_pivot['sr_v1']
upi_pivot.head()

# COMMAND ----------

upi_pivot.to_csv('/dbfs/FileStore/upi_pivot.csv', index=False)
"https://razorpay-dev.cloud.databricks.com/files/upi_pivot.csv"

# COMMAND ----------

grouped2_df = raw_data_df.groupby(by=['v2_result','v2_eligible','platform','submit_instrument','checkout_method','payment_method','payment_attempt_flag','payment_success_flag',], dropna=False).agg(
  {'checkout_id':'nunique'}
).reset_index()
grouped2_df.head()



# COMMAND ----------

raw_data_df['payment_attempts'].nunique()

# COMMAND ----------

grouped2_df.to_csv('/dbfs/FileStore/pivot_view.csv', index=False)
"https://razorpay-dev.cloud.databricks.com/files/pivot_view.csv"

# COMMAND ----------

grouped3_df = raw_data_df.groupby(by=['v2_result','v2_eligible','platform','browser','payment_method','upi_type','upi_app',], dropna=False).agg(
  {'payment_attempts':'nunique', 'payment_success':'nunique',}
).reset_index()
grouped3_df.head()



# COMMAND ----------

grouped3_df.to_csv('/dbfs/FileStore/grouped3_df.csv', index=False)
"https://razorpay-dev.cloud.databricks.com/files/grouped3_df.csv"

# COMMAND ----------

grouped3_df['payment_attempts'].sum()

# COMMAND ----------

cod_df = raw_data_df[raw_data_df['payment_method']=='cod'].groupby(by=['v2_result','v2_eligible','platform','merchant_id','payment_home_screen_loaded','submit'], dropna=False).agg(
  {'payment_attempts':'nunique', 'payment_success':'nunique',}
).reset_index()
cod_df.head()

# COMMAND ----------

cod_df.to_csv('/dbfs/FileStore/cod_df.csv', index=False)
"https://razorpay-dev.cloud.databricks.com/files/cod_df.csv"

# COMMAND ----------

raw_data_df.columns

# COMMAND ----------

upi_intent_df = raw_data_df[(raw_data_df['payment_method']=='upi')&(raw_data_df['upi_type']=='Intent')].groupby(by=['v2_result','v2_eligible','platform','merchant_id','upi_app','gateway','internal_error_code','producer_created_date'], dropna=False).agg(
  {'payment_attempts':'nunique', 'payment_success':'nunique',}
).reset_index()
upi_intent_df.head()


# COMMAND ----------

upi_pivot = upi_intent_df[upi_intent_df['v2_eligible']=='true'].pivot_table(
values=['payment_attempts','payment_success'],
index=['producer_created_date','upi_app','gateway','internal_error_code']
, columns=['v2_result'],
aggfunc='sum',
dropna=False
).reset_index()
upi_pivot.columns = ['_'.join(col).strip() for col in upi_pivot.columns.values]
upi_pivot['payment_attempts_total'] = upi_pivot['payment_attempts_v1']+upi_pivot['payment_attempts_v2']
upi_pivot['payment_attempts_v1_split'] = upi_pivot['payment_attempts_v1']*1.0/upi_pivot['payment_attempts_v1'].sum()
upi_pivot['payment_attempts_v2_split'] = upi_pivot['payment_attempts_v2']*1.0/upi_pivot['payment_attempts_v2'].sum()
upi_pivot['vol_delta'] = upi_pivot['payment_attempts_v2_split']-upi_pivot['payment_attempts_v1_split']
upi_pivot['abs_vol_delta'] = upi_pivot['vol_delta'].abs()


# COMMAND ----------

(upi_pivot.sort_values(by='abs_vol_delta', ascending=False)).head(20)

# COMMAND ----------

upi_pivot.to_csv('/dbfs/FileStore/upi_pivot_intent.csv', index=False)
"https://razorpay-dev.cloud.databricks.com/files/upi_pivot_intent.csv"

# COMMAND ----------


pivot_view = raw_data_df.pivot_table(
values=['checkout_id'],
index=['submit_instrument','checkout_method','payment_method','payment_attempt_flag','payment_success_flag','platform']
, columns=['v2_result','v2_eligible'],
aggfunc='nunique',
dropna=False
).reset_index()
pivot_view.head()

# COMMAND ----------

# MAGIC %md
# MAGIC #COD RCA

# COMMAND ----------

# MAGIC %sql 
# MAGIC describe aggregate_pa.magic_rto_reimbursement_fact

# COMMAND ----------

# MAGIC %sql
# MAGIC describe

# COMMAND ----------

rto_df = sqlContext.sql(
    """
    select 
    
   
    """
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # UPI SR DEEP DIVE
# MAGIC

# COMMAND ----------

spark.sql("REFRESH TABLE aggregate_pa.magic_checkout_fact")

# COMMAND ----------

raw_data_db = sqlContext.sql("""
WITH cte AS(
  SELECT
    a.checkout_id,
  ramp_up_date,
    max(
      get_json_object(properties, '$.data.meta.v2_result')
    ) AS v2_result,
   max(get_json_object(properties,'$.data.meta.v2_eligible')) as v2_eligible
  
  FROM
    aggregate_pa.cx_1cc_events_dump_v1 a
  inner join batch_sheets.checkout_v2_rampup_sheet b on a.merchant_id = b.merchant_id
  WHERE
    producer_created_date BETWEEN date('2024-07-01')
    AND date('2024-07-07')
  and producer_created_date > date(ramp_up_date)
  and rampup_100_percent_flag = 'false'
  and blacklisted_flag = 'false'
  GROUP BY
    1,2
),

submits as(

select 
   checkout_id,
  
  coalesce(
  get_json_object(properties,'$.data.method'),
    get_json_object(properties,'$.data.data.method')
  ) as method
    FROM
    aggregate_pa.cx_1cc_events_dump_v1 a
  WHERE
    producer_created_date BETWEEN date('2024-07-01')
    AND date('2024-07-07')
  and event_name = 'submit'



),
cte2 AS(
select 
  p.id,
  p.method, 
p.authorized_at,
  checkout_id,
  internal_error_code
  from realtime_hudi_api.payments p
  inner join realtime_hudi_api.payment_analytics pa on p.id = pa.payment_id
  inner join realtime_hudi_api.order_meta om on p.order_id = om.order_id
  where p.created_date BETWEEN ('2024-07-01')
    AND ('2024-07-07')
  AND pa.created_date BETWEEN ('2024-07-01')
    AND ('2024-07-07')
      AND om.created_date BETWEEN ('2024-07-01')
    AND ('2024-07-07')
)
select 
cte.checkout_id,
merchant_id,
open,
summary_screen_loaded,
summary_screen_continue_cta_clicked,
submit, 
cte.v2_result,
cte.v2_eligible,
submits.method as checkout_method,
cte2.method as payment_method,
id as payment_attempts,
case when authorized_at is not null or cte2.method='cod' then id else null end as payment_success,
  cte2.internal_error_code,
  mcf.browser_name,
  mcf.os_brand_family
from cte 
left join cte2 on cte.checkout_id = cte2.checkout_id
left join submits on cte.checkout_id = submits.checkout_id
left join aggregate_pa.magic_checkout_fact mcf on cte.checkout_id = mcf.checkout_id
""")

raw_data_df = raw_data_db.toPandas()
raw_data_df.head()

# COMMAND ----------

raw_data_df[~raw_data_df['payment_attempts'].isna()]

# COMMAND ----------


sr_grouped_df = raw_data_df[~raw_data_df['payment_attempts'].isna()].groupby(by=[ 'v2_result',
      'merchant_id', 'browser_name', 'os_brand_family', 'payment_method','internal_error_code',
      # 'v2_eligible',
 ], dropna=False).agg({'payment_attempts':'nunique','payment_success':'nunique'}).reset_index()
sr_grouped_df.head()

# COMMAND ----------


print(raw_data_df['payment_attempts'].nunique())
print(sr_grouped_df['payment_attempts'].sum())

# COMMAND ----------

sr_grouped_df.columns[2:-2]

# COMMAND ----------

# Correcting the way column names are set by ensuring all elements are strings
v2_eligible_set.columns = [''.join(str(col_part).strip() for col_part in col) for col in v2_eligible_set.columns.values]

# Renaming columns as before
v2_eligible_set.rename(columns={'payment_attemptsv1':'openPrev', 'payment_attemptsv2':'openPost',
                                'payment_successv1':'submitPrev', 'payment_successv2':'submitPost',
                                }, inplace=True)

# Calculating Post_CR and Pre_CR as before
v2_eligible_set['Post_CR'] = v2_eligible_set['submitPost']*1.0/ v2_eligible_set['openPost']
v2_eligible_set['Pre_CR'] = v2_eligible_set['submitPrev']*1.0 / v2_eligible_set['openPrev']

# COMMAND ----------

v2_eligible_set.head()

# COMMAND ----------

# Use this set later if needed just for V2 eligible
#v2_eligible_set.to_csv('/dbfs/FileStore/v2_eligible_set.csv', index=False)
#"https://razorpay-dev.cloud.databricks.com/files/v2_eligible_set.csv"

# COMMAND ----------

v2_eligible_set.drop(columns=['payment_attemptsnan','payment_successnan'], inplace=True)

# COMMAND ----------

v2_eligible_set.to_csv('/dbfs/FileStore/v2_complete_set.csv', index=False)
"https://razorpay-dev.cloud.databricks.com/files/v2_complete_set.csv"

# COMMAND ----------

upi_v2_set = v2_eligible_set[v2_eligible_set['payment_method']=='upi'].groupby('internal_error_code').agg({'openPrev':'sum',
                                                                                              'openPost':'sum',
                                                                                    }).reset_index()
upi_v2_set.head(10)                                                                                         

# COMMAND ----------

upi_v2_set.to_csv('/dbfs/FileStore/upi_v2_set.csv', index=False)
"https://razorpay-dev.cloud.databricks.com/files/upi_v2_set.csv"

# COMMAND ----------

card_v2_set = v2_eligible_set[v2_eligible_set['payment_method']=='card'].groupby('internal_error_code').agg({'openPrev':'sum',
                                                                                              'openPost':'sum',
                                                                                    }).reset_index()
card_v2_set.head(10)  

# COMMAND ----------

card_v2_set.to_csv('/dbfs/FileStore/card_v2_set.csv', index=False)
"https://razorpay-dev.cloud.databricks.com/files/card_v2_set.csv"

# COMMAND ----------


