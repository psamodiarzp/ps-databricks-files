# Databricks notebook source
from datetime import datetime, timedelta
from datetime import date
import pandas as pd
import numpy as np

# COMMAND ----------

'''"
base_db = sqlContext.sql(
    """
    with email_optional_table as (
        select 
        checkout_id,
        max(cast(get_json_object(properties, '$.data.meta["optional.email"]') as boolean)) as email_optional
        from aggregate_pa.cx_1cc_events_dump_v1
        where producer_created_date between date('2023-09-01') and  date('2023-10-31') 
        and event_name = 'render:1cc_summary_screen_loaded_completed'
        group by 1
    )
    
    select
    --month(producer_created_date) as mnth,
    case when producer_created_date <date('2023-10-01') then 'Prev' else 'Post' end as mnth,
    merchant_id,
    browser_name,
   case when os_brand_family in ('Android', 'iOS') then os_brand_family else 'Others' end as os,
 ---   initial_loggedin,
    case 
    when original_amount is null then null
    when original_amount < 500 then '<500'
    when original_amount < 1000 then '500 - 1k'
    when original_amount < 2000 then '1k - 2k'
    when original_amount < 5000 then '2k - 5k'
    when original_amount < 10000 then '5k - 10k'
    else '>10K' end as aov,
    email_optional,
   summary_screen_continue_cta_clicked,
 ---   payment_home_screen_loaded,
 ---   submit as submit_clicked,
    sum(open) as open,
    sum(submit) as submit
    from aggregate_pa.magic_checkout_fact a 
    left join email_optional_table b on a.checkout_id = b.checkout_id
    where producer_created_date between date('2023-09-01') and  date('2023-10-31') 
    and merchant_id in ('IH7E2OJQGEKKTN','GrFkPwp2DIMeTY','4af5pL6Gz4AElE')
    group by 1,2,3,4,5,6,7

    """
)
base_table = base_db.toPandas()
base_table.head()
'''

# COMMAND ----------

#Using funnel screen as dimensions
'''
base_db = sqlContext.sql(
    """
    with email_optional_table as (
  select
    checkout_id,
    max(
      cast(
        get_json_object(properties, '$.data.meta["optional.email"]') as boolean
      )
    ) as email_optional
  from
    aggregate_pa.cx_1cc_events_dump_v1
  where
    producer_created_date between date('2023-09-01')
    and date('2023-10-31')
    and merchant_id in (
      'IH7E2OJQGEKKTN',
      'GrFkPwp2DIMeTY',
      '4af5pL6Gz4AElE',
      'JwLCkY9NR1FtdZ'
    )
    and event_name = 'render:1cc_summary_screen_loaded_completed'
  group by
    1
),
experiments as(
   select
    checkout_id,
    max(
      case
        when experiment_name = 'one_cc_cart_validation'
          then experiment_value
      end
    ) as one_cc_cart_validation,

    max(
      case
        when experiment_name = 'one_cc_otp_verify_v2'
        then experiment_value
      end
    ) as one_cc_otp_verify_v2,

    max(
      case
        when experiment_name = '1cc_enable_shopify_taxes'
          then experiment_value
      end
    ) as one_cc_enable_shopify_taxes

  from
    aggregate_pa.cx_1cc_experiment_fact
  where
    experiment_name in (
      'one_cc_otp_verify_v2',
      'one_cc_cart_validation',
      '1cc_enable_shopify_taxes'
    )
    and producer_created_date between date('2023-10-01')
    and date('2023-11-30')
  group by
    1
)
select
  --month(producer_created_date) as mnth,
  case
    when producer_created_date < date('2023-11-01') then 'Prev'
    else 'Post'
  end as mnth,
  merchant_id,
  browser_name,
  case
    when os_brand_family in ('Android', 'iOS') then os_brand_family
    else 'Others'
  end as os,
  case
    when original_amount is null then null
    when original_amount < 500 then '<500'
    when original_amount < 1000 then '500 - 1k'
    when original_amount < 2000 then '1k - 2k'
    when original_amount < 5000 then '2k - 5k'
    when original_amount < 10000 then '5k - 10k'
    else '>10K'
  end as aov,
  case when one_cc_cart_validation is null then 'control' else one_cc_cart_validation end as one_cc_cart_validation,
case when one_cc_otp_verify_v2 is null then 'false' else one_cc_otp_verify_v2 end as one_cc_otp_verify_v2,
case when one_cc_enable_shopify_taxes is null then 'control' else one_cc_enable_shopify_taxes end as one_cc_enable_shopify_taxes,
  case
    when initial_loggedin is null then False
    else initial_loggedin
  end as initial_loggedin,
  case
    when initial_hassavedaddress is null then False
    else initial_hassavedaddress
  end as initial_hassavedaddress,
 prefill_contact_number,
 coupons_available,
 validation_successful,
  summary_screen_continue_cta_clicked,
 saved_address_screen_loaded,
 saved_address_cta_clicked,
 new_address_screen_loaded,
  pincode_serviceability_successful,
  new_address_cta_clicked
  payment_home_screen_loaded,
  /* 
      
      summary_screen_loaded,
      ---
     
    */
  submit as submit_clicked_post_payment_screen,
  sum(open) as open,
  sum(submit) as submit
from
  aggregate_pa.magic_checkout_fact a
  left join experiments b on a.checkout_id = b.checkout_id ---  left join email_optional_table b on a.checkout_id = b.checkout_id
where
  producer_created_date between date('2023-10-01')
  and date('2023-11-30') --   and a.merchant_id = 'IH7E2OJQGEKKTN'
 --- and merchant_id in ('K7RuikecA6CSyF','JUHfXse0FDfnru')
  --and merchant_id in ('IH7E2OJQGEKKTN','JUHfXse0FDfnru')
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20

    """
)
base_table = base_db.toPandas()
base_table.head()
'''

# COMMAND ----------

#base_table = base_db.toPandas()

# COMMAND ----------

# DBTITLE 1,Summary Screen Tables
#Summary 
base_db = sqlContext.sql(
    """
    with email_optional_table as (
  select
    checkout_id,
    max(
      cast(
        get_json_object(properties, '$.data.meta["optional.email"]') as boolean
      )
    ) as email_optional
  from
    aggregate_pa.cx_1cc_events_dump_v1
  where
    producer_created_date between date('2024-01-01')
    and date('2024-01-14')
    and event_name = 'render:1cc_summary_screen_loaded_completed'
  group by
    1
),
experiments as(
   select
    checkout_id,
    max(
      case
        when experiment_name = 'one_cc_cart_validation'
          then experiment_value
      end
    ) as one_cc_cart_validation,

    max(
      case
        when experiment_name = 'one_cc_otp_verify_v2'
        then experiment_value
      end
    ) as one_cc_otp_verify_v2,

    max(
      case
        when experiment_name = '1cc_enable_shopify_taxes'
          then experiment_value
      end
    ) as one_cc_enable_shopify_taxes

  from
    aggregate_pa.cx_1cc_experiment_fact
  where
    experiment_name in (
      'one_cc_otp_verify_v2',
      'one_cc_cart_validation',
      '1cc_enable_shopify_taxes'
    )
    and producer_created_date between date('2024-01-01')
    and date('2024-01-14')
  group by
    1
), summary_reasons as(
SELECT
checkout_id,
    (case
         
          when magic_checkout_fact.summary_screen_loaded = 0 then 'Summary Screen did not load'
        --  when magic_checkout_fact.summary_screen_loaded = 1 and magic_checkout_fact.summary_screen_continue_cta_clicked = 1 then 'Exited Summary Screen successfully'
          when (magic_checkout_fact.contact_email_entered=1 or magic_checkout_fact.contact_number_entered=1
    or magic_checkout_fact.clicked_change_contact=1
    or (magic_checkout_fact.contact_fill_began=1 and magic_checkout_fact.prefill_contact_number=0)
    or (magic_checkout_fact.email_fill_began=1 and magic_checkout_fact.prefill_email=0)) = True and (magic_checkout_fact.have_coupon_clicked=1 or magic_checkout_fact.coupon_screen_loaded=1) = False then 'Interacted w contact but not coupons'
          when  (magic_checkout_fact.contact_email_entered=1 or magic_checkout_fact.contact_number_entered=1
    or magic_checkout_fact.clicked_change_contact=1
    or (magic_checkout_fact.contact_fill_began=1 and magic_checkout_fact.prefill_contact_number=0)
    or (magic_checkout_fact.email_fill_began=1 and magic_checkout_fact.prefill_email=0)) = False and (magic_checkout_fact.have_coupon_clicked=1 or magic_checkout_fact.coupon_screen_loaded=1) = True then 'Interacted w coupons but not contact'
          when (magic_checkout_fact.contact_email_entered=1 or magic_checkout_fact.contact_number_entered=1
    or magic_checkout_fact.clicked_change_contact=1
    or (magic_checkout_fact.contact_fill_began=1 and magic_checkout_fact.prefill_contact_number=0)
    or (magic_checkout_fact.email_fill_began=1 and magic_checkout_fact.prefill_email=0)) = True and (magic_checkout_fact.have_coupon_clicked=1 or magic_checkout_fact.coupon_screen_loaded=1) = True then 'Interacted w both coupons and contact'
          when magic_checkout_fact.summary_screen_continue_cta_clicked = 0
          and (magic_checkout_fact.contact_email_entered=1 or magic_checkout_fact.contact_number_entered=1
    or magic_checkout_fact.clicked_change_contact=1
    or (magic_checkout_fact.contact_fill_began=1 and magic_checkout_fact.prefill_contact_number=0)
    or (magic_checkout_fact.email_fill_began=1 and magic_checkout_fact.prefill_email=0)) = False and (magic_checkout_fact.have_coupon_clicked=1 or magic_checkout_fact.coupon_screen_loaded=1) = False then 'Bounced w/o any interaction'
          when magic_checkout_fact.edit_address_clicked = 1 then 'Exited to Edit Address'
          when magic_checkout_fact.summary_screen_continue_cta_clicked = 0 then 'Summary CTA clicked'
      
          else 'Others'
          end
) AS summary_screen_dropoffs
from
  aggregate_pa.magic_checkout_fact
  where
  producer_created_date between date('2024-01-01')
    and date('2024-01-14') --   and a.merchant_id = 'IH7E2OJQGEKKTN'
 ---and merchant_id in ('K7RuikecA6CSyF','JUHfXse0FDfnru')

)
select
  --month(producer_created_date) as mnth,
  case
    when producer_created_date < date('2024-01-08') then 'Prev'
    else 'Post'
  end as mnth,
  merchant_id,
  browser_name,
  case
    when os_brand_family in ('Android', 'iOS') then os_brand_family
    else 'Others'
  end as os,
  case
    when original_amount is null then null
    when original_amount < 500 then '<500'
    when original_amount < 1000 then '500 - 1k'
    when original_amount < 2000 then '1k - 2k'
    when original_amount < 5000 then '2k - 5k'
    when original_amount < 10000 then '5k - 10k'
    else '>10K'
  end as aov,
 --- case when one_cc_cart_validation is null then 'control' else one_cc_cart_validation end as one_cc_cart_validation,
---case when one_cc_otp_verify_v2 is null then 'false' else one_cc_otp_verify_v2 end as one_cc_otp_verify_v2,
---case when one_cc_enable_shopify_taxes is null then 'control' else one_cc_enable_shopify_taxes end as one_cc_enable_shopify_taxes,
 ---summary_screen_loaded,
  case
    when initial_loggedin is null then False
    else initial_loggedin
  end as initial_loggedin,
  case
    when initial_hassavedaddress is null then False
    else initial_hassavedaddress
  end as initial_hassavedaddress,
 prefill_contact_number,
 summary_screen_continue_cta_clicked,
 payment_home_screen_loaded,
--- case when email_optional <> 1 then 0 else 1 end as email_optional,
 ---coupons_available,
 ---summary_screen_dropoffs,
 /*
 case when (summary_screen_dropoffs = 'Bounced w/o any interaction' 
 or summary_screen_dropoffs = 'Summary Screen did not load') 
 and summary_screen_dropoffs <> 'Summary CTA clicked'
 then True
 else False end as pre_engagement_dropoff,
 case when (summary_screen_dropoffs = 'Bounced w/o any interaction' 
 or summary_screen_dropoffs = 'Summary Screen did not load') 
and summary_screen_dropoffs <> 'Summary CTA clicked'
 then False
 else True end as post_engagement_dropoff,
 case when summary_screen_dropoffs = 'Summary CTA clicked'
 then True
 else False
 end as summary_screen_conversions,
*/


 ---validation_successful,
  --
  /* 
 saved_address_screen_loaded,
 saved_address_cta_clicked,
 new_address_screen_loaded,
  pincode_serviceability_successful,
  new_address_cta_clicked
  
  
      
     
      ---
      submit as submit_clicked_post_payment_screen,
      
     
    */
  
  sum(open) as open,
  sum(submit) as submit
--  sum(summary_screen_continue_cta_clicked) as summary_screen_continue_cta_clicked_total,
--sum(payment_home_screen_loaded) as payment_home_screen_loaded_total
  
from
  aggregate_pa.magic_checkout_fact a
  ---left join experiments b on a.checkout_id = b.checkout_id --- 
 ---  left join email_optional_table c on a.checkout_id = c.checkout_id
   inner join realtime_hudi_api.merchants d on a.merchant_id = d.id
    left join summary_reasons e on a.checkout_id = e.checkout_id

where
  producer_created_date between date('2024-01-01')
    and date('2024-01-14') --   and a.merchant_id = 'IH7E2OJQGEKKTN'
 and a.merchant_id in ('IH7E2OJQGEKKTN','JUHfXse0FDfnru','JkuJOHbtFLHR1p')
  ---and merchant_id in ('IH7E2OJQGEKKTN','JUHfXse0FDfnru')
    group by 1,2,3,4,5,6,7,8,9,10

    """
)
base_table = base_db.toPandas()
base_table.head()

# COMMAND ----------

#Making a copy 
base_temp = base_table.copy(deep=True)
base_temp.head()

# COMMAND ----------

#base_temp.to_csv('/dbfs/FileStore/cr_rca_sample_data.csv', index=False)
"https://razorpay-dev.cloud.databricks.com/files/cr_rca_sample_data.csv"

# COMMAND ----------

def get_percentage_share(table, col_name):
    col_num = table.columns.get_loc(col_name)
    print(col_num)
    
    

    if col_num == 0:
        table_grouped_curr = table.groupby(by=table.columns[col_num]).agg({'openPost':'sum','openPrev':'sum','submitPost':'sum','submitPrev':'sum'}).reset_index()
        print(table_grouped_curr.columns)
        #table_grouped_curr.iloc[:,-4:] = table_grouped_curr.iloc[:,-4:].add_suffix(('_'+col_name))
        #print(table_grouped_curr.columns)
        table_final = table.merge(table_grouped_curr, how='left', on=table.columns[col_num], suffixes=('', ('_'+col_name)))
        print(table_final.columns)
        for j in ['openPost','openPrev']:
            table_final[(j+'_'+col_name+'_share')] = table_final[(j+'_'+col_name)] / table[j].sum()
        
    else:
        print(table.columns[0:col_num+1])
        table_grouped_curr = table.groupby(by=list(table.columns[0:col_num+1])).agg({'openPost':'sum','openPrev':'sum','submitPost':'sum','submitPrev':'sum'}).reset_index()
        print(table_grouped_curr.columns)
        table_final = table.merge(table_grouped_curr, how='left', on=list(table.columns[:col_num+1]), suffixes=('', ('_'+col_name)))
        print(table_final.columns)
        prev_col_name = table.columns[col_num-1]
        print(prev_col_name)
        for j in ['openPost','openPrev']:
            table_final[(j+'_'+col_name+'_share')] = table_final[(j+'_'+col_name)] / table_final[(j+'_'+prev_col_name)]
        print(table_final.columns)
   

    return table_final

# COMMAND ----------

base_table.dtypes

# COMMAND ----------

'''
base_temp.loc[base_temp['email_optional'].isna(),'email_optional']=False
base_temp = base_temp.groupby(by=['mnth', 'merchant_id', 'browser_name', 'os', 'aov',
        'email_optional','summary_screen_continue_cta_clicked',
       'payment_home_screen_loaded']).agg({'open':'sum','submit':'sum',}).reset_index()
base_temp.shape
'''

# COMMAND ----------

base_temp.shape

# COMMAND ----------

base_temp

# COMMAND ----------

#initial data manipulation
#base_pivot = base_temp.pivot(columns='mnth', index=base_temp.columns[1:-4], values=['open','submit','summary_screen_continue_cta_clicked_total','payment_home_screen_loaded_total']).reset_index()
base_pivot = base_temp.pivot(columns='mnth', index=base_temp.columns[1:-2], values=['open','submit']).reset_index()
base_df = base_pivot.fillna(0)
base_df.columns = [''.join(col).strip() for col in base_df.columns.values]
base_df['Post_CR'] = base_df['submitPost']*1.00000 / base_df['openPost']
base_df['Pre_CR'] = base_df['submitPrev']*1.00000 / base_df['openPrev']
base_df.head()

# COMMAND ----------

base_df.to_csv('/dbfs/FileStore/cr_rca_sample_data.csv', index=False)
"https://razorpay-dev.cloud.databricks.com/files/cr_rca_sample_data.csv"

# COMMAND ----------

n = base_df.shape[1] - 6
for i in base_df.iloc[:,:n].columns:
    base_df = get_percentage_share(base_df,i)
    print(base_df)



# COMMAND ----------

base_df = base_df.fillna(0)
base_df.head()

# COMMAND ----------

#Fixing the Pre share=0 error 
open_prev_share_cols = [col for col in base_df.columns if 'openPrev' in col and 'share' in col]
open_post_share_cols = [col for col in base_df.columns if 'openPost' in col and 'share' in col]
for i in range(n-1):
    condition = (base_df[open_prev_share_cols[i]] == 0)
    for j in range(i+1,n):
        base_df.loc[condition, open_prev_share_cols[j]] = base_df.loc[condition, open_post_share_cols[j]]
base_df.head()




# COMMAND ----------

#Fixing where Pre CR is inf
base_df['Pre_CR'] = base_df['Pre_CR'].replace([float('inf'), float('-inf')], 0)
base_df['Post_CR'] = base_df['Post_CR'].replace([float('inf'), float('-inf')], 0)


# COMMAND ----------

base_df[open_prev_share_cols[0+1:]].prod(axis=1)

# COMMAND ----------

#Adding calculation columns
open_prev_total = base_df['openPrev'].sum()
open_post_total = base_df['openPost'].sum()
prev_cr = (base_df['submitPrev'].sum()) * 1.0 / open_prev_total
post_cr = (base_df['submitPost'].sum()) * 1.0 / open_post_total
base_df['initial_delta'] = base_df['Pre_CR'] * (base_df['openPrev']/open_prev_total)

#Required new columns
open_mix_column_names = [col + '_open_mix' for col in base_df.columns[:n]]
submit_mix_column_names = [col + '_submit_mix' for col in base_df.columns[:n]]
cr_mix_column_names = [col + '_cr_mix' for col in base_df.columns[:n]]
delta_mix_column_names = [col + '_delta_mix' for col in base_df.columns[:n]]
conv_impact_mix_column_names = [col + '_conv_impact_mix' for col in base_df.columns[:n]]
for i in range(n):
    base_df[open_mix_column_names[i]] = open_prev_total * (base_df[open_post_share_cols[:i+1]].prod(axis=1)) * (base_df[open_prev_share_cols[i+1:]].prod(axis=1))
    base_df[submit_mix_column_names[i]] = base_df[open_mix_column_names[i]] * base_df['Pre_CR'] 
    base_df[cr_mix_column_names[i]] = base_df[submit_mix_column_names[i]]*1.00/base_df[open_mix_column_names[i]]
    base_df[cr_mix_column_names[i]] = base_df[cr_mix_column_names[i]].replace([float('inf'), float('-inf')], 0)

        
    base_df[delta_mix_column_names[i]] = (base_df[cr_mix_column_names[i]] * base_df[open_mix_column_names[i]])/open_prev_total
    if i ==0:
        base_df[conv_impact_mix_column_names[i]] = base_df[delta_mix_column_names[i]] - base_df['initial_delta']
    else:
        base_df[conv_impact_mix_column_names[i]] = base_df[delta_mix_column_names[i]] - base_df[delta_mix_column_names[i-1]]
base_df.head()




# COMMAND ----------

base_df.columns

# COMMAND ----------


base_df['post_mix_normalized_opens'] = base_df[open_mix_column_names[-1]] #A
base_df['post_mix_normalized_submits'] = base_df['post_mix_normalized_opens'] * base_df['Post_CR'] #B
#C is Post CR == B/A
base_df['delta'] = base_df['Post_CR'] - base_df['Pre_CR'] #D
base_df['conversion_impact'] = (base_df['delta'] * base_df['post_mix_normalized_opens'])/open_prev_total
base_df.head()


# COMMAND ----------

base_df['conversion_impact'].sum()

# COMMAND ----------

#Summarizing Impact

feature_list = ['Pre_CR']
CR_list = [prev_cr]

#summary_df = summary_df.append({"Feature":'Pre_CR',"CR":prev_cr, "Impact":None}, ignore_index=True)
for i in range(n):
    feature_list.append(base_df.columns[i])
    CR_list.append(base_df[submit_mix_column_names[i]].sum()*1.0 / base_df[open_mix_column_names[i]].sum())
    #impact_list.append(CR_list[i+1] - CR_list[i])

feature_list.append('Post CR')
CR_list.append(post_cr)

print(feature_list)
print(CR_list)



#summary_df.head()

# COMMAND ----------

summary_df = pd.DataFrame({"Feature":feature_list,"CR":CR_list,})
summary_df['impact'] = summary_df['CR'].diff()
summary_df.loc[summary_df['Feature']=='Pre_CR','impact'] = prev_cr
summary_df

# COMMAND ----------

open_prev_total
base_df[open_mix_column_names[0]].sum()

# COMMAND ----------

base_df.sort_values(by='conversion_impact', ascending=True)

# COMMAND ----------

for i in range(len(open_mix_column_names)):
    try:
        if not round(base_df[open_mix_column_names[i]].sum(),0) == open_prev_total:
            raise ValueError(f"Total Prev Opens do not match for {open_mix_column_names[i]}")
    except ValueError as e:
        print(f"Error: {e}")
        # Add any additional error handling or logging here
        break 


# COMMAND ----------

n

# COMMAND ----------

#Conversion Impact
'''
base_df['Conversion_impact_submits']= base_df['submit_clicked_post_payment_screen_open_mix']*base_df['Post_CR']
#base_df['Conversion_impact_submits'].sum() / base_df['submit_clicked_post_payment_screen_open_mix'].sum()
base_df['Delta_CR'] = base_df['Post_CR'] - base_df['Pre_CR']
base_df['Conversion_impact'] = base_df['Delta_CR'] * base_df['submit_clicked_post_payment_screen_open_mix'] / open_prev_total
base_df['Conversion_impact'].sum()
'''

# COMMAND ----------

base_df.to_csv('/dbfs/FileStore/cr_rca_agg_sample_data.csv', index=False)
"https://razorpay-dev.cloud.databricks.com/files/cr_rca_agg_sample_data.csv"

# COMMAND ----------


'''
base_df = base_df.drop(columns=['merchant_id_open_mix', 'merchant_id_submit_mix',
       'browser_name_open_mix', 'browser_name_submit_mix', 'os_open_mix',
       'os_submit_mix', 'aov_open_mix', 'aov_submit_mix',
       'prefill_contact_number_open_mix', 'prefill_contact_number_submit_mix',
       'summary_screen_continue_cta_clicked_open_mix',
       'summary_screen_continue_cta_clicked_submit_mix',
       'payment_home_screen_loaded_open_mix',
       'payment_home_screen_loaded_submit_mix',
       'submit_clicked_post_payment_screen_open_mix',
       'submit_clicked_post_payment_screen_submit_mix'
       ])
       '''


# COMMAND ----------

base_df.shape

# COMMAND ----------

base_df['Pre_CR'].unique()

# COMMAND ----------

# DBTITLE 1,Calculating Delta
base_df['abs_aov_conv_impact_mix'] = abs(base_df['aov_conv_impact_mix'])
#base_df.sort_values(by='abs_merchant_id_conv_impact_mix', ascending=False).head(20)
(base_df.sort_values(by='abs_aov_conv_impact_mix', ascending=False)).to_csv('/dbfs/FileStore/dec_apollo_aov_breakdown.csv', index=False)
#"https://razorpay-dev.cloud.databricks.com/files/dec_apollo_aov_breakdown.csv"

# COMMAND ----------


