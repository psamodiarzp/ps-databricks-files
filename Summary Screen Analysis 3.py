# Databricks notebook source
import pandas as pd

# COMMAND ----------

source_db=sqlContext.sql(

    """
with 
events as (

    SELECT 
    checkout_id,
    max(get_json_object(context,'$.user_agent_parsed.os.family')) as os,
    max(round(get_json_object(properties,'$.options.amount'),0)) as aov
    from aggregate_pa.cx_1cc_events_dump_v1
    where producer_created_date >= date('2023-07-01')
    and event_name in ('render:1cc_summary_screen_loaded_completed','behav:1cc_summary_screen_continue_cta_clicked')
    group by 1
),
base as
 ( 
 SELECT
    (case
          when magic_checkout_fact.summary_screen_continue_cta_clicked = 1 then 'Summary Screen CTA clicked'
          when magic_checkout_fact.summary_screen_loaded = 0 then 'Summary Screen did not load'
        --  when magic_checkout_fact.summary_screen_loaded = 1 and magic_checkout_fact.summary_screen_continue_cta_clicked = 1 then 'Exited Summary Screen successfully'
          when magic_checkout_fact.summary_screen_continue_cta_clicked = 0
          and (magic_checkout_fact.contact_email_entered=1 or magic_checkout_fact.contact_number_entered=1
    or magic_checkout_fact.clicked_change_contact=1
    or (magic_checkout_fact.contact_fill_began=1 and magic_checkout_fact.prefill_contact_number=0)
    or (magic_checkout_fact.email_fill_began=1 and magic_checkout_fact.prefill_email=0)) = True and (magic_checkout_fact.have_coupon_clicked=1 or magic_checkout_fact.coupon_screen_loaded=1) = False then 'Interacted w contact but not coupons'
          when magic_checkout_fact.summary_screen_continue_cta_clicked = 0
          and (magic_checkout_fact.contact_email_entered=1 or magic_checkout_fact.contact_number_entered=1
    or magic_checkout_fact.clicked_change_contact=1
    or (magic_checkout_fact.contact_fill_began=1 and magic_checkout_fact.prefill_contact_number=0)
    or (magic_checkout_fact.email_fill_began=1 and magic_checkout_fact.prefill_email=0)) = False and (magic_checkout_fact.have_coupon_clicked=1 or magic_checkout_fact.coupon_screen_loaded=1) = True then 'Interacted w coupons but not contact'
          when magic_checkout_fact.summary_screen_continue_cta_clicked = 0
          and (magic_checkout_fact.contact_email_entered=1 or magic_checkout_fact.contact_number_entered=1
    or magic_checkout_fact.clicked_change_contact=1
    or (magic_checkout_fact.contact_fill_began=1 and magic_checkout_fact.prefill_contact_number=0)
    or (magic_checkout_fact.email_fill_began=1 and magic_checkout_fact.prefill_email=0)) = True and (magic_checkout_fact.have_coupon_clicked=1 or magic_checkout_fact.coupon_screen_loaded=1) = True then 'Interacted w both coupons and contact'
          when magic_checkout_fact.summary_screen_continue_cta_clicked = 0
          and (magic_checkout_fact.contact_email_entered=1 or magic_checkout_fact.contact_number_entered=1
    or magic_checkout_fact.clicked_change_contact=1
    or (magic_checkout_fact.contact_fill_began=1 and magic_checkout_fact.prefill_contact_number=0)
    or (magic_checkout_fact.email_fill_began=1 and magic_checkout_fact.prefill_email=0)) = False and (magic_checkout_fact.have_coupon_clicked=1 or magic_checkout_fact.coupon_screen_loaded=1) = False then 'Bounced w/o any interaction'
          when magic_checkout_fact.summary_screen_continue_cta_clicked = 0
          and magic_checkout_fact.edit_address_clicked = 1 then 'Exited to Edit Address'
          else 'Others'
          end
) AS summary_screen_dropoffs,
    merchant_id, 
    browser_name,
    producer_created_date,
    magic_checkout_fact.checkout_id,
    os,
    aov
FROM aggregate_pa.magic_checkout_fact AS magic_checkout_fact
LEFT JOIN  events on magic_checkout_fact.checkout_id = events.checkout_id
where producer_created_date >= date('2023-07-01')
and merchant_id IN (
    SELECT mid FROM batch_sheets.magic_merchant_list where segment<>'SME'
)
)
select 
summary_screen_dropoffs,
    merchant_id, 
    browser_name,
   -- producer_created_date,
    os,
    aov,
    checkout_id
from base

    """
)




# COMMAND ----------

source_df = source_db.toPandas()
source_df

# COMMAND ----------

source_df.to_csv('/dbfs/FileStore/summary_transactional_level.csv', index=False)
"https://razorpay-dev.cloud.databricks.com/files/summary_transactional_level.csv"

# COMMAND ----------

source_df = pd.read_csv('/dbfs/FileStore/summary_transactional_level.csv')

# COMMAND ----------

source_df

# COMMAND ----------

source_df['aov_bucket'] = pd.cut(source_df['aov']/100, bins=[0,1000, 2000, 5000, 10000, 20000, 50000, 100000, 500000])
source_df['aov_bucket'] = source_df['aov_bucket'].astype('str')
source_df

# COMMAND ----------




# Calculate the specified percentiles for each merchant
percentiles = [5, 10, 25, 50, 75, 90, 95]

result_temp = source_df.groupby('merchant_id')['aov'].quantile([p / 100 for p in percentiles])

# Reset the index to make the result a DataFrame
result_temp = result_temp.reset_index()

# Rename the columns for clarity
#result.columns = ['merchant_id'] + [f'{p}th_percentile' for p in percentiles]

# Print the result
result = result_temp.pivot(index='merchant_id', columns='level_1', values='aov').reset_index()
result

# COMMAND ----------

result[0.5].describe()

# COMMAND ----------

result['merchant_aov_bucket'] = pd.cut(result[0.5]/100, bins=[0,500, 1000,2000,3500, 5000, 100000])
result['merchant_aov_bucket'] = result['merchant_aov_bucket'].astype('str')
result

# COMMAND ----------

merchant_db = sqlContext.sql(
"""
select id, website, category
from realtime_hudi_api.merchants
"""

)
merchant_df = merchant_db.toPandas()
merchant_df

# COMMAND ----------

merchant_result_df = result.merge(merchant_df, left_on='merchant_id', right_on='id', how='left')
merchant_result_df[['merchant_id','merchant_aov_bucket','website','category']].to_csv('/dbfs/FileStore/merchant_result_df.csv', index=False)
"https://razorpay-dev.cloud.databricks.com/files/merchant_result_df.csv"

# COMMAND ----------

merchant_source_df = source_df.merge(result[['merchant_id','merchant_aov_bucket']], on='merchant_id', how='left' 
                                     )
merchant_source_df

# COMMAND ----------

aov_bucket_agg_temp = merchant_source_df.groupby(by=['merchant_aov_bucket','aov_bucket','summary_screen_dropoffs']).agg({'checkout_id':'nunique'}).reset_index()
aov_bucket_agg = aov_bucket_agg_temp.pivot(index=['merchant_aov_bucket','aov_bucket'], columns='summary_screen_dropoffs', values='checkout_id').reset_index()
aov_bucket_agg = aov_bucket_agg.fillna(0)
aov_bucket_agg['total'] = aov_bucket_agg.iloc[:,-6:].sum(axis=1)

aov_bucket_agg['converted'] = aov_bucket_agg['Summary Screen CTA clicked'] * 1.00 / aov_bucket_agg['total']
aov_bucket_agg['pre_load_dropoff'] = aov_bucket_agg['Summary Screen did not load'] * 1.00 / aov_bucket_agg['total']
aov_bucket_agg['post_load_bounce'] = aov_bucket_agg['Bounced w/o any interaction']  * 1.00 / aov_bucket_agg['total']
aov_bucket_agg['post_engagement_dropoff'] = 1 - (aov_bucket_agg['converted'] + aov_bucket_agg['pre_load_dropoff'] + aov_bucket_agg['post_load_bounce'])
aov_bucket_agg

# COMMAND ----------

aov_bucket_agg.to_csv('/dbfs/FileStore/aov_bucket_agg.csv', index=False)
"https://razorpay-dev.cloud.databricks.com/files/aov_bucket_agg.csv"

# COMMAND ----------

browser_selected_df = merchant_source_df[(merchant_source_df['browser_name'].isin(selected_browsers)) & 
                                     (merchant_source_df['os'].isin(selected_os))
                                     ]
browser_temp_df = browser_selected_df.groupby(by=['browser_name','os','summary_screen_dropoffs']).agg({'checkout_id':'nunique'}).reset_index()
browser_df = browser_temp_df.pivot(index=['browser_name','os',], columns='summary_screen_dropoffs', values='checkout_id').reset_index()
browser_df = browser_df.fillna(0)
browser_df['total'] = browser_df.iloc[:,-6:].sum(axis=1)

browser_df['converted'] = browser_df['Summary Screen CTA clicked'] * 1.00 / browser_df['total']
browser_df['pre_load_dropoff'] = browser_df['Summary Screen did not load'] * 1.00 / browser_df['total']
browser_df['post_load_bounce'] = browser_df['Bounced w/o any interaction']  * 1.00 / browser_df['total']
browser_df['post_engagement_dropoff'] = 1 - (browser_df['converted'] + browser_df['pre_load_dropoff'] + browser_df['post_load_bounce'])
browser_df

# COMMAND ----------

browser_df.sort_values(by='total', ascending=False)

# COMMAND ----------

browser_summary_cr = browser_df.pivot(index=['browser_name'], columns=['os'], values=['total','converted','post_load_bounce']).reset_index()
browser_summary_cr = browser_summary_cr.fillna(0)
browser_summary_cr

# COMMAND ----------

browser_summary_cr.to_csv('/dbfs/FileStore/browser_summary_cr.csv', index=False)
"https://razorpay-dev.cloud.databricks.com/files/browser_summary_cr.csv"

# COMMAND ----------

selected_browsers = [
'Chrome Mobile',
'Instagram',
'Facebook',
'Mobile Safari',
'Chrome',
'Chrome Mobile WebView',
'Chrome Mobile iOS',
'Samsung Internet',
'Google',
'Safari',
'Edge',
'Firefox']

selected_os = [
    'Android','Linux','Mac OS X','Windows','iOS'
]


# COMMAND ----------




# Calculate the specified percentiles for each merchant
percentiles = [5, 10, 25, 50, 75, 90, 95]

result_temp = source_df.groupby(['browser_name','os'])['aov'].quantile([p / 100 for p in percentiles])

result_temp = result_temp.reset_index()

# Rename the columns for clarity
#result.columns = ['merchant_id'] + [f'{p}th_percentile' for p in percentiles]

# Print the result
result = result_temp.pivot(index=['browser_name','os'], columns='level_2', values='aov').reset_index()
result


#browser_selected_df = merchant_source_df[(merchant_source_df['browser_name'].isin(selected_browsers)) & (merchant_source_df['os'].isin(selected_os))]
                                     
#browser_selected_df
#.groupby(by=['browser_name','os','summary_screen_dropoffs']).agg({'checkout_id':'nunique'}).reset_index()

# COMMAND ----------

result.to_csv('/dbfs/FileStore/result_browser.csv', index=False)
"https://razorpay-dev.cloud.databricks.com/files/result_browser.csv"

# COMMAND ----------

result[0.5].describe()  #[0,1000,1500,2000,5000,100000]

# COMMAND ----------

os_temp_df = source_df.groupby(by=[,'os','summary_screen_dropoffs']).agg({'checkout_id':'nunique'}).reset_index()
os_df = os_temp_df.pivot(index=['browser_name','os',], columns='summary_screen_dropoffs', values='checkout_id').reset_index()
browser_df = browser_df.fillna(0)
browser_df['total'] = browser_df.iloc[:,-6:].sum(axis=1)

browser_df['converted'] = browser_df['Summary Screen CTA clicked'] * 1.00 / browser_df['total']
browser_df['pre_load_dropoff'] = browser_df['Summary Screen did not load'] * 1.00 / browser_df['total']
browser_df['post_load_bounce'] = browser_df['Bounced w/o any interaction']  * 1.00 / browser_df['total']
browser_df['post_engagement_dropoff'] = 1 - (browser_df['converted'] + browser_df['pre_load_dropoff'] + browser_df['post_load_bounce'])
browser_df

# COMMAND ----------

aov_bucket_agg_temp = merchant_source_df.groupby(by=['merchant_aov_bucket','os','summary_screen_dropoffs']).agg({'checkout_id':'nunique'}).reset_index()
aov_bucket_agg = aov_bucket_agg_temp.pivot(index=['merchant_aov_bucket','os'], columns='summary_screen_dropoffs', values='checkout_id').reset_index()
aov_bucket_agg = aov_bucket_agg.fillna(0)
aov_bucket_agg['total'] = aov_bucket_agg.iloc[:,-6:].sum(axis=1)

aov_bucket_agg['converted'] = aov_bucket_agg['Summary Screen CTA clicked'] * 1.00 / aov_bucket_agg['total']
aov_bucket_agg['pre_load_dropoff'] = aov_bucket_agg['Summary Screen did not load'] * 1.00 / aov_bucket_agg['total']
aov_bucket_agg['post_load_bounce'] = aov_bucket_agg['Bounced w/o any interaction']  * 1.00 / aov_bucket_agg['total']
aov_bucket_agg['post_engagement_dropoff'] = 1 - (aov_bucket_agg['converted'] + aov_bucket_agg['pre_load_dropoff'] + aov_bucket_agg['post_load_bounce'])
os_aov_agg = aov_bucket_agg.pivot(index='os', values=['total','converted'], columns='merchant_aov_bucket').reset_index()
os_aov_agg = os_aov_agg.fillna(0)
os_aov_agg

# COMMAND ----------

os_aov_agg.to_csv('/dbfs/FileStore/os_aov_agg.csv', index=False)
"https://razorpay-dev.cloud.databricks.com/files/os_aov_agg.csv"

# COMMAND ----------

source2_db=sqlContext.sql(

    """
with 
events as (

    SELECT 
    checkout_id,
    max(get_json_object(context,'$.user_agent_parsed.os.family')) as os,
    max(round(get_json_object(properties,'$.options.amount'),0)) as aov,
    max(CASE WHEN event_name = 'render:1cc_summary_screen_loaded_completed' THEN
        ROUND(CAST(get_json_object(properties, '$.data.meta["timeSince.open"]') AS DOUBLE), 0)
    END) AS load_time

    from aggregate_pa.cx_1cc_events_dump_v1
    where producer_created_date >= date('2023-07-01')
    and event_name in ('render:1cc_summary_screen_loaded_completed','behav:1cc_summary_screen_continue_cta_clicked')
    group by 1
),
base as
 ( 
 SELECT
    (case
          when magic_checkout_fact.summary_screen_continue_cta_clicked = 1 then 'Summary Screen CTA clicked'
          when magic_checkout_fact.summary_screen_loaded = 0 then 'Summary Screen did not load'
        --  when magic_checkout_fact.summary_screen_loaded = 1 and magic_checkout_fact.summary_screen_continue_cta_clicked = 1 then 'Exited Summary Screen successfully'
          when magic_checkout_fact.summary_screen_continue_cta_clicked = 0
          and (magic_checkout_fact.contact_email_entered=1 or magic_checkout_fact.contact_number_entered=1
    or magic_checkout_fact.clicked_change_contact=1
    or (magic_checkout_fact.contact_fill_began=1 and magic_checkout_fact.prefill_contact_number=0)
    or (magic_checkout_fact.email_fill_began=1 and magic_checkout_fact.prefill_email=0)) = True and (magic_checkout_fact.have_coupon_clicked=1 or magic_checkout_fact.coupon_screen_loaded=1) = False then 'Interacted w contact but not coupons'
          when magic_checkout_fact.summary_screen_continue_cta_clicked = 0
          and (magic_checkout_fact.contact_email_entered=1 or magic_checkout_fact.contact_number_entered=1
    or magic_checkout_fact.clicked_change_contact=1
    or (magic_checkout_fact.contact_fill_began=1 and magic_checkout_fact.prefill_contact_number=0)
    or (magic_checkout_fact.email_fill_began=1 and magic_checkout_fact.prefill_email=0)) = False and (magic_checkout_fact.have_coupon_clicked=1 or magic_checkout_fact.coupon_screen_loaded=1) = True then 'Interacted w coupons but not contact'
          when magic_checkout_fact.summary_screen_continue_cta_clicked = 0
          and (magic_checkout_fact.contact_email_entered=1 or magic_checkout_fact.contact_number_entered=1
    or magic_checkout_fact.clicked_change_contact=1
    or (magic_checkout_fact.contact_fill_began=1 and magic_checkout_fact.prefill_contact_number=0)
    or (magic_checkout_fact.email_fill_began=1 and magic_checkout_fact.prefill_email=0)) = True and (magic_checkout_fact.have_coupon_clicked=1 or magic_checkout_fact.coupon_screen_loaded=1) = True then 'Interacted w both coupons and contact'
          when magic_checkout_fact.summary_screen_continue_cta_clicked = 0
          and (magic_checkout_fact.contact_email_entered=1 or magic_checkout_fact.contact_number_entered=1
    or magic_checkout_fact.clicked_change_contact=1
    or (magic_checkout_fact.contact_fill_began=1 and magic_checkout_fact.prefill_contact_number=0)
    or (magic_checkout_fact.email_fill_began=1 and magic_checkout_fact.prefill_email=0)) = False and (magic_checkout_fact.have_coupon_clicked=1 or magic_checkout_fact.coupon_screen_loaded=1) = False then 'Bounced w/o any interaction'
          when magic_checkout_fact.summary_screen_continue_cta_clicked = 0
          and magic_checkout_fact.edit_address_clicked = 1 then 'Exited to Edit Address'
          else 'Others'
          end
) AS summary_screen_dropoffs,
    merchant_id, 
    browser_name,
    producer_created_date,
    magic_checkout_fact.checkout_id,
    os,
    aov,
    load_time
FROM aggregate_pa.magic_checkout_fact AS magic_checkout_fact
LEFT JOIN  events on magic_checkout_fact.checkout_id = events.checkout_id
where producer_created_date >= date('2023-07-01')
and merchant_id IN (
    SELECT mid FROM batch_sheets.magic_merchant_list where segment<>'SME'
)
)
select 
summary_screen_dropoffs,
    merchant_id, 
    browser_name,
   -- producer_created_date,
    os,
    aov,
    checkout_id,
    load_time
from base

    """
)




# COMMAND ----------

source2_df = source2_db.toPandas()
source2_df

# COMMAND ----------

source2_df.to_csv('/dbfs/FileStore/source2_df.csv', index=False)
"https://razorpay-dev.cloud.databricks.com/files/source2_df.csv"

# COMMAND ----------

source2_df

# COMMAND ----------




# Calculate the specified percentiles for each merchant
percentiles = [5, 10, 25, 50, 75, 80,85,90, 95]

loadtime_result_temp = source2_df.groupby(['merchant_id'])['load_time'].quantile([p / 100 for p in percentiles])

loadtime_result_temp = loadtime_result_temp.reset_index()
#loadtime_result_temp
# Rename the columns for clarity
#result.columns = ['merchant_id'] + [f'{p}th_percentile' for p in percentiles]

# Print the result
loadtime_result = loadtime_result_temp.pivot(index=['merchant_id'], columns='level_1', values='load_time').reset_index()
loadtime_result


#browser_selected_df = merchant_source_df[(merchant_source_df['browser_name'].isin(selected_browsers)) & (merchant_source_df['os'].isin(selected_os))]
                                     
#browser_selected_df
#.groupby(by=['browser_name','os','summary_screen_dropoffs']).agg({'checkout_id':'nunique'}).reset_index()

# COMMAND ----------

source2_df['load_time'].quantile([p / 100 for p in percentiles])

# COMMAND ----------



# COMMAND ----------

source2_df['load_time_bucket'] = pd.cut(source2_df['load_time'], bins=[0,150,500, 1000,5000, 10000,60000])
source2_df['load_time_bucket'] = source2_df['load_time_bucket'].astype('str')
source2_df

# COMMAND ----------

load_time_bucket_temp = source2_df.groupby(by=['load_time_bucket','summary_screen_dropoffs']).agg({'checkout_id':'nunique'}).reset_index()
load_time_bucket = load_time_bucket_temp.pivot(index=['load_time_bucket',], columns='summary_screen_dropoffs', values='checkout_id').reset_index()
load_time_bucket = load_time_bucket.fillna(0)
load_time_bucket['total'] = load_time_bucket.iloc[:,-6:].sum(axis=1)

load_time_bucket['converted'] = load_time_bucket['Summary Screen CTA clicked'] * 1.00 / load_time_bucket['total']
load_time_bucket['pre_load_dropoff'] = load_time_bucket['Summary Screen did not load'] * 1.00 / load_time_bucket['total']
load_time_bucket['post_load_bounce'] = load_time_bucket['Bounced w/o any interaction']  * 1.00 / load_time_bucket['total']
load_time_bucket['post_engagement_dropoff'] = 1 - (load_time_bucket['converted'] + load_time_bucket['pre_load_dropoff'] + load_time_bucket['post_load_bounce'])
#os_aov_agg = load_time_bucket.pivot(index='os', values=['total','converted'], columns='merchant_aov_bucket').reset_index()
#os_aov_agg = os_aov_agg.fillna(0)
load_time_bucket

# COMMAND ----------

load_time_bucket.to_csv('/dbfs/FileStore/load_time_bucket.csv', index=False)
"https://razorpay-dev.cloud.databricks.com/files/load_time_bucket.csv"

# COMMAND ----------

source2_df['merchant_aov_bucket'] = pd.cut(source2_df['aov']/100, bins=[0,500, 1000,2000,3500, 5000, 100000])
source2_df['merchant_aov_bucket'] = source2_df['merchant_aov_bucket'].astype('str')
source2_df

# COMMAND ----------

load_time_aov_bucket_temp = source2_df.groupby(by=['load_time_bucket','merchant_aov_bucket','summary_screen_dropoffs']).agg({'checkout_id':'nunique'}).reset_index()
load_time_aov_bucket = load_time_aov_bucket_temp.pivot(index=['load_time_bucket','merchant_aov_bucket'], columns='summary_screen_dropoffs', values='checkout_id').reset_index()
load_time_aov_bucket = load_time_aov_bucket.fillna(0)
load_time_aov_bucket['total'] = load_time_aov_bucket.iloc[:,-6:].sum(axis=1)

load_time_aov_bucket['converted'] = load_time_aov_bucket['Summary Screen CTA clicked'] * 1.00 / load_time_aov_bucket['total']
load_time_aov_bucket['pre_load_dropoff'] = load_time_aov_bucket['Summary Screen did not load'] * 1.00 / load_time_aov_bucket['total']
load_time_aov_bucket['post_load_bounce'] = load_time_aov_bucket['Bounced w/o any interaction']  * 1.00 / load_time_aov_bucket['total']
load_time_aov_bucket['post_engagement_dropoff'] = 1 - (load_time_aov_bucket['converted'] + load_time_aov_bucket['pre_load_dropoff'] + load_time_aov_bucket['post_load_bounce'])
#os_aov_agg = load_time_bucket.pivot(index='os', values=['total','converted'], columns='merchant_aov_bucket').reset_index()
#os_aov_agg = os_aov_agg.fillna(0)
load_time_aov_bucket

# COMMAND ----------

load_time_aov_pivot = load_time_aov_bucket.pivot(index='load_time_bucket', columns='merchant_aov_bucket', values=['total','converted','post_load_bounce']).reset_index()
load_time_aov_pivot

# COMMAND ----------

load_time_aov_pivot.to_csv('/dbfs/FileStore/load_time_aov_pivot.csv', index=False)
"https://razorpay-dev.cloud.databricks.com/files/load_time_aov_pivot.csv"

# COMMAND ----------

load_aov_mid_temp = source2_df.groupby(by=['load_time_bucket','merchant_aov_bucket',]).agg({'merchant_id':'nunique'}).reset_index()
load_aov_mid = load_aov_mid_temp.pivot(index='load_time_bucket', columns='merchant_aov_bucket', values='merchant_id'
                        ).reset_index()
load_aov_mid

# COMMAND ----------

load_aov_mid.to_csv('/dbfs/FileStore/load_aov_mid.csv', index=False)
"https://razorpay-dev.cloud.databricks.com/files/load_aov_mid.csv"

# COMMAND ----------

load_browser_temp = source2_df.groupby(by=['load_time_bucket','os','browser_name',]).agg({'checkout_id':'nunique'}).reset_index()
load_browser = load_browser_temp.pivot(index=['browser_name','os'], columns='load_time_bucket', values='checkout_id'
                        ).reset_index()
load_browser = load_browser.fillna(0)
load_browser['total'] = load_browser.iloc[:,-7:].sum(axis=1)
load_browser.sort_values(by='total', ascending=False)

# COMMAND ----------

load_browser.to_csv('/dbfs/FileStore/load_browser.csv', index=False)
"https://razorpay-dev.cloud.databricks.com/files/load_browser.csv"

# COMMAND ----------

load_os_temp = source2_df.groupby(by=['load_time_bucket','os',]).agg({'merchant_id':'nunique'}).reset_index()
load_os = load_os_temp.pivot(index='os', columns='load_time_bucket', values='merchant_id'
                        ).reset_index()
load_os

# COMMAND ----------

percentiles = [5, 10, 25, 50, 75, 80,85,90, 95]



load_os_temp = source2_df.groupby(by=['load_time_bucket','os','browser_name',])['checkout_id'].quantile([p / 100 for p in percentiles]).reset_index()
#load_os = load_os_temp.pivot(index='os', columns='load_time_bucket', values='merchant_id'
                        
load_os_temp

# COMMAND ----------


