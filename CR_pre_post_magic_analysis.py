# Databricks notebook source
import pandas as pd

# COMMAND ----------

import numpy as np

# COMMAND ----------

import seaborn as sns

# COMMAND ----------

!pip install openpyxl

# COMMAND ----------

raw_df = pd.read_excel('/dbfs/FileStore/Analysis___1_5__buffer-1.xlsx')

# COMMAND ----------

raw_df.head(10)

# COMMAND ----------

#drop from V - AA
raw_df.columns
raw_df = raw_df.drop(raw_df.iloc[:,23:27], axis=1)
raw_df.head()

# COMMAND ----------

raw_df.columns = raw_df.columns.str.replace(r"[\"\',]", '_')
raw_df.columns = raw_df.columns.str.replace(' ', '_')
raw_df.columns = raw_df.columns.str.replace('-', '')
raw_df.columns

# COMMAND ----------

pre_post_sr = pd.read_csv('/dbfs/FileStore/tables/Magic_pre_post_CR_summary___pre_post_Magic_SR___1_.csv')
pre_post_sr.head()

# COMMAND ----------

pre_post_sr.columns =  pre_post_sr.iloc[2]
pre_post_sr = pre_post_sr.iloc[3:,:]
pre_post_sr.head()

# COMMAND ----------

pd.set_option('display.max_columns', None)

# COMMAND ----------

pre_post_sr.head(20)

# COMMAND ----------

mx_tag_query = '''
select merchant_id, team_owner
from aggregate_ba.final_team_tagging
'''
mx_tag_db = spark.sql(mx_tag_query)
mx_tag_df = mx_tag_db.toPandas()
mx_tag_df.head()

# COMMAND ----------

mx_final = pd.merge(raw_df, mx_tag_df, left_on = 'MID', right_on = 'merchant_id', how='inner')
mx_final.head()

# COMMAND ----------

mx_mm_df = mx_final[(mx_final['team_owner'] == 'Mid Market') & (mx_final['Checkout_Type'] == 'Single Checkout') & (mx_final['Diff'].notnull())]
mx_mm_df.reset_index().head()

# COMMAND ----------

mx_mm_df[mx_mm_df['MID']=='Aw6K8VSqA14ddz']

# COMMAND ----------

mx_mm_df = pd.merge(mx_mm_df, pre_post_sr, left_on='MID', right_on='Merchant ID', how='left')
mx_mm_df.head()

# COMMAND ----------

mx_mm_df.dtypes

# COMMAND ----------

mx_mm_df['std Payment Success'] = mx_mm_df['std Payment Success'].apply(lambda x: x.replace(',',''))
mx_mm_df['std Payment Attempts'] = mx_mm_df['std Payment Attempts'].apply(lambda x: x.replace(',',''))
mx_mm_df['Magic Payment Attempts'] = mx_mm_df['Magic Payment Attempts'].apply(lambda x: x.replace(',',''))
mx_mm_df['Magic Payment Success'] = mx_mm_df['Magic Payment Success'].apply(lambda x: x.replace(',',''))
mx_mm_df.head()

# COMMAND ----------


mx_mm_df['std SR'] = (mx_mm_df['std Payment Success'].astype(float)) / (mx_mm_df['std Payment Attempts'].astype(float))
mx_mm_df['Magic SR'] = (mx_mm_df['Magic Payment Success'].astype(float)) / (mx_mm_df['Magic Payment Attempts'].astype(float))
mx_mm_df.head()

# COMMAND ----------

mx_mm_df['SR diff'] = mx_mm_df['Magic SR'] - mx_mm_df['std SR']


# COMMAND ----------

mx_mm_df.columns

# COMMAND ----------

mx_mm_df['Post_CR(Jan__Mar)'].describe()
mx_mm_df['post_cr_bucket'] = mx_mm_df['Post_CR(Jan__Mar)'].apply(lambda x:'high' if x>0.3 else 'low')
mx_mm_df.head()

# COMMAND ----------

mx_mm_df.head(20)

# COMMAND ----------

sns.boxplot(data=mx_mm_df, x ='post_cr_bucket', y='SR diff', hue='CR_Flag')

# COMMAND ----------

mx_mm_df[mx_mm_df['CR_Flag']=='Dip']

# COMMAND ----------

dropoff_query = '''
SELECT
merchant_id,
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
    COUNT(DISTINCT magic_checkout_fact.checkout_id) AS count_of_checkout_id
FROM aggregate_pa.magic_checkout_fact AS magic_checkout_fact
WHERE (magic_checkout_fact.merchant_id ) IS NOT NULL
GROUP BY
    1,2
'''
dropoff_db = spark.sql(dropoff_query)
dropoff_df = dropoff_db.toPandas()
dropoff_df.head()

# COMMAND ----------

dropoff_df = dropoff_df.groupby(['merchant_id','summary_screen_dropoffs']).sum().reset_index()


# COMMAND ----------

dropoff_df.head(10)

# COMMAND ----------

dropoff_df['percent'] = dropoff_df['count_of_checkout_id'] / dropoff_df.groupby('merchant_id')['count_of_checkout_id'].transform('sum')
dropoff_df.head(20)

# COMMAND ----------

bounced_df = dropoff_df[dropoff_df['summary_screen_dropoffs'] == 'Bounced w/o any interaction'][['merchant_id','percent']]
bounced_df.head()

# COMMAND ----------

mx_mm_df = pd.merge(mx_mm_df,bounced_df, on='merchant_id', how='left')
mx_mm_df.head(10)

# COMMAND ----------

mx_mm_df = mx_mm_df.rename(columns={'percent':'bounce_rate'})
mx_mm_df.head()

# COMMAND ----------

mx_mm_df[mx_mm_df['MCC_code']!=5971][['bounce_rate','CR_Flag']].boxplot(by=['CR_Flag'])

# COMMAND ----------

mx_mm_df[['bounce_rate','MCC_code']].boxplot(by=['MCC_code'], grid=False, rot= 45)

# COMMAND ----------

mx_mm_df[mx_mm_df['CR_Flag']=='Dip']

# COMMAND ----------

website_query='''
select id, website from
realtime_hudi_api.merchants

'''
website_db = spark.sql(website_query)
website_df = website_db.toPandas()
website_df.head()

# COMMAND ----------

mx_mm_df = pd.merge(mx_mm_df, website_df, left_on = 'MID', right_on='id', how='left')
mx_mm_df.head(10)

# COMMAND ----------

funnel_query = '''
SELECT
merchant_id,
    (CASE
      WHEN event_name='open' THEN '0. Open'
      WHEN event_name='render:1cc_summary_screen_loaded_completed' THEN '1.1 Summary Screen Loading Completed'
      ---summary screen ends
      WHEN event_name='behav:1cc_summary_screen_continue_cta_clicked' THEN '3.1 Summary Screen Continue CTA clicked'
      --saved/new address screen starts
      when event_name='render:1cc_add_new_address_screen_loaded_completed' or event_name='render:1cc_saved_shipping_address_screen_loaded'
      then '4.1 New or Saved Address Screen Loaded'
      WHEN event_name='behav:1cc_add_new_address_screen_continue_cta_clicked' or event_name='behav:1cc_saved_address_screen_continue_cta_clicked'
      then '4.2 New Address or Saved Screen Conitnue CTA clicked'
      WHEN event_name='render:1cc_payment_home_screen_loaded' THEN '6.1 Payment Home Screen L0 Loaded'
      WHEN event_name='behav:1cc_payment_home_screen_method_selected' THEN '6.2 Payment Method selected on Payment L0 Screen'

      WHEN event_name='behav:1cc_confirm_order_summary_submitted' OR event_name='submit' THEN '6.3 Payment Submitted'
      ELSE 'NA' END
) AS funnel_events,
   
    COUNT(DISTINCT (checkout_id)) AS cnt_cid
FROM aggregate_pa.cx_1cc_events_dump_v1 
group by 1,2
'''
funnel_db = spark.sql(funnel_query)
funnel_df = funnel_db.toPandas()
funnel_df.head()

# COMMAND ----------

funnel_df['funnel_events'].unique()
funnel_df = funnel_df[funnel_df['funnel_events'] != 'NA']
funnel_df.head()

# COMMAND ----------

open_df = funnel_df[funnel_df['funnel_events'] == '0. Open']
open_df.head()

# COMMAND ----------

open_df = open_df.rename(columns = {'cnt_cid':'cnt_open'})

# COMMAND ----------

funnel_df = pd.merge(funnel_df, open_df[['merchant_id','cnt_open']], on='merchant_id', how='left')
funnel_df.head()

# COMMAND ----------

funnel_df['percent'] = funnel_df['cnt_cid']/ funnel_df['cnt_open']
funnel_df.head()

# COMMAND ----------

funnel_df[funnel_df['merchant_id'] == '4cdLRWwWy3Cn9n']

# COMMAND ----------

funnel_pivot = funnel_df.pivot(index = 'merchant_id', columns='funnel_events', values='percent' )
funnel_pivot.head(10)

# COMMAND ----------

funnel_pivot['summary_dropoffs'] = funnel_pivot['1.1 Summary Screen Loading Completed'] - funnel_pivot['3.1 Summary Screen Continue CTA clicked']
funnel_pivot['summary_to_pmt_dropoffs'] = funnel_pivot['1.1 Summary Screen Loading Completed'] - funnel_pivot['6.1 Payment Home Screen L0 Loaded']
funnel_pivot['address_dropoffs'] = funnel_pivot['4.1 New or Saved Address Screen Loaded'] - funnel_pivot['4.2 New Address or Saved Screen Conitnue CTA clicked']
funnel_pivot['pmt_dropoffs'] = funnel_pivot['6.1 Payment Home Screen L0 Loaded'] - funnel_pivot['6.3 Payment Submitted']
funnel_pivot.head(10)

# COMMAND ----------

funnel_pivot.columns

# COMMAND ----------

mx_mm_df = pd.merge(mx_mm_df, funnel_pivot[['summary_dropoffs', 'summary_to_pmt_dropoffs',
       'address_dropoffs', 'pmt_dropoffs']], left_on = 'MID', right_on='merchant_id', how='left')
mx_mm_df.head()

# COMMAND ----------

mx_mm_df['%Mweb_Checkout'].describe()

# COMMAND ----------

mx_mm_df.columns

# COMMAND ----------

mx_mm_df.groupby(by=['post_cr_bucket','CR_Flag']).agg({'%Mweb_Checkout':'median', 'MID':'count'})

# COMMAND ----------

sns.boxplot(data=mx_mm_df, x ='post_cr_bucket', y='%Mweb_Checkout', hue='CR_Flag')

# COMMAND ----------

mx_mm_df['tier3_and_above'] = mx_mm_df['%Tier_3_Checkout'] + mx_mm_df['%Unclassified_Tier_Checkout']
mx_mm_df.head()

# COMMAND ----------

mx_mm_df[['summary_dropoffs', 'summary_to_pmt_dropoffs',
       'address_dropoffs', 'pmt_dropoffs','CR_Flag']].boxplot(by=['CR_Flag'], layout=(5,1),figsize=(6,10))

# COMMAND ----------

mx_mm_df[['summary_dropoffs', 'CR_Flag']].boxplot(by=['CR_Flag'],)

# COMMAND ----------

mx_mm_df[['address_dropoffs', 'CR_Flag']].boxplot(by=['CR_Flag'],)

# COMMAND ----------

mx_mm_df[['address_dropoffs']].describe()

# COMMAND ----------

bins=[-0.05,0,0.05,0.08, 0.15, 0.4]
mx_mm_df['address_dropoffs_bins'] = pd.cut(mx_mm_df['address_dropoffs'], bins)
mx_mm_df.head()

# COMMAND ----------

df2 = mx_mm_df.groupby(by=['address_dropoffs_bins','CR_Flag']).agg({'MID':'count'}).reset_index()
df2.pivot(columns='CR_Flag', index='address_dropoffs_bins', values='MID').reset_index()

# COMMAND ----------

mx_mm_df[['pmt_dropoffs', 'CR_Flag']].boxplot(by=['CR_Flag'],)

# COMMAND ----------

mx_mm_df[mx_mm_df['CR_Flag']=='Dip']['bounce_rate'].describe()

# COMMAND ----------

bins=[0,0.2,0.3,1]
mx_mm_df['bounce_rate_bins'] = pd.cut(mx_mm_df['bounce_rate'], bins)
mx_mm_df.head()
mx_mm_df.groupby(by=['post_cr_bucket','bounce_rate_bins','CR_Flag']).agg({'MID':'count'}).reset_index()

# COMMAND ----------

sns.boxplot(data=mx_mm_df, x ='post_cr_bucket', y='bounce_rate', hue='CR_Flag')

# COMMAND ----------

sns.boxplot(data=mx_mm_df, x ='post_cr_bucket', y='summary_to_pmt_dropoffs', hue='CR_Flag')
#mx_mm_df[['summary_to_pmt_dropoffs', 'CR_Flag']].boxplot(by=['CR_Flag'],)

# COMMAND ----------

bins = [0,0.5, 0.6, 0.7,1]
mx_mm_df['summary_to_pmt_dropoffs_bins'] = pd.cut(mx_mm_df['summary_to_pmt_dropoffs'], bins)
mx_mm_df.head()

# COMMAND ----------



# COMMAND ----------

df1 = mx_mm_df.groupby(by=['post_cr_bucket','summary_to_pmt_dropoffs_bins','CR_Flag']).agg({'MID':'count'}).reset_index()
df1.pivot(index=['post_cr_bucket','summary_to_pmt_dropoffs_bins'], columns='CR_Flag', values='MID').reset_index()

# COMMAND ----------

#they are twice as likely 

# COMMAND ----------

corr = mx_mm_df[['bounce_rate','tier3_and_above','GMV_per_week','Diff']].corr()
corr.style.background_gradient(cmap='coolwarm')

# COMMAND ----------

#Diff = Post Magic CR - Pre Magic CR
sns.scatterplot(x="tier3_and_above", y="Diff", data=mx_mm_df, hue=
               'CR_Flag')

# COMMAND ----------

sns.boxplot( data=mx_mm_df,x="tier3_and_above", y="post_cr_bucket", hue=
               'CR_Flag')

# COMMAND ----------

mx_mm_df['tier3_and_above'].describe()

# COMMAND ----------

sns.scatterplot(x="bounce_rate", y="Diff", data=mx_mm_df)

# COMMAND ----------

sns.scatterplot(x="bounce_rate", y="tier3_and_above", data=mx_mm_df)

# COMMAND ----------

conditions = [
    (mx_mm_df['GMV_per_week'] < 140845.72) & (mx_mm_df['GMV_per_week'] >= 0),
     (mx_mm_df['GMV_per_week'] < 407978.26) & (mx_mm_df['GMV_per_week'] >= 140845.72 ),
     (mx_mm_df['GMV_per_week'] >= 407978.26)]
choices = ['low', 'medium', 'high']
mx_mm_df['gmv_tag'] = np.select(conditions, choices, default='na')
  

# COMMAND ----------

mx_mm_df['gmv_tag'].nunique

# COMMAND ----------

mx_mm_df[['bounce_rate', 'gmv_tag']].boxplot(by=['gmv_tag'],)

# COMMAND ----------

print(mx_mm_df['GMV_per_week'].quantile(0.67))

# COMMAND ----------

raw_2df = pd.read_excel('/dbfs/FileStore/Analysis___1_5__buffer-1.xlsx')

# COMMAND ----------

mx_mm_df = pd.merge(mx_mm_df,raw_2df[['MID','Post CR(Jan - Mar)']], on='MID', how='left')
mx_mm_df.head()

# COMMAND ----------


mx_mm_df['Post CR(Jan - Mar)'].describe()
mx_mm_df['post_cr_bucket'] = mx_mm_df['Post CR(Jan - Mar)'].apply(lambda x:'high' if x>0.3 else 'low')
mx_mm_df.head()

# COMMAND ----------

mx_mm_df['post_cr_bucket'] = mx_mm_df['Post CR(Jan - Mar)'].apply(lambda x:'high' if x>0.3 else 'low')
mx_mm_df.head()

# COMMAND ----------

sns.boxplot(data=mx_mm_df, x ='post_cr_bucket', y='bounce_rate', hue='CR_Flag')

# COMMAND ----------

mx_mm_df[['bounce_rate', 'post_cr_bucket']].boxplot(by=['post_cr_bucket'],)

# COMMAND ----------

mx_mm_df[['AOV_per_week']].describe()

# COMMAND ----------

bins=[0,500,2000,5000,17000]
mx_mm_df['aov_per_week_bins'] = pd.cut(mx_mm_df['AOV_per_week'], bins)
mx_mm_df.head()

# COMMAND ----------


