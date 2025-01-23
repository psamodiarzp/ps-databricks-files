# Databricks notebook source
from datetime import datetime, timedelta
from datetime import date
import calendar
import pandas as pd
import numpy as np

# COMMAND ----------

from scipy import stats
from scipy.stats import linregress

# COMMAND ----------

import matplotlib.pyplot as plt

# COMMAND ----------

pd.options.display.float_format = '{:.2f}'.format

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
    count(distinct checkout_id) as checkout_id
from base
group by 1,2,3,4,5
    """
)




# COMMAND ----------

source_df = source_db.toPandas()
#source_df = source_df.sort_values(by=['merchant_id', 'producer_created_date'])


# COMMAND ----------

source_df

# COMMAND ----------


summary_screen_df = source_df.groupby(by=['summary_screen_dropoffs','merchant_id','browser_name','os','aov']).agg({'checkout_id':'sum'}).reset_index()
summary_screen_df.head()

# COMMAND ----------

summary_screen_temp_df = summary_screen_df.pivot(index=['merchant_id','os','browser_name','aov'], columns='summary_screen_dropoffs', values='checkout_id').reset_index()
summary_screen_temp_df = summary_screen_temp_df.fillna(0)
summary_screen_temp_df['total'] = summary_screen_temp_df.iloc[:,-6:].sum(axis=1)
summary_screen_temp_df

# COMMAND ----------

summary_screen_temp_df['total'].sum()

# COMMAND ----------

#Pre-load dropoff	Post-load Bounce	Post engagement dropoff
summary_screen_temp_df['converted'] = summary_screen_temp_df['Summary Screen CTA clicked'] * 1.00 / summary_screen_temp_df['total']
summary_screen_temp_df['pre_load_dropoff'] = summary_screen_temp_df['Summary Screen did not load'] * 1.00 / summary_screen_temp_df['total']
summary_screen_temp_df['post_load_bounce'] = summary_screen_temp_df['Bounced w/o any interaction']  * 1.00 / summary_screen_temp_df['total']
summary_screen_temp_df['post_engagement_dropoff'] = 1 - (summary_screen_temp_df['converted'] + summary_screen_temp_df['pre_load_dropoff'] + summary_screen_temp_df['post_load_bounce'])
summary_screen_temp_df

# COMMAND ----------

summary_screen_temp_df = summary_screen_temp_df.sort_values(by='aov', )
summary_screen_temp_df

# COMMAND ----------


aov_temp_df = summary_screen_df.groupby(by=['aov','summary_screen_dropoffs']).agg({'checkout_id':'sum'}).reset_index()
aov_df = aov_temp_df.pivot(index='aov', columns='summary_screen_dropoffs', values='checkout_id').reset_index()
aov_df['aov'] = aov_df['aov'].astype('float')
aov_df = aov_df.fillna(0)
aov_df['total'] = aov_df.iloc[:,-6:].sum(axis=1)
aov_df['converted'] = aov_df['Summary Screen CTA clicked'] * 1.00 / aov_df['total']
aov_df['pre_load_dropoff'] = aov_df['Summary Screen did not load'] * 1.00 / aov_df['total']
aov_df['post_load_bounce'] = aov_df['Bounced w/o any interaction']  * 1.00 / aov_df['total']
aov_df['post_engagement_dropoff'] = 1 - (aov_df['converted'] + aov_df['pre_load_dropoff'] + aov_df['post_load_bounce'])
aov_df['aov_inr'] = aov_df['aov']/100
aov_df

# COMMAND ----------

slope, _, _, _, _ = linregress(x=aov_df['aov'], y=aov_df['converted'], )
(1.00).astype('int')

# COMMAND ----------

# Create a line chart

plt.figure(figsize=(18, 6))
plt.plot(aov_df[aov_df['aov_inr'] > 100][['aov_inr']], aov_df[aov_df['aov_inr'] > 100][['converted']], marker='o', linestyle='-', color='b', label='Line 1')

# Add labels and a title
#plt.xlabel('X-Axis')
plt.ylabel('Y-Axis')
plt.title('Line Chart Example')

# Add a legend (if multiple lines)
# plt.legend(['Line 1'])

# Show the chart
plt.grid(True)
plt.show()






# COMMAND ----------

print(np.percentile(aov_df['aov_inr'], 5))
print(np.percentile(aov_df['aov_inr'], 10))
print(np.percentile(aov_df['aov_inr'], 25))
print(np.percentile(aov_df['aov_inr'], 40))
print(np.percentile(aov_df['aov_inr'], 50))
print(np.percentile(aov_df['aov_inr'], 60))
print(np.percentile(aov_df['aov_inr'], 75))
print(np.percentile(aov_df['aov_inr'], 90))
print(np.percentile(aov_df['aov_inr'], 95))
print(np.percentile(aov_df['aov_inr'], 97))
print(np.percentile(aov_df['aov_inr'], 99))
aov_df['aov_bucket'] = pd.cut(aov_df['aov_inr'], bins=[0,2000, 6000, 10000, 18000, 50000, 100000, 500000])
aov_df



# COMMAND ----------

aov_grouped_df = aov_df.groupby(by=['aov_bucket']).sum().reset_index()
aov_grouped_df

# COMMAND ----------

aov_grouped_df.to_csv('/dbfs/FileStore/aov_grouped_df.csv', index=False)
"https://razorpay-dev.cloud.databricks.com/files/aov_grouped_df.csv"

# COMMAND ----------

plt.bar(aov_df['aov_inr'], aov_df['total'])

# Add labels
plt.xlabel('Values')
plt.ylabel('Frequency')
plt.title('Frequency of Values')

# COMMAND ----------


