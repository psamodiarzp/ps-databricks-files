# Databricks notebook source
import pandas as pd
import os
from datetime import datetime, timedelta, date
import time

# COMMAND ----------

import matplotlib.pyplot as plt
import numpy as np

# COMMAND ----------

from scipy import stats


# COMMAND ----------

from scipy.stats import linregress

# COMMAND ----------

merchants_db = sqlContext.sql("""
       
        with base as(
            select pf.merchant_id,
            created_date,
            count(distinct payment_attempt) as pa
            from aggregate_pa.magic_payment_fact pf
            group by 1,2
        ), live_mx as (
            select pf.merchant_id,
            min(created_date) as live_date
            from aggregate_pa.magic_payment_fact pf
            group by 1
        )
            select * from base left join live_mx on base.merchant_id = live_mx.merchant_id
            WHERE DATEDIFF( created_date, live_date ) between 1 and 31
            --WHERE (DATE_DIFF('day',DATE(live_date),created_date )) between 1 and 31
            
                              """)
merchants_df = merchants_db.toPandas()
merchants_df.head()

# COMMAND ----------

merchants_df = merchants_df.loc[:, ~merchants_df.columns.duplicated()]
merchants_df[merchants_df['merchant_id']=='4af5pL6Gz4AElE']


# COMMAND ----------

merchants_df = merchants_df.sort_values(by=['merchant_id','created_date'])
merchants_df['date_difference'] = merchants_df.apply(lambda row: (row['created_date'] - row['live_date']).days, axis=1)
#merchants_df['rank'] = merchants_df.groupby(by=['merchant_id',]).cumcount() + 1
mx_pivot = merchants_df.pivot(index='merchant_id', columns='date_difference', values='pa')
#mx_pivot


# COMMAND ----------



# COMMAND ----------

mx_pivot = mx_pivot.dropna().reset_index()
mx_list = mx_pivot['merchant_id']
mx_list

# COMMAND ----------

mx_list

# COMMAND ----------

mx_df = merchants_df[merchants_df['merchant_id'].isin(mx_list)]
mx_df

# COMMAND ----------

merchants_df[merchants_df['merchant_id']=='4af5pL6Gz4AElE']

# COMMAND ----------

events_source_db=sqlContext.sql(
    """
    SELECT browser_name,
    get_json_object(context,'$.user_agent_parsed.os.family') as os,
    merchant_id,
    producer_created_date,
    event_name,
    get_json_object(properties,'$.options.amount') as aov,
    count(distinct checkout_id) as cnt_cid
    from aggregate_pa.cx_1cc_events_dump_v1
    where producer_created_date >= date('2023-01-01')
    and event_name in ('render:1cc_summary_screen_loaded_completed','behav:1cc_summary_screen_continue_cta_clicked')
    GROUP BY 1,2,3,4,5,6


    """
)
events_source_df = events_source_db.toPandas()
events_source_df.head()

# COMMAND ----------

source_db=sqlContext.sql(

    """
    
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
    producer_created_date,
    COUNT(DISTINCT magic_checkout_fact.checkout_id) AS count_of_checkout_id
FROM aggregate_pa.magic_checkout_fact AS magic_checkout_fact
where merchant_id IN (
    SELECT mid FROM batch_sheets.magic_merchant_list where segment<>'SME'
)
GROUP BY 1,2,3
    """
)

source_df = source_db.toPandas()
source_df.head()

# COMMAND ----------

source_df.to_csv('/dbfs/FileStore/summary_source.csv', index=False)
"https://razorpay-dev.cloud.databricks.com/files/summary_source.csv"

# COMMAND ----------

source_df.shape

# COMMAND ----------



# COMMAND ----------

#Summary Screen Conversion Trend
summary_screen_total = source_df.groupby(by=['merchant_id','producer_created_date']).agg({'count_of_checkout_id':'sum'}).reset_index().sort_values(by=['merchant_id','producer_created_date'])
summary_screen_total['rank'] = summary_screen_total.groupby(by=['merchant_id',]).cumcount() + 1
summary_screen_total = summary_screen_total.rename(columns={'count_of_checkout_id':'summary_total'})
summary_screen_total

# COMMAND ----------

summary_screen_total[summary_screen_total['merchant_id'] == '4af5pL6Gz4AElE'].sort_values(by='rank')

# COMMAND ----------

summary_screen_df = source_df.merge(summary_screen_total, how='left', on=['merchant_id','producer_created_date']).sort_values(by=['merchant_id','producer_created_date'])
summary_screen_df['screen_division'] = round(summary_screen_df['count_of_checkout_id'] / summary_screen_df['summary_total'],4)
summary_screen_df

# COMMAND ----------

summary_screen_conversion_df

# COMMAND ----------

#summary_screen_conversion_df[['merchant_id','producer_created_date']]
source_df

# COMMAND ----------

source_df.dtypes

# COMMAND ----------

summary_screen_bounce_df = .merge(list(summary_screen_conversion_df['merchant_id'].unique()), how='inner', left_on=['merchant_id','producer_created_date'], right_on=['merchant_id','created_date'])
summary_screen_bounce_df 

# COMMAND ----------

summary_screen_bounce_temp_df = summary_screen_df[(summary_screen_df['summary_screen_dropoffs'] == 'Bounced w/o any interaction') & (summary_screen_df['rank'] < 32) & (summary_screen_df['merchant_id'].isin(summary_screen_conversion_df['merchant_id'].unique()))]
summary_screen_bounce_df = summary_screen_bounce_temp_df.pivot(index='merchant_id', columns='rank', values='screen_division').reset_index()
#summary_screen_bounce_df = summary_screen_bounce_df.dropna()
summary_screen_bounce_df

# COMMAND ----------

summary_screen_bounce_df.to_csv('/dbfs/FileStore/summary_screen_bounce_df.csv', index=False)
"https://razorpay-dev.cloud.databricks.com/files/summary_screen_bounce_df.csv"

# COMMAND ----------

summary_screen_conversion_df = summary_screen_df[summary_screen_df['summary_screen_dropoffs'] == 'Summary Screen CTA clicked'].merge(mx_df, how='inner', left_on=['merchant_id','producer_created_date'], right_on=['merchant_id','created_date'])
summary_screen_conversion_df 

# COMMAND ----------

summary_screen_df

# COMMAND ----------

summary_screen_conversion_df = pd.read_csv('/dbfs/FileStore/summary_screen_conversion_df.csv')
summary_screen_conversion_df['merchant_id'].nunique()

# COMMAND ----------

summary_screen_conversion_df['producer_created_date'].min()

# COMMAND ----------

summary_screen_conversion_df.to_csv('/dbfs/FileStore/summary_screen_conversion_df.csv')
"https://razorpay-dev.cloud.databricks.com/files/summary_screen_conversion_df.csv"

# COMMAND ----------

summary_screen_conversion_df

# COMMAND ----------

results table = events_table.merge(summary df[['mid','date','date difference']] , on = ['mid','date'], how='inner')
----> results table : mid, date, aov, date diff

# COMMAND ----------

summary_screen_conversion_df['summary_screen_dropoffs'].unique((9))

# COMMAND ----------

#Bounce off rates
summary_screen_conversion_df[summary_screen_conversion_df['summary_screen_dropoffs']=='']
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

summary_pivot_df = summary_screen_conversion_df.pivot(columns='date_difference', index='merchant_id', values='screen_division')
summary_pivot_df = summary_pivot_df[summary_pivot_df.index.isin(mx_list)]
summary_pivot_df

# COMMAND ----------

summary_pivot_df.T


# COMMAND ----------

summary_pivot_df.iloc[:,:7].T

# COMMAND ----------

summary_pivot_df.

# COMMAND ----------



def calculate_correlation(row):
    #print(row.index)
    
    # Calculate z-scores for the row's data points
    z_scores = np.abs(stats.zscore(row))
    #print(z_scores)
    
    # Set a z-score threshold for identifying outliers (e.g., z-score > 3)
    outlier_threshold = 3
    
    # Create a mask to filter outliers
    outlier_mask = (z_scores < outlier_threshold)

    
    # Calculate the correlation without outliers
    row_no_outliers = row[outlier_mask]
    #print(row_no_outliers)
    s1 = pd.Series(row_no_outliers)
    s2 = pd.Series(row_no_outliers.index)
    correlation = s1.corr(s2)
    #print(correlation)
    
    # Extract the correlation between X1, X2, and Y
    #correlation = correlation_matrix.loc['X1', row.index]
    
    return correlation

def calculate_slope(row):
    slope, _, _, _, _ = linregress(row, row.index)
    return slope

# Calculate row-wise correlations with the reference row
summary_pivot_df = summary_pivot_df.dropna()
df_transposed = summary_pivot_df.T
df_transposed_beyond7_days = summary_pivot_df.iloc[:,7:].T
df_transposed_seven_days = summary_pivot_df.iloc[:,:7].T

# Calculate column-wise correlations with respect to the first column
##correlations = df_transposed.corrwith(df_transposed.iloc[:, 0])


# Apply the calculate_slope function to each row
slope_series = df_transposed.apply(calculate_slope)
slope_beyond7day_series = df_transposed_beyond7_days.apply(calculate_slope)
slope_7day_series = df_transposed_seven_days.apply(calculate_slope)
# Apply the calculate_correlation function to each row
correlation_series = df_transposed.apply(calculate_correlation, )
correlation_beyond7day_series = df_transposed_beyond7_days.apply(calculate_correlation)
correlation_7day_series = df_transposed_seven_days.apply(calculate_correlation)

# Display the correlation for each row
correlations_base_df = pd.DataFrame({'merchant_id': correlation_series.index, 'Correlation': correlation_series.values, 
                               'Correlation_7day': correlation_7day_series.values, 'Correlation_beyond7day': correlation_beyond7day_series.values,
                                })
slope_df = pd.DataFrame({'merchant_id': slope_series.index, 'Slope':slope_series.values,
                         'Slope_7day': slope_7day_series.values, 'slope_beyond7day_series': slope_beyond7day_series.values,
                               
                                })
                                 
correlations_df= correlations_base_df.merge(slope_df, how='left', on='merchant_id')
correlations_df

#correlations_df.sort_values(by=['Correlation'])








# COMMAND ----------

correlation_series

# COMMAND ----------

summary_trends_df.iloc[:,1:-1]

# COMMAND ----------

summary_trends_df = summary_pivot_df.merge(correlations_df, how='left', on='merchant_id')
summary_trends_df = summary_trends_df.dropna()
#summary_trends_df['average'] = summary_trends_df.iloc[:,1:-1].mean(axis=1)
#summary_trends_df['Correlation'] = round(summary_trends_df['Correlation'],4)
#summary_trends_df.sort_values(by='Correlation')
summary_trends_df['Category'] = ''
summary_trends_df.loc[(summary_trends_df['Slope_7day'] < 0) & (summary_trends_df['Slope'] > 0), 'Category'] = 'Slope Increased after the first week'
summary_trends_df.loc[(summary_trends_df['Slope_7day'] > 0) & (summary_trends_df['Slope'] < 0), 'Category'] = 'Slope Decreased after the first week'
summary_trends_df.loc[(summary_trends_df['Slope_7day'] > 0) & (summary_trends_df['Slope'] > 0), 'Category'] = 'Trend stayed positive'
summary_trends_df.loc[(summary_trends_df['Slope_7day'] < 0) & (summary_trends_df['Slope'] < 0), 'Category'] = 'Trend stayed negative'

summary_trends_df


# COMMAND ----------



# COMMAND ----------

summary_trends_df.to_csv('/dbfs/FileStore/summary_trends_df.csv', index=False)
"https://razorpay-dev.cloud.databricks.com/files/summary_trends_df.csv"

# COMMAND ----------

summary_trends_df.groupby

# COMMAND ----------

summary_trends_df[summary_trends_df['Correlation'] >= -0.25].sort_values(by='Correlation', ascending=False)

# COMMAND ----------

len(x)

# COMMAND ----------

# Plot each row as a line in a line graph
#df = summary_trends_df.iloc[:, :-1]
#df = summary_trends_df[(summary_trends_df['Correlation']  >= 0.35) ]

df = summary_trends_df[summary_trends_df['merchant_id'].isin(['HHgqlHH6cINbW8','CZlY9e0UeP7LJ0','C6Orx8usxjn8OT','K7RuikecA6CSyF','IEJDxs6E57EG8a']) ]
plt.figure(figsize=(20, 10)) 
for index, row in df.iterrows():
    plt.plot(row[1:-1].index, row[1:-1], label=row['Correlation'])

# Set labels and title
plt.xlabel('Columns')
plt.ylabel('Values')
plt.title('Line Graph for Multiple Rows')

# Add a legend to distinguish rows
plt.legend()

# Show the line graph
plt.show()

# COMMAND ----------

plt.hist(summary_trends_df.Correlation, bins=10, edgecolor='k', alpha=0.7)

# Set labels and title
plt.xlabel('Correlation')
plt.ylabel('Count')
plt.title('Histogram of Correlation Values')

# Show the histogram
plt.show()

# COMMAND ----------

summary_volume_df

# COMMAND ----------

summary_volume_df = summary_screen_conversion_df.pivot(columns='date_difference', index='merchant_id', values='summary_total')
summary_volume_df = summary_volume_df[summary_pivot_df.index.isin(mx_list)]
summary_volume_df['average'] = round(summary_volume_df.mean(axis=1),0)
summary_volume_df

# COMMAND ----------

#Volume bins
bins = [0,50,100,250,500,1000,50000]
summary_volume_df['volume_bucket'] = pd.cut(summary_volume_df['average'], bins=bins)
summary_volume_df

# COMMAND ----------

summary_combined_df = summary_trends_df.merge(summary_volume_df[['volume_bucket']], how='left', on='merchant_id')
summary_combined_df['volume_bucket'] = summary_combined_df['volume_bucket'].astype('str')
#summary_combined_df['average'] = summary_combined_df.iloc[:,1:32].mean(axis=1)
summary_combined_df

# COMMAND ----------

summary_combined_df.groupby(by='volume_bucket')['merchant_id'].count()

# COMMAND ----------

result_df = summary_combined_df.groupby('volume_bucket')['average'].agg(['mean', 'median', 
                                                 lambda x: np.percentile(x, 25),  # 25th percentile
                                                 lambda x: np.percentile(x, 75),   # 75th percentile
                                                 lambda x: np.percentile(x, 90),   # 90th percentile
                                                 lambda x: np.percentile(x, 95),   # #95th percentile
                                                ]).reset_index()
result_df.columns = ['source_type', 'mean', 'median', '25th_percentile', '75th_percentile', '90th_percentile', '95th_percentile',]
result_df

# COMMAND ----------

summary_combined_df[summary_combined_df['volume_bucket']=='(0, 50]']

# COMMAND ----------

plt.figure(figsize=(20, 10)) 
plt.hist(summary_volume_df, bins=50, edgecolor='k', alpha=0.7)

# Set labels and title
plt.xlabel('Correlation')
plt.ylabel('Count')
plt.title('Histogram of Correlation Values')

# Show the histogram
plt.show()

# COMMAND ----------



# COMMAND ----------

#Repeating Users
summary_part2_db = sqlContext.sql("""
with base as(
select device_id, min(producer_created_date) as min_date
from aggregate_pa.cx_1cc_events_dump_v1
  group by 1
  
)
select 
distinct
a.device_id,
case when producer_created_date = min_date then 'new' else 'repeating' end as new_or_repeating_user,
CAST(
          get_json_object(properties, '$.data.meta.initial_loggedIn') AS boolean
        )
     AS initial_loggedin,
--DATEDIFF(producer_created_date, min_date) AS date_difference
checkout_id,
event_name,
get_json_object(properties,'$.options.amount') as order_amount,
merchant_id,
browser_name
from aggregate_pa.cx_1cc_events_dump_v1 a 
left join base on a.device_id = base.device_id
where producer_created_date >= date('2023-08-01')
and event_name in ('render:1cc_summary_screen_loaded_completed','behav:1cc_summary_screen_continue_cta_clicked')
""")

summary_part2_df = summary_part2_db.toPandas()
summary_part2_df.head()

# COMMAND ----------

summary_part2_df.to_csv('/dbfs/FileStore/summary_part2_df.csv', index=False)
"https://razorpay-dev.cloud.databricks.com/files/summary_part2_df.csv"

# COMMAND ----------

summary_part2_df=pd.read_csv('/dbfs/FileStore/summary_part2_df.csv')
summary_part2_df

# COMMAND ----------



# COMMAND ----------

summary_part2_df['initial_loggedin'].unique()

# COMMAND ----------

summary_level1_df = summary_part2_df[summary_part2_df['event_name']=='render:1cc_summary_screen_loaded_completed'].groupby(by=['new_or_repeating_user','order_amount','merchant_id','browser_name','initial_loggedin']).agg({'checkout_id':'nunique'}).reset_index()
summary_level1_df

# COMMAND ----------

summary_level2_df = summary_level1_df.pivot(index=['new_or_repeating_user','order_amount','merchant_id','browser_name','initial_loggedin'], columns=['event_name'], values='checkout_id').reset_index()
summary_level2_df

# COMMAND ----------



# COMMAND ----------

summary_level2_df = summary_level2_df.rename(columns={'behav:1cc_summary_screen_continue_cta_clicked':'cta', 'render:1cc_summary_screen_loaded_completed':'load'})
summary_level2_df['order_amount'] = summary_level2_df['order_amount'].astype(float)
summary_level2_df['order_amount'] = summary_level2_df['order_amount'] /100

summary_level2_df

# COMMAND ----------



# COMMAND ----------

summary_level2_df.dtypes

# COMMAND ----------

#plt.figure(figsize=(20, 10)) 
plt.hist(summary_level2_df['order_amount'], bins=50, edgecolor='k', alpha=0.7)

# Set labels and title
plt.xlabel('Correlation')
plt.ylabel('Count')
plt.title('Histogram of Correlation Values')

# Show the histogram
plt.show()

# COMMAND ----------

aov_bins = [-1000,0,500,1000,2000, 5000, 10000, 100000]
summary_level2_df['aov_bucket'] = pd.cut(summary_level2_df['order_amount'], bins=aov_bins)
summary_level2_df


# COMMAND ----------

result_df = summary_level2_df.groupby('new_or_repeating_user')['order_amount'].agg(['mean', 'median', 
                                                 lambda x: np.percentile(x, 25),  # 25th percentile
                                                 lambda x: np.percentile(x, 75),   # 75th percentile
                                                 lambda x: np.percentile(x, 90),   # 90th percentile
                                                 lambda x: np.percentile(x, 95),   # #95th percentile
                                                ]).reset_index()
result_df.columns = ['new_or_repeating_user', 'mean', 'median', '25th_percentile', '75th_percentile', '90th_percentile', '95th_percentile',]
result_df

# COMMAND ----------

summary_level2_df['cta'] = summary_level2_df['cta'].fillna(0)
summary_level2_df['load'] = summary_level2_df['load'].fillna(0)


# COMMAND ----------

summary_level3_df = summary_level2_df.groupby(by=['new_or_repeating_user','aov_bucket','merchant_id','browser_name']).agg({'cta':'sum','load':'sum'}).reset_index()
summary_level3_df

# COMMAND ----------

summary_level3_df['summary_cr'] = summary_level3_df['cta'] / summary_level3_df['load']
summary_level3_df.sort_values(by='summary_cr', ascending=False)

# COMMAND ----------

summary_level3_df = summary_level3_df.dropna( how='any', inplace=False)
summary_level3_df.sort_values(by='load', ascending=False)

# COMMAND ----------

summary_level3_df.to_csv('/dbfs/FileStore/summary_level3_df.csv', index=False)
"https://razorpay-dev.cloud.databricks.com/files/summary_level3_df.csv"

# COMMAND ----------

summary_level4_df = summary_level3_df.groupby(by=['new_or_repeating_user','aov_bucket','browser_name']).agg({'cta':'sum','load':'sum'}).reset_index()
summary_level4_df['summary_cr'] = summary_level4_df['cta'] / summary_level4_df['load']
summary_level4_df.sort_values(by='load', ascending=False)

# COMMAND ----------

summary_level4_df.to_csv('/dbfs/FileStore/summary_level4_df.csv', index=False)
"https://razorpay-dev.cloud.databricks.com/files/summary_level4_df.csv"

# COMMAND ----------

summary_level5_df = summary_level4_df.groupby(by=['browser_name','new_or_repeating_user']).agg({'load':'sum'}).reset_index().sort_values(by='load', ascending=False)
summary_level5_df

# COMMAND ----------

summary_level5_df.pivot(values='load',columns='new_or_repeating_user',index='browser_name').reset_index()

# COMMAND ----------

summary_level6_df = summary_level2_df.groupby(by=['merchant_id','new_or_repeating_user']).agg({'load':'sum'}).reset_index()
summary_level7_df = summary_level6_df.pivot(values='load', index='merchant_id', columns='new_or_repeating_user').reset_index()
summary_level7_df['total'] = summary_level7_df['new'] + summary_level7_df['repeating']
summary_level7_df['new_rate'] = summary_level7_df['new'] / summary_level7_df['total']
summary_level7_df['reapeating_rate'] = summary_level7_df['repeating'] / summary_level7_df['total']
summary_level7_df.sort_values(by=['total','reapeating_rate',], ascending=False)

# COMMAND ----------

summary_level8_df = summary_level2_df.groupby(by=['merchant_id','new_or_repeating_user','initial_loggedin']).agg({'load':'sum'}).reset_index()
summary_level9_df = summary_level8_df.pivot(values='load', index=['merchant_id'], columns=['initial_loggedin','new_or_repeating_user',]).reset_index()
#summary_level9_df.columns = summary_level9_df.columns.droplevel(level=0)
#summary_level9_df.columns = ['merchant_id', 'false_new', 'true_new', 'false_repeating', 'true_repeating']
#summary_level9_df = summary_level9_df.reset_index(drop=True)
#summary_level9_df['total_new'] = summary_level9_df['true_new'] + summary_level9_df['false_new']
#summary_level9_df['total_repeating'] = summary_level9_df['true_repeating'] + summary_level9_df['false_repeating']
#summary_level9_df['total'] = summary_level9_df['total_new'] + summary_level9_df['total_repeating']

#summary_level9_df['new_rate'] = summary_level9_df['new'] / summary_level9_df['total']
#summary_level9_df['reapeating_rate'] = summary_level9_df['repeating'] / summary_level9_df['total']
#summary_level9_df.sort_values(by=['total','reapeating_rate',], ascending=False)

# COMMAND ----------

summary_level9_df.columns.names = ['merchant_id', 'status']
summary_level9_df

# COMMAND ----------

summary_level10_temp_df = summary_level2_df.groupby(by=['browser_name','new_or_repeating_user','initial_loggedin']).agg({'load':'sum'}).reset_index()
summary_level10_df = summary_level10_temp_df.pivot(values='load', index=['browser_name'], columns=['initial_loggedin','new_or_repeating_user',]).reset_index()
summary_level10_df

# COMMAND ----------

summary_level10_df.to_csv('/dbfs/FileStore/summary_level10_df.csv', index=False)
"https://razorpay-dev.cloud.databricks.com/files/summary_level10_df.csv"

# COMMAND ----------

summary_level10_df.groupby(level='merchant_id', axis=1).apply(lambda x: (x['new'] / (x['new'] + x['repeating'])) * 100)

# COMMAND ----------

summary_level9_df.to_csv('/dbfs/FileStore/summary_level9_df.csv', index=False)
"https://razorpay-dev.cloud.databricks.com/files/summary_level9_df.csv"

# COMMAND ----------

summary_level10_df = summary_level9_df.groupby(level='status', axis=1).sum()
summary_level11_df = summary_level9_df.groupby(level='merchant_id', axis=1).sum()

# COMMAND ----------

#plt.figure(figsize=(20, 10)) 
plt.hist(summary_level7_df['reapeating_rate'], bins=10, edgecolor='k', alpha=0.7)

# Set labels and title
plt.xlabel('Repeat Rate')
plt.ylabel('Count')
plt.title('Histogram of Repeat Rate Values')

# Show the histogram
plt.show()

# COMMAND ----------


