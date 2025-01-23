# Databricks notebook source
import pandas as pd
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.header import Header
import smtplib
import os
from datetime import datetime, timedelta, date
import time

# COMMAND ----------

import matplotlib.pyplot as plt

# COMMAND ----------

import numpy as np

# COMMAND ----------

address_db = sqlContext.sql(
    """
    select count(*) , count(distinct entity_id), count(distinct id)
from realtime_hudi_api.addresses 
    """
)
address_df = address_db.toPandas()
address_df.head()

# COMMAND ----------

source_db = sqlContext.sql(
    """
    select 
    case when source_type is null then 'user_saved' else source_type end as address_source,
    id,
    lower(city) as city
   --- count(*) , count(distinct entity_id) as distinct_entity_id, count(distinct id) as distinct_address_id,
  ---  count(distinct id)*1.000/count(distinct entity_id)  as address_to_Entity_ratio
from realtime_hudi_api.addresses 
where type='shipping_address'
and created_date < '2023-09-16'
--group by 1
    """
)
source_df = source_db.toPandas()


# COMMAND ----------



# COMMAND ----------


source_df.to_csv('/dbfs/FileStore/source_df.csv')
"https://razorpay-dev.cloud.databricks.com/files/source_df.csv"

# COMMAND ----------

source_df['address_distribution'] = round((source_df['id'] / source_df['id'].sum()) * 100,2)

source_df

# COMMAND ----------

customer_db = sqlContext.sql(
    """
    select 
    case when source_type is null then 'user_saved' else source_type end as address_source,
    entity_id,
    count(distinct id) as distinct_address_id
from realtime_hudi_api.addresses 
where type='shipping_address'
and created_date < '2023-09-16'
group by 1,2
    """
)
customer_df = customer_db.toPandas()
customer_df.head()

# COMMAND ----------

for source in customer_df['address_source'].unique():
    subset = customer_df[customer_df['address_source'] == source]
    plt.hist(subset['distinct_address_id'], bins=20, alpha=0.5, label=f'Address Source: {source}')
    plt.xlabel('Addresses per Entity ID')
    plt.ylabel('Number of Entity IDs')
    plt.legend()
    plt.title('Histograms by Type')
    plt.show()








# COMMAND ----------

#Zooming in on Shopify 
plt.figure(figsize=(10, 6))
plt.hist(customer_df[(customer_df['address_source'] == 'shopify') & (customer_df['distinct_address_id'] < 20)]['distinct_address_id'], bins=200, alpha=0.5, label=f'Address Source: Shopify')
plt.xlabel('Addresses per Entity ID')
plt.ylabel('Number of Entity IDs')
plt.legend()
plt.title('Histograms by Type')
plt.show()

# COMMAND ----------

result_df = customer_df.groupby('address_source')['distinct_address_id'].agg(['mean', 'median', 
                                                 lambda x: np.percentile(x, 25),  # 25th percentile
                                                 lambda x: np.percentile(x, 75),   # 75th percentile
                                                 lambda x: np.percentile(x, 90),   # 90th percentile
                                                 lambda x: np.percentile(x, 95),   # #95th percentile
                                                ]).reset_index()
result_df.columns = ['source_type', 'mean', 'median', '25th_percentile', '75th_percentile', '90th_percentile', '95th_percentile',]
result_df

# COMMAND ----------

result_df = ['distinct_address_id'].agg(['mean', 'median', 
                                                 lambda x: np.percentile(x, 25),  # 25th percentile
                                                 lambda x: np.percentile(x, 75),   # 75th percentile
                                                 lambda x: np.percentile(x, 90),   # 90th percentile
                                                 lambda x: np.percentile(x, 95),   # #95th percentile
                                                ]).reset_index()
result_df.columns = ['source_type', 'mean', 'median', '25th_percentile', '75th_percentile', '90th_percentile', '95th_percentile',]
result_df

# COMMAND ----------


df1 = pd.read_json("/dbfs/FileStore/shared_uploads/pallavi.samodia@razorpay.com/city_static_attrs.json")

# COMMAND ----------

df1

# COMMAND ----------

city_db = sqlContext.sql(
    """
    select 
    case when source_type is null then 'user_saved' else source_type end as address_source,
    lower(city),
    count(distinct id) as distinct_address_id,
    count(distinct entity_id) as distinct_entity_id
from realtime_hudi_api.addresses 
where type='shipping_address'
and created_date < '2023-09-16'
group by 1,2
    """
)
city_df = city_db.toPandas()
city_df.head()

# COMMAND ----------

city_df = city_df.rename(columns={'lower(city)':'city'})
city_df['modified_city_name'] = city_df['city'].str.split(', ').str[0]
city_df

# COMMAND ----------

city_df.sort_values(by='distinct_address_id', ascending=False)

# COMMAND ----------

city_result_df = city_df[['city','address_source','distinct_address_id','distinct_entity_id']].groupby(by=['city','address_source']).agg('sum').sort_values(by='distinct_address_id', ascending=False).reset_index()
city_result_df

# COMMAND ----------

city_result_df

# COMMAND ----------

city_result_df['modified_city_name'] = city_result_df['city'].str.split(', ').str[0]
city_result_df

# COMMAND ----------

result_df = city_df[['modified_city_name','distinct_address_id','distinct_entity_id','address_source']].groupby(by=['address_source','modified_city_name']).agg('sum').sort_values(by='distinct_address_id', ascending=False).reset_index()
result_df['address_distribution'] = round((result_df['distinct_address_id'] / result_df['distinct_address_id'].sum()) * 100,2)
result_df

# COMMAND ----------

city_tier_mapping_df = df1.transpose().reset_index(drop=False,)
city_tier_mapping_df = city_tier_mapping_df.rename(columns={'index':'city'})
city_tier_mapping_df

# COMMAND ----------

city_tier_mapping_df.index

# COMMAND ----------

city_tier_mapping_df.to_csv('/dbfs/FileStore/city_tier_mapping.csv')

# COMMAND ----------

city_tier_mapping_df = pd.read_csv('/dbfs/FileStore/city_tier_mapping.csv')

# COMMAND ----------

result2_df = result_df.merge( city_tier_mapping_df[['city','cityTier']],how='left', left_on='modified_city_name',right_on='city'   )
#result2_df = result2_df[~result2_df['cityTier'].isna()]
result2_df['cityTier'] = result2_df['cityTier'].fillna('No City Tier')
result2_df

# COMMAND ----------

source_citytier_temp_df = result2_df[['cityTier','distinct_address_id','distinct_entity_id','address_distribution','address_source']].groupby(by=['address_source','cityTier']).agg('sum').reset_index()
source_citytier_df = source_citytier_temp_df.pivot(index='address_source', columns='cityTier', values=['distinct_address_id']).reset_index()
source_citytier_df

# COMMAND ----------

result2_df[result2_df['cityTier'].isna()]['distinct_address_id'].sum()

# COMMAND ----------

result2_df['distinct_address_id'].sum()

# COMMAND ----------

magic_users_db = sqlContext.sql(
"""
with checkout_data as (
select 
  
    checkout_id, 
merchant_id,
coalesce(
get_json_object(properties, '$.data.meta.address_id'),
get_json_object(properties, '$.data.address_id'),
  get_json_object(properties, '$.data.pre_selected_saved_address_id')
)  as address_id

from aggregate_pa.cx_1cc_events_dump_v1 
where producer_created_date between date('2023-08-01') and date('2023-09-15')
and event_name in ('render:1cc_saved_shipping_address_screen_loaded','render:1cc_summary_screen_loaded_completed','behav:1cc_saved_shipping_address_selected')
--and checkout_id = 'MWw8DIXTYO1LLx'
--group by 1,2

)
select
  case when source_type is null then 'user_saved' else source_type end as address_source,
  lower(a.city) as city,
  checkout_data.* from checkout_data
  left join realtime_hudi_api.addresses a on checkout_data.address_id = a.id
  where address_id is not null
  
;

"""
)
magic_users_df = magic_users_db.toPandas()
magic_users_df.head()

# COMMAND ----------

magic_users_df.to_csv('/dbfs/FileStore/magic_users_address.csv')
#magic_users_db.write.csv('/dbfs/FileStore/magic_users_address_2.csv', mode="overwrite", header=True)

# COMMAND ----------



# COMMAND ----------

magic_users_df.shape

# COMMAND ----------

magic_users_df.head()

# COMMAND ----------

city_tier_mapping_df = pd.read_csv('/dbfs/FileStore/city_tier_mapping.csv', )
city_tier_mapping_df

# COMMAND ----------

result3_df = magic_users_df.merge( city_tier_mapping_df[['city','cityTier']],how='left', left_on='city',right_on='city'   )
#result2_df = result2_df[~result2_df['cityTier'].isna()]
result3_df['cityTier'] = result3_df['cityTier'].fillna('No City Tier')
result3_df

# COMMAND ----------


result3_df.to_csv('/dbfs/FileStore/result3_df.csv')
"https://razorpay-dev.cloud.databricks.com/files/result3_df.csv"

# COMMAND ----------

mid_source_temp_df = result3_df.groupby(by=['merchant_id','address_source']).agg({'checkout_id':'nunique','address_id':'nunique'}).reset_index()
mid_source_df = mid_source_temp_df.pivot(index='merchant_id',columns='address_source', values=['address_id','checkout_id']).reset_index()
mid_source_df = mid_source_df.fillna(0)
mid_source_df

# COMMAND ----------

mid_source_df.to_csv('/dbfs/FileStore/mid_source_df.csv')
"https://razorpay-dev.cloud.databricks.com/files/mid_source_df.csv"

# COMMAND ----------

mid_tier_temp_df = result3_df.groupby(by=['merchant_id','cityTier']).agg({'checkout_id':'nunique','address_id':'nunique'}).reset_index()
mid_tier_df = mid_tier_temp_df.pivot(index='merchant_id',columns='cityTier', values=['address_id','checkout_id']).reset_index()
mid_tier_df = mid_tier_df.fillna(0)
mid_tier_df

# COMMAND ----------

mid_tier_df.to_csv('/dbfs/FileStore/mid_tier_df.csv')
"https://razorpay-dev.cloud.databricks.com/files/mid_tier_df.csv"

# COMMAND ----------

mid_source_tier_temp_df = result3_df.groupby(by=['merchant_id','address_source','cityTier']).agg({'checkout_id':'nunique','address_id':'nunique'}).reset_index()
mid_source_tier_df = mid_source_tier_temp_df.pivot(index='merchant_id',columns=['address_source','cityTier'], values=['address_id']).reset_index()
mid_source_tier_df = mid_source_tier_df.fillna(0)
mid_source_tier_df

# COMMAND ----------

mid_source_tier_df.to_csv('/dbfs/FileStore/mid_source_tier_df.csv')
"https://razorpay-dev.cloud.databricks.com/files/mid_source_tier_df.csv"

# COMMAND ----------

result3_df = pd.read_csv('/dbfs/FileStore/result3_df.csv')

# COMMAND ----------

source_tier_temp_df = result3_df.groupby(by=['address_source','cityTier']).agg({'checkout_id':'nunique','address_id':'nunique'}).reset_index()
source_tier_df = source_tier_temp_df.pivot(index='address_source',columns=['cityTier'], values=['address_id']).reset_index()
source_tier_df = source_tier_df.fillna(0)
source_tier_df

# COMMAND ----------

magic_users_df['address_source'] = magic_users_df['address_source'].fillna('No Saved Address')

# COMMAND ----------

result4_df = magic_users_df[['address_source',	'checkout_id','address_id']].groupby(by='address_source').agg({'checkout_id':'nunique','address_id':'nunique'}).reset_index()
result4_df['address_distribution'] = round((result4_df['address_id'] / result4_df['address_id'].sum()) * 100,2)
result4_df

# COMMAND ----------

result5_df = result3_df[['cityTier',	'checkout_id']].groupby(by='cityTier')['checkout_id'].agg('count').reset_index()
result5_df['checkout_distribution'] = round((result5_df['checkout_id'] / result5_df['checkout_id'].sum()) * 100,2)
result5_df

# COMMAND ----------

merchant_db = sqlContext.sql(
"""
select merchant_id, citytier, count(distinct order_id) as cnt_order_id
from aggregate_pa.magic_rto_reimbursement_fact
where order_created_date >= date('2023-08-01')
group by 1,2
"""
)
merchant_df = merchant_db.toPandas()
merchant_df.head()

# COMMAND ----------

merchant_pivot_df = merchant_df.pivot(index='merchant_id', columns='citytier', values='cnt_order_id').reset_index()
merchant_pivot_df[2.0] = merchant_pivot_df[2.0] + merchant_pivot_df[3.0]
merchant_pivot_df = merchant_pivot_df.drop([3.0], axis=1)
merchant_pivot_df = merchant_pivot_df[['merchant_id',0.0,1.0,2.0]]
merchant_pivot_df = merchant_pivot_df.rename(columns={
    0.0:'0.0',
    1.0:'1.0',
    2.0:'2.0',
    })

merchant_pivot_df = merchant_pivot_df.set_index(merchant_pivot_df.columns[0])

# Drop the first column
#merchant_pivot_df.drop(columns=[merchant_pivot_df.columns[0]], inplace=True)
merchant_pivot_df 

# COMMAND ----------

# Calculate the row totals
row_totals = merchant_pivot_df.sum(axis=1)

# Calculate the percentage of each row total
percentage_df = round(merchant_pivot_df.div(row_totals, axis=0) * 100,2)
#percentage_df = percentage_df.astype(str)
merchant_result_df = pd.concat([merchant_pivot_df, percentage_df], axis=1).reset_index()
merchant_result_df['max_tier_traffic'] = merchant_result_df.iloc[:, -3:].idxmax(axis=1)
merchant_result_df

# COMMAND ----------

merchant_result_df.groupby(by='max_tier_traffic')['merchant_id'].count()

# COMMAND ----------

merchant_result_df.to_csv('/dbfs/FileStore/merchant_result_df.csv')
"https://razorpay-dev.cloud.databricks.com/files/merchant_result_df.csv"

# COMMAND ----------



# COMMAND ----------

browser_db = sqlContext.sql(
"""
WITH summary AS
(
SELECT  browser_name,  merchant_id,
  COUNT(DISTINCT checkout_id) as cid
from aggregate_pa.cx_1cc_events_dump_v1
where event_name = 'render:1cc_summary_screen_loaded_completed'
and (get_json_object(properties,'$.data.meta.initial_loggedIn')) = 'true'
and (get_json_object(properties,'$.data.meta.initial_hasSavedAddress')) = 'true'
and producer_created_date between date('2023-08-01') and date('2023-09-15')
and checkout_id not in 
        (
            SELECT DISTINCT checkout_id
            from aggregate_pa.cx_1cc_events_dump_v1
            where event_name = 'render:1cc_saved_shipping_address_screen_loaded'
            and producer_created_date  between date('2023-08-01') and date('2023-09-15')
        
)
group by 1,2
),
saved_add as
(
select browser_name, merchant_id,
  COUNT(DISTINCT checkout_id) as cid
from aggregate_pa.cx_1cc_events_dump_v1
where event_name = 'render:1cc_saved_shipping_address_screen_loaded'
   and producer_created_date  >= date('2023-08-01') 
  group by 1,2
),
total as 
(
SELECT DISTINCT  browser_name, merchant_id,count(distinct checkout_id)  as cid
from aggregate_pa.cx_1cc_events_dump_v1
where event_name = 'render:1cc_summary_screen_loaded_completed'
   and producer_created_date  >= date('2023-08-01') 
  group by 1,2
)
  
  
SELECT 
total.*, summary.cid, saved_add.cid
FROM total 
LEFT JOIN summary ON total.merchant_id = summary.merchant_id and total.browser_name = summary.browser_name 
LEFT JOIN saved_add ON total.merchant_id = saved_add.merchant_id and total.browser_name = saved_add.browser_name 

ORDER BY 1 DESC
"""


)
browser_df = browser_db.toPandas()
browser_df.head()

# COMMAND ----------

browser_df.columns=['browser_name','merchant_id','total','summary','saved']

# COMMAND ----------

result6_df = browser_df[['merchant_id','total','summary','saved']].groupby(by='merchant_id').agg('sum').reset_index()
result6_df['prefill_rate'] = (result6_df['saved'] + result6_df['summary'] )/result6_df['total']
result6_df

# COMMAND ----------

result6_df['volume_bucket'] = pd.cut(result6_df['total'], [0,1000, 5000, 10000,100000, 1000000])
result6_df['prefill_rate_bucket'] = pd.cut(result6_df['prefill_rate'], [0,0.20, 0.30, 0.40,0.5,0.6,0.7,1])
result6_df

# COMMAND ----------

result6_df.groupby(by='volume_bucket').agg({'merchant_id':'nunique'})

# COMMAND ----------

(
    result6_df[result6_df['volume_bucket']=='(100000, 1000000]']['summary'].sum() 
    + result6_df[result6_df['volume_bucket']=='(100000, 1000000]']['saved'].sum()
)/result6_df[result6_df['volume_bucket']=='(100000, 1000000]']['total'].sum()


# COMMAND ----------

result6_df.dtypes

# COMMAND ----------

result6_df

# COMMAND ----------

result6_df['volume_bucket'] = result6_df['volume_bucket'].astype('str')

# COMMAND ----------

result6_df.dtypes

# COMMAND ----------

result6_df['volume_bucket'].unique()

# COMMAND ----------

result6_df.to_csv('/dbfs/FileStore/saved_address_rate_volume.csv')
#"https://razorpay-dev.cloud.databricks.com/files/saved_address_rate_volume.csv"

# COMMAND ----------

result6_df[(
    (result6_df['volume_bucket']=='(100000, 1000000]') | 
    (result6_df['volume_bucket']=='(10000, 100000]'))
           
           ]

# COMMAND ----------

result6_df.groupby(by='prefill_rate_bucket').agg({'merchant_id':'nunique'})

# COMMAND ----------

result9_df = result6_df.groupby(by=['volume_bucket','prefill_rate_bucket']).agg({'merchant_id':'nunique'}).reset_index()
result9_df = result9_df.pivot(index='prefill_rate_bucket', columns='volume_bucket', values='merchant_id').reset_index()
result9_df

# COMMAND ----------

result8_df = merchant_result_df.merge(result6_df, on='merchant_id', how='left')
result8_df

# COMMAND ----------



# COMMAND ----------

result7_df = browser_df[['browser_name','total','summary','saved']].groupby(by='browser_name').agg('sum').reset_index()
result7_df['prefill_rate'] = (result7_df['saved'] + result7_df['summary'] )/result7_df['total']
result7_df.sort_values(by='total', ascending=False)

# COMMAND ----------

browser_logged_db = sqlContext.sql(
"""
select 
browser_name,
get_json_object(context,'$.user_agent_parsed.os.family') as brand_family,
get_json_object(properties,'$.data.meta.initial_loggedIn') as loggedIn,
count(distinct checkout_id )
from aggregate_pa.cx_1cc_events_dump_v1
where event_name = 'render:1cc_summary_screen_loaded_completed'
   and producer_created_date  >= date('2023-08-01') 
   group by 1,2,3
   
"""

)

browser_logged_df = browser_logged_db.toPandas()
browser_logged_df.head()

# COMMAND ----------

prelogged_df = browser_logged_df.pivot(index=['browser_name','brand_family'], columns='loggedIn', values='count(DISTINCT checkout_id)').reset_index()
prelogged_df['total'] = (prelogged_df['true'] + prelogged_df['false'])
prelogged_df['pre_logged_rate'] = prelogged_df['true'] / prelogged_df['total'] 
prelogged_df.sort_values(by='total', ascending=False)

# COMMAND ----------

browser_final_df = prelogged_df.merge(result7_df, on='browser_name', how='left')
browser_final_df['saved_contribution'] = browser_final_df['saved'] / (browser_final_df['saved'] + browser_final_df['summary'])
browser_final_df['summary_contribution'] = browser_final_df['summary'] / (browser_final_df['saved'] + browser_final_df['summary'])
browser_final_df.sort_values(by='total_x', ascending=False)

# COMMAND ----------

browser_logged_df.head()

# COMMAND ----------

prelogged2_df = browser_logged_df.pivot(index=['browser_name','brand_family'], columns='loggedIn', values='count(DISTINCT checkout_id)').reset_index()
prelogged2_df['total'] = (prelogged2_df['true'] + prelogged2_df['false'])
prelogged2_df['pre_logged_rate'] = prelogged2_df['true'] / prelogged2_df['total'] 
prelogged2_df.sort_values(by='total', ascending=False)

# COMMAND ----------

browser_final_df.to_csv('/dbfs/FileStore/browser_level_Address_df.csv')
#"https://razorpay-dev.cloud.databricks.com/files/browser_level_Address_df.csv"

# COMMAND ----------

#saving address rate

# COMMAND ----------

address_creation_db = sqlContext.sql(

"""
select cast(from_unixtime(created_at+19800, 'yyyy-MM-dd') as date) as created_date,
case when source_type is null then 'user_saved' else source_type end as address_source,
count(distinct id) as cnt_address_id
from realtime_hudi_api.addresses
where created_at >=  1690848000
group by 1,2
"""
)
address_creation_df = address_creation_db.toPandas()
address_creation_df.head()

# COMMAND ----------



# COMMAND ----------

magic_traffic_db = sqlContext.sql(

"""
select 
producer_created_date,
count(distinct checkout_id )
from aggregate_pa.cx_1cc_events_dump_v1
where event_name = 'render:1cc_summary_screen_loaded_completed'
   and producer_created_date  >= date('2023-08-01') 
   group by 1
"""
)
magic_traffic_df = magic_traffic_db.toPandas()
magic_traffic_df.head()

# COMMAND ----------

address_creation_rate = magic_traffic_df.merge(address_creation_df, how='left', left_on='producer_created_date', right_on='created_date')
address_creation_rate['address_creation_percentage'] = address_creation_rate['cnt_address_id'] / address_creation_rate['count(DISTINCT checkout_id)']
address_creation_rate.pivot(columns=['address_source'], index=['producer_created_date'], values=['count(DISTINCT checkout_id)','address_creation_percentage','cnt_address_id']).reset_index()

# COMMAND ----------

merchant_page_db = sqlContext.sql(
"""
WITH summary AS
(
SELECT merchant_id,
  COUNT(DISTINCT checkout_id) as cid
from aggregate_pa.cx_1cc_events_dump_v1
where event_name = 'render:1cc_summary_screen_loaded_completed'
--and (get_json_object(properties,'$.data.meta.initial_loggedIn')) = 'true'
and get_json_object(properties, '$.data.pre_selected_saved_address_id') IS NOT NULL
and producer_created_date between date('2023-08-01') and date('2023-09-15')
and checkout_id not in 
        (
            SELECT DISTINCT checkout_id
            from aggregate_pa.cx_1cc_events_dump_v1
            where event_name in ('render:1cc_saved_shipping_address_screen_loaded','behav:1cc_saved_shipping_address_selected')
            and producer_created_date  between date('2023-08-01') and date('2023-09-15')
        
)
group by 1
),
saved_add as
(
select merchant_id,
  COUNT(DISTINCT checkout_id) as cid
from aggregate_pa.cx_1cc_events_dump_v1
where event_name in ('render:1cc_saved_shipping_address_screen_loaded','behav:1cc_saved_shipping_address_selected')
   and producer_created_date between date('2023-08-01') and date('2023-09-15')
  group by 1
),
total as 
(
SELECT DISTINCT  merchant_id,count(distinct checkout_id)  as cid
from aggregate_pa.cx_1cc_events_dump_v1
where event_name = 'open'
   and producer_created_date  between date('2023-08-01') and date('2023-09-15')
  group by 1
)
  
  
SELECT 
total.*, summary.cid, saved_add.cid
FROM total 
LEFT JOIN summary ON total.merchant_id = summary.merchant_id 
LEFT JOIN saved_add ON total.merchant_id = saved_add.merchant_id 

ORDER BY 1 DESC
"""


)
merchant_page_df = merchant_page_db.toPandas()
merchant_page_df.head()

# COMMAND ----------

merchant_page_df.columns = ['merchant_id','total','summary','saved']
merchant_page_grouped_df = merchant_page_df[['merchant_id','total','summary','saved']].groupby(by='merchant_id').agg('sum').reset_index()
merchant_page_grouped_df['prefill_rate'] = (merchant_page_grouped_df['saved'] + merchant_page_grouped_df['summary'] )/merchant_page_grouped_df['total']
merchant_page_grouped_df

# COMMAND ----------

merchant_page_grouped_df.to_csv('/dbfs/FileStore/merchant_page_grouped_df.csv')
#"https://razorpay-dev.cloud.databricks.com/files/merchant_page_grouped_df.csv"

# COMMAND ----------

merchant_page_grouped_df = pd.read_csv('/dbfs/FileStore/merchant_page_grouped_df.csv')

# COMMAND ----------

merchant_page_grouped_df['volume_bucket'] = pd.cut(merchant_page_grouped_df['total'], [0,1000, 5000, 10000,100000, 1000000])
merchant_page_grouped_df['prefill_rate_bucket'] = pd.cut(merchant_page_grouped_df['prefill_rate'], [0,0.20, 0.30, 0.40,0.5,0.6,0.7,1])
merchant_page_grouped_df

# COMMAND ----------

date_page_db = sqlContext.sql(
"""
WITH summary AS
(
SELECT 
WEEKOFYEAR(producer_created_date) as week,
  COUNT(DISTINCT checkout_id) as cid
from aggregate_pa.cx_1cc_events_dump_v1
where event_name = 'render:1cc_summary_screen_loaded_completed'
--and (get_json_object(properties,'$.data.meta.initial_loggedIn')) = 'true'
and get_json_object(properties, '$.data.pre_selected_saved_address_id') IS NOT NULL
and producer_created_date between date('2023-03-01') and date('2023-09-15')
and checkout_id not in 
        (
            SELECT DISTINCT checkout_id
            from aggregate_pa.cx_1cc_events_dump_v1
            where event_name in ('render:1cc_saved_shipping_address_screen_loaded','behav:1cc_saved_shipping_address_selected')
            and producer_created_date  between date('2023-08-01') and date('2023-09-15')
        
)
group by 1
),
saved_add as
(
select WEEKOFYEAR(producer_created_date) as week,
  COUNT(DISTINCT checkout_id) as cid
from aggregate_pa.cx_1cc_events_dump_v1
where event_name in ('render:1cc_saved_shipping_address_screen_loaded','behav:1cc_saved_shipping_address_selected')
   and producer_created_date between date('2023-08-01') and date('2023-09-15')
  group by 1
),
total as 
(
SELECT WEEKOFYEAR(producer_created_date) as week,
count(distinct checkout_id)  as cid
from aggregate_pa.cx_1cc_events_dump_v1
where event_name = 'open'
   and producer_created_date  between date('2023-08-01') and date('2023-09-15')
  group by 1
)
  
  
SELECT 
total.*, summary.cid, saved_add.cid
FROM total 
LEFT JOIN summary ON total.week = summary.week 
LEFT JOIN saved_add ON total.week = saved_add.week 

ORDER BY 1 DESC
"""


)
date_page_df = date_page_db.toPandas()
date_page_df.head()

# COMMAND ----------


