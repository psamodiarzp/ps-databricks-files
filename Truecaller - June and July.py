# Databricks notebook source
from datetime import datetime, timedelta
from datetime import date
from datetime import datetime
import pandas as pd
import matplotlib.pyplot as plt

# COMMAND ----------

import matplotlib.pyplot as plt

# COMMAND ----------

import re

# COMMAND ----------

import numpy as np

# COMMAND ----------

!pip install clipboard


# COMMAND ----------

import clipboard

# COMMAND ----------

#Intersection of Prefill and Non-Prefill
prefill_non_prefill = sqlContext.sql("""
WITH raw AS(
  SELECT
 
      get_json_object(
        properties,
        '$.backendExperiments.truecaller_1cc_for_non_prefill'
      ) AS non_prefill,
   get_json_object(
        properties,
        '$.backendExperiments.truecaller_1cc_for_prefill'
      ) AS prefill,
      (
        CASE
          WHEN event_name = 'render:1cc_summary_screen_loaded_completed' THEN checkout_id
          ELSE NULL
        END
      ) AS summary_screen_loaded,
  (
        CASE
          WHEN event_name = 'behav:1cc_summary_screen_continue_cta_clicked' THEN checkout_id
          ELSE NULL
        END
      ) AS summary_cta_clicked,
      (
        CASE
          WHEN event_name = 'submit' THEN checkout_id
          ELSE NULL
        END
      ) AS submits
      FROM
        aggregate_pa.cx_1cc_events_dump_v1
      WHERE
        producer_created_date >= date('2023-07-15')
        and producer_created_date < date('2023-08-15')
     /*   AND event_name IN (
          'render:1cc_summary_screen_loaded_completed',
          'submit',
          'behav:1cc_summary_screen_continue_cta_clicked'
        )  */
--- AND merchant_id NOT IN ('7E6oragoxHFlvV', 'GGONsupA74tYXV', 'J1rTWZUWmgNzSW', 'IH7E2OJQGEKKTN','CG3DynCFGPNAJk')
  AND browser_name in ('Chrome Mobile','Samsung Internet')
        AND (
          UPPER(
            get_json_object (context, '$.user_agent_parsed.device.family')
          ) <> UPPER('iPhone')
        )
        AND (
          (
            LOWER(get_json_object (context, '$.platform')) = 'browser'
          )
          AND (
            get_json_object (properties, '$.data.meta.is_mobile') = 'true'
          )
        )
    )
  SELECT

    non_prefill,
    prefill,
    COUNT(DISTINCT summary_screen_loaded) as cnt_summary_screen_loaded,
    COUNT(DISTINCT summary_cta_clicked) as cnt_summary_cta_clicked,
    COUNT(DISTINCT submits) as cnt_submits
    
  FROM
    raw --limit 10
  GROUP BY
    1,2
""")
prefill_non_prefill_df = prefill_non_prefill.toPandas()
prefill_non_prefill_df.head()

# COMMAND ----------

#Mx level Summary and Modal CR
mx_level_cr = sqlContext.sql("""
WITH raw AS(
  SELECT
   merchant_id,
      get_json_object(
        properties,
        '$.backendExperiments.truecaller_1cc_for_non_prefill'
      ) AS non_prefill,
      (
        CASE
          WHEN event_name = 'render:1cc_summary_screen_loaded_completed' THEN checkout_id
          ELSE NULL
        END
      ) AS summary_screen_loaded,
  (
        CASE
          WHEN event_name = 'behav:1cc_summary_screen_continue_cta_clicked' THEN checkout_id
          ELSE NULL
        END
      ) AS summary_cta_clicked,
      (
        CASE
          WHEN event_name = 'submit' THEN checkout_id
          ELSE NULL
        END
      ) AS submits
      FROM
        aggregate_pa.cx_1cc_events_dump_v1
      WHERE
        producer_created_date >= date('2023-07-15')
  and producer_created_date < date('2023-08-15')
        AND event_name IN (
          'render:1cc_summary_screen_loaded_completed',
          'submit',
          'behav:1cc_summary_screen_continue_cta_clicked'
        ) 
 -- AND merchant_id NOT IN ('7E6oragoxHFlvV', 'GGONsupA74tYXV', 'J1rTWZUWmgNzSW', 'IH7E2OJQGEKKTN','CG3DynCFGPNAJk')
  AND browser_name in ('Chrome Mobile','Samsung Internet')
        AND (
          UPPER(
            get_json_object (context, '$.user_agent_parsed.device.family')
          ) <> UPPER('iPhone')
        )
        AND (
          (
            LOWER(get_json_object (context, '$.platform')) = 'browser'
          )
          AND (
            get_json_object (properties, '$.data.meta.is_mobile') = 'true'
          )
        )
    )
  SELECT
  merchant_id,
    non_prefill,
    COUNT(DISTINCT summary_screen_loaded) as cnt_summary_screen_loaded,
    COUNT(DISTINCT summary_cta_clicked) as cnt_summary_cta_clicked,
    COUNT(DISTINCT submits) as cnt_submits
    
  FROM
    raw --limit 10
    where non_prefill = 'test'
    
  GROUP BY
    1,2
    """)
mx_level_cr_df = mx_level_cr.toPandas()
mx_level_cr_df.head()
    

# COMMAND ----------

mx_level_cr_df['summary_cr'] = mx_level_cr_df['cnt_summary_cta_clicked'] / mx_level_cr_df['cnt_summary_screen_loaded']
mx_level_cr_df['modal_cr'] = mx_level_cr_df['cnt_submits'] / mx_level_cr_df['cnt_summary_screen_loaded']
mx_level_cr_df.head()

# COMMAND ----------

#Truecaller installed
tc_installed = sqlContext.sql("""
select 
--json_extract_scalar(properties,'$.backendExperiments.truecaller_1cc_for_non_prefill') as non_prefill,
----event_name, 
get_json_object(properties,'$.data.failure_reason') as failure_reason,
round(cast(get_json_object(properties,'$.data.total_response_time') as decimal),0) as total_response_time,
get_json_object(properties,'$.data.success') as success,
--case when json_extract_scalar(properties,'$.data.failure_reason') is null then 0 else 1 end as truceller_installed_successful,
--properties,
--checkout_id
count(distinct checkout_id)
from aggregate_pa.cx_1cc_events_dump_v1
where producer_created_date >= date('2023-07-15') 
and producer_created_date < date('2023-08-15') 
AND event_name = 'api:truecaller_installed'
/*AND event_name in ('api:prefill','render:truecaller_login_show','api:truecaller_installed',
                  'behav:truecaller_login_click','api:truecaller_verification',
                   'api:login_source','behav:login_from_truecaller_clicked','behav:use_another_number'
                  )*/
      ---    AND merchant_id NOT IN ('7E6oragoxHFlvV', 'GGONsupA74tYXV', 'J1rTWZUWmgNzSW', 'IH7E2OJQGEKKTN','CG3DynCFGPNAJk')
  AND browser_name in ('Chrome Mobile','Samsung Internet')
--  AND json_extract_scalar(properties,'$.backendExperiments.truecaller_1cc_for_prefill') <> 'test'
     AND get_json_object(properties,'$.backendExperiments.truecaller_1cc_for_non_prefill') = 'test'
AND ((LOWER(get_json_object (context, '$.platform')) = 'browser') and
          (get_json_object (properties,'$.data.meta.is_mobile') = 'true')) 
  AND (UPPER(get_json_object (context,'$.user_agent_parsed.device.family') ) <> UPPER('iPhone'))
group by 1,2,3
--limit 50
    """)
tc_installed_df = tc_installed.toPandas()
tc_installed_df.head()

# COMMAND ----------

#Truecaller installed
tc_installed_overall = sqlContext.sql("""
select 
--json_extract_scalar(properties,'$.backendExperiments.truecaller_1cc_for_non_prefill') as non_prefill,
----event_name, 
get_json_object(properties,'$.data.failure_reason') as failure_reason,
--round(cast(get_json_object(properties,'$.data.total_response_time') as decimal),0) as total_response_time,
get_json_object(properties,'$.data.success') as success,
--case when json_extract_scalar(properties,'$.data.failure_reason') is null then 0 else 1 end as truceller_installed_successful,
--properties,
--checkout_id
count(distinct checkout_id)
from aggregate_pa.cx_1cc_events_dump_v1
where producer_created_date >= date('2023-07-15') 
and producer_created_date < date('2023-08-15') 
AND event_name = 'api:truecaller_installed'
  AND browser_name in ('Chrome Mobile','Samsung Internet')
     AND get_json_object(properties,'$.backendExperiments.truecaller_1cc_for_non_prefill') = 'test'
AND ((LOWER(get_json_object (context, '$.platform')) = 'browser') and
          (get_json_object (properties,'$.data.meta.is_mobile') = 'true')) 
  AND (UPPER(get_json_object (context,'$.user_agent_parsed.device.family') ) <> UPPER('iPhone'))
group by 1,2
--limit 50
    """)
tc_installed_overall_df = tc_installed_overall.toPandas()
tc_installed_overall_df.head()

# COMMAND ----------

# Create a histogram
plt.figure(figsize=(10, 6))
grouped = tc_installed_df.groupby('total_response_time')['count(DISTINCT checkout_id)'].sum().reset_index()
#plt.bar(tc_installed.index, tc_installed['count(DISTINCT checkout_id)'], color='blue', alpha=0.7)
plt.bar(grouped['total_response_time'], grouped['count(DISTINCT checkout_id)'], color='blue', alpha=0.7)
plt.xlabel('Time Taken')
plt.ylabel('Number of Users')
plt.title('Histogram of Number of Users vs. Time Taken')
plt.xticks(range(0, 20, 100))
plt.tight_layout()
plt.show()






# COMMAND ----------

grouped

# COMMAND ----------

otp_delivery = sqlContext.sql("""
with base as(
select event_timestamp, 
event_name,
get_json_object(properties,'$.message_channel_context.template') as template,
get_json_object(properties,'$.sms_type') as sms_type,
get_json_object(properties,'$.message_id') as message_id,
  case when event_name = 'message_channels.sms.delivery_status.received' then event_timestamp else null end 
  as delivery_status_time,
  case when event_name = 'message_channels.sms.created' then event_timestamp else null end 
  as delivery_created_time,
event_timestamp - LAG(event_timestamp) OVER(
  partition by get_json_object(properties,'$.message_id') 
  order by event_name asc
) as time_taken

from events.events_message_channel_sms_v2 a
--inner join batch_sheets.magic_merchant_list b on get_json_object(a.properties,'$.owner_id') = b.mid
where producer_created_date >= '2023-08-01'
--and json_extract_scalar(properties,'$.owner_id') = 'J1rTWZUWmgNzSW'
--and json_extract_scalar(properties,'$.message_id') = 'MSRBXobNiKwkoU'
and event_name in ('message_channels.sms.created','message_channels.sms.delivery_status.received') 
  and get_json_object(properties,'$.message_channel_context.template') like '%sms.checkout%'
---limit 10
  )
  select
  template,
  time_taken,
  message_id
  --count(distinct message_id) as cnt_messages
  from base
  where event_name = 'message_channels.sms.delivery_status.received'
 -- group by 1,2
  order by 1, 2
      """)
otp_delivery_df = otp_delivery.toPandas()
otp_delivery_df.head()

# COMMAND ----------

otp_delivery_df.shape

# COMMAND ----------

otp_delivery_df.groupby('template')['time_taken'].nunique("")

# COMMAND ----------

result = otp_delivery_df.groupby('template')['time_taken'].agg(non_null_count='count', total_count='size').reset_index()
result

# COMMAND ----------

#the nulls here could be result of just previous days' messages so dropping the nulls
otp_delivery_cleaned_df = otp_delivery_df.dropna(subset=['time_taken'])

# COMMAND ----------

percentiles = [10,25, 50, 75,90,95]  # Specify percentiles you want to calculate
otp_delivery_cleaned_df['type_of_otp_message'] = otp_delivery_cleaned_df['template'].apply(lambda x: re.search(r'sms\.checkout\.([\w_]+)_otp', x).group(1))
grouped = otp_delivery_cleaned_df.groupby('template')
percentile_results = grouped.apply(lambda x: np.percentile(x['time_taken'], percentiles))

# Create a new DataFrame to display percentiles
percentile_df = pd.DataFrame(percentile_results.tolist(), index=percentile_results.index, columns=[f'{p}th percentile' for p in percentiles]).reset_index()
percentile_df

# COMMAND ----------

grouped = otp_delivery_cleaned_df.groupby('type_of_otp_message')
percentile_results = grouped.apply(lambda x: np.percentile(x['time_taken'], percentiles))

# Create a new DataFrame to display percentiles
percentile_df = pd.DataFrame(percentile_results.tolist(), index=percentile_results.index, columns=[f'{p}th percentile' for p in percentiles]).reset_index()
percentile_df

# COMMAND ----------

all_otp = sqlContext.sql("""
with base as(
select event_timestamp, 
event_name,
get_json_object(properties,'$.message_channel_context.template') as template,
get_json_object(properties,'$.sms_type') as sms_type,
get_json_object(properties,'$.message_id') as message_id,
  case when event_name = 'message_channels.sms.delivery_status.received' then event_timestamp else null end 
  as delivery_status_time,
  case when event_name = 'message_channels.sms.created' then event_timestamp else null end 
  as delivery_created_time,
event_timestamp - LAG(event_timestamp) OVER(
  partition by get_json_object(properties,'$.message_id') 
  order by event_name asc
) as time_taken

from events.events_message_channel_sms_v2 a
--inner join batch_sheets.magic_merchant_list b on get_json_object(a.properties,'$.owner_id') = b.mid
where producer_created_date >= '2023-08-20'
--and json_extract_scalar(properties,'$.owner_id') = 'J1rTWZUWmgNzSW'
--and json_extract_scalar(properties,'$.message_id') = 'MSRBXobNiKwkoU'
and event_name in ('message_channels.sms.created','message_channels.sms.delivery_status.received') 
 -- and get_json_object(properties,'$.message_channel_context.template') like '%sms.checkout%'
  and get_json_object(properties,'$.sms_type') = 'otp'
---limit 10
  )
  select
  template,
  time_taken,
  message_id
  --count(distinct message_id) as cnt_messages
  from base
  where event_name = 'message_channels.sms.delivery_status.received'
 -- group by 1,2
  order by 1, 2
      """)
all_otp_df = all_otp.toPandas()
all_otp_df.head()

# COMMAND ----------

#the nulls here could be result of just previous days' messages so dropping the nulls
all_otp_cleaned_df = all_otp_df.dropna(subset=['time_taken'])

# COMMAND ----------

grouped = all_otp_cleaned_df.groupby('template')
percentile_results = grouped.apply(lambda x: np.percentile(x['time_taken'], percentiles))

# Create a new DataFrame to display percentiles
percentile_df = pd.DataFrame(percentile_results.tolist(), index=percentile_results.index, columns=[f'{p}th percentile' for p in percentiles]).reset_index()
percentile_df

# COMMAND ----------

delivery_status = sqlContext.sql("""
 select event_timestamp, 
(get_json_object(properties,'$.delivery_status') ) as delivery_status,
get_json_object(properties,'$.owner_id') as merchant_id ,
get_json_object(properties,'$.delivery_description') as delivery_description,
event_name,
get_json_object(properties,'$.message_channel_context.template') as template,
get_json_object(properties,'$.sms_type') as sms_type,
get_json_object(properties,'$.message_id') as message_id

from events.events_message_channel_sms_v2 a
--inner join batch_sheets.magic_merchant_list b on get_json_object(a.properties,'$.owner_id') = b.mid
where producer_created_date >= '2023-08-20'
--and json_extract_scalar(properties,'$.owner_id') = 'J1rTWZUWmgNzSW'
--and json_extract_scalar(properties,'$.message_id') = 'MSRBXobNiKwkoU'
and event_name in ('message_channels.sms.created','message_channels.sms.delivery_status.received') 
 -- and get_json_object(properties,'$.message_channel_context.template') like '%sms.checkout%'
  and get_json_object(properties,'$.sms_type') = 'otp'

""")
delivery_status_df = delivery_status.toPandas()
delivery_status_df.head()


# COMMAND ----------

delivery_status_df

# COMMAND ----------

delivery_status_grouped_v2_df = delivery_status_df.groupby(['template', 'delivery_status']).agg({'message_id': 'count'}).reset_index()

delivery_status_pivot_v2_df = delivery_status_grouped_v2_df.pivot(index='template', columns='delivery_status', values='message_id').reset_index()
delivery_status_pivot_v2_df

# COMMAND ----------

delivery_status_grouped_df = delivery_status_df.groupby(['template', 'delivery_status']).agg({'message_id': 'count'}).reset_index()
delivery_status_grouped_df

# COMMAND ----------

delivery_status_df['checkout_flag'] = delivery_status_df['template'].apply(lambda x: 'sms.checkout' in x if x is not None else "None")

# COMMAND ----------

delivery_status_df

# COMMAND ----------

delivery_status_pivot_df = delivery_status_grouped_df.pivot(index='template', columns='delivery_status', values='message_id').reset_index()
delivery_status_pivot_df

# COMMAND ----------

delivery_status_pivot_df = delivery_status_pivot_df.fillna(0)
delivery_status_pivot_df['total'] = delivery_status_pivot_df['delivered'] + delivery_status_pivot_df['failed'] + delivery_status_pivot_df['pending']
delivery_status_pivot_df['delivery_percentage'] = delivery_status_pivot_df['delivered'] / delivery_status_pivot_df['total']

delivery_status_pivot_df

# COMMAND ----------

delivery_status_pivot_df[delivery_status_pivot_df['checkout_flag'] == True]

# COMMAND ----------

delivery_status_merchant_df

# COMMAND ----------


delivery_status_merchant_df = delivery_status_df[(delivery_status_df['checkout_flag'] == True) & 
                                                 (delivery_status_df['event_name'] == 'message_channels.sms.delivery_status.received')
                                                 ].groupby(['merchant_id', 'delivery_status']).agg({'message_id': 'count'}).reset_index()
delivery_status_merchant_df                                                 

# COMMAND ----------



delivery_status_merchant_df = delivery_status_df[(delivery_status_df['checkout_flag'] == True) & 
                                                 (delivery_status_df['event_name'] == 'message_channels.sms.delivery_status.received')
                                                 ].groupby(['merchant_id', 'delivery_status']).agg({'message_id': 'count'}).reset_index()

delivery_status_merchant_pivot_df = delivery_status_merchant_df.pivot(index='merchant_id', columns='delivery_status', values='message_id').reset_index()
delivery_status_merchant_pivot_df = delivery_status_merchant_pivot_df.fillna(0)
delivery_status_merchant_pivot_df['total'] = delivery_status_merchant_pivot_df['delivered'] + delivery_status_merchant_pivot_df['failed'] +  delivery_status_merchant_pivot_df['pending']
delivery_status_merchant_pivot_df['delivery_percentage'] = delivery_status_merchant_pivot_df['delivered'] / delivery_status_merchant_pivot_df['total']

delivery_status_merchant_pivot_df

# COMMAND ----------

delivery_status_merchant_pivot_df.sort_values(by=['total', 'delivery_percentage'], ascending=False)

# COMMAND ----------

delivery_status_merchant_pivot_df[delivery_status_merchant_pivot_df['delivery_percentage'] < 0.95].sort_values(by=['total', 'delivery_percentage'], ascending=False)

# COMMAND ----------

# Create histogram
plt.figure(figsize=(20, 10))
plt.hist(delivery_status_merchant_pivot_df['delivery_percentage'], bins=10, alpha=0.7, rwidth=0.85, align='mid', edgecolor='black')

# Set labels and title

plt.xlabel('Delivery Percentage')
plt.ylabel('Count')
plt.title('Merchant IDs vs Delivery Percentage')

# Show the plot
plt.show()

# COMMAND ----------

# Create histogram
plt.figure(figsize=(20, 10))
plt.hist(delivery_status_merchant_pivot_df[delivery_status_merchant_pivot_df['delivery_percentage'] >= 0.8]['delivery_percentage'], bins=20, alpha=0.7, rwidth=0.85, align='mid', edgecolor='black')

# Set labels and title

plt.xlabel('Delivery Percentage')
plt.ylabel('Count')
plt.title('Merchant IDs vs Delivery Percentage')

# Show the plot
plt.show()

# COMMAND ----------


