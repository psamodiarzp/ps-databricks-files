# Databricks notebook source

import pandas as pd

# COMMAND ----------

old_query ='''
select producer_created_date, merchant_id,
--get_json_object(properties,'$.data.meta.1cc_zero_coupon_move_experiment') as experiment_variant,
    CASE
      WHEN 
      event_name='render:1cc_summary_screen_loaded_completed' and 
      coalesce(
        CAST(
          get_json_object(properties, '$.data.count_coupons_available') AS integer
        ),
        0
      ) > 0 THEN 1
      ELSE 0
    END AS coupons_available,
event_name,
count(distinct checkout_id) as cnt_cid
from
aggregate_pa.cx_1cc_events_dump_v1
where event_name in ('render:1cc_summary_screen_loaded_completed','render:1cc_coupons_screen_loaded',
                     'behav:1cc_coupons_screen_coupon_applied','behav:1cc_coupons_screen_back_button_clicked',
                    'behav:1cc_summary_screen_continue_cta_clicked')
and producer_created_date < date('2022-11-07')
and producer_created_date >= date('2022-10-01')
and merchant_id not in ('CG3DynCFGPNAJk','IH7E2OJQGEKKTN')
group by 1,2,3,4
'''

# COMMAND ----------

query='''
select producer_created_date, merchant_id,
get_json_object(properties,'$.data.meta.1cc_zero_coupon_move_experiment') as experiment_variant,
event_name,
count(distinct checkout_id) as cnt_cid
from
aggregate_pa.cx_1cc_events_dump_v1
where event_name in ('render:1cc_summary_screen_loaded_completed','render:1cc_coupons_screen_loaded',
                     'behav:1cc_coupons_screen_coupon_applied','behav:1cc_coupons_screen_back_button_clicked',
                    'behav:1cc_summary_screen_continue_cta_clicked')
and producer_created_date >= date('2022-11-07')
and merchant_id not in ('CG3DynCFGPNAJk','IH7E2OJQGEKKTN')
group by 1,2,3,4
'''

# COMMAND ----------

old_df = sqlContext.sql(old_query)
print((old_df.count(), len(old_df.columns)))
old_df = old_df.toPandas() 

# COMMAND ----------

summary_df = old_df[old_df['event_name']=='render:1cc_summary_screen_loaded_completed']
summary_df.head()

# COMMAND ----------

merged_old_df = summary_df.merge(old_df[old_df['event_name'] =='behav:1cc_summary_screen_continue_cta_clicked'], 
                on=['producer_created_date','merchant_id'], how='left'
                )
merged_old_df.head()

# COMMAND ----------

merged_old_df2 = merged_old_df.groupby(['coupons_available_x'])['cnt_cid_x','cnt_cid_y'].sum().reset_index()
display(merged_old_df2)

# COMMAND ----------

old_df2 = old_df.groupby(['event_name']).sum('cnt_cid').reset_index()
display(old_df2)old_df2 = old_df.groupby(['event_name']).sum('cnt_cid').reset_index()
display(old_df2)

# COMMAND ----------

original_df = sqlContext.sql(query)
print((original_df.count(), len(original_df.columns)))
original_df = original_df.toPandas()

# COMMAND ----------

original_df.head()

# COMMAND ----------

df = original_df.groupby(['event_name','merchant_id','experiment_variant']).sum('cnt_cid').reset_index()
df.head()

# COMMAND ----------

df4 = pd.crosstab(df['experiment_variant'],df['event_name'], margins=False)
df4.head()

# COMMAND ----------

df2 = original_df.groupby(['event_name','experiment_variant']).sum('cnt_cid').reset_index()
df2 = df2.pivot_table('cnt_cid',['experiment_variant'],'event_name').reset_index()
df2

# COMMAND ----------

display(df2)

# COMMAND ----------

df_2 = df[df['event_name']=='render:1cc_summary_screen_loaded_completed'].pivot_table('cnt_cid',['merchant_id'],'experiment_variant').reset_index()
df_2.head()

# COMMAND ----------

df3 = df_2[pd.notnull(df_2['Variant A']) & pd.notnull(df_2['Variant B'])]
df3['sum'] = df3['Variant A'] + df3['Variant B']
#df3[['Variant A','Variant B']].apply(lambda x: x/x.sum(), axis=1)
df3

# COMMAND ----------

import numpy as np

# COMMAND ----------

n1 = 7943
n2 = 26489
p1 = 4592/7943
p2 = 13549/26489
h = abs(2*np.arcsin(np.sqrt(p1))-2*np.arcsin(np.sqrt(p2)))

# COMMAND ----------

Coupon Experimentation Part 2

# COMMAND ----------

post_query = '''
select producer_created_date, merchant_id,
get_json_object(properties,'$.data.meta.1cc_zero_coupon_move_experiment') as experiment_variant,
 CASE
      WHEN 
      event_name='render:1cc_summary_screen_loaded_completed' and 
      coalesce(
        CAST(
          get_json_object(properties, '$.data.count_coupons_available') AS integer
        ),
        0
      ) > 0 THEN 1
      ELSE 0
    END AS coupons_available,
event_name,
count(distinct checkout_id) as cnt_cid
from
aggregate_pa.cx_1cc_events_dump_v1
where event_name in ('render:1cc_summary_screen_loaded_completed','render:1cc_coupons_screen_loaded',
                     'behav:1cc_coupons_screen_coupon_applied','behav:1cc_coupons_screen_back_button_clicked',
                    'behav:1cc_summary_screen_continue_cta_clicked')
and producer_created_date >= date('2022-11-01')
---and merchant_id not in ('CG3DynCFGPNAJk','IH7E2OJQGEKKTN')
group by 1,2,3,4,5
'''

# COMMAND ----------

post_df = sqlContext.sql(post_query)
print((post_df.count(), len(post_df.columns)))
post_df = post_df.toPandas() 

# COMMAND ----------

post_df.groupby(['producer_created_date','coupons_available','experiment_variant']).sum('cnt_cid')

# COMMAND ----------


