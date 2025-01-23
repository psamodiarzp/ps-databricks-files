# Databricks notebook source
from datetime import datetime, timedelta
from datetime import date
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

# COMMAND ----------

base_db = sqlContext.sql(
    """
    with open as(
select 

merchant_id,
  checkout_id,
  case when producer_created_date < date('2023-10-01') then 'Q2' else 'Q3' end as quarter
  from aggregate_pa.magic_checkout_fact

where producer_created_date >= date('2023-07-01')
and  producer_created_date < date('2024-01-01')
  ),
  
   payments_cte as (
   select
    
    DISTINCT checkout_id,
         
   -- case when a.id is not NULL then 1 else 0 end  as payment_initiated,
    Max(case when (authorized_at is not null or method='cod') and a.id is not NULL
          then 1 else 0 end) as payment_successful,
      COALESCE(SUM(CASE WHEN  a.authorized_at   IS NOT NULL or a.method = 'cod' then  a.base_amount  *1.0/100 ELSE 0 END ), 0) AS authorized_gmv_cr
          
    from realtime_hudi_api.payment_analytics as pa
    left JOIN realtime_hudi_api.payments a
    on pa.payment_id = a.id
    inner join realtime_hudi_api.order_meta b on a.order_id = b.order_id
    where date(a.created_date) >= date('2023-07-01')
    and date(b.created_date) >= date('2023-07-01')
    and date(pa.created_date) >= date('2023-07-01')
    and b.type = 'one_click_checkout'
    group by 1
  ),
  mx_segment as(
 select 
      mid,
    max(segment) as segment,
    max(sales_merchant_status) as status
    from batch_sheets.magic_merchant_list
    group by 1
  )
  select 
  quarter,
  open.merchant_id,
  case when segment is null then 'Unmarked' else segment end as mx_segment,
  status,
  count(distinct open.checkout_id) as opens,
  sum(payment_successful) as payment_successful,
  --round(sum(payment_successful)*1.0000/count(distinct open.checkout_id) ,4) as cr,
  cast(round(sum(authorized_gmv_cr),0) as int) as authorized_gmv
  
  
  from open 
  left join mx_segment on open.merchant_id = mx_segment.mid
  left join payments_cte on open.checkout_id = payments_cte.checkout_id
  group by 1,2,3,4
  
  
  """
)
base_table = base_db.toPandas()
base_table.head()


# COMMAND ----------



# COMMAND ----------

base_table['CR'] = base_table['payment_successful'] / base_table['opens'] 
base_table.head()

# COMMAND ----------

base_table.to_csv('/dbfs/FileStore/new_apollo_tables_jan24.csv', index=False)
"https://razorpay-dev.cloud.databricks.com/files/new_apollo_tables_jan24.csv"

# COMMAND ----------

base_table = pd.read_csv('/dbfs/FileStore/new_apollo_tables_jan24.csv')
base_table.head()

# COMMAND ----------

base_table.columns

# COMMAND ----------

base_pivot = base_table.pivot(columns='quarter', values=['opens','payment_successful','CR','authorized_gmv'], index=['merchant_id','mx_segment','status']).reset_index()
base_pivot

# COMMAND ----------

base_pivot.columns = ['_'.join(map(str, col)) for col in base_pivot.columns]
base_pivot

# COMMAND ----------

base_pivot = base_pivot.fillna(0)

base_pivot['CR_change'] = base_pivot['CR_Q3'] / base_pivot['CR_Q2']


base_pivot

# COMMAND ----------

base_df = base_pivot.dropna(subset=['CR_change'])
base_df.head()

# COMMAND ----------

base_df.shape

# COMMAND ----------

conditions = [
    (base_df['CR_change'] <= 0.9),
    (base_df['CR_change'] > 0.9) & (base_df['CR_change'] < 1.1),
    (base_df['CR_change'] >= 1.1)
]

categories = ['Decreasing (<= -10%)', 'Stable (-10% : 10%)', 'Increasing  (>= 10%)']

base_df['Category'] = pd.cut(base_df['CR_change'], bins=[float('-inf'), 0.9, 1.1, float('inf')], labels=categories)
base_df.head()

# COMMAND ----------

base_df['status_'].unique()

# COMMAND ----------

base_grouped = base_df.groupby(by=['mx_segment_','status_','Category']).agg({'merchant_id_':'nunique','authorized_gmv_Q3':'sum','authorized_gmv_Q2':'sum','opens_Q2':'sum','opens_Q3':'sum'}).reset_index()
base_grouped.head()

# COMMAND ----------

base_grouped['gmv_change'] = (base_grouped['authorized_gmv_Q3']- base_grouped['authorized_gmv_Q2'] ) / base_grouped['authorized_gmv_Q2']
base_grouped['volume_change'] = (  base_grouped['opens_Q3']-base_grouped['opens_Q2']) / base_grouped['opens_Q2']
final_df = base_grouped.pivot(index='mx_segment_', columns='Category', values=['merchant_id_','volume_change','gmv_change']).reset_index()
final_df

# COMMAND ----------

combined_df = base_grouped.pivot(index=['mx_segment_','status_'], columns='Category', values=['merchant_id_','volume_change','gmv_change']).reset_index()
combined_df

# COMMAND ----------


