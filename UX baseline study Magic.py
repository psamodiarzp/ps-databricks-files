# Databricks notebook source
import pyspark

# COMMAND ----------

 from pyspark.sql import SQLContext

# COMMAND ----------



# COMMAND ----------

import pandas as pd

# COMMAND ----------

print("Hello")

# COMMAND ----------

query = '''
with cte as(
select merchant_id, checkout_id, event_name, event_timestamp,
case when event_name= lag(event_name) over(order by merchant_id, checkout_id, event_timestamp asc)
then null 
else lag(event_name) over(order by merchant_id, checkout_id, event_timestamp asc)
end as next_event,
lag(event_timestamp) over(order by merchant_id, checkout_id, event_timestamp asc) as next_event_timestamp
from hive.aggregate_pa.cx_1cc_events_dump_v1 

where producer_created_date >= date('2022-08-01') and producer_created_date <= date('2022-08-31')
and event_name in ('render:1cc_summary_screen_loaded_completed','behav:1cc_summary_screen_continue_cta_clicked')


),cte2 as(

select *,  next_event_timestamp - event_timestamp as time_diff from cte 
where event_name= 'render:1cc_summary_screen_loaded_completed' and next_event is not null
), cte3 as(
select approx_percentile( time_diff,.10) as p10_summary_Screen_time_Spent,
approx_percentile( time_diff,.25) as p25_summary_Screen_time_Spent,
approx_percentile(time_diff,.50) as p50_summary_Screen_time_Spent,
approx_percentile(time_diff,.75) as p75_summary_Screen_time_Spent,
approx_percentile( time_diff,.90) as p90_summary_Screen_time_Spent,
approx_percentile( time_diff,.95) as p95_summary_Screen_time_Spent
from cte2
)
select * from cte2 where time_diff <0
'''

# COMMAND ----------

original_df = sqlContext.sql(query)
print((original_df.count(), len(original_df.columns)))
original_df = original_df.toPandas()

# COMMAND ----------


