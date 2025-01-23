# Databricks notebook source
from datetime import date, timedelta
import pandas as pd


# COMMAND ----------

saved_add_query='''
with 
summary as(
select 
checkout_id,
min(producer_created_date) as dte,
max(get_json_object(properties,'$.data.meta.initial_loggedIn')) as summary_initial_loggedIn,
max(get_json_object(properties,'$.data.meta.loggedIn')) as summary_loggedIn, 
max(get_json_object(properties,'$.data.meta.initial_hasSavedAddress')) as summary_initial_hasSavedAddress
--count(distinct checkout_id)
from aggregate_pa.cx_1cc_events_dump_v1
where event_name = 'render:1cc_summary_screen_loaded_completed'
and producer_created_date >= date('2023-03-01')
group by 1
),
saved_add as(
select 
checkout_id,
max(get_json_object(properties,'$.data.meta.initial_loggedIn')) as saved_add_initial_loggedIn,
max(get_json_object(properties,'$.data.meta.loggedIn')) as saved_add_loggedIn, 
max(get_json_object(properties,'$.data.meta.initial_hasSavedAddress')) as saved_add_initial_hasSavedAddress
--count(distinct checkout_id)
from aggregate_pa.cx_1cc_events_dump_v1
where event_name = 'render:1cc_saved_shipping_address_screen_loaded'
and producer_created_date >= date('2023-03-01')
group by 1
  )
  select  
  dte,
  summary_initial_loggedIn,
  summary_loggedIn,
  summary_initial_hasSavedAddress,
  saved_add_initial_loggedIn,
  saved_add_loggedIn,
  saved_add_initial_hasSavedAddress,
  count(distinct summary.checkout_id) as cnt_summary,
  count(distinct saved_add.checkout_id) as cnt_saved_add
  
  from summary left join saved_add on summary.checkout_id = saved_add.checkout_id
  group by 1,2,3,4,5,6,7
'''

# COMMAND ----------

saved_add_db = spark.sql(saved_add_query)
#saved_add_db.createOrReplaceTempView("dfView")
#spark.sql("""select count(*) from dfView""").show(5)
saved_add_df = saved_add_db.toPandas()
saved_add_df.head()

# COMMAND ----------

saved_add_df.groupby(by=['summary_initial_loggedIn','summary_initial_hasSavedAddress','summary_loggedIn',
                        'saved_add_initial_loggedIn','saved_add_initial_hasSavedAddress','saved_add_loggedIn'
                        ], as_index=False).sum(['cnt_summary','cnt_saved_add'])

# COMMAND ----------

saved_add_df.rename(columns = {'count(DISTINCT checkout_id)':'cnt_cid'}, inplace = True)
saved_add_df.head()


# COMMAND ----------

saved_add_pivot = saved_add_df.pivot(columns=['initial_loggedIn','loggedIn'])

# COMMAND ----------

saved_add_query='''
select 
producer_created_date,
get_json_object(properties,'$.data.meta.initial_loggedIn') as initial_loggedIn,
get_json_object(properties,'$.data.meta.loggedIn') as loggedIn, 
count(distinct checkout_id)
from aggregate_pa.cx_1cc_events_dump_v1
where event_name = 'render:1cc_saved_shipping_address_screen_loaded'
and producer_created_date >= date('2023-02-26')
group by 1,2,3
'''
