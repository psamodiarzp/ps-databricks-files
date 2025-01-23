# Databricks notebook source
from datetime import datetime, timedelta
from datetime import date
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

# COMMAND ----------

'''
Approach:
1. For ease, take merchants with mandatory login so we know we had checked for their addresses in our database
    a. Of these how many saw new address and how many saved address screen loads 
    b. of these see the address IDs generated 
'''

# COMMAND ----------

base_db = sqlContext.sql(
    """
with selected_cid as(
select distinct a.checkout_id, a.order_id,  b.citystaticfeatures_citytier as city_tier
from aggregate_pa.magic_checkout_fact a
  inner join events.events_1cc_tw_cod_score_v2 b on a.order_id = substr(b.order_id,7)
where a.producer_created_date >= date('2024-01-01')
  and b.producer_created_date >= ('2024-01-01')
  and submit=1
  and (mandatory_login_otp_screen_loaded=1 or access_address_otp_screen_loaded=1)
  and (save_address_otp_screen_loaded=1)
  )
   select 
  distinct merchant_id, c.checkout_id, get_json_object(properties,'$.data.value'), city_tier
  from aggregate_pa.cx_1cc_events_dump_v1 c 
  inner join selected_cid on c.checkout_id = selected_cid.checkout_id
  where producer_created_date >=  date('2024-01-01')
  and event_name = 'behav:contact:fill'

  and length(get_json_object(properties,'$.data.value')) = 13
---  group by 1,2,3,4
  limit 2000
    """
)
base_table = base_db.toPandas()
base_table.head()


# COMMAND ----------


