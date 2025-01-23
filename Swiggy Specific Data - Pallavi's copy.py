# Databricks notebook source
import pandas as pd
import numpy as np

# COMMAND ----------

#import libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf,countDistinct,substring, expr, when
from pyspark.sql.types import IntegerType

# COMMAND ----------

payments_mid_base=spark.sql(
'''select distinct contact as customer_contact, a.merchant_id, business_category,a.created_date, sum (base_amount/100) as gmv, count (distinct id) as txns  from realtime_hudi_api.payments a
left join realtime_hudi_api.merchant_details b on a.merchant_id=b.merchant_id
left join aggregate_ba.final_team_tagging c on a.merchant_id=c.merchant_id
where method <> 'transfer' and authorized_at is not null and date(a.created_date)>= date('2023-07-01')and date(a.created_date)<= date('2023-10-31') and contact is not null
group by 1,2,3,4''')

# COMMAND ----------

payments_mid_base2=sqlContext.sql(
'''select 
    
    merchant_category,
    business_category, 
  ---  created_date, 
    count(distinct customer_contact) as unique_users, 
    count(distinct merchant_id) as unique_mx, 
    sum(gmv) as gmv, 
    sum(txns) as txns

from 
    (select 
        case when a.merchant_id in ('HZ6OEC61YGWMQ6',
'AzaPxFRJ5Zz6Sp',
'GXpuogqZMZsqYB',
'BsiRdj7W1QAuYl',
'I62CfVRwCvf983',
'CmQnIZHp0hH88i',
'EpXeZ7cMzydDXb',
'8S0i1kWYyF2woQ',
'DiWTt9GeqFP48C',
'HZ6OEC61YGWMQ6') then 'Swiggy' else 'Non-Swiggy' end as merchant_category,
        contact as customer_contact, 
      a.merchant_id,
        business_category,
        a.created_date, 
        round(sum (base_amount*1.0/100),2) as gmv, 
        count (distinct id) as txns  
    from realtime_hudi_api.payments a
    left join realtime_hudi_api.merchant_details b on a.merchant_id=b.merchant_id
    left join aggregate_ba.final_team_tagging c on a.merchant_id=c.merchant_id
    where method <> 'transfer' and authorized_at is not null and date(a.created_date)>= date('2023-07-01')and date(a.created_date)<= date('2023-10-31') and contact is not null
   and business_category in ('food', 'grocery') 
    group by 1,2,3,4,5
    ) 
---where business_category in ('food', 'grocery') 
group by 1,2''')
payments_mid_df=payments_mid_base2.toPandas()
payments_mid_df.head()

# COMMAND ----------

#payments_mid_base2.write.mode("overwrite").saveAsTable("analytics_selfserve.payments_mid_base_food_grocery_202307_10")

# COMMAND ----------

#AOV per week
payments_mid_base2=sqlContext.sql(
'''
with base as(

    select 
        case when a.merchant_id in ('HZ6OEC61YGWMQ6',
'AzaPxFRJ5Zz6Sp',
'GXpuogqZMZsqYB',
'BsiRdj7W1QAuYl',
'I62CfVRwCvf983',
'CmQnIZHp0hH88i',
'EpXeZ7cMzydDXb',
'8S0i1kWYyF2woQ',
'DiWTt9GeqFP48C',
'HZ6OEC61YGWMQ6') then 'Swiggy' else 'Non-Swiggy' end as merchant_category,
        contact as customer_contact, 
      a.merchant_id,
        b.business_category,
        a.created_date, 
        weekofyear(a.created_date) as created_week,
        month(a.created_date) as created_month,
        sum (base_amount/100) as gmv, 
        count (distinct id) as txns  
    from realtime_hudi_api.payments a
    left join realtime_hudi_api.merchant_details b on a.merchant_id=b.merchant_id
    left join aggregate_ba.final_team_tagging c on a.merchant_id=c.merchant_id
    inner join (select distinct customer_contact from analytics_selfserve.payments_mid_base_filtered_202307_10) d on a.contact = d.customer_contact
    where method <> 'transfer' and authorized_at is not null and date(a.created_date)>= date('2023-07-01')and date(a.created_date)<= date('2023-10-31') and contact is not null
   and b.business_category in ('food', 'grocery') 
    group by 1,2,3,4,5
    
)

select 
    
    merchant_category,
    business_category, 
  ---  created_date, 
    count(distinct customer_contact) as unique_users, 
    count(distinct merchant_id) as unique_mx, 
    sum(gmv) as gmv, 
    sum(txns) as txns
    from base
    group by 1,2
''')
payments_mid_df=payments_mid_base2.toPandas()
payments_mid_df.head()

# COMMAND ----------

#gettting AOV
'''
with base as(
    select 
        case when a.merchant_id in ('HZ6OEC61YGWMQ6',
'AzaPxFRJ5Zz6Sp',
'GXpuogqZMZsqYB',
'BsiRdj7W1QAuYl',
'I62CfVRwCvf983',
'CmQnIZHp0hH88i',
'EpXeZ7cMzydDXb',
'8S0i1kWYyF2woQ',
'DiWTt9GeqFP48C',
'HZ6OEC61YGWMQ6') then 'Swiggy' else 'Non-Swiggy' end as merchant_category,
        contact as customer_contact, 
      a.merchant_id,
        b.business_category,
        a.created_date, 
        weekofyear(a.created_date) as created_week,
        month(a.created_date) as created_month,
        sum (base_amount/100) as gmv, 
        count (distinct id) as txns  
    from realtime_hudi_api.payments a
    left join realtime_hudi_api.merchant_details b on a.merchant_id=b.merchant_id
    left join aggregate_ba.final_team_tagging c on a.merchant_id=c.merchant_id
    inner join (select distinct customer_contact from analytics_selfserve.payments_mid_base_filtered_202307_10) d on a.contact = d.customer_contact
    where method <> 'transfer' and authorized_at is not null and date(a.created_date)>= date('2023-07-01')and date(a.created_date)<= date('2023-10-31') and contact is not null
  and b.business_category in ('food', 'grocery') 
    group by 1,2,3,4,5

), base2 as(
select 
customer_contact,

round(sum(txns) / count(distinct created_week),0) as avg_orders_per_week,
sum(txns) as total_txns,
sum(gmv) as total_gmv

from base
group by 1
)

select 
avg_orders_per_week, 
count(distinct base2.customer_contact ),
sum(total_txns),
sum(total_gmv)
from base2



'''
