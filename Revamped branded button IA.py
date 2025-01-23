# Databricks notebook source


# COMMAND ----------

from datetime import date, timedelta

# COMMAND ----------


import argparse
import json
import time
from datetime import datetime

import boto3
import pytz
import requests
from pyspark.sql import SparkSession

# COMMAND ----------

def get_spark():
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    return spark

spark = get_spark()

# COMMAND ----------

import pandas as pd

# COMMAND ----------

query= '''

WITH renders AS (
SELECT 
 producer_created_date Date, merchant_id, event_name Event, get_json_object(properties, '$.data.btnType') Button_type, (get_json_object (context, '$.checkout_id')) Chk_Id,
 segment
FROM events.lumberjack a
inner join batch_sheets.magic_merchant_list b on a.merchant_id = b.mid
WHERE producer_created_date >= '2023-03-16'
--AND merchant_id = 'G5lyc6JMTArWUU'
AND event_name = 'behav:1cc_shopify_checkout_click'
),

submits AS(
SELECT DISTINCT producer_created_date Date, 
  checkout_id Chk_Id
FROM aggregate_pa.cx_1cc_events_dump_v1 a
inner join batch_sheets.magic_merchant_list b on a.merchant_id = b.mid
WHERE producer_created_date >= '2023-03-16'
--AND merchant_id = 'G5lyc6JMTArWUU'
AND event_name = 'submit'
)

SELECT renders.Date Date, renders.merchant_id, segment as team_owner, renders.Button_type, COUNT( DISTINCT renders.Chk_Id) as renders, COUNT(DISTINCT submits.Chk_Id) as submits
FROM renders
LEFT JOIN submits ON renders.Chk_Id = submits.Chk_Id
GROUP BY 1, 2, 3,4
ORDER BY 1, 2, 3,4



'''

# COMMAND ----------



# COMMAND ----------

df = spark.sql(query)
db = df.toPandas()
db.head()

# COMMAND ----------

db.to_csv('/dbfs/FileStore/revamped_button.csv')

# COMMAND ----------

import pandas as pd

# COMMAND ----------

db = pd.read_csv('/dbfs/FileStore/revamped_button.csv')
db.head()

# COMMAND ----------

db.to_parquet()

# COMMAND ----------

pd.set_option('display.max_rows', 100)

# COMMAND ----------

pd.set_option('display.max_rows', 100)
selected = ((db[db['Button_type'] == 'branded']['Date'].unique()))

# COMMAND ----------

print(selected)

# COMMAND ----------



# COMMAND ----------

db[db['merchant_id'] == '9dcaZXGC

# COMMAND ----------

lfg = db.groupby(by='merchant_id').agg({'Button_type':'nunique'}).reset_index()
lfg[lfg['Button_type'] == 1].shape

# COMMAND ----------

'''
Pallavi's notes:
things to check:
- adoption of branded buttons
    - get GA data for web session, PDP, add to cart, 
- increase pre and post % of users [how to remove for bias? maybe check for non SME only]
- modal conversion
- payment share impacted
'''

# COMMAND ----------

final_df = pd.merge(lfg[lfg['Button_type'] == 1], db, on='merchant_id', how='inner')
final_df.head()

# COMMAND ----------

final_df.shape

# COMMAND ----------

d = {'count(DISTINCT Chk_Id)': ['renders', 'submits', ]}
db = db.rename(columns=lambda c: d[c].pop(0) if c in d.keys() else c)
db.head()

# COMMAND ----------

db.dtypes

# COMMAND ----------

db['Button_type'].unique()

# COMMAND ----------

db = db[db['merchant_id'] != '4af5pL6Gz4AElE']

# COMMAND ----------

db.head()

# COMMAND ----------

btn_type = db.groupby(by=["Button_type"]).sum().reset_index()
btn_type['modal_cr'] = btn_type['submits']/btn_type['renders']
btn_type

# COMMAND ----------

btn_type = btn_type.drop(columns=['Unnamed: 0'])

# COMMAND ----------

date_level = db.groupby(by=['Date','Button_type']).sum().reset_index()
date_level['modal_cr'] = date_level['submits']/date_level['renders']
date_level_pivot = date_level.pivot(index='Date', columns='Button_type', values='modal_cr').reset_index()
date_level_pivot.columns = date_level_pivot.columns.to_flat_index()
date_level_pivot

# COMMAND ----------

import seaborn as sns

# COMMAND ----------

sns.set(rc={'figure.figsize':(11.7,15)})

# COMMAND ----------

import matplotlib.pyplot as plt

# COMMAND ----------

sns.catplot(data=date_level, x="Date", y="renders", kind="point", hue='Button_type', rot=45)

# COMMAND ----------

sns.catplot(data=date_level, x="Date", y="modal_cr", kind="point", hue='Button_type', rot=45)

# COMMAND ----------

groupby_type = db.groupby(by=["Button_type",'team_owner']).sum().reset_index()
groupby_type

# COMMAND ----------

groupby_type['modal_cr'] = groupby_type['submits']/groupby_type['renders']
groupby_type

# COMMAND ----------

mid_level = db.groupby(by=["Button_type",'merchant_id']).sum().reset_index()
mid_level['modal_cr'] = mid_level['submits']/mid_level['renders']
mid_level

# COMMAND ----------

mid_level = mid_level[mid_level['merchant_id'] != '4af5pL6Gz4AElE']

# COMMAND ----------

mid_level_pivot = mid_level.pivot(index='merchant_id', columns='Button_type', values=['renders','modal_cr']).reset_index()
mid_level_pivot.head(100)

# COMMAND ----------

mid_level_pivot.columns = mid_level_pivot.columns.to_flat_index()
mid_level_pivot.head()

# COMMAND ----------

date_level = 

# COMMAND ----------

query2 = '''
select producer_created_date Date, merchant_id, event_name, count(distinct checkout_id)
from aggregate_pa.cx_1cc_events_dump_v1
where producer_created_date >= date('2023-02-15')
and producer_created_date < date('2023-03-16')
and event_name in ('open','submit')
group by 1,2,3

'''


# COMMAND ----------

prev_db = spark.sql(query2)
prev_df = prev_db.toPandas()
prev_df.head()

# COMMAND ----------

prev_df_pivot = prev_df.pivot(index=['Date','merchant_id'], columns='event_name', values='count(DISTINCT checkout_id)').reset_index()
prev_df_pivot

# COMMAND ----------

d = {'submit': ['submits']}
prev_df_pivot = prev_df_pivot.rename(columns=lambda c: d[c].pop(0) if c in d.keys() else c)
prev_df_pivot.head()

# COMMAND ----------

new_df = pd.concat([prev_df_pivot[['Date','merchant_id','renders','submits']],final_df[['Date','merchant_id','renders','submits']]], ignore_index=True, axis=0)
new_df.head()

# COMMAND ----------

joint_df = pd.merge(new_df, unique_df, on='merchant_id', how='inner')
joint_df.head()

# COMMAND ----------

joint_df['modal_cr'] = joint_df['submits']/joint_df['renders']

# COMMAND ----------

joint_date_level = joint_df.groupby(by=['Date','Button_type']).sum().reset_index()
joint_date_level['modal_cr'] = joint_date_level['submits']/joint_date_level['renders']
joint_date_level_pivot = joint_date_level.pivot(index='Date', columns='Button_type', values='modal_cr').reset_index()
joint_date_level_pivot.columns = joint_date_level_pivot.columns.to_flat_index()
joint_date_level_pivot

# COMMAND ----------

sns.catplot(data=date_level, x="Date", y="modal_cr", kind="point", hue='Button_type', rot=45)

# COMMAND ----------



# COMMAND ----------

final_df.head()
final_df= final_df.rename(columns={'Button_type_y':'Button_type'})
final_df.head()

# COMMAND ----------

unique_df = final_df[['merchant_id','Button_type']].drop_duplicates()
unique_df.head()

# COMMAND ----------

- Get the traffic 
