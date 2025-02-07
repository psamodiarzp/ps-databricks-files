# Databricks notebook source
# MAGIC %pip install -U pandas==1.5.3

# COMMAND ----------

# DBTITLE 1,Declaring Packages
import pyspark.sql.functions as f
import pyspark.sql.types as t
import requests
import json
from pyspark.sql.functions import udf, col, explode
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, ArrayType
from pyspark.sql.functions import udf, struct
from pyspark.sql import Row
from pyspark.sql.types import *
import pandas as pd
from datetime import datetime
from math import floor
import pandas as pd
import numpy as np
import requests
import json
from requests.structures import CaseInsensitiveDict
import time


# COMMAND ----------

tickets_df = spark.sql(""" 
                    --    select distinct sv_joining_id as ticket_id from aggregate_ba.service_volume_consolidated_v1
                    --    where category_type in ('Product support', 'Account configuration', 'Reports', 'Account related assistance')
                    --    and sv_component = 'Ticket' and created_date >= date '2024-11-01' and created_date <  date '2024-12-01'
                   select distinct ticket_id from (
                    select distinct 
sv.sv_joining_id as ticket_id, 
  coalesce(fd.razorpaypaymentid, fd.affectedpaymentid, fd.transactionid) as transaction_id
from (select * from aggregate_ba.service_volume_consolidated_v1 sv
      WHERE
sv.sv_component  in ('Ticket')  
and sv.ppg_subpod_flag in ('Transaction','Refund','Settlement')
 and lower(sv.sv_unique_id) not like '%reopen'
  AND sv.created_date >= date'2024-11-01' and sv.created_date < date'2024-12-01'
) sv
  left join (select * from whs_agg_v.freshdesk_base1 where update_rank = 1 and created_date >= date'2024-11-01' and created_date < date'2024-12-01' )fd
  on sv.sv_joining_id = fd.ticket_id
                   ) where transaction_id is not null and transaction_id <> '' and transaction_id <> 'undefined' and transaction_id <> 'nan' and transaction_id <> 'na'
                   limit 2000
                       """)
tickets_df_pd = tickets_df.toPandas()                   

# COMMAND ----------

display(tickets_df)
#tickets_df_pd.count()

# COMMAND ----------

# DBTITLE 1,Function Declaration & Applying Filters

#base_data = pd.read_csv("/dbfs/FileStore/shared_uploads/sukhada.deshpande@razorpay.com/Tickets_Data.csv")
final_base_data = tickets_df_pd.copy()


# def convert_to_date(row):
#     return datetime.strptime(row['Created time'], '%Y-%m-%d %H:%M:%S')


# final_base_data['Created time'] = final_base_data.apply(convert_to_date,axis=1)
# final_base_data['Created date'] = final_base_data['Created time'].dt.strftime('%Y-%m-%d')
# final_base_data['Created month'] = final_base_data['Created time'].dt.strftime('%Y-%m-01')

## Only keeping required columns
# required_cols = ['Created date','Created month','Ticket ID','Description','Group','Source']
# final_base_data = final_base_data[required_cols]

## Only keeping required sources
# required_source = ['Portal']
# final_base_data = final_base_data[final_base_data['source'].isin(required_source)]

## Only filter for month of October
# final_base_data = final_base_data[final_base_data['Created month']=='2022-11-01']



def define_headers():
    headers = CaseInsensitiveDict()
    headers["Authorization"] = "Basic YUpUR0xDWkQ5cEp2SWZiYnh5ZW0="
    return headers


def api_call(ticketid):
    return "https://razorpay-ind.freshdesk.com/api/v2/tickets/"+ticketid+"/conversations?format=json"




def parse_json(ticket_input):

    headers = CaseInsensitiveDict()
    headers["Authorization"] = "Basic YUpUR0xDWkQ5cEp2SWZiYnh5ZW0="
    try:
        response =  requests.get(api_call(ticketid = ticket_input), headers = headers)
        if response.status_code != 200:
            print(f"Received with error code - {response.status_code} ")
            response_json = None
        else:
            response_json = response.json()
            return json.dumps(response_json)

    except:
        print("Exception thrown")
        response_json = None
    return response_json

def append_text_response(ticket_row):
    return parse_json(ticket_row)


append_text_response_udf = udf(lambda row: append_text_response(row[0]),StringType())
define_headers_udf = udf(lambda z: define_headers(z),StringType())



# COMMAND ----------

# DBTITLE 1,Declaring the Spark Dataframe
ticket_df = pd.DataFrame(final_base_data['ticket_id'])
ticket_df['TicketID_str'] = ticket_df['ticket_id'].astype('str').replace('\.0', '', regex=True)
ticket_df.reset_index(drop = True, inplace = True)


ticket_sdf = spark.createDataFrame(ticket_df)\
.withColumn('headers', f.lit("Basic YUpUR0xDWkQ5cEp2SWZiYnh5ZW0="))

ticket_sdf.show()

# COMMAND ----------

len =ticket_sdf.count()
tickets_per_iter = 1000
counter = floor(len/tickets_per_iter)+1
counter

# COMMAND ----------

i=0
len =ticket_sdf.count()
tickets_per_iter = 1000
counter = floor(len/tickets_per_iter)+1

while (i < counter):
    
    
    lower = i*tickets_per_iter
    upper = (i+1)*tickets_per_iter
    print(f'Itertion: {i} starting from record {lower} till record {upper-1}')
    
    if upper < len:
        loop_df =spark.createDataFrame(ticket_sdf.collect()[lower:upper])
    else:
        loop_df =spark.createDataFrame(ticket_sdf.collect()[lower:len])
   
    res = loop_df.withColumn('json_message', append_text_response_udf(struct('TicketID_str')))
    
    if i == 0:
        # res.repartition(1).write.mode("Overwrite").format("csv").option("header", True).option("encoding", "UTF-8").option("delimiter","||||").save("/dbfs/FileStore/shared_uploads/swati.agarwal@razorpay.com/Sentiment_Analysis/FD_API/freshdesk_conversations.csv") ## specify file where this has to be saved to
        res.repartition(1).write.mode("Overwrite").format("csv").option("header", True).option("encoding", "UTF-8").option("delimiter","||||").save("/dbfs/FileStore/shared_uploads/swati.agarwal@razorpay.com/Conversation_Extraction/TSR/freshdesk_conversations.csv") ## specify file where this has to be saved to
    else:
        # res.repartition(1).write.mode("Append").format("csv").option("header", True).option("encoding", "UTF-8").option("delimiter","||||").save("/dbfs/FileStore/shared_uploads/swati.agarwal@razorpay.com/Sentiment_Analysis/FD_API/freshdesk_conversations.csv") ## specify file where this has to be saved to
        res.repartition(1).write.mode("Append").format("csv").option("header", True).option("encoding", "UTF-8").option("delimiter","||||").save("/dbfs/FileStore/shared_uploads/swati.agarwal@razorpay.com/Conversation_Extraction/TSR/freshdesk_conversations.csv") ## specify file where this has to be saved to
    
    res.show(1000) 
    time.sleep(100)
    i = i+1
    



# COMMAND ----------

base_data_1 = spark.read.format("csv").option("header", True).option("encoding", "UTF-8").option("delimiter", "||||").option("multiline","true").load("/dbfs/FileStore/shared_uploads/swati.agarwal@razorpay.com/Conversation_Extraction/TSR/freshdesk_conversations.csv") ## specify file where the data has been saved to

df_1 = base_data_1.toPandas()
df_1 = df_1[df_1['json_message'].isnull()==False]
display(df_1)

# COMMAND ----------

#df_1 = base_data_1.toPandas()
df_1.shape

# COMMAND ----------

## Not Required - checks performed
#k=parse_json("6670062")
#df = pd.DataFrame(k)
#df = df[['body_text','id','incoming','private','support_email','to_emails','from_email','cc_emails','bcc_emails','created_at','ticket_id']]
#sparkDF=spark.createDataFrame(df)
#json_array = sparkDF.toJSON().collect()
#json_array

##Below is working
#s1 = json.dumps(json_array)
#data = json.loads(s1)
#df1 = pd.DataFrame.from_dict(data,orient='columns')
#df1
#k
#json.dumps(k)
