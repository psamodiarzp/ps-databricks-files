# Databricks notebook source
from itertools import combinations
import pickle
import os
import requests
import pandas as pd
import json


# COMMAND ----------

# MAGIC %md
# MAGIC ## Readme
# MAGIC
# MAGIC # Overview 
# MAGIC This notebook contains functions to test effect of feeding some of the features nulls to cod_eligibility API.
# MAGIC
# MAGIC ## Detailed Steps to test
# MAGIC
# MAGIC 1. Create sample data: Currently hard coded in script as `sample_df`
# MAGIC
# MAGIC 2. Create versions of sample data by nulling different columns : for example if the input data is [1,2,3,4,5], version of sample data by nulling different columns would be [null,2,3,4], [1,null,3,4], [1,2,null,4], [1,2,3,null], [null,null,3,4], [1,null,null,4] and so on.
# MAGIC
# MAGIC 3. Post requests to cod eligibity api for getting predicted rto probabilities: once we have different versions of dataframes we just need to send them to api and get scores populated in datalake.
# MAGIC Wait for at least half an hour to let results populate in datalake then run next steps
# MAGIC
# MAGIC 4. Get resposnse from datalake: after scores are populated we fetch and compare them with initial dataframe score which didn't have any of the features null.
# MAGIC
# MAGIC ## Important Functions 
# MAGIC
# MAGIC 1. `create_versions_of_data`: returns set of dataframes with one feature null at a time, two features null at a time and so on till max_cols_to_null.
# MAGIC 2. `create_n_post_order_meta_payload_from_datasets`: wrapper to post created payloads to cod_eligibility_api
# MAGIC 3. `get_rto_probability`: once the scores are populated in datalake, this function aggregates the results.
# MAGIC
# MAGIC ## Snipet to Test effect of turning feature_list null
# MAGIC .
# MAGIC ```
# MAGIC # list of fields that could have null value as input.
# MAGIC feature_list = ['phone', 'email', 'line1', 'line2', 'city', 'state', 'zipcode','ip', 'device_id']
# MAGIC # we turn every features null one at a time, two at a time and so on till max_nulls. 
# MAGIC max_nulls = 3
# MAGIC # 
# MAGIC sample_df_versions = create_versions_of_data(sample_df, feature_list, max_nulls)
# MAGIC set_model_for_merchants(sample_df) # optional, to be run only once when new dataset is created
# MAGIC create_n_post_order_meta_payload_from_datasets(sample_df_versions)
# MAGIC
# MAGIC #it could take ~30 minutes to have all updated scores in datalake
# MAGIC
# MAGIC df_results = get_rto_probability(sample_df_versions, df_features)
# MAGIC display(df_results)
# MAGIC ```

# COMMAND ----------

import requests
import json
import pprint
import pandas as pd
import numpy as np
import pickle
from itertools import combinations


# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Create sample data
# MAGIC Currently we are manually creating some sample data. \
# MAGIC Sample data preparation should ideally be done from order meta table for multiple merchants.
# MAGIC

# COMMAND ----------

#PS Step 1: write the SQL query to get the below data 
sample_sql = sqlContext.sql(
"""
with cte as (
select
distinct
 a.order_id, a.created_date, a.created_at, b.merchant_id, b.device_id,
 c.category AS mcc,
  CASE
    WHEN c.category2 == 'ecommerce' THEN 1
    ELSE 0
  END AS generic_isecommercemerchant,
  CASE
    WHEN c.category2 == 'food_and_beverage' THEN 1
    ELSE 0
  END AS generic_isfnbmerchant,
get_json_object(properties,'$.data.meta.address_id' ) as address_id,
get_json_object(properties,'$.data.meta.shipping_address_contact' ) as phone,
get_json_object(properties,'$.options.amount' ) as amount,
d.ipstaticfeatures_ip as ip
from realtime_hudi_api.order_meta a
inner join aggregate_pa.cx_1cc_events_dump_v1  b
on a.order_id = substr(b.order_id,7) 
inner join realtime_hudi_api.merchants c on b.merchant_id = c.id
inner join events.events_1cc_tw_cod_score_v2 d 
on  a.order_id = substr(d.order_id,7)
where 
a.created_date >='2024-02-20'
and b.producer_created_date >= date('2024-02-20')
and d.producer_created_date >= '2024-02-20'
and b.event_name='render:1cc_payment_home_screen_loaded'
and d.ml_model_id ='category_5977'
limit 200
)
select cte.*, line1, line2, city, zipcode, state from cte inner join realtime_hudi_api.addresses b on cte.address_id = b.id
where phone is not null
limit 100

"""
)
sample_df = sample_sql.toPandas()
sample_df.head()

# COMMAND ----------

sample_df.shape

# COMMAND ----------

sample_df.to_csv('/dbfs/FileStore/iaas_sample_df_category_5977.csv', index=False)
"https://razorpay-dev.cloud.databricks.com/files/iaas_sample_df_category_5977.csv"

# COMMAND ----------

sample_df = pd.read_csv('/dbfs/FileStore/iaas_sample_df_category_5977.csv')
sample_df = sample_df.fillna('')
sample_df.head()


# COMMAND ----------

initial_order_id = sample_df['order_id'].unique()
initial_order_id

# COMMAND ----------

sample_df['amount'] = sample_df['amount'].astype(int)
sample_df['phone'] = sample_df['phone'].astype(str)
sample_df['mcc'] = sample_df['mcc'].astype(str)
sample_df['zipcode'] = sample_df['zipcode'].astype(str)
sample_df['order_id'] = 'order_'+ sample_df['order_id']
sample_df.dtypes

# COMMAND ----------

sample_df['request_id'] = 'base'

# COMMAND ----------

{"id": "+919833951615", "merchant_id": "DHaAPmQ7axDizC", "input": {"order": {"id": "NfW77XkZlQtx2m", "checkout_id": "order_J1OGn9gyJS1ZJW:54e6f53c-2a62-44bd-9aa0-d802456btest", "amount": 89145, "currency": "INR", "created_date": "2024-02-26", "created_at": 1708952431, "shipping_address": {"name": 99, "type": "shipping_address", "line1": "Silver oak bld 1  103, A Wing , Beverly Park", "line2": "Opposite kanakia police station", "zipcode": "401107", "city": "Thane", "state": "Maharashtra", "tag": "home", "landmark": "", "country": "India", "contact": "+919833951615", "id": "Ipx8ClVLABuIu7"}, "billing_address": {"name": 99, "type": "shipping_address", "line1": "Silver oak bld 1  103, A Wing , Beverly Park", "line2": "Opposite kanakia police station", "zipcode": "401107", "city": "Thane", "state": "Maharashtra", "tag": "home", "landmark": "", "country": "India", "contact": "+919833951615", "id": "Ipx8ClVLABuIu7"}, "customer": {"id": "+919833951615", "phone": "+919833951615", "email": ""}, "device": {"id": "", "user_agent": "PostmanRuntime/7.29.0", "ip": "", "pathname": "/v1/1cc/check_cod_eligibility", "search": ""}}}}

# COMMAND ----------


og_sample_df = pd.DataFrame([{'order_id': '10012221111111222888','mcc':'5691','merchant_id':'11111111111111',
                            'amount' : 100000, 'created_date': '2024-01-12', 'created_at': 1705052053, 
                            'customer_name' : ' Rahul Gandhi', 'phone': '+917598244444','email': "rahul.gandhi@gmail.com", 'ip': '59.89.670.350', 'device_id': '34634634623',
                            "line1": "hostel room 209 ,Bps medical college swaranjyanti,", "line2": "Gohana , Sonipat, HR", "zipcode": "131301", "city": "sonipat", "state": "haryana", 'request_id': 'base'},
                            {'order_id': '10022221111112222999','mcc':'5977','merchant_id':'100000Razorpay',
                            'amount' : 250000, 'created_date': '2024-01-12', 'created_at': 1705052063, 
                            'customer_name' : ' Neha Sharma', 'phone': '+918943467856','email': "neha.22309@gmail.com", 'ip': '79.19.689.454', 'device_id': '2sdfds4634A623',
                            "line1": "1/2345, Sri Kabil street", "line2": "Chinna koil", "zipcode": "628205", "city": "Tuticorin", "state": "Tamil Nadu", 'request_id': 'base'}
                          ])
display(og_sample_df)    
              

# COMMAND ----------

og_sample_df.dtypes

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Create copies of sample data by null columns
# MAGIC create multiple versions of sample_df created above by nulling different columns like  phone, email, address , ip, device_id etc. 1 at a time, 2 at a time

# COMMAND ----------

def create_versions_of_data(sample_df, columns_to_null, max_cols_to_null):
    """
    Args:
        sample_df: pandas df, a sample data frame of order meta info
        columns_to_null: list of columns that can be passed as null while placing an order
        num_of_cols_to_null: int, num of columns from above list to null together
    """
    sample_df_versions = {'base': sample_df}
    
    # nulling num_of_cols=1 or 2 or 3 at a time
    power_set_col_group = []
    for k in range(1,max_cols_to_null+1):
        power_set_col_group.extend(combinations(columns_to_null, k))
    print("Following set of columns will be nulled together:")
    for col_names in power_set_col_group:
        col_names = list(col_names)
        col_gourp_name = '_'.join(col_names)
        temp = sample_df.copy()
        print(col_names)
        temp[col_names] = ''
        # display(temp)
        sample_df_versions[col_gourp_name+'_null'] = temp
        sample_df_versions[col_gourp_name+'_null']['request_id'] = col_gourp_name+'_null'
    return sample_df_versions

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data creation Main

# COMMAND ----------

import os
import shutil
columns_to_null = ['phone', 'email', 'line1', 'line2', 'city', 'state', 'zipcode','ip','device_id']
sample_df_versions = create_versions_of_data(sample_df, columns_to_null, 2)
# save this for later use
# Create the directory if it doesn't exist
directory = "/dbfs/mnt/rto/"
if not os.path.exists(directory):
    #shutil.rmtree(directory)
    os.makedirs(directory)

# Save the pickle file
file_path = os.path.join(directory, "sample_datasets.pickle")
with open(file_path, "wb") as file:
    pickle.dump(sample_df_versions, file)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Hit cod_egibility api with data from different sample datasets
# MAGIC - Loop though all versions of sample_df and post the order info it to cod eligibility api to get rto probablities
# MAGIC - Store results for each in a df like: “order_id”, “pred_prob_base”, “pred_prob_comb1”, “”pred_prob_comb2" etc. 
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Set endpoints

# COMMAND ----------

merchant_mapping_endpoint = "https://rto-prediction-service-concierge.razorpay.com/twirp/rzp.rto_prediction.merchant_ml_models.v1.MerchantMLModelsAPI/Update"

cod_eligibility_endpoint = "https://rto-prediction-service-concierge.razorpay.com/twirp/rzp.rto_prediction.cod_eligibility.v1.CODEligibilityAPI/Evaluate"

headers = {
  'Content-Type': 'application/json',
  'Authorization': 'Basic YXBpOnVkSGFGWGRTNFFLcDQ1TXY='
}
session = requests.Session()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Set model for merchants

# COMMAND ----------

def set_model_for_merchants(sample_df):
    """
    Args:
        sample_df containg merchant_id and mcc used to select model
    """
    print("_"*50)
    print("Setting model for merchants")
    merchant_df = sample_df.groupby(['merchant_id','mcc']).order_id.count().reset_index()
    for i,row in merchant_df.iterrows():
        if row.mcc == '5399' or row.mcc == '5691' or row.mcc == '5977':
            ml_model_id = "category_"+str(row.mcc)
        else:
            ml_model_id = 'category_infrequent_mcc_group'
        payload = {
                    "merchant_id": row.merchant_id,
                    "model_id": ml_model_id
                }
        response = session.post(merchant_mapping_endpoint,data=json.dumps(payload),headers=headers)
        print(response.text)

def get_cod_eligibility(requests_json):
    """
    Args:
        requests_json: dict containing order info to be posted to cod eligibility api
    """
    payload = json.dumps(requests_json)
    print(payload)
    response = session.post(cod_eligibility_endpoint,
                            data=payload,
                            headers=headers, 
                            timeout=600)
    response_dict = json.loads(response.text)
    if response.status_code != 200:
        print("block failed")
        print(response.text)
    else:
        return response_dict

# COMMAND ----------

def create_n_post_order_meta_payload_from_datasets(sample_df_versions):
    """
    Args:
        sample_df_versions: different data frames to create cod eligibity api payload from
        response from api is printed only
    """
    print("_"*50)
    print("Sending requests for cod eligibility")
    for key, df in sample_df_versions.items():
        print("-"*50)
        print(key)
        # create payload from every row in sample dataset"
        for i, row in df.iterrows(): 
            print('#'*50)
            print("Row number:", i)
            requests_json  = { 
            "id": row.request_id, 
            #"id": row.phone,
            "merchant_id": row.merchant_id,
            "input": {
            "order": {
                "id": row.order_id,
                "checkout_id": "order_J1OGn9gyJS1ZJW:54e6f53c-2a62-44bd-9aa0-d802456btest",
                "amount": row.amount,
                "currency": "INR",
                "created_date": row.created_date,
                "created_at": row.created_at,
                "shipping_address": {
                   # "name": row.name,
                   "name": "Temporary Name",
                    "type": "shipping_address",
                    "line1": row.line1,
                    "line2": row.line2,
                    "zipcode": row.zipcode,
                    "city": row.city,
                    "state": row.state,
                    "tag":  "home",
                    "landmark": "",
                    "country": "India",
                    "contact": row.phone,
                    "id": "Ipx8ClVLABuIu7"
                },
                "billing_address": {
                      # "name": row.name,
                    "name": "Temporary Name",
                    "type": "shipping_address",
                    "line1": row.line1,
                    "line2": row.line2,
                    "zipcode": row.zipcode,
                    "city": row.city,
                    "state": row.state,
                    "tag":  "home",
                    "landmark": "",
                    "country": "India",
                    "contact": row.phone,
                    "id": "Ipx8ClVLABuIu7"
                },
                "customer": {
                    "id": row.phone,
                    "phone": row.phone,
                   # "email": row.email 
                   "email": ""
                },
                "device": {
                    "id": row.device_id,
                    "user_agent": "PostmanRuntime/7.29.0",
                    "ip": row.ip,
                    "pathname": "/v1/1cc/check_cod_eligibility",
                    "search": ""
                }
            }
            }
            }
            response_dict = get_cod_eligibility(requests_json)
            print(".."*50)
            print(response_dict)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Posting Requests Main

# COMMAND ----------

# posting to api 
set_model_for_merchants(sample_df) # optional, to be run only once when new dataset is created
create_n_post_order_meta_payload_from_datasets(sample_df_versions)

# COMMAND ----------

# please wait for at least half an hour before running next cells for the results to be populated in datalake

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Get rto response and features in datalake
# MAGIC Use request_id to filter order we just created from datalake and check what values the features take when the input is null for a variable. E.g. is phone is null check features like - phonestaticfeatures_countdistinctdigits, phonemerchantfeatures_rtopercent etc.

# COMMAND ----------

def get_features_from_datalake(request_ids,order_ids):
   df_features = sqlContext.sql(""" select * from events.events_1cc_tw_cod_score_v2
                                 where request_id in {0}
                                 and order_id in {1}
                                 and producer_created_date >= '2024-01-16'
                              """.format(request_ids,order_ids)
                              )
   df_features = df_features.toPandas()
   df_features = df_features.drop_duplicates(['order_id','request_id'],keep='last')
   return df_features


# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Access impact
# MAGIC Notice if the average results from all nulling versions of sample dataset (avg(pred_prob_comb*)) is going up or down w.r.t. base result (pred_prob_base)

# COMMAND ----------


def check_features_from_null_columns(sample_df_versions, df_features):
    """
    Args:
        sample_df_versions: dictionary of different datasets be created for testing nulls, 
                            should also have base key for datasets having no nulls
        df_features: dataframe containing results from datalake for all cod eligibity request sent 
                     from above sample_df_versions
    prints those features that are impacted by nulling columns from order info payload
    """
    for key, df in sample_df_versions.items():
        print(key)
        # join to get all features for a sample df version
        df_features_ = df_features[df_features.request_id==key].set_index('order_id')
        df_with_features = df.join(df_features_,on='order_id',how='left',lsuffix="_l", rsuffix ="_r")
        # check columns that are impacted by nulling
        str_feature_keys = key.replace("_null","")
        feature_keys = str_feature_keys.split("_")
        list_features = []
        for feature_key in feature_keys:
            if feature_key=='id':
                continue
            if feature_key == 'ip':
                feature_key = 'ipstatic'
            if (feature_key == 'line1') or (feature_key == 'line2'):
                feature_key ='addressstatic'
            list_features += [col for col in df_with_features.columns if feature_key in col]
        list_features += ['rules_evaluated','feature_contribution']
        #print(list_features)
        #print("checking ",str_feature_keys)
        display(df_with_features[list_features])

def get_rto_probability(sample_df_versions, df_features):
    """
    Args:
        sample_df_versions: dictionary of different datasets be created for testing nulls, 
                            should also have base key for datasets having no nulls
        df_features: dataframe containing results from datalake for all cod eligibity request sent 
                     from above sample_df_versions
    Returns:
        df_results: dataframe containing scores obtained by nulling different columns
    """
    df_results = pd.DataFrame()
    sample_df = sample_df_versions['base']
    df_results['order_id'] = sample_df['order_id']
    for key, df in sample_df_versions.items():
        # join to add results for this version to results df
        df_version_results = df_features.loc[df_features.request_id==key,['order_id','score']].set_index('order_id')
        df_results = df_results.join(df_version_results, on='order_id',rsuffix= "_"+key)
    
    df_results_inverse = df_results.transpose() # for better readability
    df_results_inverse.columns = df_results.order_id
    #df_results_inverse.columns = df_results.order_id.map(lambda x: "order_" + x)
    df_results_inverse["info"] = df_results.columns
    df_results_inverse = df_results_inverse.iloc[1:,]
    return df_results_inverse
        
    

# COMMAND ----------

# MAGIC %md
# MAGIC # Result Fetching Main

# COMMAND ----------

import pickle
import pandas as pd
sample_df_versions = pickle.load(open("/dbfs/mnt/rto/sample_datasets.pickle","rb"))
request_ids = tuple(sample_df_versions.keys())
sample_df = sample_df_versions['base']
order_ids = tuple(sample_df.order_id.values)
df_features = get_features_from_datalake(request_ids,order_ids)
print(df_features.shape)
df_results = get_rto_probability(sample_df_versions, df_features)
display(df_results)


# COMMAND ----------

df_results.columns

# COMMAND ----------

check_features_from_null_columns(sample_df_versions, df_features)

# COMMAND ----------


