# Databricks notebook source
pip install gspread

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import gspread
from google.oauth2.service_account import Credentials
import base64
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import smtplib
from email.message import EmailMessage

import json
import datetime

# COMMAND ----------


def to_create_spreadsheet(credentials_file):
    scopes = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive"
    ]
    creds = Credentials.from_service_account_file(credentials_file,scopes=scopes)
    client = gspread.authorize(creds)
    
    #create a spreadsheet with current month's name
    spreadsheet = client.create(f"Call centre calling analysis")

    # Open the Google Sheet
    # Get the spreadsheet ID
    spreadsheet_id = spreadsheet.id

    email_to_share_with = ['deekonda.sai@razorpay.com']
    for user in email_to_share_with:
        # Share the sheet with the specified email
        spreadsheet.share(user, perm_type='user', role='writer', notify=True)
    
    return(spreadsheet_id)

credentials_file = "/dbfs/FileStore/Manish/google_ads_json/amg_service_acc.json"

spreadsheet_id = to_create_spreadsheet(credentials_file)
print(spreadsheet_id)

# COMMAND ----------

import pandas as pd
import re
from pyspark.sql import SparkSession

spreadsheet_id = '1EOu8AKyiEXRuSLNYAgDuPHrI-dKoiRCLMEzKse79aT4'
sheet_name = str("2024-01-21")
#"23_and_24_dec_Calls"
#datetime.datetime.now().date()

def read_spreadsheet(credentials_file,spreadsheet_id,sheet_name):
    scopes = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive"
     ]
    creds = Credentials.from_service_account_file(credentials_file,scopes=scopes)
    client = gspread.authorize(creds)

    # Open the Google Sheet
    # Get the spreadsheet ID
    spreadsheet = client.open_by_key(spreadsheet_id)
    sheet=spreadsheet.worksheet(sheet_name)
    data = sheet.get_all_values()

    df = pd.DataFrame(data[1:], columns=data[0])
    # remove spaces and special characters from column names
    df.columns = [re.sub(r'\W+', '_', col.strip()) for col in df.columns]
    
    spark = SparkSession.builder.getOrCreate()
    spark_df = spark.createDataFrame(df)
    spark_df.createOrReplaceTempView("temp_view_of_the_read_calling_data")
    return spark_df

credentials_file = "/dbfs/FileStore/Manish/google_ads_json/amg_service_acc.json"
df = read_spreadsheet(credentials_file,spreadsheet_id,sheet_name)

select * from temp_view_of_the_read_calling_data limit 1

# COMMAND ----------

# %sql
# delete from aggregate_ba.call_centre_calling_analyses
# where Calling_date in (cast('2024-01-05' as date),cast('2024-01-06' as date))

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from temp_view_of_the_read_calling_data limit 1

# COMMAND ----------

## TAKING A BACKUP OF THE TABLE
##Dropping the table existed before
sqlContext.sql("drop table if exists aggregate_ba.call_centre_calling_analyses_backup")

sqlContext.sql("create table aggregate_ba.call_centre_calling_analyses_backup as (select * from aggregate_ba.call_centre_calling_analyses)")

# ## Dropping the table existed before
sqlContext.sql("drop table if exists aggregate_ba.call_centre_calling_analyses")

## Creating the final table with union of today's calling data and till yesterday's data
sqlContext.sql("create table aggregate_ba.call_centre_calling_analyses as ((select * from aggregate_ba.call_centre_calling_analyses_backup) union all (select * from temp_view_of_the_read_calling_data))")

# COMMAND ----------

# MAGIC %sql
# MAGIC select Calling_date, count(distinct mid), count(mid)
# MAGIC from 
# MAGIC -- aggregate_ba.call_centre_calling_analyses_backup
# MAGIC aggregate_ba.call_centre_calling_analyses
# MAGIC group by 1
# MAGIC order by 1

# COMMAND ----------

# MAGIC %sql
# MAGIC REFRESH TABLE aggregate_pa.merchant_fact_hourly_incremental
# MAGIC --pg_acquisition_product_lead_score_data
# MAGIC --aggregate_pa.merchant_fact_hourly_incremental

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists aggregate_ba.call_centre_calling_analyses_final;
# MAGIC create table aggregate_ba.call_centre_calling_analyses_final as (
# MAGIC   select a.*, 
# MAGIC   b.first_txn_date, b.lifetime_gmv, b.channel, b.media_channel_detailed, b.merchant_type, b.partnership_flag, 
# MAGIC   case when Go_live_date = 'Immediate' then first_called_date
# MAGIC   when Go_live_date = '7 days' then first_called_date+7
# MAGIC   when Go_live_date = '15 days' then first_called_date+15
# MAGIC   when Go_live_date in('1 month','1 month - 2 month') then first_called_date+30
# MAGIC   when Go_live_date = '2 month+' then first_called_date+60 else Go_live_date end as Go_live_date_date,
# MAGIC
# MAGIC   CASE WHEN Total_monthly_potential ='5 lakhs - 10 lakhs' THEN 750000
# MAGIC WHEN Total_monthly_potential ='10000-50000' THEN 30000
# MAGIC WHEN Total_monthly_potential ='25 lakhs - 50 lakhs' THEN 3500000
# MAGIC WHEN Total_monthly_potential ='50000 - 1 lakh' THEN 75000
# MAGIC WHEN Total_monthly_potential ='1 lakh - 5 lakhs' THEN 300000
# MAGIC WHEN Total_monthly_potential ='<10000' THEN 5000
# MAGIC WHEN Total_monthly_potential ='10 lakhs - 25 lakhs' THEN 1750000
# MAGIC WHEN Total_monthly_potential ='1 Cr - 5 Cr' THEN 25000000
# MAGIC WHEN Total_monthly_potential ='5 Cr +' THEN 50000000
# MAGIC WHEN Total_monthly_potential ='50 lakhs - 1 Cr' THEN 7500000 else Total_monthly_potential end as Total_monthly_potential_rs,
# MAGIC
# MAGIC CASE WHEN Total_offline_potential ='10 lakhs - 25 lakhs' THEN 1750000
# MAGIC WHEN Total_offline_potential ='10000-50000' THEN 30000
# MAGIC WHEN Total_offline_potential ='50000 - 1 lakh' THEN 75000
# MAGIC WHEN Total_offline_potential ='<10000' THEN 5000
# MAGIC WHEN Total_offline_potential ='1 lakh - 5 lakhs' THEN 300000
# MAGIC WHEN Total_offline_potential ='5 lakhs - 10 lakhs' THEN 750000
# MAGIC WHEN Total_offline_potential ='25 lakhs - 50 lakhs' THEN 3500000
# MAGIC WHEN Total_offline_potential ='5 Cr +' THEN 50000000
# MAGIC WHEN Total_offline_potential ='50 lakhs - 1 Cr' THEN 7500000
# MAGIC WHEN Total_offline_potential ='1 Cr - 5 Cr' THEN 25000000 else Total_offline_potential end as Total_offline_potential_rs,
# MAGIC
# MAGIC case when b.first_txn_date >= a.first_called_date then 'MTU_post_callng'
# MAGIC when b.first_txn_date < a.first_called_date then 'mtu_from_before'
# MAGIC when b.first_txn_date is null then 'Non-MTU' else 'Other' end as MTU_status,
# MAGIC
# MAGIC case when status = 'Call later' then 'Call later'
# MAGIC when status = 'Mtu done on call' then 'Interested'
# MAGIC when status = 'Interested' then 'Interested'
# MAGIC when status = 'Call dropped' then 'Call later'
# MAGIC when status = 'Move to competion' then 'Move to competion'
# MAGIC when status = 'Not interested' then 'Not interested'
# MAGIC when status = 'call dropped' then 'Not interested'
# MAGIC when status = 'Voice not clear' then 'Call later'
# MAGIC when status = 'Others' then 'Interested'
# MAGIC when status = 'RNR' then 'Call later'
# MAGIC when status = 'Language different' then 'Language different'
# MAGIC when status = 'Busy' then 'Call later' else status end as status_updated,
# MAGIC
# MAGIC case when Competition = 'AXIS BANK' then 'AXIS BANK'
# MAGIC when Competition = 'Banks' then 'Banks'
# MAGIC when Competition = 'Bharatpe' then 'Bharatpe'
# MAGIC when Competition = 'Billdesk' then 'Billdesk'
# MAGIC when Competition = 'Cashfree' then 'Cashfree'
# MAGIC when Competition = 'CCAvenue' then 'CCAvenue'
# MAGIC when Competition = 'cosmoseed' then 'cosmoseed'
# MAGIC when Competition = 'Eazebuzz' then 'Eazebuzz'
# MAGIC when Competition = 'GooglePe' then 'GooglePe'
# MAGIC when Competition = 'GoQuick' then 'GoQuick'
# MAGIC when Competition = 'HDFC BANK' then 'HDFC BANK'
# MAGIC when Competition = 'ICICI BANK' then 'ICICI BANK'
# MAGIC when Competition = 'ICICI Bank' then 'ICICI BANK'
# MAGIC when Competition = 'IDBI BANK' then 'IDBI BANK'
# MAGIC when Competition = 'Insta mojo' then 'Instamojo'
# MAGIC when Competition = 'Instamojo' then 'Instamojo'
# MAGIC when Competition = 'instamojo' then 'Instamojo'
# MAGIC when Competition = 'nimbbl' then 'nimbbl'
# MAGIC when Competition = 'OTHER BANK' then 'OTHER BANK'
# MAGIC when Competition = 'Others' then 'OTHER BANK'
# MAGIC when Competition = 'OTHERS BANK' then 'OTHER BANK'
# MAGIC when Competition = 'Paypal' then 'PayPal'
# MAGIC when Competition = 'PayPal' then 'PayPal'
# MAGIC when Competition = 'PayTm PG' then 'PayTm PG'
# MAGIC when Competition = 'PayTm UPI' then 'PayTm UPI'
# MAGIC when Competition = 'PayU' then 'PayU'
# MAGIC when Competition = 'PhonePe PG' then 'PhonePe PG'
# MAGIC when Competition = 'Phonepe PG' then 'PhonePe PG'
# MAGIC when Competition = 'PhonePe UPI' then 'PhonePe UPI'
# MAGIC when Competition = 'SBI BANK' then 'SBI BANK'
# MAGIC when Competition = 'Stripe' then 'Stripe'
# MAGIC when Competition = 'UpWork' then 'UpWork' else lower(Competition) end as Competition_updated,
# MAGIC
# MAGIC case when final_score_for_hql >= 55 then 'HQL' else 'Non-HQL' end as HQL_Flag
# MAGIC
# MAGIC   FROM 
# MAGIC   (
# MAGIC     select *, 
# MAGIC    rank() over(partition by mid order by calling_date desc) as duplicacy,
# MAGIC    min(calling_date) over(partition by mid) as first_called_date 
# MAGIC   from aggregate_ba.call_centre_calling_analyses
# MAGIC   ) a 
# MAGIC   left join aggregate_pa.merchant_fact_hourly_incremental b on a.mid = b.merchant_id 
# MAGIC   left join (select distinct merchant_id, coalesce(gstin_score,0) as gstin_score, coalesce(domain_score,0) as domain_score, 
# MAGIC   (coalesce(gstin_score,0)+ coalesce(domain_score,0)) as final_score_for_hql
# MAGIC    from aggregate_pa.pg_acquisition_product_lead_score_data
# MAGIC   ) c on a.mid = c.merchant_id
# MAGIC    where duplicacy = 1
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from aggregate_ba.call_centre_calling_analyses_final

# COMMAND ----------

import datetime
import json

data_to_upload = sqlContext.sql("""SELECT merchant_id FROM merchants limit 100""")

data_to_upload_df=data_to_upload.toPandas()

# function to convert datetime.date object to string
def datetime_handler(x):
    if isinstance(x, datetime.date):
        return x.isoformat()
    raise TypeError("Unknown type")

# modify the code to dump datetime.date objects to string
data_to_upload = data_to_upload_df.values.tolist()

# convert any non-serializable objects to serializable ones
data_to_upload = [[json.dumps(datetime_handler(cell)).replace('"', '') if isinstance(cell, datetime.date) else json.dumps(cell).replace('"', '') if not isinstance(cell, (int, float, bool)) else cell for cell in row] for row in data_to_upload]

header = data_to_upload_df.columns.tolist()
# data_to_upload = [header] + data_to_upload

# COMMAND ----------

def upload_to_google_sheet(data, credentials_file):
    # Load credentials
    scopes = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive"
    ]
    creds = Credentials.from_service_account_file(credentials_file,scopes=scopes)
    client = gspread.authorize(creds)
    
    #create a sheet with current month's name
    # spreadsheet = client.create(f"Churned MIDs in {month}")

    # Open the Google Sheet
    # Get the spreadsheet ID
    spreadsheet_id = '1ph4i1bs_mkw1DVmSpHM1OpD6MBT9kbtmsAnSL_YuBMQ'

    sheet_name = "Raw data"
    spreadsheet = client.open_by_key(spreadsheet_id)

    # email_to_share_with = ['harshita.bhardwaj@razorpay.com']
    # for user in email_to_share_with:
    #     # Share the sheet with the specified email
    #     spreadsheet.share(user, perm_type='user', role='writer', notify=False)

    sheet=spreadsheet.worksheet(sheet_name)
    
    # print(f"Spreadsheet shared! ID: {spreadsheet_id}")

    # find the last row in the sheet
    rowCount = sheet.row_count
    lastRow = rowCount + 1

    data_range='A3:AB' + str(lastRow)

    # Clear the specific range in the sheet
    # sheet.clear('A5:H' + str(lastRow))
    #sheet.clear(start='A5', end='H' + str(lastRow))
    
    sheet.resize(rows=len(data_to_upload)+1000)

    # Upload data to the sheet
    # sheet.insert_rows(data,6)
    sheet.batch_update([{'range':data_range,'values':data_to_upload}],value_input_option="user_entered")

    return spreadsheet_id

##start of Gsheet Upload

credentials_file = "/dbfs/FileStore/Manish/google_ads_json/main_cred.json"

try:
    spreadsheet_id = upload_to_google_sheet(data_to_upload, credentials_file)

    print("Gsheet data uploaded successfully for this day ",spreadsheet_id)

    sheet_link = "https://docs.google.com/spreadsheets/d/"+spreadsheet_id

    try:
        sqlContext.sql("""INSERT INTO aggregate_ba.service_account_logs SELECT 'Cohort_dashboard_update' as automaiton_name,'monthly' as schedule_type, '"""+sheet_link+"""' as sheet_link, current_date as updated_date,'Success' as status""")
    except:
        sqlContext.sql("""create table aggregate_ba.service_account_logs as (SELECT 'Cohort_dashboard_update' as automaiton_name,'monthly' as schedule_type, '"""+sheet_link+"""' as sheet_link, current_date as updated_date,'Success' as status)""")

    # send_email('deekonda.sai@razorpay.com', 'deekonda.sai@razorpay.com', 'Churn MIDs List for this month', sheet_link)
except Exception as e:
    if(spreadsheet_id is None):
        sheet_link=None
    try:
        print(e)
        sqlContext.sql("""INSERT INTO aggregate_ba.service_account_logs SELECT 'Cohort_dashboard_update' as automaiton_name,'monthly' as schedule_type, '"""+sheet_link+"""' as sheet_link, current_date as updated_date,'Failed' as status""")
    except:
        sqlContext.sql("""create table aggregate_ba.service_account_logs as (SELECT 'Cohort_dashboard_update' as automaiton_name,'monthly' as schedule_type, '"""+sheet_link+"""' as sheet_link, current_date as updated_date,'Failed' as status)""")


# COMMAND ----------

drop table if exists analytics_selfserve.segment_mid_attribution_page_inc;
create table analytics_selfserve.segment_mid_attribution_page_inc (
  signup_date date,
  merchant_id varchar,
  anonymousid varchar,
  easy_onboarding varchar,
  signup_method varchar,
  acquisition_medium varchar,
  acquisition_source varchar,
  acquisition_campaign varchar,
  acquisition_keyword varchar,
  acquisition_adgroup varchar,
  acquisition_content varchar,
  campaign_matchtype varchar,
  campaign_location varchar,
  campaign_network varchar,
  campaign_device varchar,
  campaign_devicemodel varchar,
  landing_page varchar,
  last_acquisition_medium varchar,
  last_acquisition_source varchar,
  last_acquisition_campaign varchar,
  last_acquisition_keyword varchar,
  last_acquisition_adgroup varchar,
  last_acquisition_content varchar,
  last_campaign_matchtype varchar,
  last_campaign_location varchar,
  last_campaign_network varchar,
  last_campaign_device varchar,
  last_campaign_devicemodel varchar,
  last_landing_page varchar

)
WITH (format = 'PARQUET');
insert into analytics_selfserve.segment_mid_attribution_page_inc

With attribution as(
SELECT distinct a.anonymousid,merchant_id,easy_onboarding,
a.acquisition_medium as s_acquisition_medium,
a.acquisition_source as s_acquisition_source,
a.acquisition_campaign as s_acquisition_campaign,
a.acquisition_keyword as s_acquisition_keyword,
a.acquisition_adgroup as s_acquisition_adgroup,
a.acquisition_content as s_acquisition_content,
a.signup_method,a.device as s_device,
source,a.timestamp as s_timestamp, a.date as s_date,a.landing_page as s_landing_page, b.acquisition_medium,b.acquisition_source,b.acquisition_campaign,b.acquisition_keyword,
b.acquisition_adgroup,b.acquisition_content,b.campaign_matchtype,
 b.campaign_location,
 b.campaign_network,
 b.campaign_device,
 b.campaign_devicemodel,b.landing_page,b.timestamp,b.date,
rank() over(partition by a.merchant_id order by a.timestamp,b.timestamp) first_rank,
rank() over(partition by a.merchant_id order by a.timestamp,b.timestamp desc) last_rank
from (select *
from(
select *,rank() over(partition by merchant_id order by timestamp) as signuprank
from(
select distinct json_extract_scalar(properties,'$.client_id') as client_id, properties,context,s.userid,s.anonymousid,

if(json_extract_scalar(properties,'$.MID')<>'null' and json_extract_scalar(properties,'$.MID') is not null and json_extract_scalar(properties,'$.MID')<>'', json_extract_scalar(properties,'$.MID'),
   if(json_extract_scalar(properties,'$.merchantId')<>'null' and json_extract_scalar(properties,'$.merchantId') is not null and json_extract_scalar(properties,'$.merchantId')<>'', json_extract_scalar(properties,'$.merchantId'),
      if(json_extract_scalar(properties,'$.mid')<>'null' and json_extract_scalar(properties,'$.mid') is not null and json_extract_scalar(properties,'$.mid')<>'', json_extract_scalar(properties,'$.mid'),if(c.mid is not null, c.mid, m.merchant_id )))) as merchant_id,

if(json_extract_scalar(properties,'$.easyOnboarding')='true','true','false') as easy_onboarding,

coalesce(json_extract_scalar(context,'$.campaign.medium'),json_extract_scalar(properties,'$.attribUtm.utm_medium'),json_extract_scalar(properties,'$.attrib_utm.utm_medium')) as acquisition_medium,
coalesce(json_extract_scalar(context,'$.campaign.source'),json_extract_scalar(properties,'$.attribUtm.utm_source'),json_extract_scalar(properties,'$.attrib_utm.utm_source')) as acquisition_source,

coalesce(json_extract_scalar(context,'$.campaign.adgroup'),json_extract_scalar(properties,'$.attribUtm.utm_adgroup'),json_extract_scalar(properties,'$.attrib_utm.utm_adgroup')) as acquisition_adgroup,

coalesce(json_extract_scalar(context,'$.campaign.name'),json_extract_scalar(properties,'$.attribUtm.utm_campaign'),json_extract_scalar(properties,'$.attrib_utm.utm_campaign')) as acquisition_campaign,

coalesce(json_extract_scalar(context,'$.campaign.content'),json_extract_scalar(properties,'$.attribUtm.utm_content'),json_extract_scalar(properties,'$.attrib_utm.utm_content')) as acquisition_content,

coalesce(json_extract_scalar(context,'$.campaign.term'),json_extract_scalar(properties,'$.attribUtm.utm_keyword'),json_extract_scalar(properties,'$.attrib_utm.utm_keyword')) as acquisition_keyword,
json_extract_scalar(properties,'$.type') as signup_method,
coalesce(json_extract_scalar(properties,'$.device_type'),json_extract_scalar(properties,'$.deviceType')) as device,
json_extract_scalar(properties,'$.source') as source,
  
coalesce(json_extract_scalar(context,'$.page.referrer'),json_extract_scalar(properties,'$.referrer')) as landing_page,
cast(from_iso8601_timestamp(originaltimestamp) as timestamp) as timestamp,
cast(from_iso8601_timestamp(originaltimestamp) as date) as date

from hive.aggregate_pa.segment_to_dl s
left join (select user_id,merchant_id,count() over(partition by user_id) as mapping
           from realtime_hudi_api.merchant_users
           where role='owner'
           and product='primary') m on (s.userid=m.user_id and m.mapping=1)
left join (SELECT *
           from hive.aggregate_pa.mid_anonymousid_mapping
           where mid is not null
          ) c on c.anonymousid=s.anonymousid
where ((lower(event) = 'sign up create account result' and json_extract_scalar(properties,'$.status')='success'))

and cast(ts_date as date)>=  cast('2023-08-01' as date)
  --date_add('day',-4,current_date)
)

) where signuprank=1) a
left join (select *
from (
    select anonymousid,properties,context,
coalesce(json_extract_scalar(context,'$.campaign.medium'),
if((lower(json_extract_scalar(context,'$.page.referrer')) like '%razorpay%' or lower(json_extract_scalar(context,'$.page.referrer')) like '%curlec%' or lower(json_extract_scalar(context,'$.page.referrer')) is null or lower(json_extract_scalar(context,'$.page.referrer')) =''), 'website','organic')) as acquisition_medium,

coalesce(json_extract_scalar(context,'$.campaign.source'),
if((lower(json_extract_scalar(context,'$.page.referrer')) like '%razorpay%' or lower(json_extract_scalar(context,'$.page.referrer')) like '%curlec%' or lower(json_extract_scalar(context,'$.page.referrer')) is null or lower(json_extract_scalar(context,'$.page.referrer')) =''), 'direct',if(lower(json_extract_scalar(context,'$.page.referrer')) like '%google%','google',if(lower(json_extract_scalar(context,'$.page.referrer')) like '%bing%','bing','others')))) as acquisition_source,

json_extract_scalar(context,'$.campaign.adgroup') as acquisition_adgroup,

json_extract_scalar(context,'$.campaign.name') as acquisition_campaign,

json_extract_scalar(context,'$.campaign.content') as acquisition_content,

json_extract_scalar(context,'$.campaign.term') as acquisition_keyword,

json_extract_scalar(context,'$.campaign.matchtype') as campaign_matchtype,

json_extract_scalar(context,'$.campaign.location') as campaign_location,

json_extract_scalar(context,'$.campaign.network') as campaign_network,

json_extract_scalar(context,'$.campaign.device') as campaign_device,

json_extract_scalar(context,'$.campaign.devicemodel') as campaign_devicemodel,
  
coalesce(json_extract_scalar(context,'$.page.url'),json_extract_scalar(properties,'$.url'),json_extract_scalar(properties,'$.title'),json_extract_scalar(context,'$.page.title')) as landing_page,

    cast(from_iso8601_timestamp(originaltimestamp) as timestamp) as timestamp,
    cast(from_iso8601_timestamp(originaltimestamp) as date) as date
    from hive.aggregate_pa.segment_to_dl
    where type='page'
    and ts_date >= cast('2023-05-01' as date)
  --date_add('day',-65,current_date)
)

where acquisition_source is not null
and acquisition_medium is not null
and anonymousid is not null) b on (a.anonymousid=b.anonymousid and b.timestamp < a.timestamp and date_diff('day',b.timestamp,a.timestamp)<=60)

)

SELECT b1.s_date as signup_date, b1.merchant_id,b1.anonymousid,b1.easy_onboarding,b1.signup_method,

 b1.acquisition_medium as acquisition_medium,
 b1.acquisition_source as acquisition_source,
 b1.acquisition_campaign as acquisition_campaign,
 b1.acquisition_keyword as acquisition_keyword,
 b1.acquisition_adgroup as acquisition_adgroup,
 b1.acquisition_content as acquisition_content,
 b1.campaign_matchtype,
 b1.campaign_location,
 b1.campaign_network,
 b1.campaign_device,
 b1.campaign_devicemodel,
 b1.landing_page as landing_page,

 b2.acquisition_medium as last_acquisition_medium,
 b2.acquisition_source as last_acquisition_source,
 b2.acquisition_campaign as last_acquisition_campaign,
 b2.acquisition_keyword as last_acquisition_keyword,
 b2.acquisition_adgroup as last_acquisition_adgroup,
 b2.acquisition_content as last_acquisition_content,
 b1.campaign_matchtype as last_campaign_matchtype,
 b1.campaign_location as last_campaign_location,
 b1.campaign_network as last_campaign_network,
 b1.campaign_device as last_campaign_device,
 b1.campaign_devicemodel as last_campaign_devicemodel,
 b2.landing_page as last_landing_page

from
(select * from attribution where first_rank=1) b1
left join (select * from attribution where last_rank=1) b2 on b1.merchant_id=b2.merchant_id

# COMMAND ----------

drop table if exists analytics_selfserve.segment_mid_attribution_web_inc;
create table analytics_selfserve.segment_mid_attribution_web_inc
(
  signup_date date,
  merchant_id varchar,
  anonymousid varchar,
  easy_onboarding varchar,
  signup_method varchar,
  acquisition_medium varchar,
  acquisition_source varchar,
  acquisition_campaign varchar,
  acquisition_keyword varchar,
  acquisition_adgroup varchar,
  acquisition_content varchar,
  landing_page varchar,
  last_acquisition_medium varchar,
  last_acquisition_source varchar,
  last_acquisition_campaign varchar,
  last_acquisition_keyword varchar,
  last_acquisition_adgroup varchar,
  last_acquisition_content varchar,
  last_landing_page varchar
)
WITH (format = 'PARQUET');
insert into analytics_selfserve.segment_mid_attribution_web_inc
With attribution as(
SELECT distinct a.anonymousid,merchant_id,easy_onboarding,
a.acquisition_medium as s_acquisition_medium,
a.acquisition_source as s_acquisition_source,
a.acquisition_campaign as s_acquisition_campaign,
a.acquisition_keyword as s_acquisition_keyword,
a.acquisition_adgroup as s_acquisition_adgroup,
a.acquisition_content as s_acquisition_content,
a.signup_method,
source,a.timestamp as s_timestamp, a.date as s_date,a.landing_page as s_landing_page, b.acquisition_medium,b.acquisition_source,b.acquisition_campaign,b.acquisition_keyword,
b.acquisition_adgroup,b.acquisition_content,b.landing_page,b.timestamp,b.date,
rank() over(partition by a.merchant_id order by a.timestamp,b.timestamp) first_rank,
rank() over(partition by a.merchant_id order by a.timestamp,b.timestamp desc) last_rank
from (select *
from(
select *,rank() over(partition by merchant_id order by timestamp) as signuprank
from(
select distinct json_extract_scalar(properties,'$.client_id') as client_id, properties,context,s.userid,s.anonymousid,
if(json_extract_scalar(properties,'$.MID')<>'null' and json_extract_scalar(properties,'$.MID') is not null and json_extract_scalar(properties,'$.MID')<>'', json_extract_scalar(properties,'$.MID'),
   if(json_extract_scalar(properties,'$.merchantId')<>'null' and json_extract_scalar(properties,'$.merchantId') is not null and json_extract_scalar(properties,'$.merchantId')<>'', json_extract_scalar(properties,'$.merchantId'),
      if(json_extract_scalar(properties,'$.mid')<>'null' and json_extract_scalar(properties,'$.mid') is not null and json_extract_scalar(properties,'$.mid')<>'', json_extract_scalar(properties,'$.mid'),if(c.mid is not null, c.mid, m.merchant_id )))) as merchant_id,
if(json_extract_scalar(properties,'$.easyOnboarding')='true','true','false') as easy_onboarding,
coalesce(json_extract_scalar(context,'$.campaign.medium'),json_extract_scalar(properties,'$.attribUtm.utm_medium'),json_extract_scalar(properties,'$.attrib_utm.utm_medium')) as acquisition_medium,
coalesce(json_extract_scalar(context,'$.campaign.source'),json_extract_scalar(properties,'$.attribUtm.utm_source'),json_extract_scalar(properties,'$.attrib_utm.utm_source')) as acquisition_source,
coalesce(json_extract_scalar(context,'$.campaign.adgroup'),json_extract_scalar(properties,'$.attribUtm.utm_adgroup'),json_extract_scalar(properties,'$.attrib_utm.utm_adgroup')) as acquisition_adgroup,
coalesce(json_extract_scalar(context,'$.campaign.name'),json_extract_scalar(properties,'$.attribUtm.utm_campaign'),json_extract_scalar(properties,'$.attrib_utm.utm_campaign')) as acquisition_campaign,
coalesce(json_extract_scalar(context,'$.campaign.content'),json_extract_scalar(properties,'$.attribUtm.utm_content'),json_extract_scalar(properties,'$.attrib_utm.utm_content')) as acquisition_content,
coalesce(json_extract_scalar(context,'$.campaign.term'),json_extract_scalar(properties,'$.attribUtm.utm_keyword'),json_extract_scalar(properties,'$.attrib_utm.utm_keyword')) as acquisition_keyword,
json_extract_scalar(properties,'$.type') as signup_method,
coalesce(json_extract_scalar(properties,'$.device_type'),json_extract_scalar(properties,'$.deviceType')) as device,
json_extract_scalar(properties,'$.source') as source,
coalesce(json_extract_scalar(context,'$.page.referrer'),json_extract_scalar(properties,'$.referrer')) as landing_page,
cast(from_iso8601_timestamp(originaltimestamp) as timestamp) as timestamp,
cast(from_iso8601_timestamp(originaltimestamp) as date) as date
from hive.aggregate_pa.segment_to_dl s
left join (select user_id,merchant_id,count() over(partition by user_id) as mapping
           from realtime_hudi_api.merchant_users
           where role='owner'
           and product='primary') m on (s.userid=m.user_id and m.mapping=1)
left join (SELECT *
           from hive.aggregate_pa.mid_anonymousid_mapping
           where mid is not null
          ) c on c.anonymousid=s.anonymousid
where ((lower(event) = 'sign up create account result' and json_extract_scalar(properties,'$.status')='success'))
and ts_date >= cast('2023-08-01' as date)
  --date_add('day',-4,current_date)
)
) where signuprank=1) a
left join (select *
from (
    select anonymousid,properties,context,
                                                         coalesce(json_extract_scalar(a.properties,'$.attrib_utm.utm_medium'), json_extract_scalar(a.properties,'$.attribUtm.utmMedium'),                                           json_extract_scalar(context,'$.campaign.medium'),
json_extract_scalar(a.properties,'$.attribUtm.utm_medium')                                                                ) as acquisition_medium,
coalesce(json_extract_scalar(a.properties,'$.attrib_utm.utm_source'), json_extract_scalar(a.properties,'$.attribUtm.utmSource'),
json_extract_scalar(a.properties,'$.attribUtm.utm_source'),
json_extract_scalar(context,'$.campaign.source')) as acquisition_source,
coalesce(json_extract_scalar(a.properties,'$.attrib_utm.utm_adgroup'), json_extract_scalar(a.properties,'$.attribUtm.utmAdgroup'),
         json_extract_scalar(a.properties,'$.attribUtm.utm_adgroup'),
         json_extract_scalar(context,'$.campaign.adgroup')) as acquisition_adgroup,
coalesce(json_extract_scalar(a.properties,'$.attrib_utm.utm_campaign'), json_extract_scalar(a.properties,'$.attribUtm.utmCampaign'),
         json_extract_scalar(a.properties,'$.attribUtm.utm_campaign'), json_extract_scalar(context,'$.campaign.name')) as acquisition_campaign,
coalesce(json_extract_scalar(a.properties,'$.attrib_utm.utm_content'), json_extract_scalar(a.properties,'$.attribUtm.utmContent'),
         json_extract_scalar(a.properties,'$.attribUtm.utm_content'),json_extract_scalar(context,'$.campaign.content')) as acquisition_content,
coalesce(json_extract_scalar(a.properties,'$.attrib_utm.utm_keyword'), json_extract_scalar(a.properties,'$.attribUtm.utmTerm'),
         json_extract_scalar(a.properties,'$.attribUtm.utm_term'),  json_extract_scalar(context,'$.campaign.term')) as acquisition_keyword,
    json_extract_scalar(properties,'$.device_type') as device,
  coalesce(json_extract_scalar(properties, '$.url'),json_extract_scalar(properties, '$.page_url'),json_extract_scalar(properties, '$.pageUrl')) AS landing_page,
coalesce(json_extract_scalar(properties,'$.referrer'),json_extract_scalar(context,'$.page.referrer')) AS last_landing_page,
    cast(from_iso8601_timestamp(originaltimestamp) as timestamp) as timestamp,
    cast(from_iso8601_timestamp(originaltimestamp) as date) as date
    from hive.aggregate_pa.segment_to_dl a
    where lower(event) in ('website page viewed','sign up displayed','signup display signup page success','signup displayed success','page viewed')
    and ts_date >= cast('2023-05-01' as date)
  --date_add('day',-65,current_date)
)
where acquisition_source is not null
and acquisition_medium is not null
and anonymousid is not null
) b on (a.anonymousid=b.anonymousid and b.timestamp < a.timestamp and date_diff('day',b.timestamp,a.timestamp)<=60)
)
SELECT b1.s_date as signup_date, b1.merchant_id,b1.anonymousid,b1.easy_onboarding,b1.signup_method,
 b1.acquisition_medium as acquisition_medium,
 b1.acquisition_source as acquisition_source,
 b1.acquisition_campaign as acquisition_campaign,
 b1.acquisition_keyword as acquisition_keyword,
 b1.acquisition_adgroup as acquisition_adgroup,
 b1.acquisition_content as acquisition_content,
 b1.landing_page as landing_page,
 b2.acquisition_medium as last_acquisition_medium,
 b2.acquisition_source as last_acquisition_source,
 b2.acquisition_campaign as last_acquisition_campaign,
 b2.acquisition_keyword as last_acquisition_keyword,
 b2.acquisition_adgroup as last_acquisition_adgroup,
 b2.acquisition_content as last_acquisition_content,
 b2.last_landing_page as last_landing_page
from
(select * from attribution where first_rank=1) b1
left join (select * from attribution where last_rank=1) b2 on b1.merchant_id=b2.merchant_id
