# Databricks notebook source
from datetime import datetime, timedelta, timezone
from datetime import date
import pandas as pd
import numpy as np

# COMMAND ----------

from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.header import Header
import smtplib
import os

# COMMAND ----------

from itertools import combinations

# COMMAND ----------

import plotly.graph_objects as go

# COMMAND ----------

#%pip install gspread

# COMMAND ----------

# import gspread
# from google.oauth2.service_account import Credentials
# import base64


# COMMAND ----------



# def to_create_spreadsheet(credentials_file):
#     scopes = [
#     "https://spreadsheets.google.com/feeds",
#     "https://www.googleapis.com/auth/drive"
#     ]
#     creds = Credentials.from_service_account_file(credentials_file,scopes=scopes)
#     client = gspread.authorize(creds)
    
#     #create a spreadsheet with any naming nomenclature u want
#     spreadsheet = client.create(f"Call centre calling analysis")

#     # Open the Google Sheet
#     # Get the spreadsheet ID
#     spreadsheet_id = spreadsheet.id


#     #mention the email's that u wanna share this new gsheet to
#     email_to_share_with = ['sanjay.garg@razorpay.com']
#     for user in email_to_share_with:
#         # Share the sheet with the specified emails
#         spreadsheet.share(user, perm_type='user', role='writer', notify=True)
    
#     return(spreadsheet_id)

# credentials_file = "/dbfs/FileStore/Manish/google_ads_json/amg_service_acc.json"

# spreadsheet_id = to_create_spreadsheet(credentials_file)
# print(spreadsheet_id)

# COMMAND ----------



# spreadsheet_id = "1sfq362Pl-SQhcwuLeeouvATTrd1rm39HovDSRh9dNtk"
# sheet_name = "Responses"
# credentials_file = "/dbfs/FileStore/Manish/google_ads_json/amg_service_acc.json"

# def read_spreadsheet(credentials_file,spreadsheet_id,sheet_name):
#     scopes = [
#         "https://spreadsheets.google.com/feeds",
#         "https://www.googleapis.com/auth/drive"
#      ]
#     creds = Credentials.from_service_account_file(credentials_file,scopes=scopes)
#     client = gspread.authorize(creds)

#     # Open the Google Sheet
#     # Get the spreadsheet ID
#     spreadsheet = client.open_by_key(spreadsheet_id)
#     sheet=spreadsheet.worksheet(sheet_name)
#     data = sheet.get_all_values()

#     df = pd.DataFrame(data[1:], columns=data[0])
#     return df
#     # remove spaces and special characters from column names
#     # df.columns = [re.sub(r'\W+', '_', col.strip()) for col in df.columns]
    
#     # spark = SparkSession.builder.getOrCreate()
#     # spark_df = spark.createDataFrame(df)
#     # spark_df.createOrReplaceTempView("temp_view_of_the_read_calling_data")
#     # return spark_df




# COMMAND ----------

# df = read_spreadsheet(credentials_file,spreadsheet_id,sheet_name)
# mid = df.loc[len(df)-1,'merchant_id']
# Pre_Period_Starting_Date = datetime.strptime(df.loc[len(df)-1,'start_date1'] , "%m/%d/%Y").strftime("%d-%m-%Y")
# Pre_Period_Ending_Date = datetime.strptime(df.loc[len(df)-1,'end_date1'] , "%m/%d/%Y").strftime("%d-%m-%Y")
# Post_Period_Starting_Date = datetime.strptime(df.loc[len(df)-1,'end_date1'] , "%m/%d/%Y").strftime("%d-%m-%Y")
# Post_Period_Ending_Date = datetime.strptime(df.loc[len(df)-1,'end_date2'] , "%m/%d/%Y").strftime("%d-%m-%Y")
# timestamp = datetime.strptime(df.loc[len(df)-1,'Timestamp'] ,  "%m/%d/%Y %H:%M:%S")

# COMMAND ----------



# def get_current_timestamp_ist():
#   """Gets the current timestamp in IST (India Standard Time).

#   Returns:
#       A datetime object representing the current time in IST.
#   """

#   # Get the current UTC time
#   now_utc = datetime.now(timezone.utc)

#   # IST is UTC+05:30
#   ist_offset = timezone(timedelta(hours=5, minutes=30))

#   # Convert UTC time to IST by applying the offset
#   current_datetime_ist = now_utc.astimezone(ist_offset)
  
#   return current_datetime_ist.replace(tzinfo=None)
# get_current_timestamp_ist()

# COMMAND ----------


mid = 'V2 Comparison'
Pre_Period_Starting_Date = '2024-06-01'
Pre_Period_Ending_Date='2024-06-06'
Post_Period_Starting_Date='2024-06-07'
Post_Period_Ending_Date='2024-06-10'
timestamp= datetime.strptime('5/7/2024 10:44:21', "%m/%d/%Y %H:%M:%S")
timestamp

# COMMAND ----------

# current_time = get_current_timestamp_ist()
# #ADD A LOOP TO CHECK FOR ALL FORM REQS
# time_difference = abs(current_time - timestamp).total_seconds() / 60
# if time_difference >= 10:
#     exit()



# COMMAND ----------

def percentage_conversion(value):
    return f'{value:.2%}'

# COMMAND ----------

#base_table = base_db.toPandas()

# COMMAND ----------

# DBTITLE 1,Summary Screen Tables: Base Code
# Summary
base_db = sqlContext.sql(
    """
    with email_optional_table as (
  select
    checkout_id,
    max(
      cast(
        get_json_object(properties, '$.data.meta["optional.email"]') as boolean
      )
    ) as email_optional
  from
    aggregate_pa.cx_1cc_events_dump_v1
  where
      ((producer_created_date  between date('{1}')
    and date('{2}'))
     or (producer_created_date  between date('{3}')
    and date('{4}')))
    and event_name = 'render:1cc_summary_screen_loaded_completed'
  group by
    1
),
experiments as(
   select
    checkout_id,
    max(
      case
        when experiment_name = 'checkout_redesign'
          then experiment_value
      end
    ) as checkout_redesign

  from
    aggregate_pa.cx_1cc_experiment_fact
  where
    experiment_name in (
      'checkout_redesign'
    )
    and        ((producer_created_date  between date('{1}')
    and date('{2}'))
     or (producer_created_date  between date('{3}')
    and date('{4}')))
  group by
    1
), summary_reasons as(
SELECT
checkout_id,
    (case
         
          when magic_checkout_fact.summary_screen_loaded = 0 then 'Summary Screen did not load'
        --  when magic_checkout_fact.summary_screen_loaded = 1 and magic_checkout_fact.summary_screen_continue_cta_clicked = 1 then 'Exited Summary Screen successfully'
          when (magic_checkout_fact.contact_email_entered=1 or magic_checkout_fact.contact_number_entered=1
    or magic_checkout_fact.clicked_change_contact=1
    or (magic_checkout_fact.contact_fill_began=1 and magic_checkout_fact.prefill_contact_number=0)
    or (magic_checkout_fact.email_fill_began=1 and magic_checkout_fact.prefill_email=0)) = True and (magic_checkout_fact.have_coupon_clicked=1 or magic_checkout_fact.coupon_screen_loaded=1) = False then 'Interacted w contact but not coupons'
          when  (magic_checkout_fact.contact_email_entered=1 or magic_checkout_fact.contact_number_entered=1
    or magic_checkout_fact.clicked_change_contact=1
    or (magic_checkout_fact.contact_fill_began=1 and magic_checkout_fact.prefill_contact_number=0)
    or (magic_checkout_fact.email_fill_began=1 and magic_checkout_fact.prefill_email=0)) = False and (magic_checkout_fact.have_coupon_clicked=1 or magic_checkout_fact.coupon_screen_loaded=1) = True then 'Interacted w coupons but not contact'
          when (magic_checkout_fact.contact_email_entered=1 or magic_checkout_fact.contact_number_entered=1
    or magic_checkout_fact.clicked_change_contact=1
    or (magic_checkout_fact.contact_fill_began=1 and magic_checkout_fact.prefill_contact_number=0)
    or (magic_checkout_fact.email_fill_began=1 and magic_checkout_fact.prefill_email=0)) = True and (magic_checkout_fact.have_coupon_clicked=1 or magic_checkout_fact.coupon_screen_loaded=1) = True then 'Interacted w both coupons and contact'

  
  --        when magic_checkout_fact.summary_screen_continue_cta_clicked = 0
   ---       and (magic_checkout_fact.contact_email_entered=1 or magic_checkout_fact.contact_number_entered=1
   --- or magic_checkout_fact.clicked_change_contact=1
    ---or (magic_checkout_fact.contact_fill_began=1 and magic_checkout_fact.prefill_contact_number=0)
    --or (magic_checkout_fact.email_fill_began=1 and magic_checkout_fact.prefill_email=0)) = False and --(magic_checkout_fact.have_coupon_clicked=1 or magic_checkout_fact.coupon_screen_loaded=1) = False then 
  
        --  when magic_checkout_fact.edit_address_clicked = 1 then 'Exited to Edit Address'
         -- when magic_checkout_fact.summary_screen_continue_cta_clicked = 1 then 'Summary CTA clicked directly'
      
        --  else 'Bounced w/o any interaction'
        else 'No interaction (Bounce/Direct)'
          end
) AS summary_screen_dropoffs
from
  aggregate_pa.magic_checkout_fact
  where
     ((producer_created_date  between date('{1}')
    and date('{2}'))
     or (producer_created_date  between date('{3}')
    and date('{4}')))--   and a.merchant_id = 'IH7E2OJQGEKKTN'
 ---and merchant_id in ('K7RuikecA6CSyF','JUHfXse0FDfnru')

)
select
  case
    when producer_created_date between date('{1}') and date('{2}') then 'Prev'
    else 'Post'
  end as mnth,
  merchant_id,
  checkout_redesign,
  /*browser_name, -- ADD IT AGAIN #########3
  case
    when os_brand_family in ('Android', 'iOS') then os_brand_family
    else 'Others'
  end as os,
  case
    when original_amount is null then null
    when original_amount < 500 then '<500'
    when original_amount < 1000 then '500 - 1k'
    when original_amount < 2000 then '1k - 2k'
    when original_amount < 5000 then '2k - 5k'
    when original_amount < 10000 then '5k - 10k'
    else '>10K'
  end as aov, 
  case
    when initial_loggedin is null then False
    else initial_loggedin
  end as initial_loggedin,
  case
    when initial_hassavedaddress is null then False
    else initial_hassavedaddress
  end as initial_hassavedaddress,
 prefill_contact_number,

 summary_screen_dropoffs as summary_screen_paths,
*/
  sum(open) as open,
  sum(submit) as submit
  --sum(summary_screen_continue_cta_clicked) as submit
from
  aggregate_pa.magic_checkout_fact a
  left join experiments b on a.checkout_id = b.checkout_id 
 ---  left join email_optional_table c on a.checkout_id = c.checkout_id
   --inner join realtime_hudi_api.merchants d on a.merchant_id = d.id
   -- left join summary_reasons e on a.checkout_id = e.checkout_id

where
      ((producer_created_date  between date('{1}')
    and date('{2}'))
     or (producer_created_date  between date('{3}')
    and date('{4}')))
   --and a.merchant_id = '{0}'
   ---and a.browser_name = 'Instagram' ---REMOVE THIS LATER
   --and a.original_amount < 500 ---REMOVE THIS LATER
    and a.merchant_id in  (
      'BSliv8OO93Akaw',
'KmF0FC4TH6Xf2y','Gjg0faqjOOxjmp','FAXmtb8O7qfTA0',
'KaMpKmqcN46zJw','HTBmzHJZ0K1ZWE',
'H6g3zLCqDwNhv4','FlahYTGP94b8pb',
'BOHnLhjiXMZgrP','HFmJ5BoCIzaVA9','Ig8szpBio1i2ob',
'BTfA2QYslX2w3y',
'I7AsvpbJy5c2yw',
'KijiyepMpjPn0p','IkLuiiPC0ADyIU','KfdyK44zhZDA7k',
'KkdjiHZPb6ezJc',
'GV63Iyo8dLpC9B','It1geJMGBsXJnL',
'HNIzKxF0WPpGOU',
'IhWbzZurckWfCL',
'HnQKIhB7ejcvn7',
'HJHKPXHXzIav45',
'H3yu5w5i3jbuoY',
'HV6rHtbH7cMevu',
'IB5HcwRWOzopSP',
'Id7SMPi18AIUSI',
'HJMKNEhWNoBLaL',
'JE1122CVCBDUip',
'GhIkXyVr93QEe1',
'GhnE3eFk6m7Lc3',
'H9R0VL3l3JKEwe',
'HD8fk7cdOQiQGp',
'JccWh1TlKpF2rS',
'JbnvfnIvxZxmBD',
'Gdi9LckYb2kd24',
'HW0UTDrBlH9T2f',
'DuT1rTxdpsbrjL',
'HbCID9rsH8YWJy',
'GvDnPboYahJOHO',
'FATrjRRkPxPUX3',
'Eo9LwLiuacmgry',
'HAG1Rvo8dcr6i7',
'DzWd9gnNybIHBz',
'IqImt8Xo5T2zEv',
'HbwqR3IGqMe7LY',
'JCdhfzRcU0ymaX',
'HxL1tJD03nVzrg',
'FD8INInuhtAsGM',
'H1YEXa8ZeKirSh',
'C9WXcgfn2N19Md',
'JLXtMMcUYpJXOD',
'GMQrGpZM5C95Z6',
'I611zkDR28Qaeg',
'GIVzvkEGAQkAiB',
'HKUn1Skg66Gohi',
'FxnBGdlXBfHP32',
'H4lTmtttRTjlpj',
'CuESwiSkCkm5Go',
'C1ErEVtGw0K4yG',
'Iv4UUmYW8dprWl',
'ITWRkAKjSpMAcv',
'I6mgJJ4dVcfIBi',
'Klqq69liELf0Dg',
'JTut7i6rwLx4Ct',
'K0G8U8Dd7gp3pQ',
'FipHp1HK9PGrRG',
'KhmLypv5QqOWIX',
'HDlxTlrBXYR6Ng',
'JhCKs8DTeAOTSk',
'KVW7mZCzfXKwkQ',
'JrKZee9eLf8or1',
'HbBMMdO5aQ2uFB',
'FewhAO9J6HKu8c',
'IJr2PJrzG8zAip',
'IbHFCDWA6YSny8',
'HM9SX9yyh9g6J1',
'Hdv00wiAvfJzJe',
'JvvchGHFsdIXjx',
'GcCu3RdpOUS2rs',
'GSRwa6RYhiYsRb',
'JnLrOqCLRS0PgJ',
'KdyfmtBTMrzQCv',
'KQ4GrVUbpOvGWk',
'HNomzjcB1CVVRv',
'D20A1BciwXdCcS',
'K0oge7haoPly06',
'KLITHExSmqbGiH',
'H4iGQunwAyhvSx',
'I6M7aODAmNKPsG',
'IzgcsDMzaDaTjb',
'CoPX13i2kztC9z',
'IymCx1CjwCpA8b',
'FIJnn7SuNgXWhL',
'FEgdIYS2JoHuFA',
'GpapbBhJPsL236',
'CPTrQI1gpsmGnE',
'KoDato99RwREej',
'GXtA692Iz0xCBm',
'JCUnvIpuAYobqq',
'HlmiLCCRRcnFjG',
'KXJXfW7UVk9nKG',
'DOKspiCbhj5mNK',
'D0zXioS1Srlb6c',
'FgsJzMJlzeLUAA',
'KAepcQGVe44EIX',
'CpCHYpXUonfbmZ',
'G2XauZuE60pZme',
'HkaYbbqOIGLxmk',
'IbJ48gvnG3c332',
'JH9sMAgIq2jgop',
'JcGHzqvkObR45f',
'JLA1apWbsonSUr',
'FlZboajmwJg1Vh',
'Jb6TMQ5dQmrKE6',
'DK9Few4Bi7fAiT',
'Et6O4hlZBrWp4w',
'GlHbofhuspwr8q',
'K1sPELj50pK2P0',
'EjGw93xhVfStgc',
'GCvEpm5JEwCxUV',
'IknnDr4GgG2StD',
'MX25crZfM064gU',
'D717VUIhQlVv1k',
'FHWcZif91d7fap',
'7DPluguWErARzy',
'DqqzKDf5P58Oms',
'J9nQva5xc0OKGX',
'DI7apkc7s2pm76',
'AC6Pu8nxiomq4o',
'HcK2riqWdNWmSp',
'JFddzPmRkF4wCT',
'GRUtOYRM70AngO',
'JTEWOkyXNFbelJ',
'65GoQIB3RU1Yvh',
'IufyHcl48Mf4X1',
'IYEVRwlRwhlE4E',
'FeXSQ6KOsE8AKM',
'Js2IcFiSQ1kmVi',
'K4fIyNLMHvcGyx',
'HtNa4tsOhgTVC2',
'DtakY6fXb0LP04',
'HJSoRFbW8eT8GQ',
'IU9kdbnVUmJtiu',
'CzPMALZEk99LoR',
'FYuETR0kQyPG6K',
'KibISso6vJU9Ow',
'GLsp4flN0SLmPD',
'KgZYcJCtavonTj',
'JT4kbpUCiqsgLy',
'CsDegqII5l92R8',
'HIA4H0w4brGoC3',
'KZwBy5CnUbthGj',
'JjKijdZKj0Srmr',
'KFppROGLb3OW4W',
'FlGdDSfsCgVWo5',
'Iwf3JxBVW2gVdB',
'CjB8A2eg8toQoP',
'DMkw3zplq1wf5s',
'GPvUThKsBj6Z84',
'KZgwZyIAv7UTb0',
'JvBjh9L9nuiFKi',
'G8cF9rlCVG04yf',
'Jqrt4np3L9PGiY',
'FtXLgA4jkhBrMO',
'KRyU6i4pz3kjHj',
'HIwEv0VjJIhPjf',
'HIx5fG2ruC5qAx',
'DNzDt51nqmpNHQ',
'EWm0YqdDXRDCla',
'J5KLPwngQYfFm2',
'IoOVf7uaiMCXOB',
'IcnkQdJQMvTZqh',
'FO8wE5y3FsjsYd',
'KgkpSZNlIUdR0t',
'KBSgIZFvwKyPBk',
'G5IjGUNHZ5gCOz',
'MsnC2wA83sM6x4',
'MYEW99env1SQL9',
'LpHjT492BzmvUg',
'ND9aKHcIZ7sJ4e',
'MupiEOcn7OA7jY',
'LcyBrBmQrmz7Rz',
'9i9CtFaNKDHwUZ',
'KF8M225Pbvus13',
'KQVwo9IInieiaP',
'NExx32pR22SWzW',
'HQSlXfoj6o8uvh',
'J5rj9dWLXzuZhu',
'LQU6UrtOfbEUXW',
'FczZHutchzagkZ',
'Fqo9UcuEHtsDJf',
'GAwYaL6wFf5Az9',
'NGzQBcuyxSWHgM',
'JfIKkCFjbfFjjX',
'M6SZ0LIYUPGdKQ',
'NKDNkhulk1AhKO',
'NJpfmeAuP13RDX',
'NJhXYEhE4NQpIf',
'LCaqQQ8k7KFqsk',
'N4yGqlXsLnc0G8',
'Lc6ar0MUzJZ40r',
'LpuVZHWmn9zIhL',
'J80vcVntxwYEk0',
'LyWqIUl8mvutUD',
'NIOn2z4SkXrZHR',
'LZgnkQCzZSJJEG',
'MVTTGoNHyetZzK',
'LwgJCAjE55tVVT',
'Jrx3EMLvIoWkeN',
'LqDmgNxDCdB8d6',
'NK83EfeU093bXM',
'NCCnhXvn9EfN2A',
'LT4wkHAeKGifRy',
'J6Wcn9mZV5anRh',
'JYxRVAsCrlqcnv',
'NEyq9VfKI0yWvi',
'NI9byfUwVflShr',
'JWgEGaQfE6iKKX',
'EjPGwNqcg1a0zo',
'JRuo1QbPjcSLkL',
'LaRXtUjv9GEX3z',
'Ms59BJLxaIkSPH',
'N3tyVh2DMWTl1V',
'DAaRk5TBsc4pSD',
'NIvtFogPMf4qE2',
'NOxIS24ZQ306ly',
'6qj1ZQkRkD8PCu',
'Mx9DKnBn0KnZ2v',
'NHjbYnLENAlZuh',
'NNgfRGiazhZvlj',
'MdLS60qBE8xP0E',
'MvBo4WpiN5TBsD',
'KIEXgjqQ2rPLXL',
'B57odQhW4egIri',
'HkIQwblZUXPM9l',
'KgEExBxvE3LJP5',
'NM1d0DcWzBFznx',
'Fo2g8LOS7RvwAP',
'NOtxkzih80xB4f',
'N9I95pEWQyx8vV',
'NGzZ0wx4CWayqN',
'K3Emlzd0UY4VVO',
'LVE5dZZriZrZTR',
'Jpy8JuBow3o93s',
'NPCAUSRaV9GWWe',
'J532gaxXszOqXl',
'ED73SB37KMBlvc',
'K9nMHIPDVHFtCj',
'HZ9QoltdNk3Vvl',
'NRvQITc6SjjAgW',
'Ig0lZbKakPNKyx',
'FOwnwJRs35rERL',
'KcMRkcfg4MSmZq',
'EKvyLJJ7sqJRfl',
'Mq2rnieJN5P1pC',
'KPCA6H7SbXdyuH',
'NFIVrdjI7DpSqA',
'A2cD5S7atEg9Oj',
'NFl3bHAdIrt15p',
'IAMlTzEE39OeTj',
'Kj2kUOIemrwvz0',
'FD4ln23ZmgU6uG',
'FGd8WBgAox1P4Z',
'NHfjUIWy7rjBMN',
'KaN70Dzklrl65y',
'FxX04NUKPJn2oj',
'LgIUQJ6SPsWvPD',
'NRfHwbHjbbOikh',
'NQs1Or1UJvjTOK',
'GauTGhBWzaR7fd',
'NTZbDIjNVXWYda',
'HVAS7QdhFxKFcd',
'LlZ4ZIJTnUQf1L',
'MnhpWBJKlWescR',
'C0YKPqVYaYZXNs',
'JJuXrdBN6p6XN2',
'MRzVMnmy3y4xN9',
'8heOQEfqqJ9nsO',
'JGnXqLRqwN795x',
'CYxohuAtWqsKUP',
'FbN6hGm2oiTyMS',
'KHg3gEuOqu84In',
'HUO0PtpXFZoCS0',
'MRVL87R8KP9bk0',
'N3DQf6dXY9utIM',
'KI9AHchMAn8yDP',
'JGtT9UftgZ8Yg1',
'JraSUB0Z3WdFVu',
'K3xH68kMevHWWS',
'NVEbkycTyys2Ax',
'MgrhkXhPjMuvTH',
'IuetDVDnWJTEy2',
'MRVkBlEjc0LSLk',
'NSWwH34r7gqiGA',
'IC1VHWq9ZnGttg',
'AcaKecjwqkO2At',
'JLsV4He5XPZtmG',
'O5gQTmb3JFLfi8',
'FntA6kCJFotJkB',
'DqG0dbfsIBqCcm',
'NZt5rnLt0epHNb',
'ItCGG1mNhjSczM',
'Cq6bxbykXwkqN0',
'N9sGWlo5yGucZB',
'GJ81glEuXt9JWq',
'IQZDhfQvOhAdlM',
'MgXH7otDGpVu02',
'NPzN4Nzy3DaXuF',
'NEVhoQYszs0VnD',
'DCcqdtpJn0ICWM',
'KkBnsHri5PKjm7',
'J9iWylMotnLb9d',
'CQwwlh9oa7Y8Nb',
'NaJ0WzcbH3awlU',
'NiC6BTuokSbBuL',
'NI5HBe7I0o5Ivf',
'GO2zp5qHbglNa3',
'O63JhOOHnMuhnR',
'N2oGWeQXwfOMMN',
'LrniKujH8jlBY5',
'MdlMocj4pdedJt',
'LY8TIQOiqn4WOM',
'HcjOn4OcvYb3BF',
'LWZCEvqWHy3CXv',
'GcxnanEcoVPDcA',
'JezfSQ0Cn4PMn8',
'CdjFXPOOxdqhFY',
'Ihxx32pdofjFCJ',
'LqpVVB32hgyfqo',
'NYk63zrYDSF89D',
'NhuXuSl2wxOn0R',
'FGf2q0akXk9h6N',
'NEHiz3KdavPRCF',
'Gk7LBwydd7KZMR',
'O5XXcVzVE30tFi',
'N3uFtHCvLdSPSA',
'J0YfPfvfLpwbzp',
'NYQaDpptgixrZ2',
'GIWWUkv9rbyUb7',
'NbUgOFJ9buMdQf',
'KO7IoAHtKwd6bO',
'K3eOgu2L1PXwmF',
'Llsn2lmoHtPBOi',
'M4yaP62XVhBc8z',
'JmVmXf918pNUG6',
'NlP3uf1ZU6WwWK',
'I0WweY4WFv3bSZ',
'Fc0yDPuPGLNYwz',
'JnzExRNcbjXjjz',
'KHm2rWUFdeKakp',
'NZs1jqmgJdFHRm',
'NnQu4JduzXgm87',
'LcQCFNkf8Y4XD4',
'MYAAR5fRcEJiqn',
'MI0XxhHyofh3mP',
'KTdpFLBeaSah8v',
'O0qEABD6x28flD',
'O4Sac6Z3FSHmKH',
'Feqrn6hUvXovT6',
'Mfo7VN22J1m4qg',
'O2nVveeVHmNmoF',
'JI0M4J8UgBrh0i',
'G1lCHflrrUy1nk',
'NS0kRPH0T4mPYb',
'JtayA4JJu7a5wg',
'EHkgVPavUPHkxy',
'GWc9dopniGX60B',
'NEuyiU0OaEQu9E',
'M9jxnFJk70Q14p',
'MTr0iXqNKxVlnK',
'NyVu90LzoJJ8KV',
'NStoG9Ey7Nbgnv',
'O5YVBeKAOz8Joq',
'LSXFAgbMAmE8PY',
'Jsl5tkzF3M4IfZ',
'CrY42hS5MgeZN8',
'KFSYmRrUgA0LEl',
'KdqBOfF2MWZxE3',
'LelXiF8R02nFiB',
'Nwsm3uVpEXZYBV',
'7uRqOkF3kAG2U5',
'NvGdABx3ZgTLoR',
'I7wak2JrFtBPpP',
'NJmuDy921ZuHhK',
'NypDfk6lv2fXOe',
'Ne06aMeCFdxr1e',
'MwnmBVDVpxtN3j',
'NPem1jlHvfMnXL',
'IIH9Z4VN6LQmbY',
'M4Sh0Q25W01Qpq',
'Nw0l1iRDaVji3R',
'N76XKBTZnb10IQ',
'HXXgaQK2SHZFAR',
'F3dDgZfneRXhUs',
'FBYCwRG79DiUvo',
'JTpcUxPbx6bKhe',
'NJGnnEUOg7BqQu',
'F3GE0VSxKde1Kf',
'GP4xKOnykfL8mM',
'FxnrUv86RwDmDi',
'NHnL91LWWSAHSM',
'Ns6FxFgsAC0TK1',
'MCnOcrpJWQ2NOV',
'NEa0dJv5r7qCfS',
'O1yNK0QnVngiXZ',
'F0logvgmEixA1v',
'AIOzWQ3yUI3WlK',
'O7WDa6F7DVpgag',
'Moz6xKvXbtiAMw',
'LtnpREALM9H9Uz',
'Jcij61E37DW0Sg',
'MkCpq9pxBPqe3m',
'LUE4RcGxmgvjGK',
'Ly6tyNr7ffwMvw',
'CxVqWKrBI2PQ2q',
'NhaWlsocHc0Tx7',
'Kif0Vv9MkZLZ8G'
)
and library <> 'magic-x'
        and (is_magic_x <> 1 or is_magic_x is NULL) 

    group by 1,2,3

    """.format(
        mid,
        Pre_Period_Starting_Date,
        Pre_Period_Ending_Date,
        Post_Period_Starting_Date,
        Post_Period_Ending_Date,
    )
)

base_table = base_db.toPandas()
base_table.head()

# COMMAND ----------

#Making a copy 
base_temp = base_table.copy(deep=True)
base_temp.head()

# COMMAND ----------

#base_temp.to_csv('/dbfs/FileStore/cr_rca_sample_data.csv', index=False)
"https://razorpay-dev.cloud.databricks.com/files/cr_rca_sample_data.csv"

# COMMAND ----------

def get_percentage_share(table, col_name):
    col_num = table.columns.get_loc(col_name)
    print(col_num)
    
    

    if col_num == 0:
        table_grouped_curr = table.groupby(by=table.columns[col_num]).agg({'openPost':'sum','openPrev':'sum','submitPost':'sum','submitPrev':'sum'}).reset_index()
        print(table_grouped_curr.columns)
        #table_grouped_curr.iloc[:,-4:] = table_grouped_curr.iloc[:,-4:].add_suffix(('_'+col_name))
        #print(table_grouped_curr.columns)
        table_final = table.merge(table_grouped_curr, how='left', on=table.columns[col_num], suffixes=('', ('_'+col_name)))
        print(table_final.columns)
        for j in ['openPost','openPrev']:
            table_final[(j+'_'+col_name+'_share')] = table_final[(j+'_'+col_name)] / table[j].sum()
        
    else:
        print(table.columns[0:col_num+1])
        table_grouped_curr = table.groupby(by=list(table.columns[0:col_num+1])).agg({'openPost':'sum','openPrev':'sum','submitPost':'sum','submitPrev':'sum'}).reset_index()
        print(table_grouped_curr.columns)
        table_final = table.merge(table_grouped_curr, how='left', on=list(table.columns[:col_num+1]), suffixes=('', ('_'+col_name)))
        print(table_final.columns)
        prev_col_name = table.columns[col_num-1]
        print(prev_col_name)
        for j in ['openPost','openPrev']:
            table_final[(j+'_'+col_name+'_share')] = table_final[(j+'_'+col_name)] / table_final[(j+'_'+prev_col_name)]
        print(table_final.columns)
   

    return table_final

# COMMAND ----------

base_table.dtypes

# COMMAND ----------

'''
base_temp.loc[base_temp['email_optional'].isna(),'email_optional']=False
base_temp = base_temp.groupby(by=['mnth', 'merchant_id', 'browser_name', 'os', 'aov',
        'email_optional','summary_screen_continue_cta_clicked',
       'payment_home_screen_loaded']).agg({'open':'sum','submit':'sum',}).reset_index()
base_temp.shape
'''

# COMMAND ----------

base_temp.shape

# COMMAND ----------

base_temp

# COMMAND ----------

#initial data manipulation
base_pivot = base_temp.pivot(columns='mnth', index=base_temp.columns[1:-2], values=['open','submit']).reset_index()
base_df = base_pivot.fillna(0)
base_df.columns = [''.join(col).strip() for col in base_df.columns.values]
base_df['Post_CR'] = base_df['submitPost']*1.0/ base_df['openPost']
base_df['Pre_CR'] = base_df['submitPrev']*1.0 / base_df['openPrev']
base_df.head()

# COMMAND ----------

n = base_df.shape[1] - 6
for i in base_df.iloc[:,:n].columns:
    base_df = get_percentage_share(base_df,i)
    print(base_df)



# COMMAND ----------

base_df = base_df.fillna(0)
base_df.head()

# COMMAND ----------

#Fixing the Pre share=0 error 
open_prev_share_cols = [col for col in base_df.columns if 'openPrev' in col and 'share' in col]
open_post_share_cols = [col for col in base_df.columns if 'openPost' in col and 'share' in col]
for i in range(n):
    print(open_prev_share_cols[i])
    #print(base_df.loc[400,'openPost_browser_name'])
    condition = (base_df[open_prev_share_cols[i]] == 0)

    for j in range(i+1,n):
        print('hello second loop')
        print(open_prev_share_cols[j])
        base_df.loc[condition, open_prev_share_cols[j]] = base_df.loc[condition, open_post_share_cols[j]]
        #print(base_df.loc[400,'openPrev_browser_name'])
base_df.head()




# COMMAND ----------

#Fixing where Pre CR is inf
base_df['Pre_CR'] = base_df['Pre_CR'].replace([float('inf'), float('-inf')], 0)
base_df['Post_CR'] = base_df['Post_CR'].replace([float('inf'), float('-inf')], 0)


# COMMAND ----------



# COMMAND ----------

base_df[open_prev_share_cols[0+1:]].prod(axis=1)

# COMMAND ----------

#Adding calculation columns
open_prev_total = base_df['openPrev'].sum()
open_post_total = base_df['openPost'].sum()
prev_cr = (base_df['submitPrev'].sum()) * 1.0 / open_prev_total
post_cr = (base_df['submitPost'].sum()) * 1.0 / open_post_total
base_df['volume_share_Prev'] = base_df['openPrev']*1.0 / open_prev_total
base_df['volume_share_Post'] = base_df['openPost']*1.0 / open_post_total
base_df['initial_delta'] = base_df['Pre_CR'] * (base_df['openPrev']*1.0/open_prev_total)

#Required new columns
open_mix_column_names = [col + '_open_mix' for col in base_df.columns[:n]]
submit_mix_column_names = [col + '_submit_mix' for col in base_df.columns[:n]]
cr_mix_column_names = [col + '_cr_mix' for col in base_df.columns[:n]]
delta_mix_column_names = [col + '_delta_mix' for col in base_df.columns[:n]]
conv_impact_mix_column_names = [col + '_conv_impact_mix' for col in base_df.columns[:n]]

for i in range(n):
    base_df[open_mix_column_names[i]] = open_prev_total * (base_df[open_post_share_cols[:i+1]].prod(axis=1)) * (base_df[open_prev_share_cols[i+1:]].prod(axis=1))
    base_df[submit_mix_column_names[i]] = base_df[open_mix_column_names[i]] * base_df['Pre_CR'] 
    base_df[cr_mix_column_names[i]] = base_df[submit_mix_column_names[i]]*1.00/base_df[open_mix_column_names[i]]
    base_df[cr_mix_column_names[i]] = base_df[cr_mix_column_names[i]].replace([float('inf'), float('-inf')], 0)

        
    # base_df[delta_mix_column_names[i]] = (base_df[cr_mix_column_names[i]] * base_df[open_mix_column_names[i]]*1.00)/open_prev_total
    base_df[delta_mix_column_names[i]] = (base_df[submit_mix_column_names[i]]*1.00)/open_prev_total
    if i ==0:
        base_df[conv_impact_mix_column_names[i]] = base_df[delta_mix_column_names[i]] - base_df['initial_delta']
    else:
        base_df[conv_impact_mix_column_names[i]] = base_df[delta_mix_column_names[i]] - base_df[delta_mix_column_names[i-1]]
base_df.head()




# COMMAND ----------

base_df.to_csv('/dbfs/FileStore/cr_rca_sample_data.csv', index=False)
"https://razorpay-dev.cloud.databricks.com/files/cr_rca_sample_data.csv"

# COMMAND ----------


base_df['post_mix_normalized_opens'] = base_df[open_mix_column_names[-1]] #A
base_df['post_mix_normalized_submits'] = base_df['post_mix_normalized_opens'] * base_df['Post_CR'] #B
#C is Post CR == B/A
base_df['delta'] = base_df['Post_CR'] - base_df['Pre_CR'] #D
base_df['conversion_impact'] = (base_df['delta'] * base_df['post_mix_normalized_opens'])/open_prev_total
base_df.head()


# COMMAND ----------

base_df['conversion_impact'].sum()

# COMMAND ----------

#Summarizing Impact

feature_list = ['Pre_CR']
CR_list = [prev_cr]

#summary_df = summary_df.append({"Feature":'Pre_CR',"CR":prev_cr, "Impact":None}, ignore_index=True)
for i in range(n):
    feature_list.append(base_df.columns[i])
    CR_list.append(base_df[submit_mix_column_names[i]].sum()*1.0 / base_df[open_mix_column_names[i]].sum())
    #impact_list.append(CR_list[i+1] - CR_list[i])

feature_list.append('Post CR')
CR_list.append(post_cr)

print(feature_list)
print(CR_list)



#summary_df.head()

# COMMAND ----------

summary_df = pd.DataFrame({"Feature":feature_list,"CR":CR_list,})
summary_df['impact'] = summary_df['CR'].diff()
summary_df.loc[summary_df['Feature']=='Pre_CR','impact'] = prev_cr
summary_df

# COMMAND ----------

# MAGIC %md
# MAGIC HTML CODE BEGINS HERE
# MAGIC

# COMMAND ----------


data=[
    ['Pre Period CR','CR in the initial period','Pre Period CR' ],
    ['merchant_id','Change in the mix of merchants. Share of high CR merchants increasing will result in a positive change','Merchant Mix' ],
    ['browser_name','Change in mix of browsers i.e. given the users do not do anything differently but just volume shifts on browsers','Browser Mix' ],
    ['os','Change in mix of OS i.e. given the users do not do anything differently but just volume shifts on OS within browsers', 'OS Mix'],
    ['aov','Change in transaction values. Typically higher transaction values correlate with lower conversion, and lower transaction values with higher correlation', 'Transaction Value Mix'],
    ['initial_loggedin','Change in share of users who were pre-logged in on Magic', 'Pre Logged-In Percentage Mix'],
    ['initial_hassavedaddress','Change in share of users who were shown their addresses on Summary Screen', 'Pre-filled Address Mix'],
    ['prefill_contact_number','Change in share of users who had their contact number prefilled upon arriving on Magic', 'Contact Prefill Mix'],
    ['summary_screen_paths','Change in share of users based on how they interacted with different components of Summary Screen ', 'Summary Screen Path Mix'],
    ['Conversion impact','The actual conversion change after normalizing for all the mix changes listed above','True Conversion impact',],
    ]
feature_description = pd.DataFrame(data, columns=['Feature', 'Description','Feature Name'])
feature_description

# COMMAND ----------

# DBTITLE 1,HTML Supporting Functions
def color_negative_red(val):
    """
    Converts value to a string with HTML color formatting
    based on its sign.
    """
    color = 'red' if val < 0 else 'green'
    return f'<font color="{color}">{val:.2%}</font>'

def percentage_conversion(value):
    return f'{value:.2%}'

def list_to_string(data):
  """
  Converts a list to a string, handling single-element lists.

  Args:
      data: A list of elements.

  Returns:
      A string representation of the list, with elements joined by 'x'.
  """
  if len(data) == 1:
    return data[0]  # Return the single element directly
  else:
    return ' x '.join(data)  # Join elements with 'x'

def max_feature_impact(conversion_impact):
    if conversion_impact < 0:
        print(min(summary_df.iloc[1:-1]['impact']))
        print(np.argmin(summary_df.iloc[1:-1]['impact']))
        return summary_df.loc[np.argmin(summary_df.iloc[1:-1]['impact'])+1,'Feature']
    elif conversion_impact > 0:
        return summary_df.loc[np.argmax(summary_df.iloc[1:-1]['impact'])+1,'Feature']
    else:
        return None

# COMMAND ----------

np.min(summary_df.iloc[1:-1]['impact'])
#summary_df.iloc[1:-1]['impact']

# COMMAND ----------


cr_diff = post_cr - prev_cr
html_code = """
<html>
<head>
    <title>RCA for {0} </title>
    <style>
    td, th {{
  border: 1px solid #ddd;
  padding: 6px;
  text-align: left;  }}

    th {{
    background-color:  #0b5394;
    color: white; 
  }}
  </style>
</head>
<body>
<b>Pre Period:</b> {1} to {2}<br>
 <b>Post Period: </b>{3} to {4}<br>
""".format(
        mid,
           datetime.strptime(Pre_Period_Starting_Date, '%Y-%m-%d').strftime('%b %d, %Y'),
           datetime.strptime(Pre_Period_Ending_Date, '%Y-%m-%d').strftime('%b %d, %Y'),
           datetime.strptime(Post_Period_Starting_Date, '%Y-%m-%d').strftime('%b %d, %Y'),
           datetime.strptime(Post_Period_Ending_Date, '%Y-%m-%d').strftime('%b %d, %Y')
           )
html_code += """
<b>Pre Summary Screen CR: </b> {0}<br>
<b>Post Summary Screen CR:</b> {1}<br>
<b>Summary Screen CR changed by:</b>  {2}
<br><br>

""".format(percentage_conversion(prev_cr), percentage_conversion(post_cr),color_negative_red(cr_diff))
print(html_code)

# COMMAND ----------

'''
— change Post Cr in funnel to “Conversion Impact”
— summary_screen_dropoffs -> summary screen paths
— remove running_total_conversion_impact, abs conversation impact
— add line to explain “these three lenses are explaining summary cr max”
— make rounding off abs impact consistent across all tables
— don’t show the two tables twice
— add insight about where the max impact is coming in every table
'''

# COMMAND ----------

html_code += """
</br></br>
Here is the overall funnel. The following represents the volume impact of various dimensions:
</br></br>
"""
volume_impact = percentage_conversion(summary_df[1:-1]['impact'].sum())
converstion_impact = percentage_conversion(round(summary_df.iloc[-1:]['impact'],4).sum())
volume_impact_feature = max_feature_impact(summary_df[1:-1]['impact'].sum())

temp_df = summary_df.copy()
temp_df.loc[temp_df.index[0], 'Feature'] = 'Pre Period CR'
temp_df.loc[temp_df.index[-1], 'Feature'] = 'Conversion impact'
temp_df['impact'] = temp_df['impact'].apply(color_negative_red)
temp_df = temp_df.merge(feature_description, how='left', on='Feature')
#temp_df['impact'] = temp_df['impact'].apply(percentage_conversion)
html_code += temp_df[['Feature','impact','Description']].to_html(index=False,escape=False,  )
html_code += """
<br>
<mark><b>Total volume impact:</b> {0} </mark> <br>
<b> Max Volume Impact came from:</b> {2} </br>
""".format(volume_impact, converstion_impact, volume_impact_feature)


#Explaining volume impact
volume_impact_feature_conv_impact_col = ''.join([volume_impact_feature,'_conv_impact_mix'])
html_code +="""<p> <b>Volume Impact Breakdown:</b> Tells how much impact to CR was caused by shift in  <br>"""
volume_impact_df = base_df.groupby(by=[volume_impact_feature]).agg(
    {volume_impact_feature_conv_impact_col:'sum',
    'volume_share_Prev':'sum',
    'volume_share_Post':'sum'},
    ).reset_index()
volume_impact_df['abs'] = abs(volume_impact_df[volume_impact_feature_conv_impact_col])
volume_impact_df = volume_impact_df.sort_values(by='abs', ascending=False)
volume_impact_df[volume_impact_feature_conv_impact_col] = volume_impact_df[volume_impact_feature_conv_impact_col].apply(color_negative_red)
volume_impact_df['volume_share_Prev'] = volume_impact_df['volume_share_Prev'].apply(percentage_conversion)
volume_impact_df['volume_share_Post'] = volume_impact_df['volume_share_Post'].apply(percentage_conversion)
volume_impact_df = volume_impact_df.rename(columns={
    'volume_share_Prev':'Pre Volume %',
    'volume_share_Post':'Post Volume %',
    volume_impact_feature_conv_impact_col:'Volume Impact',})
#[[volume_impact_feature,'Volume Impact','Pre Volume %','Post Volume %',]]
                            
html_code += volume_impact_df[[volume_impact_feature,'Pre Volume %','Post Volume %','Volume Impact',]].to_html(index=False,escape=False,  )
html_code +="""</p>"""
html_code += """<p> <mark><b>Total Pure Conversion Impact:</b> {0}</mark></p>""".format(converstion_impact)
print(html_code)


# COMMAND ----------

volume_impact_df = base_df.groupby(by=['aov']).agg(
    {'aov_conv_impact_mix':'sum',
    'volume_share_Prev':'sum',
    'volume_share_Post':'sum'},
    ).reset_index()
volume_impact_df['abs'] = abs(volume_impact_df['aov_conv_impact_mix'])
volume_impact_df = volume_impact_df.sort_values(by='abs', ascending=False)
volume_impact_df

# COMMAND ----------

# volume_impact_df = base_df.groupby(by=['browser_name','os']).agg(
#     {'os_conv_impact_mix':'sum',
#      'browser_name_conv_impact_mix':'sum',
#     'volume_share_Prev':'sum',
#     'volume_share_Post':'sum'},
#     ).reset_index()
# #volume_impact_df['abs'] = abs(volume_impact_df['browser_name_conv_impact_mix'])
# volume_impact_df['volume_delta'] =volume_impact_df['volume_share_Post'] - volume_impact_df['volume_share_Prev'] 
# volume_impact_df['abs_volume_delta'] = abs(volume_impact_df['volume_delta'])
# volume_impact_df.sort_values(by='abs_volume_delta', ascending=False)

# COMMAND ----------

base_df.sort_values(by='conversion_impact', ascending=True)

# COMMAND ----------

for i in range(len(open_mix_column_names)):
    try:
        if not round(base_df[open_mix_column_names[i]].sum(),0) == open_prev_total:
            raise ValueError(f"Total Prev Opens do not match for {open_mix_column_names[i]}")
    except ValueError as e:
        print(f"Error: {e}")
        # Add any additional error handling or logging here
        break 


# COMMAND ----------

#base_df.to_csv('/dbfs/FileStore/cr_rca_agg_sample_data.csv', index=False)
#"https://razorpay-dev.cloud.databricks.com/files/cr_rca_agg_sample_data.csv"

# COMMAND ----------

n

# COMMAND ----------

columns = base_df.columns[0:n]  # Exclude the 'impact' column and #merchant Id column for now
combinations_list = [comb for i in range(1, len(columns) + 1) for comb in combinations(columns, i)]
combinations_list

# COMMAND ----------

base_df['abs_conversion_impact'] = abs(base_df['conversion_impact'])
base_df.head()

# COMMAND ----------

feature_combination = []
rows_required_for_explaination = []
total_feature_rows = []
min_impact_explained = summary_df['impact'].iloc[-1] * 0.8
for i in range(len(combinations_list)-1):
    feature_combination.append(combinations_list[i])
    temp_df = base_df.groupby(list(combinations_list[i])).agg({'conversion_impact':'sum'}).reset_index()
    total_feature_rows.append(temp_df.shape[0])
    temp_df['abs_conversion_impact'] = abs(temp_df['conversion_impact'])
    temp_df = temp_df.sort_values(by='abs_conversion_impact', ascending=False)
    temp_df['running_total_conversion_impact'] = temp_df['conversion_impact'].cumsum()
    rows_required = temp_df[temp_df['running_total_conversion_impact'] <= min_impact_explained].shape[0]
    rows_required_for_explaination.append(rows_required)

impact_consolidated_df = pd.DataFrame({"Feature":feature_combination,"combinations_required":rows_required_for_explaination,"total_combinations":total_feature_rows})
impact_consolidated_df['percentage_reqd'] = impact_consolidated_df['combinations_required']*1.0 / impact_consolidated_df['total_combinations']
impact_consolidated_df = impact_consolidated_df.sort_values(by='percentage_reqd')

# COMMAND ----------

impact_consolidated_df['weightage'] = impact_consolidated_df['combinations_required'] * impact_consolidated_df['percentage_reqd']
impact_consolidated_df = impact_consolidated_df.sort_values(by=['weightage'])
impact_consolidated_filtered_df = impact_consolidated_df[
    (impact_consolidated_df['combinations_required'] != 0)
    & (impact_consolidated_df['combinations_required'] != 1)
    & (impact_consolidated_df['percentage_reqd'] != 1)
]
impact_consolidated_filtered_df.head(20)
#impact_consolidated_filtered_df = impact_consolidated_df ##### REMOVIE IT


# COMMAND ----------

temp_df = base_df.groupby(['aov']).agg({'conversion_impact':'sum'}).reset_index()
temp_df['abs_conversion_impact'] = abs(temp_df['conversion_impact'])
temp_df = temp_df.sort_values(by='abs_conversion_impact', ascending=False)
temp_df.drop(columns='abs_conversion_impact')

# COMMAND ----------

#List to maintain that a feature has already not been added to the email to remove redundancy
de_duplication_list = []

# COMMAND ----------

def generate_combination_impact(combination_index):
    min_impact_explained = summary_df['impact'].iloc[-1] * 0.8
    #temp_df = base_df.groupby(list(['browser_name', 'os', 'initial_loggedin', 'initial_hassavedaddress'])).agg({'conversion_impact':'sum'}).reset_index()
    temp_df = base_df.groupby(list(combinations_list[combination_index])).agg({'conversion_impact':'sum'}).reset_index()
    temp_df['abs_conversion_impact'] = abs(temp_df['conversion_impact'])
    temp_df = temp_df.sort_values(by='abs_conversion_impact', ascending=False)
    #temp_df['running_total_conversion_impact'] = temp_df['conversion_impact'].cumsum()
    return temp_df.drop(columns='abs_conversion_impact')


# COMMAND ----------

def get_feature_summary(combination_index, combination_impact_df,html_code):
    
    #grouped_rows = []
    combo_length = len(list(combinations_list[combination_index]))

    for i in range(combo_length):
        col_name = list(combinations_list[combination_index])[i]
        print('col name')
        print([col_name])

        if [col_name] in de_duplication_list:
            print('rejected in second loop:')
            print(col_name)
            continue
        else:
            print('accepted in second loop:')
            print(col_name)
            de_duplication_list.append([col_name])


        html_code+="""</br></br>"""
        html_code+="""<p><b>{0}</b>""".format(list_to_string([col_name]))

        grouped_df = combination_impact_df.groupby(col_name)['conversion_impact',].sum().reset_index()
        grouped_df['abs_conversion_impact'] = abs(grouped_df['conversion_impact'] )

    
        # Convert the grouped result to a DataFrame with column names
        #grouped_df = pd.DataFrame(grouped).reset_index()
    
        # Rename the columns
        #grouped_df.columns = ['Categories', 'conversion_impact'] --- TEMPORARY
    
        # Round off the values to 4 decimal places
        #grouped_df['conversion_impact'] = grouped_df['conversion_impact'].apply(percentage_conversion)
        grouped_df['conversion_impact'] = grouped_df['conversion_impact'].apply(color_negative_red)
        #grouped_df['running_total_conversion_impact'] = grouped_df['conversion_impact'].cumsum()
        grouped_df = grouped_df.sort_values(by='abs_conversion_impact', ascending=False)
        grouped_df = grouped_df.drop(columns=['abs_conversion_impact'], axis=1)
      
        html_code+=grouped_df.to_html(index=False, escape=False, )
        html_code+="""</p>"""

    
        # Append the grouped DataFrame to the list
        #grouped_rows.append(grouped_df)

    # Concatenate the list of DataFrames into a single DataFrame
    #grouped_table = pd.concat(grouped_rows, ignore_index=True)

    # Print the DataFrame
    #return(grouped_table.sort_values(by='abs_conversion_impact', ascending=False))
    return(html_code)


# COMMAND ----------

# DBTITLE 1,Adding the contributing factors to the conversion impact
html_code+="""<p>
<br>The impact due to conversion change was further analysed across different features and all possible combinations of features. op 3 features or combination of features explaining the conversion impact best are found to be:
<br>
"""

# COMMAND ----------

num_factors = 3
#For adding the summary note
html_code += """<ol>"""
for index, row in impact_consolidated_filtered_df.head(num_factors).iterrows():
    html_code += """<li>{0}</li>""".format(list_to_string(list(combinations_list[index])))
html_code += """</ol></p>"""

#For adding the detailed tables
for index, row in impact_consolidated_filtered_df.head(num_factors).iterrows():
    
    
    if list(combinations_list[index]) in de_duplication_list:
        print('rejected:')
        print(combinations_list[index])
        continue
    else:
        print('accepted:')
        print(combinations_list[index])
        de_duplication_list.append(list(combinations_list[index]))

    # Printing to the email
    html_code += """<li><p> <ol> <b>{0} </b>""".format(list_to_string(list(combinations_list[index])))

    combination_impact_df = generate_combination_impact(index)
    
    combo_length = len(list(combinations_list[index]))
    if combo_length != 1:
        html_code = get_feature_summary(index, combination_impact_df, html_code)

    html_code += """</br></br>"""  
    combination_impact_df['conversion_impact'] = combination_impact_df['conversion_impact'].apply(color_negative_red)
    html_code += combination_impact_df.to_html(index=False, escape=False, )
    html_code += """</br></br></p>"""  
    html_code += """</li>"""

html_code += """</ol></br></body></html>"""

# COMMAND ----------

de_duplication_list

# COMMAND ----------

html_code


# COMMAND ----------

# DBTITLE 1,Email credentials Required
user = 'pallavi.samodia@razorpay.com'
app_password = 'sibbzwuopwzqbzst' # Guide for app passwords: https://support.google.com/accounts/answer/185833?hl=en
host = 'smtp.gmail.com'
port = 465
server = smtplib.SMTP_SSL(host, port) 
server.login(user, app_password)

# COMMAND ----------

# DBTITLE 1,Function to Send the Email
def send_emails(mid,email_id,html_t):
    today = datetime.now().strftime('%Y-%m-%d')
    subject = 'RCA for {0} | Date: {1}'.format(mid, today)
    to = email_id
    message = MIMEMultipart()
    # add From 
    message['From'] = Header(user)
    # add To
    message['To'] = Header(to)
    # add Subject
    message['Subject'] = Header(subject)
    # add content text
    message.attach(MIMEText(html_t, 'html', 'utf-8'))
    ### Send email ###
    server.sendmail(user, to, message.as_string()) 
    print('Sent email successfully for: ',email_id)





# COMMAND ----------

# DBTITLE 1,Calling the email function
email_id = ['manjeet.singh@razorpay.com','pallavi.samodia@razorpay.com','chetna.handa@razorpay.com']
for i in email_id:
    send_emails(mid,i,html_code)

# COMMAND ----------

server.quit()

# COMMAND ----------

temp_df

# COMMAND ----------

unique_nodes

# COMMAND ----------

#Sankey Diagrams

unique_nodes = pd.unique(temp_df[['prefill_contact_number', 'summary_screen_dropoffs' ]].values.ravel('K'))
node_mapping = {label: idx for idx, label in enumerate(unique_nodes)}

temp_df['SourceIndex'] = temp_df['prefill_contact_number'].map(node_mapping)
temp_df['TargetIndex'] = temp_df['summary_screen_dropoffs'].map(node_mapping)

node_colors = []
for i in unique_nodes:
  impact = temp_df[i]['conversion_impact'].sum()
  if impact > 0:
      node_colors.append('green')
  else:
      node_colors.append('red')
      

fig = go.Figure(data=[go.Sankey(
    node = dict(
      pad = 15,
      thickness = 20,
      line = dict(color = "black", width = 0.5),
      label = unique_nodes,
      color = node_colors
    ),
    link = dict(
      source = temp_df['SourceIndex'], # indices correspond to labels, eg A1, A2, A1, B1, ...
      target = temp_df['TargetIndex'],
      value = temp_df['abs_conversion_impact'],
       label = temp_df['conversion_impact']
  ))])

fig.update_layout(title_text="Basic Sankey Diagram", font_size=10,
                    xaxis=dict(title="prefill_contact_number"),
    xaxis2=dict(title="summary_screen_dropoffs")
                  )
fig.show()

# COMMAND ----------



# COMMAND ----------

temp_df.head(10)

# COMMAND ----------

base_df[(base_df['browser_name']=='Instagram') &
 (base_df['os']=='Android') &
 (base_df['initial_loggedin']==False) &
  (base_df['initial_hassavedaddress' ]==False) 
 #& (base_df['summary_screen_dropoffs']=='Interacted w contact but not coupons'  )
 ].sort_values(by='abs_conversion_impact', ascending=False).head(20)

# COMMAND ----------

base_df[(base_df['browser_name']=='Instagram') &
 (base_df['os']=='Android') &
 (base_df['initial_loggedin']==False) &
  (base_df['initial_hassavedaddress' ]==False) 
 #& (base_df['summary_screen_dropoffs']=='Interacted w contact but not coupons'  )
 ].sort_values(by='abs_conversion_impact', ascending=False).head(20)

# COMMAND ----------



# COMMAND ----------

impact_consolidated_df.to_csv('/dbfs/FileStore/impact_consolidated_df.csv', index=False)
"https://razorpay-dev.cloud.databricks.com/files/impact_consolidated_df.csv"

# COMMAND ----------

# DBTITLE 1,Calculating Delta
base_df['abs_conversion_impact'] = abs(base_df['conversion_impact'])
base_df.sort_values(by='abs_conversion_impact', ascending=False).head(20)
#(base_df.sort_values(by='conversion_impact', ascending=False)).to_csv('/dbfs/FileStore/jan_apollo_abs_conversion_impact.csv', index=False)
#"https://razorpay-dev.cloud.databricks.com/files/jan_apollo_abs_conversion_impact.csv"

# COMMAND ----------

#(base_df.sort_values(by='conversion_impact', ascending=False).head(20)).to_csv('/dbfs/FileStore/jan_apollo_conversion_impact.csv', index=False)
"https://razorpay-dev.cloud.databricks.com/files/jan_apollo_abs_conversion_impact.csv"

# COMMAND ----------

#(base_df.sort_values(by='abs_conversion_impact', ascending=False).head(20))

# COMMAND ----------

# To do [Feb 9]
# Consolidation:  Consolidating by the various dimensions (and combinations of dimensions)
# Summary CTA flow
# To do [Feb 12]
# Add total rows for every combination and divide the percentage to get which combo explains with min

# COMMAND ----------


