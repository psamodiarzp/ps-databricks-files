# Databricks notebook source
import pandas as pd



# COMMAND ----------

eligible_merchants_db = sqlContext.sql(
    """
    SELECT
    distinct merchant_key as merchant_key_raw
    from aggregate_pa.cx_lo_fact_ism_v1
where producer_created_date BETWEEN '2024-06-01' AND '2024-06-30'
and get_json_object(render_properties,'$.data.meta.v2_eligible') = 'true'
    """)

eligible_merchants_df = eligible_merchants_db.toPandas()
eligible_merchants_df['merchant_key'] = eligible_merchants_df['merchant_key_raw'].str.rsplit('_', n=1).str.get(-1)
eligible_merchants_df['eligible'] ='true'
eligible_merchants_df.head()

# COMMAND ----------

#Updating the window from June to June 1 - July 15 so if any merchants are eligible now, we can ramp up on them
eligible_merchants_db = sqlContext.sql(
    """
    SELECT
    DISTINCT
   --- merchant_key as merchant_key_raw,
    ---order_id,
    a.merchant_id
    from aggregate_pa.cx_1cc_events_dump_v1 a 
   -- left join realtime_hudi_api.orders b on a.order_id = b.id
where producer_created_date BETWEEN date('2024-06-01') AND date('2024-07-15')
--and b.created_date BETWEEN '2024-06-01' AND '2024-07-15'
and get_json_object(properties,'$.data.meta.v2_eligible') = 'true'
    """)

eligible_merchants_df = eligible_merchants_db.toPandas()
#eligible_merchants_df['merchant_key'] = eligible_merchants_df['merchant_key_raw'].str.rsplit('_', n=1).str.get(-1)
eligible_merchants_df['eligible'] ='true'
eligible_merchants_df.head()

# COMMAND ----------


print('Null merchant_key' , eligible_merchants_df[eligible_merchants_df['merchant_key'].isnull()].shape)
print('Null eligible merchant_id' , eligible_merchants_df[eligible_merchants_df['merchant_id'].isnull()].shape)
print('Total eligible merchant_id' , eligible_merchants_df['merchant_id'].nunique())
eligible_merchants_df.shape

# COMMAND ----------

ineligible_merchants_db = sqlContext.sql(
    """
    SELECT
    DISTINCT
    
    merchant_key as merchant_key_raw,
    b.merchant_id
    ---order_id,
    
    from aggregate_pa.cx_1cc_events_dump_v1 a 
    left join realtime_hudi_api.orders b on a.order_id = b.id
where producer_created_date BETWEEN '2024-06-01' AND '2024-06-30'
and b.created_date BETWEEN '2024-06-01' AND '2024-06-30'
and get_json_object(properties,'$.data.meta.v2_eligible') = 'false'
    """)

ineligible_merchants_df = ineligible_merchants_db.toPandas()
ineligible_merchants_df['merchant_key'] = ineligible_merchants_df['merchant_key_raw'].str.rsplit('_', n=1).str.get(-1)
ineligible_merchants_df.head()

# COMMAND ----------

print('Total ineligible merchant_key' , ineligible_merchants_df[ineligible_merchants_df['merchant_key'].isnull()].shape)
print('Total ineligible merchant_id' , ineligible_merchants_df[ineligible_merchants_df['merchant_id'].isnull()].shape)
print('Total ineligible merchant_id' , ineligible_merchants_df['merchant_id'].nunique())
eligible_merchants_df.shape

# COMMAND ----------

ineligible_merchants_df['not_eligible'] = 'true'

# COMMAND ----------

ineligible_merchants_df.to_csv('/dbfs/FileStore/ineligible_merchants_df.csv', index=False)
"https://razorpay-dev.cloud.databricks.com/files/ineligible_merchants_df.csv"

# COMMAND ----------

ineligible_merchants_df['merchant_key_raw'].nunique()

# COMMAND ----------

ineligible_merchants_db = sqlContext.sql(
    """
    SELECT
    distinct  merchant_key as merchant_key_raw
    from aggregate_pa.cx_lo_fact_ism_v1
where producer_created_date >= '2024-06-01'
and get_json_object(render_properties,'$.data.meta.v2_eligible') = 'false'
    """)

ineligible_merchants_df = ineligible_merchants_db.toPandas()
ineligible_merchants_df['merchant_key'] = ineligible_merchants_df['merchant_key_raw'].str.rsplit('_', n=1).str.get(-1)
ineligible_merchants_df.head()

# COMMAND ----------

ineligible_reasons_db = sqlContext.sql(
    """
    SELECT
    
    b.merchant_id,
    get_json_object(render_properties,'$.data.meta.v2_ineligible_reason') as v2_ineligible_reason
    from aggregate_pa.cx_1cc_events_dump_v1 a 
    left join realtime_hudi_api.orders b on a.order_id = b.id
where producer_created_date BETWEEN '2024-06-01' AND '2024-06-30'
and b.created_date BETWEEN '2024-06-01' AND '2024-06-30'
and get_json_object(properties,'$.data.meta.v2_ineligible_reason') is not null 
and get_json_object(properties,'$.data.meta.v2_ineligible_reason') <> ''
GROUP BY 1,2
    """)

ineligible_reasons_df = ineligible_reasons_db.toPandas()
#ineligible_reasons_df['merchant_key'] = ineligible_reasons_df['merchant_key_raw'].str.rsplit('_', n=1).str.get(-1)
ineligible_reasons_df.head()

# COMMAND ----------

ineligible_reasons_cnt_df = ineligible_reasons_df.groupby(by='v2_ineligible_reason').agg({'merchant_id': 'nunique'}).reset_index()
ineligible_reasons_cnt_df.sort_values(by='merchant_id', ascending=False).head(100)

# COMMAND ----------

ineligible_reasons_cnt_df.to_csv('/dbfs/FileStore/ineligible_reasons_cnt_df.csv', index=False)
"https://razorpay-dev.cloud.databricks.com/files/ineligible_reasons_cnt_df.csv"

# COMMAND ----------

ineligible_reasons_df = ineligible_reasons_db.toPandas()
ineligible_reasons_df['merchant_key'] = ineligible_reasons_df['merchant_key_raw'].str.rsplit('_', n=1).str.get(-1)
ineligible_reasons_df.head()

# COMMAND ----------

rampup_master_db =  sqlContext.sql( """
select * from batch_sheets.checkout_v2_rampup_sheet
""")
rampup_master_df = rampup_master_db.toPandas()
rampup_master_df.head()

# COMMAND ----------

print(rampup_master_df.shape)

# COMMAND ----------

rampup_master_df['status'] = 'not live'
rampup_master_df.loc[rampup_master_df['blacklisted_flag'] == 'false', 'status'] = 'live'
rampup_master_df.head()

# COMMAND ----------

# Group and concatenate
concatenated_reasons = ineligible_reasons_df.groupby('merchant_id')['v2_ineligible_reason'].transform(lambda x: ', '.join(x))

# Combine columns into a new DataFrame
ineligible_reasons_grouped_df = pd.DataFrame({'merchant_id': ineligible_reasons_df['merchant_id'], 'concatenated_reasons': concatenated_reasons})

ineligible_reasons_grouped_df.head()


# COMMAND ----------

ineligible_reasons_grouped_df = ineligible_reasons_grouped_df.drop_duplicates(subset=['merchant_id','concatenated_reasons',], keep='first')
ineligible_reasons_grouped_df.shape

# COMMAND ----------

keys_db = sqlContext.sql(
    """
select id, merchant_id from realtime_hudi_api.keys
""")
keys_df = keys_db.toPandas()
keys_df.head()


# COMMAND ----------

ineligible_mid_df = ineligible_merchants_df.merge(keys_df, how='left', left_on='merchant_key', right_on='id')
eligible_merchants_df = eligible_merchants_df.merge(keys_df, how='left', left_on='merchant_key', right_on='id')
eligible_merchants_df.shape

# COMMAND ----------



segment_db = sqlContext.sql(
    """
select merchant_id, team_owner from aggregate_ba.final_team_tagging
""")
segment_df = segment_db.toPandas()
segment_df.head()



# COMMAND ----------

may_std_mtu_db = sqlContext.sql(
    """
select 
distinct
 a.merchant_id  
 from realtime_hudi_api.payments a 
 inner join realtime_hudi_api.payment_analytics b on a.id = b.payment_id


 where a.created_date between '2024-05-01' and '2024-05-31'
 and b.created_date between '2024-05-01' and '2024-05-31'
 and library in (1,4)
 and (method='cod' or authorized_at is not null)
 """)
may_std_mtu_df = may_std_mtu_db.toPandas()
may_std_mtu_df.head()

# COMMAND ----------

may_std_mtu_df = pd.read_csv('/dbfs/FileStore/may_std_mtu_df.csv')
may_std_mtu_df.head()

# COMMAND ----------

june_std_mtu_db = sqlContext.sql(
    """
select 
distinct
 a.merchant_id  
 from realtime_hudi_api.payments a 
 inner join realtime_hudi_api.payment_analytics b on a.id = b.payment_id


 where a.created_date between '2024-06-01' and '2024-06-30'
 and b.created_date between '2024-06-01' and '2024-06-30'
 and library in (1,4)
 and (method='cod' or authorized_at is not null)
 """)
june_std_mtu_df = june_std_mtu_db.toPandas()
june_std_mtu_df.head()

# COMMAND ----------

june_std_mtu_df.to_csv('/dbfs/FileStore/june_std_mtu_df.csv', index=False)
"https://razorpay-dev.cloud.databricks.com/files/june_std_mtu_df.csv"

# COMMAND ----------

june_magic_mtu_db = sqlContext.sql(
    """
select 
distinct
 a.merchant_id  
 from realtime_hudi_api.payments a 
-- inner join realtime_hudi_api.payment_analytics b on a.id = b.payment_id
 inner join realtime_hudi_api.order_meta c on a.order_id = c.order_id


 where a.created_date between '2024-06-01' and '2024-06-30'
 ---and b.created_date between '2024-06-01' and '2024-06-30'
 and c.created_date between '2024-06-01' and '2024-06-30'
 ---and library in (1,4)
 and (method='cod' or authorized_at is not null)
 """)
june_magic_mtu_df = june_magic_mtu_db.toPandas()
june_magic_mtu_df.head()

# COMMAND ----------

june_magic_mtu_df.to_csv('/dbfs/FileStore/june_magic_mtu_df.csv', index=False)
"https://razorpay-dev.cloud.databricks.com/files/june_magic_mtu_df.csv"

# COMMAND ----------



# COMMAND ----------

print(june_magic_mtu_df.shape)

# COMMAND ----------

# MAGIC %md
# MAGIC #Magic coverage %

# COMMAND ----------

print(segment_df.dtypes)

# COMMAND ----------

import pandas as pd
eligible_merchants_df = pd.read_csv('/Workspace/Users/pallavi.samodia@razorpay.com/june_july_magic_eligible_mx')
eligible_merchants_df['eligible'] = 'eligible'


# COMMAND ----------

june_magic_mtu_df['merchant_id'] = june_magic_mtu_df['merchant_id'].astype(str)
segment_df['merchant_id'] = segment_df['merchant_id'].astype(str)
rampup_master_df['merchant_id'] = rampup_master_df['merchant_id'].astype(str)
june_magic_coverage_df = june_magic_mtu_df.merge(segment_df, how='left', on='merchant_id')
june_magic_coverage_df = june_magic_coverage_df.merge(rampup_master_df, how='left', on='merchant_id')



# COMMAND ----------

june_magic_coverage_df = pd.read_csv('/dbfs/FileStore/june_magic_coverage_df.csv')
june_magic_coverage_df.head()

# COMMAND ----------

june_magic_coverage_df = june_magic_coverage_df.merge(eligible_merchants_df, how='left', on='merchant_id')
print(june_magic_coverage_df.shape)
june_magic_coverage_df.head()

# COMMAND ----------

print('June Magic MTU ', june_magic_coverage_df.shape[0])
print('Ramped Up ', june_magic_coverage_df[june_magic_coverage_df['status']=='live'].shape[0])
print('Coverage % ', (june_magic_coverage_df[june_magic_coverage_df['status']=='live'].shape[0] * 1.0) /  june_magic_coverage_df.shape[0])

# COMMAND ----------

june_magic_coverage_df.to_csv('/dbfs/FileStore/june_magic_coverage_df.csv', index=False)
"https://razorpay-dev.cloud.databricks.com/files/june_magic_coverage_df.csv"

# COMMAND ----------

# MAGIC %md #Final Table compilation with May base
# MAGIC

# COMMAND ----------

final_df = may_std_mtu_df.merge(ineligible_merchants_df[['merchant_id','not_eligible']], how='left', on='merchant_id')
final_df = final_df.merge(eligible_merchants_df[['merchant_id','eligible']], how='left', on=['merchant_id',])
final_df = final_df.merge(segment_df, how='left', on=['merchant_id',])
final_df = final_df.merge(ineligible_reasons_grouped_df, how='left', on=['merchant_id',])
final_df = final_df.merge(rampup_master_df[['merchant_id','status']], how='left', on=['merchant_id',])
final_df.head()



# COMMAND ----------

final_df['both_eligible_and_ineligible'] = (final_df['not_eligible'] == 'true') & (final_df['eligible'] == 'true')
final_df.head()

# COMMAND ----------

final_view = final_df.groupby(by=['eligible','team_owner','status'], dropna=False).agg({'merchant_id':'nunique'}).reset_index()
final_view['Percentage of May MTU'] = (final_view['merchant_id']/ int(final_df['merchant_id'].nunique())) * 100
final_view.head(100)

# COMMAND ----------

print("Total May MTU ", final_df['merchant_id'].nunique())
print("Ineligible merchants ", final_df[final_df['not_eligible']=='true']['merchant_id'].nunique())
#print("Ineligible merchants w reasons ", final_df[~final_df['concatenated_reasons'].isna()]['merchant_id'].nunique())
print("Eligible merchants", final_df[final_df['eligible']=='true']['merchant_id'].nunique())
print("Both Eligible & Ineligible merchants ", final_df[final_df['both_eligible_and_ineligible']==True]['merchant_id'].nunique())


# COMMAND ----------

# MAGIC %md
# MAGIC Final table compilation with June MTU

# COMMAND ----------

june_final_df = june_std_mtu_df.merge(ineligible_merchants_df[['merchant_id','not_eligible']], how='left', on='merchant_id')
june_final_df = june_final_df.merge(eligible_merchants_df[['merchant_id','eligible']], how='left', on=['merchant_id',])
june_final_df = june_final_df.merge(segment_df, how='left', on=['merchant_id',])
june_final_df = june_final_df.merge(ineligible_reasons_grouped_df, how='left', on=['merchant_id',])
june_final_df = june_final_df.merge(rampup_master_df[['merchant_id','status']], how='left', on=['merchant_id',])

june_final_df.head()


# COMMAND ----------

#Other columns for support
june_final_df['both_eligible_and_ineligible'] = (june_final_df['not_eligible'] == 'true') & (june_final_df['eligible'] == 'true')
june_final_df['not_received_call'] = (june_final_df['not_eligible'].isnull()) & (june_final_df['eligible'].isnull())
june_final_df['eligible_and_live'] = (june_final_df['eligible'] == 'true') & (june_final_df['status'] == 'live')
june_final_df['eligible_and_not_live'] = (june_final_df['eligible'] == 'true') & (june_final_df['status'] != 'live')
june_final_df['all_ineligible'] = (june_final_df['not_eligible'] == 'true') & (june_final_df['eligible'] != 'true')
june_final_df.head()

# COMMAND ----------

june_final_view_v2 = june_final_df.groupby(by=['team_owner','not_received_call','eligible_and_live','eligible_and_not_live','all_ineligible'], dropna=False).agg({'merchant_id':'nunique'}).reset_index()
june_final_view_v2.head(100)

# COMMAND ----------


print("Total May MTU ", june_final_df['merchant_id'].nunique())
print("Ineligible merchants ", june_final_df[june_final_df['not_eligible']=='true']['merchant_id'].nunique())
#print("Ineligible merchants w reasons ", final_df[~final_df['concatenated_reasons'].isna()]['merchant_id'].nunique())
print("Eligible merchants", june_final_df[june_final_df['eligible']=='true']['merchant_id'].nunique())
print("Both Eligible & Ineligible merchants ", june_final_df[june_final_df['both_eligible_and_ineligible']==True]['merchant_id'].nunique())

# COMMAND ----------

june_final_view = june_final_df.groupby(by=['eligible','not_eligible','status'], dropna=False).agg({'merchant_id':'nunique'}).reset_index()
june_final_view['Percentage of May MTU'] = (june_final_view['merchant_id']/ int(june_final_df['merchant_id'].nunique())) * 100
june_final_view.head(100)

# COMMAND ----------

#example checkout IDs
june_final_df[(june_final_df['eligible']!='True') & (june_final_df['team_owner']!='SME') ].head()

# COMMAND ----------

june_final_view = june_final_df[june_final_df['team_owner']=='SME'].groupby(by=['eligible','not_eligible','status'], dropna=False).agg({'merchant_id':'nunique'}).reset_index()
june_final_view['Percentage of May MTU'] = (june_final_view['merchant_id']/ int(june_final_df['merchant_id'].nunique())) * 100
june_final_view.head(100)

# COMMAND ----------

#For MM + Ent
june_final_view = june_final_df[june_final_df['team_owner']!='SME'].groupby(by=['eligible','not_eligible','status'], dropna=False).agg({'merchant_id':'nunique'}).reset_index()
june_final_view['Percentage of May MTU'] = (june_final_view['merchant_id']/ int(june_final_df['merchant_id'].nunique())) * 100
june_final_view.head(100)

# COMMAND ----------

# MAGIC %md
# MAGIC Creating confusion matrix for June and May
# MAGIC

# COMMAND ----------

may_set = set(final_df['merchant_id'])
june_set = set(june_final_df['merchant_id'])
print("common merchants in 2 months",print(len(list(may_set.intersection(june_set)))))
print("Mx in May but not in June",print(len(list(may_set.difference(june_set)))))
print("Mx in June but not in May",print(len(list(june_set.difference(may_set)))))
print("total unique merchants in May and June",print(len(list(may_set.union(june_set)))))

# COMMAND ----------

#Doing the same just for SME
may_set = set(final_df[final_df['team_owner']=='SME']['merchant_id'])
june_set = set(june_final_df[june_final_df['team_owner']=='SME']['merchant_id'])
print("common merchants in 2 months",print(len(list(may_set.intersection(june_set)))))
print("Mx in May but not in June",print(len(list(may_set.difference(june_set)))))
print("Mx in June but not in May",print(len(list(june_set.difference(may_set)))))
print("total unique merchants in May and June",print(len(list(may_set.union(june_set)))))

# COMMAND ----------

june_final_dfsearch_strings = ['merchant_policy', 'paypal','sdk','ios_sdk','raas','bajaj_emi']

# Function to check if any string in the list is present
def contains_any(text, search_list):
  """
  This function checks if any string in the search_list is present in the text.

  Args:
      text: The text to search within.
      search_list: A list of strings to search for.

  Returns:
      True if any string in search_list is found in the text, False otherwise.
  """
  return any(s in text for s in search_list) if pd.notna(text) else False  # Handle missing values (NA)

final_df['eligible_now'] = final_df.apply(lambda row: contains_any(row['concatenated_reasons'], search_strings), axis=1)

final_df.head()

# COMMAND ----------

new_final_df = final_df.drop_duplicates(subset=['merchant_id','merchant_key_x','concatenated_reasons','eligible','both_eligible_and_ineligible'], keep='first')
new_final_df.shape

# COMMAND ----------

new_final_df.head(20)

# COMMAND ----------

final_modified_df = final_df[~final_df['merchant_key_raw'].isna()]
final_modified_df.shapenew_final_df

# COMMAND ----------



# COMMAND ----------

print("Total May MTU ", may_std_mtu_df['merchant_id'].nunique())
print("Ineligible merchants ", final_df[final_df['not_eligible']=='true']['merchant_id'].nunique())
#print("Ineligible merchants w reasons ", final_df[~final_df['concatenated_reasons'].isna()]['merchant_id'].nunique())
print("Eligible merchants", final_df[final_df['eligible']=='true']['merchant_id'].nunique())
print("Both Eligible & Ineligible merchants ", final_df[final_df['both_eligible_and_ineligible']==True]['merchant_id'].nunique())


# COMMAND ----------

final_df.head()

# COMMAND ----------

final_df[['merchant_id','not_eligible','eligible','team_owner','concatenated_reasons','both_eligible_and_ineligible','eligible_now']].to_csv('/dbfs/FileStore/final_df.csv', index=False)
"https://razorpay-dev.cloud.databricks.com/files/final_df.csv"

# COMMAND ----------

final_df.shape

# COMMAND ----------

print("Total May MTU ", new_final_df['merchant_id'].nunique())
print("Ineligible merchants ", new_final_df[~new_final_df['merchant_key_raw_x'].isna()]['merchant_id'].nunique())
print("Ineligible merchants w reasons ", new_final_df[~new_final_df['concatenated_reasons'].isna()]['merchant_id'].nunique())
print("Eligible merchants", new_final_df[new_final_df['eligible']=='true']['merchant_id'].nunique())
print("Both Eligible & Ineligible merchants ", new_final_df[new_final_df['both_eligible_and_ineligible']==True]['merchant_id'].nunique())

# COMMAND ----------

reasons_df = new_final_df.groupby(by=['concatenated_reasons']).agg({'merchant_id': 'nunique'}).reset_index()
reasons_df.sort_values('merchant_id', ascending=False)

# COMMAND ----------



# Sample DataFrame
data = {'column_name': ['rzp_live_partner_IMGhoulrfdK0Cm',
'rzp_live_oauth_O5XLAZP9vVe5R3',
'rzp_live_Rpz42g4amx8uBz', ]}
df = pd.DataFrame(data)

# Extract value after the last underscore using str.rsplit
df['extracted_value'] = df['column_name'].str.rsplit('_', n=1).str.get(-1)

print(df)


# COMMAND ----------


