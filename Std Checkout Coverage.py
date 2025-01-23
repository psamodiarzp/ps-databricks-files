# Databricks notebook source
import pandas as pd

# COMMAND ----------

eligible_merchants_db = sqlContext.sql(
    """
    SELECT
    distinct  merchant_key as merchant_key_raw
    from aggregate_pa.cx_lo_fact_ism_v1
where producer_created_date BETWEEN '2024-06-01' AND '2024-06-26'
and get_json_object(render_properties,'$.data.meta.v2_eligible') = 'true'
    """)

eligible_merchants_df = eligible_merchants_db.toPandas()
eligible_merchants_df['merchant_key'] = eligible_merchants_df['merchant_key_raw'].str.rsplit('_', n=1).str.get(-1)
eligible_merchants_df['eligible'] ='true'
eligible_merchants_df.head()

# COMMAND ----------


print('Total eligible merchant_key' , eligible_merchants_df['merchant_key'].nunique())
print('Total eligible merchant_id' , eligible_merchants_df['merchant_id'].nunique())

# COMMAND ----------



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
    distinct  merchant_key as merchant_key_raw,
    get_json_object(render_properties,'$.data.meta.v2_ineligible_reason') as v2_ineligible_reason
    from aggregate_pa.cx_lo_fact_ism_v1
where producer_created_date between '2024-06-01' and '2024-06-26'
and get_json_object(render_properties,'$.data.meta.v2_ineligible_reason') is not null 
and get_json_object(render_properties,'$.data.meta.v2_ineligible_reason') <> ''
    """)

ineligible_reasons_df = ineligible_reasons_db.toPandas()
ineligible_reasons_df['merchant_key'] = ineligible_reasons_df['merchant_key_raw'].str.rsplit('_', n=1).str.get(-1)
ineligible_reasons_df.head()

# COMMAND ----------

ineligible_reasons_df = ineligible_reasons_db.toPandas()
ineligible_reasons_df['merchant_key'] = ineligible_reasons_df['merchant_key_raw'].str.rsplit('_', n=1).str.get(-1)
ineligible_reasons_df.head()

# COMMAND ----------

ineligible_reasons_grouped_df.shape

# COMMAND ----------

# Group and concatenate
concatenated_reasons = ineligible_reasons_df.groupby('merchant_key_raw')['v2_ineligible_reason'].transform(lambda x: ', '.join(x))

# Combine columns into a new DataFrame
ineligible_reasons_grouped_df = pd.DataFrame({'merchant_key_raw': ineligible_reasons_df['merchant_key_raw'], 'concatenated_reasons': concatenated_reasons})

ineligible_reasons_grouped_df.head()

ineligible_reasons_grouped_df.shape

# COMMAND ----------

ineligible_reasons_grouped_df['merchant_key'] = ineligible_reasons_grouped_df['merchant_key_raw'].str.rsplit('_', n=1).str.get(-1)
ineligible_reasons_grouped_df = ineligible_reasons_grouped_df.merge(keys_df, how='left', left_on='merchant_key', right_on='id')
ineligible_reasons_grouped_df.shape
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


may_std_mtu_df.to_csv('/dbfs/FileStore/may_std_mtu_df.csv', index=False)
"https://razorpay-dev.cloud.databricks.com/files/may_std_mtu_df.csv"

# COMMAND ----------

final_df = may_std_mtu_df.merge(ineligible_mid_df, how='left', on='merchant_id')
final_df = final_df.merge(ineligible_reasons_grouped_df, how='left', on=['merchant_id',])
final_df = final_df.merge(eligible_merchants_df, how='left', on=['merchant_id',])
final_df.head()

# COMMAND ----------

final_df['both_eligible_and_ineligible'] = (final_df['merchant_key_raw_x'].notna()) & (final_df['eligible'] == 'true')
final_df.head()

# COMMAND ----------



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
print("Ineligible merchants ", final_df[~final_df['merchant_key_raw_x'].isna()]['merchant_id'].nunique())
print("Ineligible merchants w reasons ", final_df[~final_df['concatenated_reasons'].isna()]['merchant_id'].nunique())
print("Eligible merchants", final_df[final_df['eligible']=='true']['merchant_id'].nunique())
print("Both Eligible & Ineligible merchants ", final_df[final_df['both_eligible_and_ineligible']==True]['merchant_id'].nunique())


# COMMAND ----------

new_final_df.to_csv('/dbfs/FileStore/new_final_df.csv', index=False)
"https://razorpay-dev.cloud.databricks.com/files/new_final_df.csv"

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


