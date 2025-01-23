# Databricks notebook source
import pandas as pd
import numpy as np

# COMMAND ----------

from scipy.stats import chi2_contingency

# COMMAND ----------

# DBTITLE 1,Gets merchant list with Pre-Magic RTO available
tracker_details_db = sqlContext.sql(
    """
    select * from batch_sheets.magic_merchant_list
 ---   where pre_magic_rto is not null and pre_magic_rto <> ''
    """
)
tracker_details_df = tracker_details_db.toPandas()
tracker_details_df.head()

# COMMAND ----------

tracker_details_df.shape

# COMMAND ----------

orders_db = sqlContext.sql(
    """
with payments_table as(
select 
merchant_id, order_id
from realtime_hudi_api.payments
where created_date >= '2022-06-01' 
and created_date < '2023-12-01' 
and method='cod'
)

select * from aggregate_pa.magic_rto_reimbursement_fact where order_id in (select order_id from payments_table)
"""
)
orders_df = orders_db.toPandas()
orders_df.head()

# COMMAND ----------

base_df = pd.merge(orders_df,tracker_details_df,  how='left', left_on='merchant_id', right_on='mid')
base_df.head()

# COMMAND ----------

print(base_df.shape)

# COMMAND ----------

base_df.shape

# COMMAND ----------

# DBTITLE 1,Data Evaluation


# Assuming 'order_created_date' and 'live_date' are in string format
base_df['order_created_date'] = pd.to_datetime(base_df['order_created_date'])
base_df['live_date'] = pd.to_datetime(base_df['live_date'])
nat_mask = pd.isna(base_df['live_date'])
base_df = base_df[~nat_mask]
nat_mask = pd.isna(base_df['order_created_date'])
base_df = base_df[~nat_mask]
print(base_df.shape)

base_df['months_since_live'] = ((base_df['order_created_date'] - base_df['live_date']) / pd.Timedelta(days=30)).astype(int)

base_df.head()

# COMMAND ----------

base_df.dtypes

# COMMAND ----------

base_df.to_csv('/dbfs/FileStore/rto_deep_dive_base_df.csv', index=False)
"https://razorpay-dev.cloud.databricks.com/files/rto_deep_dive_base_df.csv"

# COMMAND ----------

valid_delivery_status = ['delivered','rto','cancelled','lost','partially_delivered','returned']
mx_overall_df = base_df.groupby(by='merchant_id').agg(
    live_date = ('live_date','max'),
    pre_magic_rto = ('pre_magic_rto','max'),
    total_order_count=('order_id', 'count'),
    order_count_w_delivery=('status', lambda x: x[x.isin(valid_delivery_status)].count()),
    order_count_w_rto=('status', lambda x: x[x.isin(['rto'])].count()),
    order_count_w_cod_intelligence=('cod_intelligence_enabled', lambda x: x[x.isin([True])].count()),
).reset_index()
mx_overall_df['delivery_percentage'] = round(mx_overall_df['order_count_w_delivery']*1.0 / mx_overall_df['total_order_count'],4)
mx_overall_df['rto_percentage'] = round(mx_overall_df['order_count_w_rto']*1.0 / mx_overall_df['total_order_count'],4)
mx_overall_df.head()

# COMMAND ----------

for i in [1,3,6,9,12]:
    temp_df = base_df[base_df['months_since_live']<=i].groupby(by='merchant_id').agg(
    total_order_count=('order_id', 'count'),
    order_count_w_delivery=('status', lambda x: x[x.isin(valid_delivery_status)].count()),
    order_count_w_rto=('status', lambda x: x[x.isin(['rto'])].count()),
    order_count_w_cod_intelligence=('cod_intelligence_enabled', lambda x: x[x.isin([True])].count()),
    )
    temp_df['delivery_percentage'] = round(temp_df['order_count_w_delivery']*1.0 / temp_df['total_order_count'],4)
    temp_df['rto_percentage'] = round(temp_df['order_count_w_rto']*1.0 / temp_df['total_order_count'],4)
    temp_df = temp_df.add_suffix(f'_{i}_months')
    temp_df = temp_df.reset_index()
    mx_overall_df = pd.merge(mx_overall_df, temp_df, how='left', on='merchant_id')

print(mx_overall_df.shape)
mx_overall_df.head()
    

# COMMAND ----------

mx_overall_df.sort_values(by='order_count_w_cod_intelligence', ascending=False)

# COMMAND ----------

# DBTITLE 1,data cleansing 
mx_overall_df.fillna(0, inplace=True)
#Removing merchants from live data prior to June 1 as we dont have organzied reimbursement data for them

mx_overall_df = mx_overall_df[mx_overall_df['live_date'] >= '2022-06-01']
print(mx_overall_df.shape)
mx_overall_df.head()

# COMMAND ----------

# Define the bins
bins = [0, 0.5, 0.8,1]

# Create a new column 'Bin' to store the bin labels
mx_overall_df['delivery_percentage_bin'] = pd.cut(mx_overall_df['delivery_percentage'], bins=bins)
mx_overall_df['delivery_percentage_bin'] = mx_overall_df['delivery_percentage_bin'].astype(str)

# Count occurrences in each bin
bin_counts = (mx_overall_df['delivery_percentage_bin'].value_counts())*1.0/513

# Print the result
print(bin_counts)

# COMMAND ----------

# Define the bins
bins = [mx_overall_df['total_order_count'].min(), 500,1000,mx_overall_df['total_order_count'].max()]

# Create a new column 'Bin' to store the bin labels
mx_overall_df['order_count_bin'] = pd.cut(mx_overall_df['total_order_count'], bins=bins)
mx_overall_df['order_count_bin'] = mx_overall_df['order_count_bin'].astype(str)

# Count occurrences in each bin
bin_counts = (mx_overall_df['order_count_bin'].value_counts())*1.0/777

# Print the result
print(bin_counts)

# COMMAND ----------

num_check = mx_overall_df.groupby(by=['order_count_bin','delivery_percentage_bin']).agg({'merchant_id':'nunique'}).reset_index()
num_check['percent_divide'] = num_check['merchant_id'] / num_check['merchant_id'] .sum()
num_check['percent_divide'] = num_check['percent_divide'].cumsum()
num_check

# COMMAND ----------

Add cod intelligence/ manual review flag 

# COMMAND ----------

mx_overall_df.to_csv('/dbfs/FileStore/mx_overall_df.csv', index=False)
"https://razorpay-dev.cloud.databricks.com/files/mx_overall_df.csv"

# COMMAND ----------

mx_filtered_df = mx_overall_df[(mx_overall_df['delivery_percentage_bin'].isin(['(0.8, 1.0]', '(0.5, 0.8]'])) & (mx_overall_df['order_count_bin'].isin(['(500, 1000]', '(1000, 309610]']))]
mx_filtered_df.shape


# COMMAND ----------

mx_filtered_df.to_csv('/dbfs/FileStore/mx_filtered_df.csv', index=False)
"https://razorpay-dev.cloud.databricks.com/files/mx_filtered_df.csv"

# COMMAND ----------

# Calculate MoM percentage change for each quarter
mom_changes = mx_filtered_df[['rto_percentage_1_months','rto_percentage_3_months','rto_percentage_6_months','rto_percentage_12_months']].pct_change()

df_with_mom = pd.concat([mx_filtered_df[['rto_percentage_1_months','rto_percentage_3_months','rto_percentage_6_months','rto_percentage_9_months','rto_percentage_12_months']], mom_changes.add_prefix('MoM_')], axis=1)

# Print the result
print(df_with_mom)
# Print the result
#print(mom_changes)

# COMMAND ----------

mx_filtered_df.columns

# COMMAND ----------

mx_selected_df = mx_filtered_df[mx_filtered_df['merchant_id'].isin([
    'CZk3srcCNBvyHx',
'CjaeQlPFXFvaLN',
'FiVc88jM7AeSta',
'G4WBGNh0UU0RUp',
'Glk1BQ6cvvA8DJ',
'H3sNyWSIaNvJNG',
'HpV1CS2H4GXtsC',
'ICmqJzaoWUpyj6',
'IH7E2OJQGEKKTN',
'INmjGFuOqIvFAT',
'J1rTWZUWmgNzSW',
'J48RMdC1b73dYg',
'JNrymGlbgxRl04',
'JUbNvNRp3egEAc',
'Je6WBJUrSvp7Ut',
])]

print(mx_selected_df.shape)
mx_selected_df.head()

# COMMAND ----------

for i in [2,4,5,7,8,]:
    temp_df = base_df[base_df['months_since_live']<=i].groupby(by='merchant_id').agg(
    total_order_count=('order_id', 'count'),
    order_count_w_delivery=('status', lambda x: x[x.isin(valid_delivery_status)].count()),
    order_count_w_rto=('status', lambda x: x[x.isin(['rto'])].count()),
    order_count_w_cod_intelligence=('cod_intelligence_enabled', lambda x: x[x.isin([True])].count()),
    )
    temp_df['delivery_percentage'] = round(temp_df['order_count_w_delivery']*1.0 / temp_df['total_order_count'],4)
    temp_df['rto_percentage'] = round(temp_df['order_count_w_rto']*1.0 / temp_df['total_order_count'],4)
    temp_df = temp_df.add_suffix(f'_{i}_months')
    temp_df = temp_df.reset_index()
    mx_selected_df = pd.merge(mx_selected_df, temp_df, how='left', on='merchant_id')

print(mx_selected_df.shape)
mx_selected_df.head()

# COMMAND ----------

mx_selected_df.to_csv('/dbfs/FileStore/mx_selected_df.csv', index=False)
"https://razorpay-dev.cloud.databricks.com/files/mx_selected_df.csv"

# COMMAND ----------

events_db = sqlContext.sql(
    """
    with fact as (
        select checkout_id,
merchant_id,
order_id,
case when os_brand_family in ('Android','iOS') then os_brand_family else 'others' end as os,
case when validation_successful=1 then 1 else 0 end as coupon_applied,
original_amount
 from aggregate_pa.magic_checkout_fact
 where producer_created_date between date('2023-09-04') and date('2023-11-30')
 and merchant_id in (
     'ASPQeKnE5x4NqA',
'CZk3srcCNBvyHx',
'CjaeQlPFXFvaLN',
'CtHTcKWCa4ZDeF',
'CyFgPUrwzJ5VhG',
'DRA8pvry1C4oTC',
'DkYdOAXPbbqFwT',
'EFuW7roPMY4a1a',
'EJu3xUe1qpgeB8',
'EjVxgGR8XuJ2Hk',
'FQLy8isl8cBT4q',
'FXRmq6ewcJnw64',
'FZm8tqV6t8hIcY',
'FiVc88jM7AeSta',
'G47JazKN1lLzua',
'G4WBGNh0UU0RUp',
'GJJXcN2y68zim0',
'GaFX8ZqIdqtVd0',
'GgAASeeGAAslfn',
'Gj0QPGyxLH5XiQ',
'GlLXGUtKUrQHtj',
'Glk1BQ6cvvA8DJ',
'GpnsGJIN8c3kA2',
'Gta0c1In9uWe5A',
'GwKl7TCo0HdCZR',
'H3sNyWSIaNvJNG',
'HDlxTlrBXYR6Ng',
'HDo52FrQhJj1iQ',
'HJHvrI58d1PqIe',
'HNomzjcB1CVVRv',
'HakfWT9UoxpRxO',
'HpV1CS2H4GXtsC',
'Hs7yvRJD8z8x6c',
'HsAWE0CNIbJGGX',
'ICmqJzaoWUpyj6',
'IEJDxs6E57EG8a',
--'IH7E2OJQGEKKTN',
'IJosFOZfgVbMAM',
'IMJgZNyYq5tWOj',
'INmjGFuOqIvFAT',
'ISVXnsQ10XzCN4',
'J1rTWZUWmgNzSW',
'J48RMdC1b73dYg',
'J4WJ969WSDaI1L',
'JCSgHHjSsYK0ra',
'JNrymGlbgxRl04',
'JUHfXse0FDfnru',
'JUbNvNRp3egEAc',
'JWOhiDzI1AfUCS',
'JaErdQ0mRfZdyR',
'Je6WBJUrSvp7Ut',
'JkYgXkOWXVEmTF',
'JnLrOqCLRS0PgJ',
'JyvlSIMrRKs0tu',
'K9XeiYWyLZ2eUM',
'KZ6thrKylzl3ZQ',
'Kb86PrdlM26Wf5',
'KgD8Y7ddMd6dP6'
 )
    ),
    risk_reasons as (
        Select 
order_id,
max(rules_evaluated) as rules_evaluated

from events.events_1cc_tw_cod_score_v2
where producer_created_date between ('2023-09-04') and ('2023-11-30')
 
group by 1
    ),
    mx_category as (
        select distinct merchants.id as merchant_id, merchants.website, merchants.category ,  mcc_description.* 
from realtime_hudi_api.merchants AS merchants 
inner JOIN aggregate_pa.mcc_description ON substr(mcc_description.mcc,1,4)= merchants.category
inner JOIN aggregate_pa.magic_checkout_fact as fact on merchants.id = fact.merchant_id
    ), rto_table as (
        Select 
        order_id,
        order_status,
        case when cod_intelligence_enabled=True then 1 else 0 end as cod_intelligence_enabled,
        case when manual_control_cod_order='true' then 1 else 0 end as manual_control_cod_order,
        case when status='rto' then 1 else 0 end as order_rto
        from aggregate_pa.magic_rto_reimbursement_fact
        where order_created_date between date('2022-06-01') and  date('2023-11-30') 
 and merchant_id in (
    'ASPQeKnE5x4NqA',
'CZk3srcCNBvyHx',
'CjaeQlPFXFvaLN',
'CtHTcKWCa4ZDeF',
'CyFgPUrwzJ5VhG',
'DRA8pvry1C4oTC',
'DkYdOAXPbbqFwT',
'EFuW7roPMY4a1a',
'EJu3xUe1qpgeB8',
'EjVxgGR8XuJ2Hk',
'FQLy8isl8cBT4q',
'FXRmq6ewcJnw64',
'FZm8tqV6t8hIcY',
'FiVc88jM7AeSta',
'G47JazKN1lLzua',
'G4WBGNh0UU0RUp',
'GJJXcN2y68zim0',
'GaFX8ZqIdqtVd0',
'GgAASeeGAAslfn',
'Gj0QPGyxLH5XiQ',
'GlLXGUtKUrQHtj',
'Glk1BQ6cvvA8DJ',
'GpnsGJIN8c3kA2',
'Gta0c1In9uWe5A',
'GwKl7TCo0HdCZR',
'H3sNyWSIaNvJNG',
'HDlxTlrBXYR6Ng',
'HDo52FrQhJj1iQ',
'HJHvrI58d1PqIe',
'HNomzjcB1CVVRv',
'HakfWT9UoxpRxO',
'HpV1CS2H4GXtsC',
'Hs7yvRJD8z8x6c',
'HsAWE0CNIbJGGX',
'ICmqJzaoWUpyj6',
'IEJDxs6E57EG8a',
--'IH7E2OJQGEKKTN',
'IJosFOZfgVbMAM',
'IMJgZNyYq5tWOj',
'INmjGFuOqIvFAT',
'ISVXnsQ10XzCN4',
'J1rTWZUWmgNzSW',
'J48RMdC1b73dYg',
'J4WJ969WSDaI1L',
'JCSgHHjSsYK0ra',
'JNrymGlbgxRl04',
'JUHfXse0FDfnru',
'JUbNvNRp3egEAc',
'JWOhiDzI1AfUCS',
'JaErdQ0mRfZdyR',
'Je6WBJUrSvp7Ut',
'JkYgXkOWXVEmTF',
'JnLrOqCLRS0PgJ',
'JyvlSIMrRKs0tu',
'K9XeiYWyLZ2eUM',
'KZ6thrKylzl3ZQ',
'Kb86PrdlM26Wf5',
'KgD8Y7ddMd6dP6'
 )
        
    )
    select *
    from fact 
    left join mx_category on fact.merchant_id = mx_category.merchant_id
    --left join pmt_offer_applied_sql on fact.merchant_id = pmt_offer_applied_sql.merchant_id 
    ---and fact.checkout_id = pmt_offer_applied_sql.checkout_id
    left join risk_reasons on (fact.order_id) = substr(risk_reasons.order_id,7)
    inner join rto_table on (fact.order_id) = rto_table.order_id
    where  rto_table.order_status='placed'
       """
)
events_df = events_db.toPandas()
events_df.head()

# COMMAND ----------

events_df.to_csv('/dbfs/FileStore/rto_events_df.csv', index=False)
"https://razorpay-dev.cloud.databricks.com/files/rto_events_df.csv"

# COMMAND ----------

events_df.shape

# COMMAND ----------

events_df.columns

# COMMAND ----------

events_df.describe()

# COMMAND ----------



# COMMAND ----------

# Assume df is your DataFrame
events_df = events_df.loc[:, ~events_df.columns.duplicated()] # Keep the first occurrence of each duplicated column
events_df.columns

# COMMAND ----------

rules_df = events_df.groupby(by='rules_evaluated').agg({'order_rto':'sum'}).reset_index()

rules_df['Percentage'] = (rules_df['order_rto'] / rules_df['order_rto'].sum()) * 100
rules_df.sort_values(by='order_rto', ascending=False)

# COMMAND ----------

unique_merchant_ids = events_df['merchant_id'].unique()
feature_list = ['coupon_applied','original_amount','cod_intelligence_enabled','manual_control_cod_order',]
correlation_df = events_df.groupby(by=['merchant_id']).agg({'order_id':'nunique','cod_intelligence_enabled':'sum','order_rto':'sum','mcc_description':'max','website':'max','original_amount':'mean'}).reset_index()
for feature in feature_list:
    correlations = []
    for merchant_id in unique_merchant_ids:
        # Select rows for the current merchant ID
        subset_df = events_df[events_df['merchant_id'] == merchant_id]
        
        # Calculate the correlation between column1 and column2 for the current merchant ID
        correlation = round(subset_df[feature].corr(subset_df['order_rto']),4)

        # Append the correlation to the list
        correlations.append(correlation)
    
    # Add the correlations list as a new column in the correlation_df
    correlation_df[feature + '_correlation'] = correlations
        
        # Print the result for each merchant ID
        # print(f"Merchant ID {merchant_id}: Correlation between {feature} and RTO: {correlation}")
correlation_df

# COMMAND ----------

events_df.groupby(by=['merchant_id']).agg({'cod_intelligence_enabled':'sum','order_rto':'sum','mcc_description':'max','website':'max','original_amount':'mean'}).reset_index()

# COMMAND ----------

correlation_df.to_csv('/dbfs/FileStore/rto_correlation_df.csv', index=False)
"https://razorpay-dev.cloud.databricks.com/files/rto_correlation_df.csv"


# COMMAND ----------

for feature in feature_list:
    correlations = []
        
    # Calculate the correlation between column1 and column2 for the current merchant ID
    correlation = round(events_df[feature].corr(events_df['order_rto']),4)

    # Append the correlation to the list
    correlations.append(correlation)
    
    # Add the correlations list as a new column in the correlation_df
    #correlation_df[feature + '_correlation'] = correlations
        
    # Print the result for each merchant ID
    print(f"Correlation between {feature} and RTO: {correlation}")

# COMMAND ----------

# Get distinct merchant IDs
unique_merchant_ids = events_df['merchant_id'].unique()

# List of features for which you want to calculate Cramér's V
feature_list = ['coupon_applied', 'original_amount', 'cod_intelligence_enabled', 'manual_control_cod_order', ]

# Create a new DataFrame to store Cramér's V results
cramer_v_df = events_df.groupby(by=['merchant_id']).agg({'order_id':'nunique','cod_intelligence_enabled':'sum','order_rto':'sum','mcc_description':'max','website':'max','original_amount':'mean'}).reset_index()

# Loop through each feature
for feature in feature_list:
    cramer_vs = []
    
    # Loop through each unique merchant ID
    for merchant_id in unique_merchant_ids:
        # Create a contingency table
        contingency_table = pd.crosstab(events_df[events_df['merchant_id'] == merchant_id][feature], events_df[events_df['merchant_id'] == merchant_id]['order_rto'])

        # Perform chi-square test for independence
        chi2, _, _, _ = chi2_contingency(contingency_table)

        # Calculate Cramér's V
        n = contingency_table.sum().sum()
        v = min(contingency_table.shape) - 1
        cramers_v = (chi2 / n) / v**0.5

        # Append the Cramér's V to the list
        cramer_vs.append(cramers_v)
    
    # Add the Cramér's V list as a new column in the cramer_v_df
    cramer_v_df[feature + '_cramers_v'] = cramer_vs

# Print the result
cramer_v_df

# COMMAND ----------

cramer_v_df.to_csv('/dbfs/FileStore/cramers_v.csv', index=False)
"https://razorpay-dev.cloud.databricks.com/files/cramers_v.csv"


# COMMAND ----------

# Get distinct merchant IDs
unique_merchant_ids = events_df['merchant_id'].unique()

# List of features for which you want to calculate Cramér's V
feature_list = ['coupon_applied', 'original_amount', 'cod_intelligence_enabled', 'manual_control_cod_order', ]

# Create a new DataFrame to store Cramér's V results
#cramer_v_df = pd.DataFrame({'merchant_id': unique_merchant_ids})

# Loop through each feature
for feature in feature_list:
    #cramer_vs = []
    
    # Loop through each unique merchant ID
    #for merchant_id in unique_merchant_ids:
    # Create a contingency table
    contingency_table = pd.crosstab(events_df[feature], events_df['order_rto'])

    # Perform chi-square test for independence
    chi2, _, _, _ = chi2_contingency(contingency_table)

    # Calculate Cramér's V
    n = contingency_table.sum().sum()
    v = min(contingency_table.shape) - 1
    cramers_v = (chi2 / n) / v**0.5

    print(f"Correlation between {feature} and RTO: {cramers_v}")
    
    

# COMMAND ----------


