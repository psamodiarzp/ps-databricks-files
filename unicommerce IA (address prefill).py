# Databricks notebook source
import matplotlib.pyplot as plt
import pandas as pd

# COMMAND ----------

total_addresses_db = sqlContext.sql(
        """
select
created_date,
case when source_type is null then 'user_saved' else source_type end as address_source,
        count(distinct id) as Count_of_Addresses_Added
        from realtime_hudi_api.addresses 
        where created_date < '2024-06-25'
        and created_date > '2024-06-10'
        group by 1,2
        """
    )
total_addresses_df = total_addresses_db.toPandas()
#total_addresses_df['Total Addresses'] = total_addresses_df.iloc[:,1:].sum(axis=1)
#total_addresses_df['Running Total'] = total_addresses_df['Total Addresses'].cumsum()
total_addresses_df.head()

# COMMAND ----------

total_addresses_df.pivot

# COMMAND ----------

total_addresses_modified_df= total_addresses_df.groupby('created_date').agg({'Count_of_Addresses_Added':'sum'}).reset_index()
total_addresses_modified_df.head()

# COMMAND ----------

plt.plot(total_addresses_df['created_date'], total_addresses_df['Count_of_Addresses_Added'])

# Set the x-axis label
plt.xlabel('Created Date')

# Set the y-axis label
plt.ylabel('Total Addresses')

# Set the title of the chart
plt.title('Total Addresses Added by Date')

# Increase the width of the x-axis
plt.rcParams['figure.figsize'] = [28, 10]

for i, (count, source) in enumerate(zip(total_addresses_df['Count_of_Addresses_Added'], total_addresses_df['address_source'])):
    plt.text(total_addresses_df['created_date'][i], count, f"{count} ({source})", )

# Add reference line

# Add reference line
#plt.axvline(x='2024-06-18', color='red', linestyle='--')
#plt.axvline(x='2024-06-19', color='red', linestyle='--')
#plt.text('2024-06-18', max(total_addresses_df['Count_of_Addresses_Added']), 'unicommerce went live 100%', ha='center', va='bottom')

# Display the chart
plt.show()

# COMMAND ----------

uc_add_available_db = sqlContext.sql(
    """
    select event,
    count(distinct checkout_id)
    from events.checkout_v2 a 
    inner join batch_sheets.checkout_v2_rampup_sheet b on a.merchant_id = b.merchant_id
    where producer_created_date < '2024-06-25'
    and producer_created_date > '2024-06-10'
    and event in (
        '1cc_load_saved_address_bottom_sheet_shown',
        'api:1cc_partner_address_count_start',
        'api:1cc_partner_address_count_end'
    )
    group by 1
    """
)
uc_add_available_df = uc_add_available_db.toPandas()
uc_add_available_df.head()

# COMMAND ----------

uc_add_available_db = sqlContext.sql(
    """
    select 
    producer_created_date,
    event_name,
    get_json_object(properties, '$.data.status_key') as status_key,
    get_json_object(properties, '$.data.partner_name') as partner_name,
 get_json_object(properties, '$.data.unicommerce_address_count'),
     --case when get_json_object(properties, '$.data.unicommerce_address_count') >0  then 'uc address found' else 'uc address not found' end as address_count,
     get_json_object(properties, '$.data.otp_reason') as otp_reason,

    count(distinct checkout_id)
    from aggregate_pa.cx_1cc_events_dump_v1 a 
    left join batch_sheets.checkout_v2_rampup_sheet b on a.merchant_id = b.merchant_id
    where producer_created_date < date('2024-06-25')
    and producer_created_date > date('2024-06-10')
and b.merchant_id is null
    and event_name in (
        'render:1cc_summary_screen_loaded_completed',
        'render:1cc_load_saved_address_bottom_sheet_shown',
        'api:1cc_partner_address_count_start',
        'api:1cc_partner_address_count_end',
        'render:1cc_rzp_otp_screen_loaded',
        'render:1cc_saved_shipping_address_screen_loaded',
        'behav:1cc_saved_address_screen_continue_cta_clicked',
        'behav:1cc_summary_screen_continue_cta_clicked'
    )
    group by 1,2,3,4,5,6
    """
)
uc_add_available_df = uc_add_available_db.toPandas()
uc_add_available_df.head()

# COMMAND ----------

# Convert to numeric, coercing errors to NaN
uc_add_available_df['unicommerce_address_count'] = pd.to_numeric(
    uc_add_available_df['get_json_object(properties, $.data.unicommerce_address_count)'],
    errors='coerce'
)

# Optionally, fill NaN values with 0 (or another default value of your choice)
uc_add_available_df['unicommerce_address_count'].fillna(0, inplace=True)

# Convert to int
uc_add_available_df['unicommerce_address_count'] = uc_add_available_df['unicommerce_address_count'].astype(int)

# COMMAND ----------

uc_add_available_df['event_name'].unique()

# COMMAND ----------

# Assuming you want to flag rows based on your conditions
uc_add_available_df['api_returns_address'] = 0  # Initialize the column with 0 or False
condition = (uc_add_available_df['event_name'] == 'api:1cc_partner_address_count_end') & \
            (uc_add_available_df['unicommerce_address_count'] > 0)

# Update the 'api_returns_address' column based on the condition
uc_add_available_df.loc[condition, 'api_returns_address'] = uc_add_available_df['count(DISTINCT checkout_id)']  # or True, if you're flagging



uc_add_available_df['access_address_otp'] = 0 
condition = (uc_add_available_df['event_name'] == 'api:1cc_rzp_otp_screen_loaded') & \
            (uc_add_available_df['otp_reason'].str.contains('access_address'))
uc_add_available_df.loc[condition, 'access_address_otp'] = uc_add_available_df['count(DISTINCT checkout_id)']  # or True, if you're flagging


uc_add_available_df.head()

# COMMAND ----------

uc_add_available_df[uc_add_available_df['event_name'] == 'api:1cc_partner_address_count_end']

# COMMAND ----------

uc_add_available_df[uc_add_available_df['event_name'] == 'api:1cc_partner_address_count_end']['partner_name'].unique()

# COMMAND ----------

#Next steps:
#take the above data and plot line charts for these two: date wise access address otp over summary address cta clciked. this will give you address availability 
#address prefill will depend on saved address screen load
#for both these graphs, mark the lines for 18th - 19th

# COMMAND ----------


