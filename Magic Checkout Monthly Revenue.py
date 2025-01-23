# Databricks notebook source
# MAGIC %md
# MAGIC #Imports

# COMMAND ----------

import pandas as pd
import numpy as np

# COMMAND ----------

# MAGIC %md
# MAGIC #Inputs

# COMMAND ----------

reporting_month = '2024-12'

# COMMAND ----------

# MAGIC %md
# MAGIC # COD Pricing

# COMMAND ----------

# magic_checkout_pricing_mx_temp = pd.read_csv('/Workspace/Users/pallavi.samodia@razorpay.com/magic_checkout_pricing_merchants',)

# magic_checkout_pricing_mx_temp = magic_checkout_pricing_mx_temp.drop(columns=['Merchant ID','Payments Successful [Cod + Non_cod]','GMV_authorised COD'])
# magic_checkout_pricing_mx_temp.head()

# COMMAND ----------

# magic_checkout_pricing_mx_temp.shape

# COMMAND ----------

'''Link to the ref doc: https://docs.google.com/spreadsheets/d/1mMw-9Brc6OcjSQ-swhgNVLZUzsCRzY-5DrUQ29V73Ao/edit?gid=0#gid=0'''

magic_checkout_pricing_mx_db = sqlContext.sql("""
select * from batch_sheets.magic_checkout_pricing_merchants
                             """)
magic_checkout_pricing_mx_df = magic_checkout_pricing_mx_db.toPandas()
print(magic_checkout_pricing_mx_df.shape)
magic_checkout_pricing_mx_df = magic_checkout_pricing_mx_df.rename(columns={'mid': 'merchant_id'})
magic_checkout_pricing_mx_df.head()

# COMMAND ----------

magic_checkout_pricing_mx_df['pricing_live_date'].max()

# COMMAND ----------

cod_payments_db = sqlContext.sql("""
                                  select 
                                  payments.merchant_id,
                                  payments.created_date ,
                                  (payments.base_amount  /100.00 ) as cod_gmv,
                                  id as cod_txn
                                  ---  count(distinct id) as cod_txn,
                                  ---  sum(payments.base_amount  /100.00 ) as cod_gmv
                                   from realtime_hudi_api.payments 
                                  --- inner join batch_sheets.magic_checkout_pricing_merchants 
                                   ---on payments.merchant_id = magic_checkout_pricing_merchants.mid
                                   ---and payments.created_date >= magic_checkout_pricing_merchants.pricing_live_date
                        
                                   where date_format(date(created_date), 'yyyy-MM') = '{0}'
                                   and method='cod'
                                   and order_id in (
                                     select order_id from realtime_hudi_api.order_meta
                                     where date_format(date(created_date), 'yyyy-MM') = '{0}'
                                     and type='one_click_checkout'
                                   )
                                  --- group by 1
                                   
                                    """.format(reporting_month))
cod_payments_df = cod_payments_db.toPandas()
print(cod_payments_df.shape)
cod_payments_df.head()

                                              

# COMMAND ----------

mid_gr_df = cod_payments_df.groupby(by='merchant_id').agg({'cod_gmv':'sum', 'cod_txn':'nunique',}).reset_index()
mid_gr_df['cod_gmv'].sum()

# COMMAND ----------

mid_gr_df.to_csv('/dbfs/FileStore/mid_gr_df.csv', index=False)
"https://razorpay-dev.cloud.databricks.com/files/mid_gr_df.csv"

# COMMAND ----------

magic_checkout_pricing_mx_df['pricing_live_date'] = pd.to_datetime(magic_checkout_pricing_mx_df['pricing_live_date']).dt.strftime('%Y-%m-%d')
merged_temp = cod_payments_df.merge(magic_checkout_pricing_mx_df, how='inner', on='merchant_id', )
filtered_merged_temp = merged_temp[merged_temp['created_date'] >= merged_temp['pricing_live_date']]
filtered_merged_temp.head()

# COMMAND ----------

filtered_merged_temp = filtered_merged_temp.groupby(by=['merchant_id','merchant_name']).agg({'cod_gmv':'sum',
                                                                                             'cod_txn':'nunique',
                                                                                        'prepaid':'max',
                                                                                         'cod':'max',
                                                                                        'pricing_live_date':'max',
                                                                                        }).reset_index()
filtered_merged_temp.head()                                                                                       

# COMMAND ----------

filtered_merged_temp.sort_values(by='cod_gmv', ascending=False).head(20)

# COMMAND ----------

magic_reimbursement_df = pd.read_csv('/dbfs/FileStore/magic_rto_reimbursement_tracker_summary.csv',)
magic_reimbursement_df.head()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Compiling COD Results

# COMMAND ----------

#cod_final_df = pd.merge(cod_payments_df, magic_checkout_pricing_mx_df,  on='merchant_id',how='inner')
#cod_final_df = pd.merge(cod_final_df, magic_reimbursement_df[(magic_reimbursement_df['payment_month_year']==reporting_month) & (magic_reimbursement_df['invoice_status']=='Success')],  on='merchant_id',how='left')

# temp additions
cod_final_df = pd.merge(filtered_merged_temp, magic_reimbursement_df[(magic_reimbursement_df['payment_month_year']==reporting_month) & (magic_reimbursement_df['invoice_status']=='Success')],  on='merchant_id',how='left')
cod_final_df.head()

# COMMAND ----------

cod_final_df = cod_final_df.rename(
    columns={
        'merchant_id': 'MID',
        'cod_txn': '# of txns',
        'cod_gmv': 'COD GMV',
        'cod': 'Magic fee (%)',
        'rto_amount':'Reimbursement',
    
    }
    
)
cod_final_df.head()

# COMMAND ----------

cod_final_df[cod_final_df['MID']=='DkYdOAXPbbqFwT']

# COMMAND ----------

cod_final_df.dtypes

# COMMAND ----------

def percentage_to_float(percentage_str):
    return float(percentage_str.strip('%')) / 100

# COMMAND ----------

cod_final_df['Month'] = reporting_month
cod_final_df['Magic fee (%)'] = cod_final_df['Magic fee (%)'].astype(str).apply(percentage_to_float)
cod_final_df['Magic fees (to be debited)'] = cod_final_df['COD GMV'].astype(float) * cod_final_df['Magic fee (%)']
cod_final_df['Reimbursement'] = cod_final_df['Reimbursement'].fillna(0)
cod_final_df['NR'] = cod_final_df['Magic fees (to be debited)'] - cod_final_df['Reimbursement']
cod_final_df.head()

# COMMAND ----------

mid_final = cod_final_df.groupby(by='MID').agg({'NR':'sum'}).reset_index()
mid_final.head()

# COMMAND ----------

mid_final['NR'].sum()

# COMMAND ----------

mid_final.sort_values(by='NR').head(20)

# COMMAND ----------


cod_final_df[['MID', '# of txns', 'COD GMV','Magic fee (%)','Magic fees (to be debited)','Month','Reimbursement','rto_invoiced','NR'  ]].to_csv('/dbfs/FileStore/cod_monthly_finance_report.csv', index=False)
"https://razorpay-dev.cloud.databricks.com/files/cod_monthly_finance_report.csv"

# COMMAND ----------

exit()

# COMMAND ----------

# MAGIC %md
# MAGIC #Prepaid

# COMMAND ----------

all_merchants_db = sqlContext.sql("""
                                  select 
                                  distinct merchant_id
                                   from realtime_hudi_api.payments
                        
                                   where date_format(date(created_date), 'yyyy-MM') = '{0}'
                                   and authorized_at is not null
                                   and order_id in (
                                     select order_id from realtime_hudi_api.order_meta
                                     where date_format(date(created_date), 'yyyy-MM') = '{0}'
                                     and type='one_click_checkout'
                                   )
                                    """.format(reporting_month))
all_merchants_df = all_merchants_db.toPandas()
print(all_merchants_df.shape)
all_merchants_df.head()

# COMMAND ----------

prepaid_db = sqlContext.sql("""
SELECT payments.merchant_id AS merchant_id,
/*
merchants.name AS "Merchant Name",
merchants.website AS "Merchant Website",

payments.gateway AS "Gateway", 
payments.method AS "Method", 
CASE 
		   	WHEN ((payments.method = 'card' ) AND payments.base_amount <= 200000) THEN 'LTE_2k'
		   	WHEN ((payments.method = 'card' ) AND payments.base_amount > 200000) THEN 'GT_2k' 
		   	ELSE 'Other method' 
	   END AS "Amount Range Flag",
payments.terminal_id AS "Terminal ID",
payments.settled_by AS "Settled By",
 */
IF ((pricing.percent_rate = 0 AND pricing.fixed_rate > 0) OR (pricing.percent_rate = 0 AND pricing.fixed_rate = 0 AND pricing.min_fee > 0) OR (pricing.max_fee IS NOT NULL AND fees_breakup.amount = pricing.max_fee), 'Flat Pricing', IF ((pricing.percent_rate = 0 AND pricing.fixed_rate = 0) or fees_breakup.id is null, 'Zero Pricing', 'Percentage Pricing')) AS pricing_type, 
	   
	   IF( (pricing.percent_rate = 0 AND pricing.fixed_rate > 0), pricing.fixed_rate/100.00, IF( (pricing.percent_rate = 0 AND pricing.fixed_rate = 0 AND pricing.min_fee > 0), pricing.min_fee/100.00, IF( (pricing.max_fee IS NOT NULL AND fees_breakup.amount = pricing.max_fee), pricing.max_fee/100.00, IF((pricing.percent_rate = 0 AND pricing.fixed_rate = 0) or fees_breakup.id is null, 0, pricing.percent_rate/100.00 )))) AS pricing_rate,
    
--date_format(from_unixtime(payments.captured_at+19800), '%d-%m-%Y') AS "Date",
COUNT(*) AS count,
SUM(payments.base_amount) / 100.00 AS amount,
SUM(COALESCE(fees_breakup.amount,0)) / 100.00 AS magic_checkout_income
FROM realtime_hudi_api.payments
JOIN realtime_hudi_api.terminals ON terminals.id = payments.terminal_id
JOIN realtime_hudi_api.order_meta  AS order_meta ON payments.order_id =order_meta.order_id
LEFT JOIN realtime_hudi_api.fees_breakup ON payments.transaction_id = fees_breakup.transaction_id AND fees_breakup.name = 'magic_checkout' AND fees_breakup.created_date >= '{0}-01' 
LEFT JOIN realtime_hudi_api.pricing ON fees_breakup.pricing_rule_id = pricing.id 
where captured_at is not NULL
and date_format(from_unixtime(payments.captured_at+19800), 'yyyy-MM') = '{0}'
--where payments.captured_at between 1656613800 AND 1659292199 
--and payments.created_date >= '2022-08-01'
--and payments.created_date < '2022-08-02'
---AND order_meta.created_date  >= '{0}-01' 
AND order_meta.type = 'one_click_checkout'
group by 1,2,3
 """.format(reporting_month))
prepaid_df = prepaid_db.toPandas()
print(prepaid_df.shape)
prepaid_df.head()

# COMMAND ----------

prepaid_df['magic_checkout_income'].sum()
