# Databricks notebook source
import pandas as pd

# COMMAND ----------

base_db = sqlContext.sql(
    """

With magic_cr AS
(SELECT d.merchant_id,-- sales_merchant_status, segment, live_date,
month(producer_created_date) as mnth,
NULLIF(COUNT(DISTINCT CASE WHEN event_name = 'open' THEN session_id ELSE NULL END),0) as opens

FROM aggregate_pa.cx_1cc_events_dump_v1 d
---INNER JOIN aggregate_ba.PA_mhi_magic_checkout_cr as m
--ON m.merchant_id = d.merchant_id
WHERE event_name = 'open'
AND d.merchant_id<>'Hb4PVe74lPmk0k'
AND producer_created_date between date('2024-01-01') and date('2024-04-30')
--(DATE_DIFF('day', date(producer_created_date), NOW()) between 1 and 31)
GROUP BY 1,2),

--- added this new cte to get the successful payments MID level
payment_success as (
    select
    p.merchant_id,
    month(p.created_date) as mnth,
    NULLIF(COUNT(distinct case when p.authorized_at is not null or p.method='cod' then p.id ELSE NULL END),0) as payment_successful

    from realtime_hudi_api.payments as p
    INNER JOIN realtime_hudi_api.order_meta as om
    on om.order_id = p.order_id
    INNER JOIN aggregate_ba.PA_mhi_magic_checkout_cr as magic_base
    ON magic_base.merchant_id = p.merchant_id
    where  date(p.created_date) between date('2024-01-01') and date('2024-04-30')
    group by 1,2
)
select
magic_cr.merchant_id ,
magic_cr.mnth,
round((payment_success.payment_successful*1.0)*100.00/(magic_cr.opens*1.0),2)  as magic_cr_percentage
from magic_cr
left join payment_success 
ON magic_cr.merchant_id =payment_success.merchant_id
and magic_cr.mnth = payment_success.mnth
ORDER BY 2,1
        """)
base_table = base_db.toPandas()
base_table.head()

# COMMAND ----------

base_table.head(10)

# COMMAND ----------

base_pivot = base_table.pivot(columns='mnth', index='merchant_id', values='magic_cr_percentage').reset_index()
base_pivot.head(20)

# COMMAND ----------



# COMMAND ----------

base_pivot = pd.read_csv('/dbfs/FileStore/mom_cr_pivot.csv')
base_pivot.head()

# COMMAND ----------

base_pivot['2-1'] = base_pivot['2'] - base_pivot['1']
base_pivot['3-2'] = base_pivot['3'] - base_pivot['2']
base_pivot['4-3'] = base_pivot['4'] - base_pivot['3']


# COMMAND ----------

base_pivot.head(20)

# COMMAND ----------

base_pivot.to_csv('/dbfs/FileStore/mom_cr_pivot.csv', index=False)
"https://razorpay-dev.cloud.databricks.com/files/mom_cr_pivot.csv"

# COMMAND ----------


