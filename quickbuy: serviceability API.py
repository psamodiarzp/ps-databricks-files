# Databricks notebook source
start_date = '2024-12-01'
end_date = '2024-12-07'

# COMMAND ----------

raw_data_db = sqlContext.sql(
    """
with cte as(
  select 
 get_json_object(properties,'$.data.meta.is_quickbuy_flow') as is_quickbuy_flow,
get_json_object(  properties, '$.data.meta.quickbuy_eligible'   ) AS v2_eligible,
get_json_object(properties,'$.data.name') as api_name,
checkout_id
---event_name, properties
from aggregate_pa.cx_1cc_events_dump_v1
where producer_created_date between date('{0}') and date('{1}')
and event_name = 'network_request_success'
and get_json_object(properties,'$.data.name') in ('cod_eligibility','serviceability_fetch')
),
cte2 AS(
with payments_cte as(
select 
id,
method, 
authorized_at,
gateway,
error_code,
  error_description,
  internal_error_code,
  order_id

from realtime_hudi_api.payments 
where created_date between ('{0}') and ('{1}') 
 and merchant_id <> 'NSqAZqldhXIvVF'                                                       --- TEMPORARY DELETE
  )
  select 
  p.id,
  p.method, 
p.authorized_at,
p.gateway,

  checkout_id,
  error_code,
  error_description,
  internal_error_code
      from payments_cte as p
 inner join (
    select payment_id, checkout_id from realtime_hudi_api.payment_analytics 
    WHERE created_date between ('{0}') and ('{1}') 
  )pa on p.id = pa.payment_id
  inner join (
   select order_id from realtime_hudi_api.order_meta
    WHERE created_date between ('{0}') and ('{1}') 
  )om on p.order_id = om.order_id
)
select 
*
from cte 
left join cte2 on cte.checkout_id = cte2.checkout_id
left join aggregate_pa.magic_checkout_fact mcf on cte.checkout_id = mcf.checkout_id
where mcf.producer_created_date between date('{0}') and date('{1}') 
and (mcf.library <> 'magic-x' or mcf.library is null)
and (mcf.checkout_integration_type <> 'x' or mcf.checkout_integration_type is null)
""".format(
        start_date, end_date
    )
)
raw_data_df = raw_data_db.toPandas()
raw_data_df.head()

# COMMAND ----------


