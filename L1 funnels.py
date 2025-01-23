# Databricks notebook source
# MAGIC %md
# MAGIC ###UPI funnel:  Intent app selected (Generic)
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC with cte as(
# MAGIC
# MAGIC   SELECT
# MAGIC   weekofyear(date(producer_created_date)) as wk,
# MAGIC event_name,
# MAGIC  get_json_object (context,'$.checkout_id') as checkout_id
# MAGIC     
# MAGIC      
# MAGIC FROM events.lumberjack_intermediate 
# MAGIC WHERE  LOWER(get_json_object(context, '$.library')) in ('checkoutjs', 'hosted') 
# MAGIC   AND producer_created_date between '2024-09-16' and '2024-09-29'
# MAGIC   AND upper(event_name) = upper('UPI:APP_SELECTED') 
# MAGIC )  
# MAGIC select 
# MAGIC wk,
# MAGIC event_name,
# MAGIC count(distinct cte.checkout_id)
# MAGIC from cte
# MAGIC where checkout_id in (
# MAGIC select  checkout_id from aggregate_pa.cx_lo_fact_ism_v1
# MAGIC where producer_created_date between '2024-09-16' and '2024-09-29'
# MAGIC )
# MAGIC group by 1,2
# MAGIC order by 1

# COMMAND ----------

# MAGIC %md
# MAGIC ### UPI funnel : Intent apps shown (generic) 

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Intent apps shown (generic) : render:checkoutUpiIntentAppsShownOnHome
# MAGIC -- Intent app selected (Generic) : 'UPI:APP_SELECTED'
# MAGIC
# MAGIC -- others selected : 'UPI:OTHERS_SELECTED'
# MAGIC
# MAGIC -- Intent apps shown (generic) : 
# MAGIC with cte as(
# MAGIC
# MAGIC   SELECT
# MAGIC   weekofyear(date(producer_created_date)) as wk,
# MAGIC event_name,
# MAGIC  get_json_object (context,'$.checkout_id') as checkout_id
# MAGIC     
# MAGIC      
# MAGIC FROM events.lumberjack_intermediate 
# MAGIC WHERE  LOWER(get_json_object(context, '$.library')) in ('checkoutjs', 'hosted') 
# MAGIC   AND producer_created_date between '2024-09-16' and '2024-09-29'
# MAGIC   AND lower(event_name) = lower('render:checkoutUpiIntentAppsShownOnHome')
# MAGIC )  
# MAGIC select 
# MAGIC wk,
# MAGIC event_name,
# MAGIC count(distinct cte.checkout_id)
# MAGIC from cte
# MAGIC where checkout_id in (
# MAGIC select  checkout_id from aggregate_pa.cx_lo_fact_ism_v1
# MAGIC where producer_created_date between '2024-09-16' and '2024-09-29'
# MAGIC )
# MAGIC group by 1,2
# MAGIC order by 1

# COMMAND ----------

# MAGIC %md
# MAGIC ### VI: Upi selection and submission

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select 
# MAGIC weekofyear(date(producer_created_date)) as wk,
# MAGIC count(distinct checkout_id) as renders,
# MAGIC count(distinct case when select_method = 'upi' then checkout_id else null end) as  upi_select,
# MAGIC count(distinct case when submit_method = 'upi' then checkout_id else null end) as  upi_submit
# MAGIC from aggregate_pa.cx_lo_fact_ism_v1
# MAGIC where producer_created_date between '2024-09-16' and '2024-09-29'
# MAGIC and get_json_object(render_properties, '$.data.meta.v2_result') <> 'v2'
# MAGIC group by 1

# COMMAND ----------

# MAGIC %md
# MAGIC ### UPI Prefill Method shown

# COMMAND ----------

# MAGIC
# MAGIC %sql      
# MAGIC with cte as(
# MAGIC
# MAGIC   SELECT
# MAGIC   weekofyear(date(producer_created_date)) as wk,
# MAGIC event_name,
# MAGIC  get_json_object (context,'$.checkout_id') as checkout_id
# MAGIC     
# MAGIC      
# MAGIC FROM events.lumberjack_intermediate 
# MAGIC WHERE  LOWER(get_json_object(context, '$.library')) in ('checkoutjs', 'hosted') 
# MAGIC   AND producer_created_date between '2024-09-16' and '2024-09-29'
# MAGIC    AND event_name = 'render:complete'
# MAGIC       and (length(get_json_object(properties, '$.options.prefill.method')) > 0  or get_json_object(properties, '$.options.prefill.method') is not null)
# MAGIC )  
# MAGIC select 
# MAGIC wk,
# MAGIC event_name,
# MAGIC count(distinct cte.checkout_id),
# MAGIC count(distinct submit_checkout_id)
# MAGIC from cte
# MAGIC  inner join (
# MAGIC select  checkout_id, submit_checkout_id from aggregate_pa.cx_lo_fact_ism_v1
# MAGIC where producer_created_date between '2024-09-16' and '2024-09-29'
# MAGIC ) lo on cte.checkout_id = lo.checkout_id
# MAGIC group by 1,2
# MAGIC order by 1

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Intent apps shown (p13) 
# MAGIC with base as 
# MAGIC (
# MAGIC   SELECT
# MAGIC  weekofyear(date(producer_created_date)) as wk,
# MAGIC event_name,
# MAGIC      get_json_object (context,'$.checkout_id') as checkout_id,
# MAGIC      get_json_object(properties, '$.data.instrumentsShown[0].method') as method_1,
# MAGIC      get_json_object(properties, '$.data.instrumentsShown[0].flow') as flow_1,
# MAGIC         get_json_object(properties, '$.data.instrumentsShown[1].method') as method_2,
# MAGIC      get_json_object(properties, '$.data.instrumentsShown[1].flow') as flow_2,
# MAGIC         get_json_object(properties, '$.data.instrumentsShown[2].method') as method_3,
# MAGIC      get_json_object(properties, '$.data.instrumentsShown[2].flow') as flow_3
# MAGIC      
# MAGIC FROM events.lumberjack_intermediate
# MAGIC WHERE  LOWER(get_json_object(context, '$.library')) in ('checkoutjs', 'hosted') 
# MAGIC   AND producer_created_date  between '2024-09-16' and '2024-09-29'
# MAGIC   AND upper(event_name) = upper('metric:p13n:instruments_shown')
# MAGIC   )
# MAGIC   
# MAGIC select wk, event_name, count(distinct checkout_id)
# MAGIC from base
# MAGIC where (method_1 = 'upi' or method_2 = 'upi' or method_3 = 'upi')
# MAGIC and (flow_1 = 'intent' or flow_2 = 'intent' or flow_3 = 'intent')
# MAGIC group by 1,2

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC ---intent apps selected p13n 
# MAGIC with cte as
# MAGIC (
# MAGIC   SELECT
# MAGIC    weekofyear(date(producer_created_date)) as wk,
# MAGIC event_name,
# MAGIC      get_json_object (context,'$.checkout_id') as checkout_id,
# MAGIC      get_json_object(properties, '$.data.instrumentMeta.block.code') as type,
# MAGIC      get_json_object(properties, '$.data.instrument.method') as method,
# MAGIC      get_json_object(properties, '$.data.instrument.flows[0]') as flow,
# MAGIC *
# MAGIC FROM events.lumberjack_intermediate
# MAGIC WHERE  LOWER(get_json_object(context, '$.library')) in ('checkoutjs', 'hosted') 
# MAGIC   AND producer_created_date between '2024-09-16' and '2024-09-29'
# MAGIC   AND upper(event_name) = upper('behav:instrument:select')
# MAGIC   )
# MAGIC   select wk, event_name, count(distinct checkout_id)
# MAGIC from cte
# MAGIC   where type = 'rzp.preferred'
# MAGIC   and method = 'upi'
# MAGIC   and flow = 'intent'
# MAGIC   group by 1,2

# COMMAND ----------

# MAGIC %sql
# MAGIC ---'UPI:OTHERS_SELECTED'
# MAGIC with cte as(
# MAGIC
# MAGIC   SELECT
# MAGIC   weekofyear(date(producer_created_date)) as wk,
# MAGIC event_name,
# MAGIC  get_json_object (context,'$.checkout_id') as checkout_id
# MAGIC     
# MAGIC      
# MAGIC FROM events.lumberjack_intermediate 
# MAGIC WHERE  LOWER(get_json_object(context, '$.library')) in ('checkoutjs', 'hosted') 
# MAGIC   AND producer_created_date between '2024-09-16' and '2024-09-29'
# MAGIC   AND lower(event_name) = lower('UPI:OTHERS_SELECTED')
# MAGIC )  
# MAGIC select 
# MAGIC wk,
# MAGIC event_name,
# MAGIC count(distinct cte.checkout_id)
# MAGIC from cte
# MAGIC where checkout_id in (
# MAGIC select  checkout_id from aggregate_pa.cx_lo_fact_ism_v1
# MAGIC where producer_created_date between '2024-09-16' and '2024-09-29'
# MAGIC )
# MAGIC group by 1,2
# MAGIC order by 1

# COMMAND ----------


