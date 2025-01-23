# Databricks notebook source
# Pseudo Code
# Cte: get data from date >= -1
# Cte2: group data of 
    ## 1. Cte
    ## 2. Data from existing agg table where checkout id from cte is in the existing table
# Cte3: Maximize on cte2 and have it in order as required for partitioned tables
# Delete from existing agg table where checkout id from cte3 is in the existing table
# Insert temp table data into existing table
# Empty temp table

# Please see that unlike magic checkout fact, here we do not delete existing table at any point.




# COMMAND ----------

# MAGIC %sql
# MAGIC select *  FROM events.lumberjack_intermediate as l
# MAGIC             WHERE source = 'checkoutjs'
# MAGIC    and producer_created_date = cast(DATEADD(day,-1,current_date) as string)
# MAGIC    limit 4

# COMMAND ----------

base_db = sqlContext.sql(
    """
    alter table analytics_selfserve.checkout_fact_emr
    add column (method_selected int)
        """)
base_table = base_db.toPandas()
base_table.head()

# COMMAND ----------

base_db = sqlContext.sql(
    """
    describe analytics_selfserve.delta_checkout_fact_emr

        """)
base_table = base_db.toPandas()
base_table

# COMMAND ----------

base_db = sqlContext.sql(
    """
    select producer_created_date, count(*)
    from analytics_selfserve.checkout_fact_emr
    group by 1
    order by 1 desc
    """)
base_table = base_db.toPandas()
base_table.head()

# COMMAND ----------

# DBTITLE 1,SQL2: Insertion Code
# MAGIC %sql
# MAGIC   INSERT INTO
# MAGIC   analytics_selfserve.delta_checkout_fact_emr
# MAGIC with cte as(
# MAGIC SELECT
# MAGIC   get_json_object(context, '$.checkout_id') AS checkout_id,
# MAGIC date(producer_created_date) as producer_created_date,
# MAGIC   CASE
# MAGIC     WHEN event_name = 'open' THEN 1
# MAGIC     ELSE 0
# MAGIC   END AS open,
# MAGIC   CASE
# MAGIC     WHEN event_name = 'render:complete' THEN 1
# MAGIC     ELSE 0
# MAGIC   END AS checkout_render_complete,
# MAGIC   CASE
# MAGIC     WHEN event_name = 'render:contact_page' THEN 1
# MAGIC     ELSE 0
# MAGIC   END AS contact_page_rendered,
# MAGIC   CASE
# MAGIC     WHEN event_name = 'render:1cc_payment_home_screen_loaded' THEN 1
# MAGIC     ELSE 0
# MAGIC   END AS payment_screen_loaded,
# MAGIC   CASE
# MAGIC     WHEN event_name = 'behav:contact:fill' THEN 1
# MAGIC     ELSE 0
# MAGIC   END AS contact_filled,
# MAGIC
# MAGIC   CASE
# MAGIC     WHEN event_name = 'behav:continue:click' THEN 1
# MAGIC     ELSE 0
# MAGIC   END AS contact_page_cta_clicked,
# MAGIC   CASE
# MAGIC     WHEN event_name = 'instrument:selected' THEN 1
# MAGIC     ELSE 0
# MAGIC   END AS instrument_selected,
# MAGIC   CASE
# MAGIC     WHEN event_name = 'submit' THEN 1
# MAGIC     ELSE 0
# MAGIC   END AS submit,
# MAGIC   CASE
# MAGIC     WHEN event_name = 'oncomplete' THEN 1
# MAGIC     ELSE 0
# MAGIC   END AS payment_completed,
# MAGIC     current_date as last_updated_date,
# MAGIC     CASE
# MAGIC     WHEN event_name = 'method:selected' THEN 1
# MAGIC     ELSE 0
# MAGIC      END AS method_selected
# MAGIC    FROM events.lumberjack_intermediate as l
# MAGIC             WHERE source = 'checkoutjs'
# MAGIC    and date(producer_created_date) BETWEEN DATEADD(day,-2,current_date) and DATEADD(day,-1,current_date)
# MAGIC    -->= DATEADD(day,-1,current_date)
# MAGIC
# MAGIC ), 
# MAGIC --- ### CHECKS IF THERE IS DATA FOR THE NEW CHECKOUT IDS ALREADY IN TABLE
# MAGIC cte2 as(
# MAGIC select 
# MAGIC checkout_id,
# MAGIC open,
# MAGIC checkout_render_complete,
# MAGIC contact_page_rendered,
# MAGIC payment_screen_loaded,
# MAGIC contact_filled,
# MAGIC contact_page_cta_clicked,
# MAGIC instrument_selected,
# MAGIC submit,
# MAGIC payment_completed,
# MAGIC last_updated_date,
# MAGIC producer_created_date,
# MAGIC method_selected
# MAGIC from cte
# MAGIC UNION ALL
# MAGIC select * from analytics_selfserve.checkout_fact_emr 
# MAGIC where checkout_id in ( 
# MAGIC select distinct checkout_id from cte)
# MAGIC )
# MAGIC --- ### GETS THE MAX VALUE FOR THE NEW + REPEATING CHECKOUT IDS ALREADY IN TABLE
# MAGIC select 
# MAGIC checkout_id,
# MAGIC    max(open) as open,
# MAGIC max(checkout_render_complete) as checkout_render_complete,
# MAGIC max(contact_page_rendered) as contact_page_rendered,
# MAGIC     max(payment_screen_loaded) as payment_screen_loaded,
# MAGIC     max(contact_filled) as contact_filled,
# MAGIC     max(contact_page_cta_clicked) as contact_page_cta_clicked,
# MAGIC    max(instrument_selected) as instrument_selected,
# MAGIC     max(submit) as submit,
# MAGIC     max(payment_completed) as payment_completed,
# MAGIC  max(last_updated_date) as last_updated_date,
# MAGIC  min(producer_created_date) as producer_created_date,
# MAGIC      max( method_selected) as method_selected
# MAGIC
# MAGIC    from cte2
# MAGIC       group by 1

# COMMAND ----------

base_table.head(10)

# COMMAND ----------

#STEP 4
sqlContext.sql(
    """
DELETE FROM analytics_selfserve.checkout_fact_emr 
WHERE checkout_id in ( 
select distinct checkout_id from analytics_selfserve.delta_checkout_fact_emr )
""")

# COMMAND ----------

#STEP 5
sqlContext.sql(
    """
INSERT INTO analytics_selfserve.checkout_fact_emr 
SELECT * FROM analytics_selfserve.delta_checkout_fact_emr
""")

# COMMAND ----------

# MAGIC %md
# MAGIC method:selected

# COMMAND ----------

#STEP 6
sqlContext.sql(
    """
DELETE FROM analytics_selfserve.delta_checkout_fact_emr
""")

# COMMAND ----------

# MAGIC %md
# MAGIC FINAL STEP: STOP HERE!!
# MAGIC

# COMMAND ----------

base_db = sqlContext.sql(
    """
    select method_selected, count(distinct checkout_id)
    from analytics_selfserve.checkout_fact_emr
    --where event_name='method:selected'
   group by 1
    order by 1 desc
    """)
base_table = base_db.toPandas()
base_table.head(20)

# COMMAND ----------

base_table.head(10)

# COMMAND ----------

sqlContext.sql(
    """

explain Delete analytics_selfserve.checkout_fact_emr a
from analytics_selfserve.checkout_fact_emr a
inner join analytics_selfserve.delta_checkout_fact_emr b 
on a.checkout_id = b.checkout_id
where b.checkout_id is not null

    """


)

# COMMAND ----------

#====== CREATING THE REQUIRED TABLES =========================
sqlContext.sql(
    """
Create table if not exists analytics_selfserve.temp_checkout_fact(
checkout_id string,
open integer,
checkout_render_complete  integer,
contact_page_rendered integer,
payment_screen_loaded integer,
contact_filled integer,
contact_page_cta_clicked integer,
instrument_selected integer,
submit integer,
payment_completed integer,
last_updated_date date,
producer_created_date date

)USING PARQUET
  PARTITIONED BY (producer_created_date)
    """


)

# COMMAND ----------

#====== CREATING THE REQUIRED TABLES =========================
sqlContext.sql(
    """
Create table if not exists analytics_selfserve.checkout_fact(
checkout_id string,
open integer,
checkout_render_complete  integer,
contact_page_rendered integer,
payment_screen_loaded integer,
contact_filled integer,
contact_page_cta_clicked integer,
instrument_selected integer,
submit integer,
payment_completed integer,
last_updated_date date,
producer_created_date date

)USING PARQUET
  PARTITIONED BY (producer_created_date)
    """


)

# COMMAND ----------

### INSERTION CODE FOR YESTERDAY'S DATA #####
sqlContext.sql(
    """
    INSERT INTO
  analytics_selfserve.temp_checkout_fact
with cte as(
SELECT
  json_extract_scalar (context, '$.checkout_id') AS checkout_id,
date(producer_created_date) as producer_created_date,
  CASE
    WHEN event_name = 'open' THEN 1
    ELSE 0
  END AS open,
  CASE
    WHEN event_name = 'render:complete' THEN 1
    ELSE 0
  END AS checkout_render_complete,
  CASE
    WHEN event_name = 'render:contact_page' THEN 1
    ELSE 0
  END AS contact_page_rendered,
  CASE
    WHEN event_name = 'render:1cc_payment_home_screen_loaded' THEN 1
    ELSE 0
  END AS payment_screen_loaded,
  CASE
    WHEN event_name = 'behav:contact:fill' THEN 1
    ELSE 0
  END AS contact_filled,

  CASE
    WHEN event_name = 'behav:continue:click' THEN 1
    ELSE 0
  END AS contact_page_cta_clicked,
  CASE
    WHEN event_name = 'instrument:selected' THEN 1
    ELSE 0
  END AS instrument_selected,
  CASE
    WHEN event_name = 'submit' THEN 1
    ELSE 0
  END AS submit,
  CASE
    WHEN event_name = 'oncomplete' THEN 1
    ELSE 0
  END AS payment_completed,
    current_date as last_updated_date
   FROM hive.events.lumberjack_intermediate as l
            WHERE source = 'checkoutjs'
   and producer_created_date >= cast(DATE_ADD('day',-1,current_date) as varchar)
), 
--- ### CHECKS IF THERE IS DATA FOR THE NEW CHECKOUT IDS ALREADY IN TABLE
cte2 as(
select 
checkout_id,
open,
checkout_render_complete,
contact_page_rendered,
payment_screen_loaded,
contact_filled,
contact_page_cta_clicked,
instrument_selected,
submit,
payment_completed,
last_updated_date,
producer_created_date
from cte
UNION ALL
select * from analytics_selfserve.checkout_fact 
where checkout_id in ( 
select distinct checkout_id from cte)
)
--- ### GETS THE MAX VALUE FOR THE NEW + REPEATING CHECKOUT IDS ALREADY IN TABLE
select 
checkout_id,
min(producer_created_date) as producer_created_date,
   max(open) as open,
max(checkout_render_complete) as checkout_render_complete,
max(contact_page_rendered) as contact_page_rendered,
    max(payment_screen_loaded) as payment_screen_loaded,
    max(contact_filled) as contact_filled,
    max(contact_page_cta_clicked) as contact_page_cta_clicked,
   max(instrument_selected) as instrument_selected,
    max(submit) as submit,
    max(payment_completed) as payment_completed,
 max(last_updated_date) as last_updated_date
   from cte2
   group by 1
   
    """
)

# COMMAND ----------

### DELETES REPEATING CHECKOUT IDS FROM THE EXISTING TABLES

sqlContext.sql(

    """

    DELETE FROM analytics_selfserve.checkout_fact 
WHERE checkout_id in ( 
select distinct checkout_id from analytics_selfserve.temp_checkout_fact )

    """
)

# COMMAND ----------

### INSERTS NEW + REPEATING CHECKOUT IDS TO THE EXISTING TABLES

sqlContext.sql(

    """

INSERT INTO analytics_selfserve.checkout_fact 
SELECT * FROM analytics_selfserve.temp_checkout_fact 

    """
)
