# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Load data from MySQL to Delta Lake
# MAGIC
# MAGIC This notebook shows you how to import data from JDBC MySQL databases into a Delta Lake table using Python.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step 1: Connection information
# MAGIC
# MAGIC First define some variables to programmatically create these connections.
# MAGIC
# MAGIC Replace all the variables in angle brackets `<>` below with the corresponding information.

# COMMAND ----------

driver = "org.mariadb.jdbc.Driver"

database_host = "<database-host-url>"
database_port = "3306" # update if you use a non-default port
database_name = "<database-name>"
table = "<table-name>"
user = "<username>"
password = "<password>"

url = f"jdbc:mysql://{database_host}:{database_port}/{database_name}"

print(url)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC The full URL printed out above should look something like:
# MAGIC
# MAGIC ```
# MAGIC jdbc:mysql://localhost:3306/my_database
# MAGIC ```
# MAGIC
# MAGIC ### Check connectivity
# MAGIC
# MAGIC Depending on security settings for your MySQL database and Databricks workspace, you may not have the proper ports open to connect.
# MAGIC
# MAGIC Replace `<database-host-url>` with the universal locator for your MySQL implementation. If you are using a non-default port, also update the 3306.
# MAGIC
# MAGIC Run the cell below to confirm Databricks can reach your MySQL database.

# COMMAND ----------

# MAGIC %sh
# MAGIC nc -vz "<database-host-url>" 3306

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step 2: Reading the data
# MAGIC
# MAGIC Now that you've specified the file metadata, you can create a DataFrame. Use an *option* to infer the data schema from the file. You can also explicitly set this to a particular schema if you have one already.
# MAGIC
# MAGIC First, create a DataFrame in Python, referencing the variables defined above.

# COMMAND ----------

remote_table = (spark.read
    .format("jdbc")
    .option("driver", driver)
    .option("url", url)
    .option("dbtable", table)
    .option("user", user)
    .option("password", password)
    .load()
)

# COMMAND ----------

# MAGIC %md
# MAGIC You can view the results of this remote table query.

# COMMAND ----------

display(remote_table)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step 3: Create a Delta table
# MAGIC
# MAGIC The DataFrame defined and displayed above is a temporary connection to the remote database.
# MAGIC
# MAGIC To ensure that this data can be accessed by relevant users througout your workspace, save it as a Delta Lake table using the code below.

# COMMAND ----------

target_table_name = "<target-schema>.<target-table-name>"
remote_table.write.mode("overwrite").saveAsTable(target_table_name)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC This table will persist across cluster sessions, notebooks, and personas throughout your organization.
# MAGIC
# MAGIC The code below demonstrates querying this data with Python and SQL.

# COMMAND ----------

display(spark.table(target_table_name))

# COMMAND ----------

# MAGIC %sql
# MAGIC  WITH cte AS(
# MAGIC   SELECT
# MAGIC     order_id,
# MAGIC     checkout_id,
# MAGIC     merchant_id,
# MAGIC     CASE
# MAGIC       WHEN event_name = 'render:complete'
# MAGIC       AND LOWER(get_json_object (context, '$.platform')) = 'mobile_sdk' THEN 1
# MAGIC       WHEN event_name = 'render:complete'
# MAGIC       AND LOWER(get_json_object (context, '$.platform')) = 'browser'
# MAGIC       AND event_name = 'render:complete'
# MAGIC       AND get_json_object (properties, '$.data.meta.is_mobile') = 'true' THEN 2
# MAGIC       WHEN event_name = 'render:complete'
# MAGIC       AND LOWER(get_json_object (context, '$.platform')) = 'browser'
# MAGIC       AND (
# MAGIC         get_json_object (properties, '$.data.meta.is_mobile') = 'false'
# MAGIC         OR get_json_object (properties, '$.data.meta.is_mobile') IS NULL
# MAGIC       ) THEN 3
# MAGIC       ELSE 0
# MAGIC     END as platform,
# MAGIC   CASE
# MAGIC       WHEN event_name = 'render:complete' THEN get_json_object(properties, '$.data.meta.initial_loggedIn')
# MAGIC     END AS initial_loggedin,
# MAGIC     /*  CASE
# MAGIC               WHEN event_name = 'render:1cc_summary_screen_loaded_completed' THEN json_extract(
# MAGIC                 properties,
# MAGIC                 '$.data.meta.initial_hasSavedAddress'
# MAGIC               )
# MAGIC             END AS initial_hasSavedAddress,
# MAGIC             CASE
# MAGIC               WHEN event_name =  'render:1cc_coupons_screen_loaded' THEN json_extract(properties, '$.data.meta.loggedIn')
# MAGIC             END AS loginstatus_couponpage, */
# MAGIC     CASE
# MAGIC       WHEN event_name = 'open' THEN 1
# MAGIC       ELSE 0
# MAGIC     END AS open,
# MAGIC     CASE
# MAGIC       WHEN event_name = 'render:1cc_summary_screen_loaded_completed' THEN 1
# MAGIC       ELSE 0
# MAGIC     END AS summary_screen_loaded,
# MAGIC     CASE
# MAGIC       WHEN event_name = 'behav:1cc_summary_screen_continue_cta_clicked' THEN 1
# MAGIC       ELSE 0
# MAGIC     END AS summary_screen_continue_cta_clicked,
# MAGIC     -- Contact screen events
# MAGIC     CASE
# MAGIC       WHEN event_name = 'behav:1cc_summary_screen_contact_email_entered' THEN 1
# MAGIC       ELSE 0
# MAGIC     END AS contact_email_entered,
# MAGIC     CASE
# MAGIC       WHEN event_name = 'behav:1cc_summary_screen_contact_number_entered' THEN 1
# MAGIC       ELSE 0
# MAGIC     END AS contact_number_entered,
# MAGIC     -- Coupon screen events
# MAGIC     CASE
# MAGIC       WHEN event_name = 'render:1cc_coupons_screen_loaded' THEN 1
# MAGIC       ELSE 0
# MAGIC     END AS coupon_screen_loaded,
# MAGIC     CASE
# MAGIC       WHEN event_name = 'behav:1cc_coupons_screen_custom_coupon_entered' THEN 1
# MAGIC       ELSE 0
# MAGIC     END AS custom_coupon_entered,
# MAGIC     CASE
# MAGIC       WHEN event_name = 'behav:1cc_coupons_screen_coupon_applied' THEN 1
# MAGIC       ELSE 0
# MAGIC     END AS coupon_applied,
# MAGIC     CASE
# MAGIC       WHEN event_name = 'metric:1cc_coupons_screen_coupon_validation_completed'
# MAGIC       AND CAST(
# MAGIC         get_json_object(properties, '$.data.is_coupon_valid') AS boolean
# MAGIC       ) THEN 1
# MAGIC       ELSE 0
# MAGIC     END AS validation_successful,
# MAGIC     CASE
# MAGIC       WHEN event_name = 'behav:1cc_coupons_screen_back_button_clicked' THEN 1
# MAGIC       ELSE 0
# MAGIC     END AS coupon_back_button_clicked,
# MAGIC     CASE
# MAGIC       WHEN coalesce(
# MAGIC         CAST(
# MAGIC           get_json_object(properties, '$.data.count_coupons_available') AS integer
# MAGIC         ),
# MAGIC         0
# MAGIC       ) > 0 THEN 1
# MAGIC       ELSE 0
# MAGIC     END AS coupons_available
# MAGIC     /*  CASE
# MAGIC               WHEN COALESCE(
# MAGIC                 json_extract_scalar(properties, '$.data.contact'),
# MAGIC                 json_extract_scalar(properties, '$.data.data.contact')
# MAGIC               ) IS NOT NULL THEN COALESCE(
# MAGIC                 json_extract_scalar(properties, '$.data.contact'),
# MAGIC                 json_extract_scalar(properties, '$.data.data.contact')
# MAGIC               )
# MAGIC               ELSE NULL
# MAGIC             END AS contact, 
# MAGIC             CASE
# MAGIC               WHEN event_name = 'render:1cc_add_new_address_screen_loaded_completed' THEN 1
# MAGIC               ELSE 0
# MAGIC             END AS add_new_address_screen_loaded_completed,
# MAGIC             CASE
# MAGIC               WHEN event_name = 'behav:1cc_add_new_address_name_entered' THEN 1
# MAGIC               ELSE 0
# MAGIC             END AS add_new_address_name_entered
# MAGIC           */
# MAGIC   FROM
# MAGIC     aggregate_pa.cx_1cc_events_dump_v1
# MAGIC   WHERE
# MAGIC     producer_created_date >= date('2022-09-30')
# MAGIC     AND producer_created_date < date('2022-10-01')
# MAGIC     AND merchant_id IS NOT NULL
# MAGIC     AND merchant_id <> 'Hb4PVe74lPmk0k'
# MAGIC ),
# MAGIC cte2 AS(
# MAGIC   SELECT
# MAGIC     a.merchant_id,
# MAGIC     a.checkout_id,
# MAGIC    -- cast(initial_loggedin as BOOLEAN) as initial_loggedin,
# MAGIC     /*   (
# MAGIC               CASE
# MAGIC                 WHEN (
# MAGIC                   CASE
# MAGIC                     WHEN payments.method = 'cod' THEN 'yes'
# MAGIC                     ELSE 'no'
# MAGIC                   END = 'yes'
# MAGIC                 )
# MAGIC                 OR (
# MAGIC                   (
# MAGIC                     CASE
# MAGIC                       WHEN payments.method = 'cod' THEN 'yes'
# MAGIC                       ELSE 'no'
# MAGIC                     END = 'no'
# MAGIC                   )
# MAGIC                   AND (NOT (payments.authorized_at IS NULL))
# MAGIC                 ) THEN 1
# MAGIC                 ELSE NULL
# MAGIC               END
# MAGIC             ) AS payment_successful,
# MAGIC           */
# MAGIC     sum(DISTINCT platform) AS platform,
# MAGIC     sum(DISTINCT open) AS open,
# MAGIC     sum(DISTINCT summary_screen_loaded) AS summary_screen_loaded,
# MAGIC     sum(DISTINCT contact_number_entered) AS contact_number_entered,
# MAGIC     sum(DISTINCT contact_email_entered) AS contact_email_entered,
# MAGIC     sum(DISTINCT coupon_screen_loaded) AS coupon_screen_loaded,
# MAGIC     sum(DISTINCT custom_coupon_entered) AS custom_coupon_entered,
# MAGIC     sum(DISTINCT coupon_applied) AS coupon_applied,
# MAGIC     sum(DISTINCT validation_successful) AS validation_successful,
# MAGIC     sum(DISTINCT coupon_back_button_clicked) AS coupon_back_button_clicked,
# MAGIC     sum(DISTINCT coupons_available) AS coupons_available,
# MAGIC     sum(DISTINCT summary_screen_continue_cta_clicked) AS summary_screen_continue_cta_clicked
# MAGIC   FROM
# MAGIC     cte a
# MAGIC   GROUP BY
# MAGIC     1,
# MAGIC     2
# MAGIC )
# MAGIC SELECT
# MAGIC   ---  merchants.website,
# MAGIC   *
# MAGIC FROM
# MAGIC   cte2 -- LEFT JOIN realtime_hudi_api.merchants merchants ON cte2.merchant_id = merchants.id

# COMMAND ----------

original_df = _sqldf.toPandas()
original_df.head()

# COMMAND ----------

original_df.shape

# COMMAND ----------

#0: NA 1: mobile_sdk 2: mweb 3: desktop_browser
original_df.groupby(['platform','open']).checkout_id.nunique()

# COMMAND ----------

df = original_df[original_df['open'] == 1]
df.head()

# COMMAND ----------

df['lack_of_intent'] = df.iloc[:,5:].sum(axis=1)
df.head()

# COMMAND ----------

platform = {0: 'NA', 1:'mobile_sdk', 2: 'mweb', 3: 'desktop_browser'}
opens = []
summary_screen_loaded = []
summary_screen_continue_cta_clicked = []
lack_of_intent = []
for i in range(4):
  print(platform[i])
  opens.append(df[df['platform']==i].shape[0])
  summary_screen_loaded.append(df[(df['platform']==i) & (df['summary_screen_loaded']==1)].shape[0])
  summary_screen_continue_cta_clicked.append(df[(df['platform']==i) & (df['summary_screen_continue_cta_clicked']==1)].shape[0])
  lack_of_intent.append(df[(df['platform']==i) & (df['lack_of_intent']==0)].shape[0])
  print('count of opens', opens[i])
  print('count of summary_screen_loaded', summary_screen_loaded[i])
  print('count of summary_screen_continue_cta_clicked', summary_screen_continue_cta_clicked[i])
  print('count of lack_of_intent', lack_of_intent[i])
  print('opens -> summary screen loaded -> summary cta clicked',round(summary_screen_loaded[i]/opens[i], 2), round(summary_screen_continue_cta_clicked[i]/summary_screen_loaded[i],2))
  print('dropped wo doing anything', lack_of_intent[i], round(lack_of_intent[i]/opens[i],2))
  print('\n')

# COMMAND ----------

df['']
