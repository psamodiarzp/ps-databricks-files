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
# MAGIC      INSERT INTO
# MAGIC       aggregate_pa.cx_1cc_events_dump_v1
# MAGIC     SELECT
# MAGIC       *
# MAGIC     FROM
# MAGIC       (
# MAGIC         WITH raw AS (
# MAGIC           SELECT
# MAGIC             get_json_object (context, '$.checkout_id') AS checkout_id,
# MAGIC             get_json_object(properties, '$.options.order_id') AS order_id,
# MAGIC             get_json_object (properties, '$.options.key') AS merchant_key,
# MAGIC             b.merchant_id AS merchant_id,
# MAGIC             b.customer_id,
# MAGIC             b.product_type,
# MAGIC             event_timestamp,
# MAGIC             l.event_name,
# MAGIC             get_json_object (properties, '$.options.one_click_checkout') AS is_1cc_checkout,
# MAGIC             CAST(
# MAGIC               get_json_object (properties, '$.data.meta.first_screen') AS varchar(10)
# MAGIC             ) AS first_screen_name,
# MAGIC             CAST(
# MAGIC               get_json_object (properties, '$.data.meta.is_mandatory_signup') AS varchar(10)
# MAGIC             ) is_mandatory_signup,
# MAGIC             CAST(
# MAGIC               get_json_object (properties, '$.data.meta.coupons_enabled') AS varchar(10)
# MAGIC             ) is_coupons_enabled,
# MAGIC             CAST(
# MAGIC               get_json_object (
# MAGIC                 properties,
# MAGIC                 '$.data.meta.available_coupons_count'
# MAGIC               ) AS varchar(10)
# MAGIC             ) available_coupons_count,
# MAGIC             CAST(
# MAGIC               get_json_object (properties, '$.data.meta.address_enabled') AS varchar(10)
# MAGIC             ) is_address_enabled,
# MAGIC             get_json_object (properties, '$.data.meta.address_screen_type') AS address_screen_type,
# MAGIC             CAST(
# MAGIC               get_json_object (properties, '$.data.meta.saved_address_count') AS varchar(10)
# MAGIC             ) AS saved_address_count,
# MAGIC             CAST(
# MAGIC               get_json_object (properties, '$.data.meta["count.savedCards"]') AS varchar(10)
# MAGIC             ) count_saved_cards,
# MAGIC             get_json_object (properties, '$.data.meta.loggedIn') AS logged_in,
# MAGIC             event_timestamp_raw,
# MAGIC             LOWER(get_json_object (context, '$.platform')) platform,
# MAGIC             get_json_object(context, '$.user_agent_parsed.user_agent.family') AS browser_name,
# MAGIC             coalesce(
# MAGIC               LOWER(
# MAGIC                 get_json_object (properties, '$.data.data.method')
# MAGIC               ),
# MAGIC               LOWER(
# MAGIC                 get_json_object (properties, '$.data.method')
# MAGIC               )
# MAGIC             ) AS method,
# MAGIC             get_json_object(properties, '$.data.meta.p13n') shown_p13n,
# MAGIC             get_json_object(context, '$.mode') AS is_test_mode,
# MAGIC             /* live / test */
# MAGIC             IF(
# MAGIC               event_name = 'checkoutCODOptionShown'
# MAGIC               AND (
# MAGIC                 get_json_object (properties, '$.data.disabled') = 'false'
# MAGIC                 OR get_json_object (properties, '$.data.disabled') IS NULL
# MAGIC               ),
# MAGIC               'yes',
# MAGIC               'no'
# MAGIC             ) AS is_order_COD_eligible,
# MAGIC             CAST(
# MAGIC               get_json_object (properties, '$.data.otp_reason') AS varchar(10)
# MAGIC             ) AS rzp_OTP_reason,
# MAGIC             get_json_object (properties, '$.data.opted_to_save_address') is_user_opted_to_save_address,
# MAGIC             get_json_object (properties, '$.data.addressSaved') is_new_address_saved,
# MAGIC             get_json_object (properties, '$.data.is_saved_address') AS is_saved_address,
# MAGIC             get_json_object(properties, '$.data.address_id') AS address_id,
# MAGIC             properties,
# MAGIC             context,
# MAGIC             producer_timestamp,
# MAGIC             from_unixtime(producer_timestamp) AS producer_time,
# MAGIC             substr(
# MAGIC               CAST(
# MAGIC                 get_json_object(context, '$["device.id"]') AS varchar(100)
# MAGIC               ),
# MAGIC               1,
# MAGIC               42
# MAGIC             ) AS device_id,
# MAGIC             CAST(producer_created_date AS date) AS producer_created_date
# MAGIC           FROM
# MAGIC             (
# MAGIC               SELECT
# MAGIC                 event_name,
# MAGIC                 properties,
# MAGIC                 context,
# MAGIC                 producer_created_date,
# MAGIC                 event_timestamp,
# MAGIC                 event_timestamp_raw,
# MAGIC                 producer_timestamp
# MAGIC               FROM
# MAGIC                 events.lumberjack_intermediate
# MAGIC               WHERE
# MAGIC                 source = 'checkoutjs'
# MAGIC                 AND LOWER(get_json_object(context, '$.library')) = 'checkoutjs'
# MAGIC                 AND CAST(producer_created_date AS date)  = date('2022-10-31')
# MAGIC                 ---'''+"date('" + single_date.strftime("%Y-%m-%d")+"')" +'''
# MAGIC           ) AS l
# MAGIC             LEFT JOIN (
# MAGIC               SELECT
# MAGIC                 id AS order_id,
# MAGIC                 merchant_id,
# MAGIC                 customer_id,
# MAGIC                 product_type
# MAGIC               FROM
# MAGIC                 realtime_hudi_api.orders
# MAGIC               WHERE
# MAGIC                 CAST(created_date AS date) = date('2022-10-31')
# MAGIC                 ---'''+"date('" + single_date.strftime("%Y-%m-%d")+"')" +'''
# MAGIC             ) b ON substr(
# MAGIC               get_json_object(properties, '$.options.order_id'),
# MAGIC               7
# MAGIC             ) = b.order_id
# MAGIC             INNER JOIN (
# MAGIC               SELECT
# MAGIC                 order_id,
# MAGIC                 type
# MAGIC               FROM
# MAGIC                 realtime_hudi_api.order_meta
# MAGIC               WHERE
# MAGIC                 type = 'one_click_checkout'
# MAGIC             ) c ON substr(
# MAGIC               get_json_object(properties, '$.options.order_id'),
# MAGIC               7
# MAGIC             ) = c.order_id
# MAGIC         ),
# MAGIC         step1 AS(
# MAGIC           SELECT
# MAGIC             *,
# MAGIC             event_timestamp - (
# MAGIC               lead(event_timestamp) OVER (
# MAGIC                 partition BY merchant_id,
# MAGIC                 producer_created_date,
# MAGIC                 device_id
# MAGIC                 ORDER BY
# MAGIC                   event_timestamp_raw DESC
# MAGIC               )
# MAGIC             ) AS lag_diff
# MAGIC           FROM
# MAGIC             raw
# MAGIC         ),
# MAGIC         step2 AS(
# MAGIC           SELECT
# MAGIC             *,
# MAGIC             CASE
# MAGIC               WHEN lag_diff >= 1800
# MAGIC               OR lag_diff IS NULL THEN 1
# MAGIC               ELSE 0
# MAGIC             END AS is_new_session
# MAGIC           FROM
# MAGIC             step1
# MAGIC         ),
# MAGIC         step3 AS(
# MAGIC           SELECT
# MAGIC             *,
# MAGIC             concat(
# MAGIC               CAST(
# MAGIC                 sum(is_new_session) over(
# MAGIC                   partition BY merchant_id,
# MAGIC                   producer_created_date,
# MAGIC                   device_id
# MAGIC                   ORDER BY
# MAGIC                     event_timestamp_raw
# MAGIC                 ) AS varchar(100)
# MAGIC               ),
# MAGIC               '-',
# MAGIC               CAST(producer_created_date AS varchar(10)),
# MAGIC               '-',
# MAGIC               device_id
# MAGIC             ) AS session_id
# MAGIC           FROM
# MAGIC             step2
# MAGIC         )
# MAGIC         SELECT
# MAGIC           checkout_id,
# MAGIC           order_id,
# MAGIC           merchant_key,
# MAGIC           merchant_id,
# MAGIC           customer_id,
# MAGIC           product_type,
# MAGIC           event_timestamp,
# MAGIC           event_name,
# MAGIC           is_1cc_checkout,
# MAGIC           first_screen_name,
# MAGIC           is_mandatory_signup,
# MAGIC           is_coupons_enabled,
# MAGIC           available_coupons_count,
# MAGIC           is_address_enabled,
# MAGIC           address_screen_type,
# MAGIC           saved_address_count,
# MAGIC           count_saved_cards,
# MAGIC           logged_in,
# MAGIC           event_timestamp_raw,
# MAGIC           platform,
# MAGIC           browser_name,
# MAGIC           method,
# MAGIC           shown_p13n,
# MAGIC           is_test_mode,
# MAGIC           is_order_cod_eligible,
# MAGIC           rzp_otp_reason,
# MAGIC           is_user_opted_to_save_address,
# MAGIC           is_new_address_saved,
# MAGIC           is_saved_address,
# MAGIC           address_id,
# MAGIC           properties,
# MAGIC           context,
# MAGIC           device_id,
# MAGIC           session_id,
# MAGIC           producer_timestamp,
# MAGIC           producer_time,
# MAGIC           producer_created_date
# MAGIC         FROM
# MAGIC           step3
# MAGIC       );

# COMMAND ----------


