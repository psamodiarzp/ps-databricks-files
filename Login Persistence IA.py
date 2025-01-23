# Databricks notebook source
import pandas as pd

# COMMAND ----------

from pyspark import SparkContext
from pyspark.sql import SparkSession

# COMMAND ----------

import pyarrow

# COMMAND ----------

import pyarrow.parquet as pq

# COMMAND ----------

!pip uninstall typing_extensions
!pip uninstall fastapi
!pip install --no-cache fastapi

# COMMAND ----------

import s3fs

# COMMAND ----------



# COMMAND ----------

# WTU
wtu_db = sqlContext.sql("""

WITH denominator AS
(
 select distinct device_id, date
 FROM
 (
 select distinct device_id, date, row_number() over(partition by device_id order by date desc) rnk 
 from
   (
      select DISTINCT device_id,
      get_json_object(properties, '$.data.meta.initial_loggedIn')  as initial_loggedIn,
      producer_created_date date
      from aggregate_pa.cx_1cc_events_dump_v1
      where producer_created_date BETWEEN DATE('2023-04-11') AND DATE('2023-05-22')
      and merchant_id not in ('GlLXGUtKUrQHtj','Hb4PVe74lPmk0k')
      and get_json_object(properties, '$.data.meta.initial_loggedIn') = 'true'
   )
  )
  WHERE rnk=1
),

magic AS
(
    SELECT distinct device_id, date
    FROM (
       SELECT DISTINCT producer_created_date date, m.device_id,
       MAX(case when lower(get_json_object(properties, '$.data.meta.initial_loggedIn'))='true' then            1 else 0 end) initial_loggedIn,
       MAX(case when lower(get_json_object(properties, '$.data.meta.loggedIn'))='true' then                    1 else 0 end) loggedIn
       from aggregate_pa.cx_1cc_events_dump_v1 m
       left join denominator d on d.device_id = m.device_id 
       where producer_created_date BETWEEN date('2023-01-01') AND date('2023-05-22')
       and merchant_id not in ('GlLXGUtKUrQHtj','Hb4PVe74lPmk0k')
       and d.date>m.producer_created_date
       group by 1,2
     )
     WHERE ((initial_loggedIn = 1) OR (loggedIn = 1))
),

std AS
(
      SELECT distinct device_id, date
      FROM
        (
        SELECT DATE(producer_created_date) date,
        substr(
              CAST(
                    get_json_object(context, '$["device.id"]') as varchar(42)
               ),
            1,
           42
            ) device_id,
        MAX(CASE WHEN LOWER(logged_in) = 'true' THEN 1 ELSE 0 END) Logged_In
        FROM aggregate_pa.cx_data_events_22_v1
        where producer_created_date BETWEEN '2023-01-01' AND '2023-05-22'
        GROUP BY 1, 2
        ORDER BY 1, 2
        )
      WHERE Logged_In = 1
),

Truecaller AS
(
    SELECT distinct device_id, date
      FROM(
        select device_id, 
        get_json_object(properties,'$.data.failure_reason') Failure_Reason,
        producer_created_date date
        from aggregate_pa.cx_1cc_events_dump_v1
        where producer_created_date BETWEEN DATE('2023-01-01') AND DATE('2023-05-22')
        and merchant_id not in ('GlLXGUtKUrQHtj','Hb4PVe74lPmk0k')
        and event_name='api:truecaller_verification'
        group by 1,2,3
        )
      WHERE Failure_Reason IS NULL
),


Mast AS
(
SELECT distinct d.device_id, d.date denominator_date, m.date magic_date, s.date std_date, tc.date truecaller_date
FROM denominator d
LEFT JOIN magic m ON d.device_id = m.device_id
LEFT JOIN std s ON d.device_id = s.device_id
LEFT JOIN Truecaller tc ON d.device_id = tc.device_id
WHERE (d.date>m.date
  OR d.date>s.date
  OR d.date>tc.date)
),

magic_date AS
(
SELECT distinct device_id, denominator_date, magic_date
FROM
(
SELECT device_id, denominator_date, magic_date, row_number() over (partition by device_id order by magic_date desc) rnk
FROM Mast
)
WHERE rnk = 1
),

std_date AS
(
SELECT distinct device_id, denominator_date, std_date
FROM
(
SELECT device_id, denominator_date, std_date, row_number() over (partition by device_id order by std_date desc) rnk
FROM Mast
)
WHERE rnk = 1
),

truecaller_date AS
(
SELECT distinct device_id, denominator_date, truecaller_date
FROM
(
SELECT device_id, denominator_date, truecaller_date, row_number() over (partition by device_id order by truecaller_date desc) rnk
FROM Mast
)
WHERE rnk = 1
)


SELECT distinct m.device_id, m.denominator_date, m.magic_date, s.std_date, t.truecaller_date
FROM magic_date m
LEFT JOIN std_date s ON m.device_id = s.device_id
LEFT JOIN truecaller_date t ON m.device_id = t.device_id
--LIMIT 100


    """
)




# COMMAND ----------

wtu_df = wtu_db.toPandas()

# COMMAND ----------

wtu_df.head()

# COMMAND ----------

wtu_df[wtu_df['denominator_date'] <= wtu_df['truecaller_date']].count()

# COMMAND ----------

final_filename = f's3://rzp-1642-payments/ppg_transient/magic_login_persistance'

# COMMAND ----------


wtu_db.write.mode('append').partitionBy(
    'denominator_date',
).parquet(
    final_filename,
)

# COMMAND ----------

!pip install pyarrow

# COMMAND ----------

!/databricks/python3/bin/python -m pip install --upgrade pip

# COMMAND ----------

import pandas as pd

# Specify the path to the Parquet file
parquet_file_path = "s3://rzp-1642-payments/ppg_transient/magic_login_persistance.parquet"

# Read the Parquet file into a Pandas DataFrame
df = pd.read_parquet(parquet_file_path)

# You can now use the DataFrame for further processing or analysis
print(df.head())


# COMMAND ----------

arrow_dataset = pyarrow.parquet.ParquetDataset(final_filename+'.parquet')
arrow_table = arrow_dataset.read()
pandas_df = arrow_table.to_pandas()

# COMMAND ----------

df1 = pd.read_parquet('s3://rzp-1642-payments/ppg_transient/magic_login_persistance/denominator_date=2023-05-20/part-00000-tid-5933169487304862351-655bc547-517c-4590-99c4-5c376fb8253e-15652-37.c000.snappy.parquet')
df1.head()

# COMMAND ----------

# Specify the directory path containing the partitioned Parquet files
parquet_directory = 's3://rzp-1642-payments/ppg_transient/magic_login_persistance'

# Create a Parquet dataset from the directory
#dataset = pq.ParquetDataset(parquet_directory)


all_files = fs.glob(path=s3_location)
# Get the list of file paths in the dataset
file_paths = dataset.get_paths()

# Initialize an empty list to store the individual dataframes
dfs = []

# Iterate over each file path and read the Parquet file into a dataframe
for file_path in all_files:
    # Read the Parquet file into a PyArrow Table
    table = pq.read_table(file_path)
    
    # Convert the PyArrow Table to a Pandas DataFrame
    df = table.to_pandas()
    
    # Append the dataframe to the list
    dfs.append(df)

# Concatenate all the dataframes into a single dataframe
combined_df = pd.concat(dfs, ignore_index=True)

# You can now use the combined dataframe for further processing or analysis
print(combined_df.head())








# COMMAND ----------

db = spark.read.csv('s3://rzp-1642-payments/ppg_transient/magic_login_persistance.parquet')
df = pd.read_csv(db)
dh.heaD()

# COMMAND ----------

sample_s3_data_df = pd.read_parquet(spark.read.parquet('s3://rzp-1642-payments/ppg_transient/magic_login_persistance.parquet'))
sample_s3_data_df.head()

# COMMAND ----------

fs = s3fs.core.S3FileSystem()

# COMMAND ----------

s3_location = 's3://rzp-1642-payments/ppg_transient/magic_login_persistance/*/*.parquet'
all_files = fs.glob(path=s3_location)

# COMMAND ----------

all_files

# COMMAND ----------

file_path = 's3://'+all_files[0]
df1 = pd.read_parquet(file_path)
df1['denominator']
df1.head()

# COMMAND ----------



# COMMAND ----------

new_filename = f's3://rzp-1642-payments/ppg_transient/magic_login_persistance_updated'

# COMMAND ----------

wtu_db.write.parquet(
    new_filename,
)

# COMMAND ----------

df1 = pd.read_parquet(new_filename)
df1.head()

# COMMAND ----------

df1.shape

# COMMAND ----------

denominator = sqlContext.sql(
    """
 select distinct device_id, date
 FROM
 (
 select distinct device_id, date, row_number() over(partition by device_id order by date desc) rnk 
 from
   (
      select DISTINCT device_id,
      get_json_object(properties, '$.data.meta.initial_loggedIn')  as initial_loggedIn,
      producer_created_date date
      from aggregate_pa.cx_1cc_events_dump_v1
      where producer_created_date BETWEEN DATE('2023-04-11') AND DATE('2023-05-22')
      and merchant_id not in ('GlLXGUtKUrQHtj','Hb4PVe74lPmk0k')
      and get_json_object(properties, '$.data.meta.initial_loggedIn') = 'true'
   )
  )
  WHERE rnk=1
  """
)

denominator_df = denominator.toPandas()

# COMMAND ----------

denominator_df.shape

# COMMAND ----------

login_query = sqlContext.sql(
    """

WITH denominator AS
(
 select distinct device_id, date
 FROM
 (
 select distinct device_id, date, row_number() over(partition by device_id order by date desc) rnk 
 from
   (
      select DISTINCT device_id,
      get_json_object(properties, '$.data.meta.initial_loggedIn')  as initial_loggedIn,
      producer_created_date date
      from aggregate_pa.cx_1cc_events_dump_v1
      where producer_created_date BETWEEN DATE('2023-04-11') AND DATE('2023-05-22')
      and merchant_id not in ('GlLXGUtKUrQHtj','Hb4PVe74lPmk0k')
      and get_json_object(properties, '$.data.meta.initial_loggedIn') = 'true'
   )
  )
  WHERE rnk=1
),

magic AS
(
    SELECT distinct device_id, date
    FROM (
       SELECT DISTINCT producer_created_date date, m.device_id,
       MAX(case when lower(get_json_object(properties, '$.data.meta.initial_loggedIn'))='true' then            1 else 0 end) initial_loggedIn,
       MAX(case when lower(get_json_object(properties, '$.data.meta.loggedIn'))='true' then                    1 else 0 end) loggedIn
       from aggregate_pa.cx_1cc_events_dump_v1 m
       left join denominator d on d.device_id = m.device_id 
       where producer_created_date BETWEEN date('2023-01-01') AND date('2023-05-22')
       and merchant_id not in ('GlLXGUtKUrQHtj','Hb4PVe74lPmk0k')
       and d.date>m.producer_created_date
       group by 1,2
     )
     WHERE ((initial_loggedIn = 1) OR (loggedIn = 1))
),

std AS
(
      SELECT distinct device_id, date
      FROM
        (
        SELECT DATE(producer_created_date) date,
        substr(
              CAST(
                    get_json_object(context, '$["device.id"]') as varchar(42)
               ),
            1,
           42
            ) device_id,
        MAX(CASE WHEN LOWER(logged_in) = 'true' THEN 1 ELSE 0 END) Logged_In
        FROM aggregate_pa.cx_data_events_22_v1
        where producer_created_date BETWEEN '2023-01-01' AND '2023-05-22'
        GROUP BY 1, 2
        ORDER BY 1, 2
        )
      WHERE Logged_In = 1
),

Truecaller AS
(
    SELECT distinct device_id, date
      FROM(
        select device_id, 
        get_json_object(properties,'$.data.failure_reason') Failure_Reason,
        producer_created_date date
        from aggregate_pa.cx_1cc_events_dump_v1
        where producer_created_date BETWEEN DATE('2023-01-01') AND DATE('2023-05-22')
        and merchant_id not in ('GlLXGUtKUrQHtj','Hb4PVe74lPmk0k')
        and event_name='api:truecaller_verification'
        group by 1,2,3
        )
      WHERE Failure_Reason IS NULL
),


Mast AS
(
SELECT distinct d.device_id, d.date denominator_date, m.date magic_date, s.date std_date, tc.date truecaller_date
FROM denominator d
LEFT JOIN magic m ON d.device_id = m.device_id
LEFT JOIN std s ON d.device_id = s.device_id
LEFT JOIN Truecaller tc ON d.device_id = tc.device_id
WHERE (d.date>m.date
  OR d.date>s.date
  OR d.date>tc.date)
),

magic_date AS
(
SELECT distinct device_id, denominator_date, magic_date
FROM
(
SELECT device_id, denominator_date, magic_date, row_number() over (partition by device_id order by magic_date desc) rnk
FROM Mast
)
WHERE rnk = 1
),

std_date AS
(
SELECT distinct device_id, denominator_date, std_date
FROM
(
SELECT device_id, denominator_date, std_date, row_number() over (partition by device_id order by std_date desc) rnk
FROM Mast
)
WHERE rnk = 1
),

truecaller_date AS
(
SELECT distinct device_id, denominator_date, truecaller_date
FROM
(
SELECT device_id, denominator_date, truecaller_date, row_number() over (partition by device_id order by truecaller_date desc) rnk
FROM Mast
)
WHERE rnk = 1
)


SELECT distinct d.device_id, d.date, m.magic_date, s.std_date, t.truecaller_date
FROM denominator d
LEFT JOIN magic_date m ON d.device_id = m.device_id
LEFT JOIN std_date s ON d.device_id = s.device_id
LEFT JOIN truecaller_date t ON d.device_id = t.device_id
--LIMIT 100
    """
)

# COMMAND ----------

updated_filename = f's3://rzp-1642-payments/ppg_transient/magic_login_persistance_updated_new'

# COMMAND ----------

login_query.write.parquet(
    updated_filename,
)

# COMMAND ----------


df1 = pd.read_parquet(updated_filename)
df1.head()

# COMMAND ----------

print(df1.shape)
print(df1.device_id.count())
print(df1.device_id.nunique())

# COMMAND ----------

df1[df1.iloc[:, 2:5].isna().all(axis=1)]

# COMMAND ----------

df1['magic_date'] = pd.to_datetime(df1['magic_date'])
df1['std_date'] = pd.to_datetime(df1['std_date'])
df1['truecaller_date'] = pd.to_datetime(df1['truecaller_date'])
df1['max_date'] = df1[['magic_date','std_date','truecaller_date']].max(axis=1)
df1.head()

# COMMAND ----------

df1['max_date'].describe()


# COMMAND ----------

df1['date'] = pd.to_datetime(df1['date'])
df1['diff'] = df1['max_date'] - df1['date']
df1.head()

# COMMAND ----------

df1['modified_max_date'] =  df1.apply(lambda row: row['max_date'] if row['max_date'] < row['date'] else sorted([row['magic_date'], row['std_date'], row['truecaller_date']])[-2], axis=1)
df1['modified_diff'] = df1['modified_max_date'] - df1['date']
df1.head()

# COMMAND ----------

df1['final_max_date'] =  df1.apply(lambda row: row['modified_max_date'] if row['modified_max_date'] < row['date'] else sorted([row['magic_date'], row['std_date'], row['truecaller_date']])[0], axis=1)
df1['final_diff'] = df1['final_max_date'] - df1['date']
df1.head()

# COMMAND ----------

df1.describe()

# COMMAND ----------

cnt_modifed = df1.groupby(by='modified_diff').agg({'device_id':'count'}).reset_index()
print(cnt_modifed[cnt_modifed['modified_diff'] > '0 days'].device_id.sum())
print(cnt_modifed[cnt_modifed['modified_diff'] <= '0 days'].device_id.sum())
cnt_modifed

# COMMAND ----------

pd.set_option('display.max_rows', 500)

# COMMAND ----------

df1[df1['final_diff'] > '0 days']

# COMMAND ----------

cnt_final = df1.groupby(by='final_diff').agg({'device_id':'count'}).reset_index()
print(cnt_final[cnt_final['final_diff'] > '0 days'].device_id.sum())
print(cnt_final[cnt_final['final_diff'] <= '0 days'].device_id.sum())
cnt_final

# COMMAND ----------

df1.to_csv('/dbfs/FileStore/shared_transfer/Pallavi_Samodia/login_persistance_raw_data.csv')

# COMMAND ----------

cnt_final.to_csv('/dbfs/FileStore/shared_transfer/Pallavi_Samodia/login_persistance_results.csv')

# COMMAND ----------

cnt_all = df1.groupby(by='diff').agg({'device_id':'count'}).reset_index()
cnt_all.head()

# COMMAND ----------

cnt_all.sum()

# COMMAND ----------

cnt_all[cnt_all['diff'] >= '0 days'].sum()

# COMMAND ----------

cnt_all[cnt_all['diff'] < '0 days'].sum()

# COMMAND ----------

cnt_all[cnt_all['diff'] >= '0 days']

# COMMAND ----------


