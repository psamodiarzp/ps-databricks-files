# Databricks notebook source
# MAGIC %pip install gspread

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import gspread
from oauth2client.service_account import ServiceAccountCredentials

# COMMAND ----------


