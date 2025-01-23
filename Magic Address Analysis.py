# Databricks notebook source
import pandas as pd
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.header import Header
import smtplib
import os
from datetime import datetime, timedelta, date
import time

# COMMAND ----------

addresses_db = sqlContext.sql("""select * from addresses""")

