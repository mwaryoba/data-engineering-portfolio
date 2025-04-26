# Databricks notebook source
# MAGIC %run general_utilities/EltUtil

# COMMAND ----------

# MAGIC %run general_utilities/AzureKeyVault
 
# COMMAND ----------

# MAGIC %run sixsense_pipeline/utilities/six_sense_util

# COMMAND ----------

import logging
import datetime
import os
from pyspark.sql.functions import concat, col, lit

# COMMAND ----------

for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)

# COMMAND ----------

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# COMMAND ----------

try:
    environment = dbutils.widgets.get("env") 
except:
    environment = 'dev'

# COMMAND ----------

az = AzureKv(environment)
snow = SnowflakeUtil(env = environment,  
                     user = az.get_secret('azure-snowflake-user'), 
                     password = az.get_secret('azure-snowflake-password'), 
                     database ='DSV_ELT_INGESTION' , 
                     schema = 'SIX_SENSE', 
                     warehouse = 'DSV_ELT_CP'
                     )

# COMMAND ----------

six_sense_util = SixSenseUtil(
    host = az.get_secret('sftp-6sense-host'),
    username = az.get_secret('sftp-6sense-username'),
    password = az.get_secret('sftp-6sense-password'),
)

# COMMAND ----------

schema = [
    'CRM Lead ID', 'CRM Account Name', 'CRM Account Country', 'CRM Account Domain',
    'Product Category', 'Buying Stage', 'Intent Score', 'Profile Fit', 'Profile Fit Score',
    'Contact Intent Grade', 'Contact Intent Score', 'Contact Profile Fit',
    'Contact Profile Fit Score', 'Date', '6sense MID', '6sense Account Name',
    '6sense Account Country', '6sense Account Domain'
]

# COMMAND ----------

sftp = six_sense_util._connect_sftp_server()
files = sftp.listdir("/outgoing")
sftp.close()

# COMMAND ----------

pack = "predictiveleads"
pack_files = []
for file in files:
    if file.endswith(f"{pack}.csv"):
        pack_files.append(file)
        logger.info(f"pack file: {file}")

# COMMAND ----------

try:
    loaded_files_df = snow.read_query(f"SELECT DISTINCT file_name from DSV_ELT_INGESTION.SIX_SENSE.RAW_{pack.upper()}")
    loaded_files = loaded_files_df.collect()

    loaded_files_list = []
    for file in loaded_files:
        file = file["FILE_NAME"]
        loaded_files_list.append(file)
        logger.info(f"loaded file: {file}")
except Exception as e:
    logger.warning("Table doesn't exist yet.")
    loaded_files_list = []

# COMMAND ----------

for file_name in pack_files:
    if file_name in loaded_files_list:
        logger.info(f"{file_name} is already loaded. Skipping...")
        continue
    logger.info(f"loading {file_name}")
    df = six_sense_util._pull_file_from_sftp(
        source=f"/outgoing/{file_name}",
        destination=f"/dbfs/tmp/{file_name}"
    )
    df = df.toDF(*schema)
    df = df.filter(col(schema[0]) != schema[0])
    df = df.withColumn("file_name", lit(file_name))
    snow.write_df(df, f"raw_{pack}", "append")
    logger.info(f"Successfully loaded {file_name}")