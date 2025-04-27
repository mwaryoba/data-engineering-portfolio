# Databricks notebook source
# MAGIC %run jira_api_ingestion/utilities/JiraUtil

# COMMAND ----------

from datetime import datetime, timedelta
import time

# COMMAND ----------

try:
    environment = dbutils.widgets.get("env") 
except:
    environment = 'dev'

# COMMAND ----------

extract_date = to_utc_timestamp(current_timestamp(),"UTC")

# COMMAND ----------

#Create Objects for class
az = AzureKv(environment)

snow = SnowflakeUtil(env = environment, 
                     user = az.get_secret('azure-snowflake-user'), 
                     password = az.get_secret('azure-snowflake-password'), 
                     database ='DSV_ELT_INGESTION' , 
                     schema = 'JIRA', 
                     warehouse = 'DSV_ELT_CP')
                    
ods = SnowflakeUtil(env = environment, 
                     user = az.get_secret('azure-snowflake-user'), 
                     password = az.get_secret('azure-snowflake-password'), 
                     database ='DSV_ODS' , 
                     schema = 'JIRA', 
                     warehouse = 'DSV_ELT_CP'
                     )

jira = JiraUtil(environment=environment)


# COMMAND ----------

#get the updated date from existing dataset
try:
    newdate=ods.read_query("select max(extract_date) as update_date from DSV_ODS.JIRA.STG_JIRA_BOARD_API")
    if not newdate:
        raise exception ('empty table')
    update_date = newdate.collect()[0][0]
    updateAt = update_date.strftime('%Y-%m-%d')
    print (updateAt)
except:
    updateAt = '2014-01-01'

# COMMAND ----------

##Get the total records number
all_data = jira.send_request(object_suffix = "agile/1.0/board", maxResults = 1, startAt=0)
total_data = all_data["total"]

# COMMAND ----------

startIndex = 0
haslength = True
maxrecords = 50
total_page = total_data
while haslength:
    all_data = jira.send_request(object_suffix = "agile/1.0/board", maxResults = maxrecords, startAt=startIndex)
    all_data = all_data["values"]
    if not all_data:
        dbutils.notebook.exit("no data")
    sdf = DatabricksUtil.df_jsonize_array_w_extract_date(all_data, extract_date)
    num_records=sdf.count()
    print("records_count:", num_records)
    startIndex += num_records
    if startIndex >= total_page:
        haslength = False
    sdf = sdf.withColumn('load_date', to_utc_timestamp(current_timestamp(),"UTC"))
    print("startIndex:",startIndex)
    snow.write_df(sdf,'RAW_JIRA_BOARD','append')