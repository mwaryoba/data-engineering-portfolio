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

fields = ["id","key"]
# The raw table will be used as comparison only so we just need to pull ID and Key fields

# COMMAND ----------

##Get the total records number
all_data = jira.send_request(object_suffix = "api/3/search", maxResults = 1, startAt=0,jql = None, fields = fields)
total_data = all_data["total"]

# COMMAND ----------

startIndex = 0
haslength = True
res = spark.createDataFrame([], StructType())
total_page = total_data
response_list = []
extract_date = current_timestamp()
sdf = None
while haslength:
    all_data = jira.send_request(object_suffix="api/3/search", maxResults=5000, startAt=startIndex, fields = fields)
    # to paginate the data every time run 50 records only.
    all_data = all_data["issues"]
    if not all_data or len(all_data)==0:
        dbutils.notebook.exit("no data")
    sdf = DatabricksUtil.df_jsonize_array_w_extract_date(all_data, extract_date)
    if res.count() == 0:
        res = sdf
    else:
        res = res.union(sdf)
    num_records=sdf.count()
    startIndex += num_records
    if startIndex >= total_page:
        haslength = False
        res = res.withColumn('load_date', to_utc_timestamp(current_timestamp(),"UTC"))
        print("startIndex:",startIndex)
        snow.write_df(res,'RAW_JIRA_ISSUE_ACTIVE','overwrite')
        #We need the table to be refreshed once daily to make sure all the deleted JIRA Issue get catched by comparison. So the write_df has to be put into the if statement to make sure it only overwrite with the data that already unioned together.