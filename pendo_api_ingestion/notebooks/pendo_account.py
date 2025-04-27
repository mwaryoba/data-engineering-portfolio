# Databricks notebook source
# MAGIC %run pendo_api_ingestion/utilities/PendoApiUtil

# COMMAND ----------

# MAGIC %run general_utilities/EltUtil

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
                     schema = 'PENDO', 
                     warehouse = 'DSV_ELT_CP')

# COMMAND ----------

try:
    pendo_subscription_type = dbutils.widgets.get("pendo_subscription")
    integ_key = az.get_secret('pendo-customer-portal-api-integration-key') if pendo_subscription_type else None
except:
    pendo_subscription_type = None
    integ_key = None

# COMMAND ----------

print(pendo_subscription_type)

# COMMAND ----------

pendo = PendoApiUtil('accounts', integ_key=integ_key)

# COMMAND ----------

extract_date = to_utc_timestamp(current_timestamp(),"UTC")
res = pendo.send_request()

# COMMAND ----------

sdf = DatabricksUtil.df_jsonize_array_w_extract_date(res.json().get('results'), extract_date)

# COMMAND ----------

sdf = sdf.withColumn('load_date', to_utc_timestamp(current_timestamp(),"UTC"))

# COMMAND ----------

if pendo_subscription_type:
    snow.write_df(sdf, 'pendo.raw_pendo_customer_portal_account', 'append')
else:
    snow.write_df(sdf, 'pendo.raw_pendo_account', 'append')

# COMMAND ----------

sdf.unpersist()