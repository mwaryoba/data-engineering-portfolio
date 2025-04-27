# Databricks notebook source
# MAGIC %run ../Utilities/PendoApiUtil

# COMMAND ----------

# MAGIC %run ../../../Utilities/Elt/EltUtil

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

try:
    start_date = datetime.strptime(dbutils.widgets.get('start_date'), '%Y-%m-%dT%H:%M:%S%z')
except:
    start_date = datetime.now() - timedelta(days=1) # Yesterday in UTC/server-time.
    print('Using default date.')

print(f'{start_date=}')

# COMMAND ----------

start_date= start_date - timedelta(days=1)  #Move the start date to a date earlier to make sure all the data get imported due to different time zone.
startdate = start_date.date()
print(startdate)

# COMMAND ----------

print(pendo_subscription_type)

# COMMAND ----------

pendo = PendoApiUtil('visitor',start_date=start_date, integ_key=integ_key)

# COMMAND ----------

extract_date = to_utc_timestamp(current_timestamp(),"UTC")
res = pendo.send_request()

# COMMAND ----------

if not res.json().get('results'):
    dbutils.notebook.exit("no data")
sdf = DatabricksUtil.df_jsonize_array_w_extract_date(res.json().get('results'), extract_date)

# COMMAND ----------

sdf = sdf.withColumn('load_date', to_utc_timestamp(current_timestamp(),"UTC"))

# COMMAND ----------

if pendo_subscription_type:
    snow.write_df(sdf, 'pendo.raw_pendo_customer_portal_visitor', 'append')
else:
    snow.write_df(sdf, 'pendo.raw_pendo_visitor', 'append')

# COMMAND ----------

sdf.unpersist()