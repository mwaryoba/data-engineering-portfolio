# Databricks notebook source
# MAGIC %md # Utility classes for ELT and data ingestion
# MAGIC
# MAGIC ### Summary
# MAGIC
# MAGIC - This notebook contains a series of calasses that would help with data ingestion and some data tranformation. 

# COMMAND ----------

# MAGIC %run general_utilities/AzureKeyVault

# COMMAND ----------

import pandas as pd
import datetime as dt
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

class DatabricksUtil:
    def df_to_csv(self, df, path, delimiter):
        df.toPandas().to_csv(path, sep= delimiter ,index = False)
  
    def upper_key(self,in_dict):
        if type(in_dict) is dict:
            out_dict = {}
            for key, item in in_dict.items():
                out_dict[key.upper()] = self.upper_key(item)
            return out_dict
        elif type(in_dict) is list:
            return [self.upper_key(obj) for obj in in_dict]
        else:
            return in_dict
    
  #flatten_array_struct_df flattens nested json
    def flatten_structs(self,nested_df):
        stack = [((), nested_df)]
        columns = []

        while len(stack) > 0:
        
            parents, df = stack.pop()
        
            array_cols = [
                c[0]
                for c in df.dtypes
                if c[1][:5] == "array"
            ]
        
            flat_cols = [
                col(".".join(parents + (c[0],))).alias("_".join(parents + (c[0],)))
                for c in df.dtypes
                if c[1][:6] != "struct" 
            ]

            nested_cols = [
                c[0]
                for c in df.dtypes
                if c[1][:6] == "struct"
            ]
        
            columns.extend(flat_cols)

            for nested_col in nested_cols:
                projected_df = df.select(nested_col + ".*")
                stack.append((parents + (nested_col,), projected_df))
        
        return nested_df.select(columns)

    def flatten_array_struct_df(self,df):

        array_cols = [
                c[0]
                for c in df.dtypes
                if c[1][:5] == "array"
            ]
        if len(array_cols) == 0:
            df = self.flatten_structs(df)
            array_cols = [
                      c[0]
                      for c in df.dtypes
                      if c[1][:5] == "array"
                  ]
        while len(array_cols) > 0:

            for array_col in array_cols:

                cols_to_select = [x for x in df.columns if x != array_col ]
                df = df.withColumn(array_col, explode_outer(col(array_col)))
            df = self.flatten_structs(df)
            array_cols = [
                        c[0]
                        for c in df.dtypes
                        if c[1][:5] == "array"
                    ]
        return df

    @classmethod
    def df_jsonize_response(cls, resp):
        """Puts the response data into a Spark DF with a single json-string column.
        If the Response.json() is an array, this will result in multiple-record
        dataframe. If Response.json() is a dict, it will result in a single-record.

        :param resp requests.response object.
        :return: Spark DataFrame.
        """

        return cls.df_jsonize_array(resp.json())

    @staticmethod
    def df_w_extract_date(sdf, extract_date=None):
        """Adds a 'extract_date' UTC column to the provided DataFrame and returns it."""

        if extract_date is None:
            my_timestamp = to_utc_timestamp(current_timestamp(),"UTC")
        else:
            my_timestamp = extract_date

        return sdf.withColumn('extract_date', my_timestamp)

    @classmethod
    def df_jsonize_response_w_extract_date(cls, resp, extract_date=None):
        """Puts the response data into a Spark DF with the
        data in a single json column and the addition of a
        'extract_date' column in UTC.

        :param resp requests.response object.
        :return: Spark DataFrame.
        """

        return cls.df_w_extract_date(cls.df_jsonize_response(resp), extract_date)

    @staticmethod
    def df_jsonize_array(array):
        """Puts the array into a Spark DF with a single json-string column.

        :param array List or other iterable, of likely Dict objects.
        :return: Spark DataFrame.
        """

        # Convert the array into a list of strings.
        # This way, we can transfer it over as a single field; data frames
        #   won't try to interpret the various columns.
        string_list = [json.dumps(x) for x in array]

        # Creating a Pandas DF then to Spark DF; Spark is not interpreting the data correctly
        #   when done directly.
        sdf = spark.createDataFrame(pd.DataFrame(string_list), ['value'])
        return sdf

    @classmethod
    def df_jsonize_array_w_extract_date(cls, array, extract_date=None):
        """Puts the array into a Spark DF with the
        data in a single json column and the addition of a
        'extract_date' column in UTC.

        :param array List or other iterable, of likely Dict objects.
        :return: Spark DataFrame.
        """

        return cls.df_w_extract_date(cls.df_jsonize_array(array), extract_date)
    
    def df_jsonize_api_response(self,resp,extract_date=None):
        """Puts the response data into a Spark DF.

        :param resp requests.response object.
        :param extract_date. timestamp
        :return: Spark DataFrame.
        """
        if extract_date is None:
            my_timestamp = to_utc_timestamp(current_timestamp(),"UTC")
        else:
            my_timestamp = extract_date
        df = spark.read.json(spark.sparkContext.parallelize([resp.text]))
        column_list = df.columns
        schema = df.schema
        df = df.withColumn("results", to_json(struct([x for x in column_list]),options={"ignoreNullFields":False})).drop(*column_list)
        df = df.withColumn("results", from_json(col("results"), schema))
        return df.withColumn("extract_date", lit(my_timestamp))


class SnowflakeUtil:
    def __init__(self, env, user, password, database, schema, warehouse):
        self.env = "prd" if env == "prod" else "dev"
        self.user = user
        self.password = password
        self.database = database
        self.schema = schema
        self.warehouse  = warehouse
        self.options = {"sfUrl": "https://oz29387-stdsv"+self.env+"01.snowflakecomputing.com/",
                                  "sfUser": self.user,
                                  "sfAuthenticator" : "SNOWFLAKE_JWT",
                                  "pem_private_key" : self.password,
                                  "sfDatabase": self.database,
                                  "sfSchema": self.schema,
                                  "sfWarehouse": self.warehouse
                        }
    def read_query(self, query):
        df = spark.read \
              .format("snowflake") \
              .options(**self.options) \
              .option("query",  query) \
              .load()
        return df
    
    def write_df(self, df, table, mode):
        df.write \
          .format("snowflake") \
          .options(**self.options) \
          .option("dbtable", table) \
          .mode(mode) \
          .save()
        
    def get_schema(self, table, columns = []):
        df = self.read_query("select * from {} limit 1".format(table))
        # skipping columns that are not present in the raw data file
        for column in columns:
            df = df.drop(column)
        return df.schema
      
    def run_dml_query(self, query):  
        spark.sparkContext._jvm.net.snowflake.spark.snowflake.Utils.runQuery(self.options, query)

# COMMAND ----------

# MAGIC %scala
# MAGIC import net.snowflake.spark.snowflake.Utils
# MAGIC class SnowflakeDML(var env : String , var user : String , var password :  String , var database : String , var schema : String, var warehouse: String ){
# MAGIC   
# MAGIC   val options = Map(
# MAGIC     "sfUrl" -> "https://oz29387-stdsv%s01.snowflakecomputing.com/".format(env),
# MAGIC     "sfUser" -> user,
# MAGIC     "sfPassword" -> password,
# MAGIC     "sfDatabase" -> database,
# MAGIC     "sfSchema" -> schema,
# MAGIC     "sfWarehouse" -> warehouse
# MAGIC   )
# MAGIC   
# MAGIC   def run_dml_query(query : String){
# MAGIC     Utils.runQuery(options, query )
# MAGIC   }
# MAGIC }

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### StorageUtil
# MAGIC - For each S3 bucket or Blob storage account, a mount needs yo be created. Blob storages need to have at least one container.
# MAGIC - list useful dbutils command:
# MAGIC     - dbutils.fs.ls("/mnt/-mount-name-/-optional-direcoty-") ---> get list of sub-directories and folders under given mount/directory
# MAGIC     - dbutils.fs.mounts() ---> get list of mounts
# MAGIC     - dbutils.fs.unmount("/mnt/-mount-name-") ---> unmount a mount
# MAGIC     
# MAGIC - please refer to link below to see how to read files like json, parquet, etc using mounts:
# MAGIC https://docs.microsoft.com/en-us/azure/databricks/_static/notebooks/data-sources/mount-azure-blob-storage.html

# COMMAND ----------

class StorageUtil:
    def __init__(self, **kwargs):
        self.mount_name = kwargs.get('mount_name', None)
    def get_mounts_list(self):
        lst = dbutils.fs.mounts()
        return lst
      
    def create_azure_mount(self, mount_name, container_name, blob_storage_account_name, blob_storage_account_key ):
        #key should be coming from secrets
        mount_name = "/mnt/%s" % mount_name
        dbutils.fs.mount(
                   source = f"wasbs://{container_name}@{blob_storage_account_name}.blob.core.windows.net/",
                   mount_point = mount_name,
                   extra_configs = {f"fs.azure.account.key.{blob_storage_account_name}.blob.core.windows.net":blob_storage_account_key})
        # display(dbutils.fs.ls("/mnt/%s" % mount_name)) 
  
    def create_aws_mount(self, mount_name, aws_bucket_name, access_key, secret_key ):
        #key and secret should be coming from secrets
        encoded_secret_key = secret_key.replace("/", "%2F")
        dbutils.fs.mount("s3a://%s:%s@%s" % (access_key, encoded_secret_key, aws_bucket_name), "/mnt/%s" % mount_name)
        display(dbutils.fs.ls("/mnt/%s" % mount_name))