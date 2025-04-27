# Databricks notebook source
# MAGIC %run general_utilities/ApiAbstract

# COMMAND ----------

# MAGIC %run general_utilities/EltUtil

# COMMAND ----------

# MAGIC %run general_utilities/AzureKeyVault

# COMMAND ----------

from requests import post, auth, get
from requests.auth import HTTPBasicAuth
import json
from pyspark.sql.types import *
from pyspark.sql.functions import *
import requests
import base64

# COMMAND ----------

class JiraUtil(ApiAbstract):
    def __init__(self, environment):
        self.access_token = None
        #self.json = None
        self.az = AzureKv(environment)

    def send_request(self, object_suffix=None , body = None, header = None,  params = None , timestamp= None, maxResults = None, startAt = None, jql = None, fields = None, accountId = None, start = None):
        url = "https://servicetitan.atlassian.net/rest/"+ object_suffix  #api/3/search
        UserEmail = az.get_secret('JiraUserEmail')
        APIToken = az.get_secret('JiraAPIToken')
        auth = HTTPBasicAuth(UserEmail, APIToken)
        headers = {
                    "Accept": "application/json"
                }
        query = {
                    'jql': jql,
                    "maxResults": maxResults,
                    "startAt": startAt,
                    "fields": fields,
                    "accountId": accountId,
                    "start": start
                }
        response = requests.request(
                                    "GET",
                                    url,
                                    headers=headers,
                                    params=query,
                                    auth=auth
                                    )
        return response.json()
    
    def send_request_downstream(self, object_suffix=None , body = None, header = None,  params = None , timestamp= None, maxResults = None, startAt = None, col = None):
        #num_records=0
        startIndex = 0
        haslength = True
        res = spark.createDataFrame([], StructType())
        all_data = self.send_request(object_suffix, maxResults = 1, startAt=0)
        if "total" not in all_data and "values" not in all_data:
            return None
        if "total" in all_data:
            total_page = all_data["total"]
        else:
            total_page = 5000  # The epic api does not return total, we have to default a max by this case.
        response_list = []
        extract_date = current_timestamp()
        sdf = None
        while haslength:
            all_data = self.send_request(object_suffix, maxResults=50, startAt=startIndex)
            # to paginate the data every time run 50 records only.
            if col not in all_data:
                return None
            all_data = all_data[col]
            if not all_data or len(all_data)==0:
                return sdf
            sdf = DatabricksUtil.df_jsonize_array_w_extract_date(all_data, extract_date)
            if res.count() == 0:
                res = sdf
            else:
                res = res.union(sdf)
            num_records=sdf.count()
            startIndex += num_records
            if startIndex >= total_page:
                haslength = False
                return res
            
        
    def get_id(self,id_list, i):
        if id_list == None or i < 0:
            return None
        else:
            return id_list[i][0]
        
    def json_to_dataframe(self, json_data):
        string = json.dumps(json_data)
        value = spark.createDataFrame([string], StringType()).toDF("value")
        return value

        
    def send_request_sla(self, object_suffix=None , body = None, header = None,  params = None , timestamp= None, limit = None, start = None, col = None):
        #num_records=0
        startIndex = 0
        haslength = True
        res = spark.createDataFrame([], StructType())
        all_data = self.send_request(object_suffix, start =0)
        if "size" not in all_data and "values" not in all_data:
            return None
        if "size" in all_data:
            total_page = all_data["size"]
        else:
            total_page = 50  # The epic api does not return total, we have to default a max by this case.
        response_list = []
        extract_date = current_timestamp()
        sdf = None
        while haslength:
            all_data = self.send_request(object_suffix, start =startIndex)
            # by default data every time run 50 records only.
            if col not in all_data:
                return None
            all_data = all_data[col]
            if not all_data or len(all_data)==0:
                return sdf
            sdf = DatabricksUtil.df_jsonize_array_w_extract_date(all_data, extract_date)
            if res.count() == 0:
                res = sdf
            else:
                res = res.union(sdf)
            num_records=sdf.count()
            startIndex += num_records
            if startIndex >= total_page:
                haslength = False
                return res
            

    def send_update_request(self, object_suffix=None , body = None, header = None,  params = None , timestamp= None, unique_association_count= None):
        url = "https://servicetitan.atlassian.net/rest/"+ object_suffix  
        UserEmail = az.get_secret('JiraUserEmail')
        APIToken = az.get_secret('JiraAPIToken')
        auth = HTTPBasicAuth(UserEmail, APIToken)
        headers = {
                    "Accept": "application/json",
                    "Content-Type": "application/json"
                }
        payload = json.dumps( {
                                "fields": {
                                            "customfield_15537": int(unique_association_count)
                                        }
                            } )
        response = requests.request(
                                    "PUT",
                                    url,
                                    data=payload,
                                    headers=headers,
                                    auth=auth
                                    )
        return response