# Databricks notebook source
# MAGIC %md # AzureKv class to help with setting and getting secrets
# MAGIC
# MAGIC -https://docs.microsoft.com/en-us/azure/databricks/security/secrets/
# MAGIC
# MAGIC ### Summary
# MAGIC - In Azure Databricks we are using Azure KeyVault backed scopes for secret management. The utility class helps with setting and retrieving credentials from KeyVault
# MAGIC
# MAGIC #Please note:
# MAGIC
# MAGIC - for getting full list of all keys in specific scope you can run : #dbutils.secrets.list(your scope)
# MAGIC
# MAGIC
# MAGIC - for getting full list of all scopes you can run: #dbutils.secrets.listScopes()

# COMMAND ----------

import requests

# COMMAND ----------

class AzureKv: 
    def __init__(self, env = None):
        self.environment = "prd" if env == "prod" else "dev"
        dev_scope = "dev-keyvault-scope"
        dev_kv_url = "https://itopsedsdevkeyvault.vault.azure.net/secrets/"
        prd_scope = "shared-creds"
        prd_kv_url =  "https://itopsedskeyvault.vault.azure.net/secrets/"
        
        self.env_map = {"dev" : (dev_scope, dev_kv_url), 
                        "prd" : (prd_scope, prd_kv_url)}

        self.token_url = "https://login.microsoftonline.com/fc1f6b68-bb68-429e-8230-5fe7bb899d52/oauth2/v2.0/token"
        self.__token = None
    
    def autheticate(self):
        scope = self.env_map.get(self.environment)[0]
        token_body = {"client_id" : dbutils.secrets.get(scope = scope, key="azkvClientId"),
                      "client_secret" : dbutils.secrets.get(scope = scope, key="azkvClientSecret"),
                      "scope" : "https://vault.azure.net/.default",
                      "grant_type": "client_credentials"
        }
        token_headers = {
            "Content-Type": "application/x-www-form-urlencoded"
        }
        get_token = requests.post(self.token_url, data=token_body, headers = token_headers )
        self.__token = get_token.json().get("access_token")

        
    def set_secret(self, key, value): 
        #set token
        self.autheticate()
        kv_url = self.env_map.get(self.environment)[1]
        secret_headers = {"Authorization" : "Bearer " + self.__token,
                 "Content-Type" : "application/json"}
        secret_url = kv_url + key
        secret_body = {
            "value" : value
        }
        querystring = {"api-version":"7.2"}
        response = requests.request("PUT", secret_url, json= secret_body, headers= secret_headers, params=querystring)
        return response.text
        
    def get_secret(self, key):
        scope = self.env_map.get(self.environment)[0]
        return dbutils.secrets.get(scope = scope, key = key)