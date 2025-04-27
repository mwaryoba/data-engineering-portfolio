# Databricks notebook source
# MAGIC %md # ApiAbstract class
# MAGIC
# MAGIC ### Summary
# MAGIC - This class is intended to unify all API calls for ingesting data from 3rd party vendors. All Api calls classes MUST inherit from this abstract class. 

# COMMAND ----------

from abc import ABC, abstractmethod  

# COMMAND ----------

class ApiAbstract(ABC):
  @classmethod
  @abstractmethod
  def send_request(object_suffix=None , header = None, body = None, params = None , timestamp= None):
    pass
  
  def autheticate(self, *args, **kwargs):
    raise NotImplementedError("This method wasn't implemented")