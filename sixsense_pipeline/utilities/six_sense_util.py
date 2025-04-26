# Databricks notebook source
# MAGIC %%capture
# MAGIC !pip install paramiko

# COMMAND ----------

import paramiko
import os

# COMMAND ----------

class SixSenseUtil():
    
    def __init__(self, host:str, username:str, password:str):
        self.host = host
        self.username = username
        self.password = password

    
    def _connect_sftp_server(self):
        transport = paramiko.Transport((self.host, 22))
        transport.connect(
            username=self.username,
            password=self.password
        )
        sftp = paramiko.SFTPClient.from_transport(transport)
        return sftp
    
    def _pull_file_from_sftp(self, source:str, destination:str):
        sftp = self._connect_sftp_server()
        sftp.get(source, destination)
        print("transfer ok")
        df = spark.read.format("csv").load(destination.replace("/dbfs/","dbfs:/"))
        print("df ok")
        sftp.close()

        return df
    
    def _drop_file_locally(self, files:list):
        for file in files:
            os.remove(file)
        print("remove ok")
        return None