# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Axure data lake using SAS Token
# MAGIC 1. set the spark config for SAS Token
# MAGIC 1. list files from demo container
# MAGIC 1. read data from circuits.csv file

# COMMAND ----------

dbutils.fs.ls("abfss://container@storageAccount.dfs.core.windows.net/external-location/path/to/data")

spark.read.format("parquet").load("abfss://container@storageAccount.dfs.core.windows.net/external-location/path/to/data")

spark.sql("SELECT * FROM parquet.`abfss://container@storageAccount.dfs.core.windows.net/external-location/path/to/data`")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dl2311.dfs.core.windows.net"))
