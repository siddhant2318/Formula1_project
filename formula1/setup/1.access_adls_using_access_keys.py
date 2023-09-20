# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Axure data lake using access keys
# MAGIC 1. set the spark config fs.azure.account.key
# MAGIC 1. list files from demo container
# MAGIC 1. read data from circuits.csv file

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.formula1dl2311.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.formula1dl2311.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.formula1dl2311.dfs.core.windows.net","sp=rl&st=2023-09-07T10:02:25Z&se=2023-09-07T18:02:25Z&spr=https&sv=2022-11-02&sr=c&sig=jUcsdN%2BFXdjmZ0NHnRFdPdCYSmIX8i6HJTpO%2FcnGzEo%3D")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dl2311.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1dl2311.dfs.core.windows.net"))
