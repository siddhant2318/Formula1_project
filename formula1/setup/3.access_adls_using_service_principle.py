# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Axure data lake Service Principal
# MAGIC ##Steps to follow
# MAGIC 1. Register Azure AD application/service principal
# MAGIC 1. generate a secret password for the application
# MAGIC 1. set spark config with app/client id,directory tenant id and secret
# MAGIC 1. Assign role 'Storage Blob Data Contributor' to the Data lake

# COMMAND ----------

client_id = "80bf8428-e183-471b-8ba1-9d824d8db6e7"
tenant_id ="80b42f78-1bff-4235-83c8-494826352bf9"
client_secret="y6c8Q~08FCBpsf0.Yi_eC8gRgj.~4x1nXG3n7aCs"

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.formula1dl2311.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.formula1dl2311.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.formula1dl2311.dfs.core.windows.net",client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.formula1dl2311.dfs.core.windows.net",client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.formula1dl2311.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dl2311.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1dl2311.dfs.core.windows.net"))
