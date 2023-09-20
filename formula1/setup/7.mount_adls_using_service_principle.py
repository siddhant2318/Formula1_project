# Databricks notebook source
# MAGIC %md
# MAGIC #### Mount azure datalake using Service Principal
# MAGIC ##Steps to follow
# MAGIC 1. Get client id,tenant id ,client secret from key vault
# MAGIC 1. set spark config with app/client id,directory/tenant id and secret
# MAGIC 1. Call file system utility mount to mount the storage
# MAGIC 1. Explore other file system utilities related to mount(list all mount and unmounts)

# COMMAND ----------

client_id = "80bf8428-e183-471b-8ba1-9d824d8db6e7"
tenant_id ="80b42f78-1bff-4235-83c8-494826352bf9"
client_secret="y6c8Q~08FCBpsf0.Yi_eC8gRgj.~4x1nXG3n7aCs"

# COMMAND ----------

# spark.conf.set("fs.azure.account.auth.type.formula1dl2311.dfs.core.windows.net", "OAuth")
# spark.conf.set("fs.azure.account.oauth.provider.type.formula1dl2311.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
# spark.conf.set("fs.azure.account.oauth2.client.id.formula1dl2311.dfs.core.windows.net",client_id)
# spark.conf.set("fs.azure.account.oauth2.client.secret.formula1dl2311.dfs.core.windows.net",client_secret)
# spark.conf.set("fs.azure.account.oauth2.client.endpoint.formula1dl2311.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}


# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://demo@formula1dl2311.dfs.core.windows.net/",
  mount_point = "/mnt/formula1dl2311/demo",
  extra_configs = configs)

# COMMAND ----------

display(dbutils.fs.ls("/mnt/formula1dl2311/demo"))

# COMMAND ----------

display(spark.read.csv("/mnt/formula1dl2311/demo/circuits.csv"))

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

dbutils.fs.unmount("/mnt/formula1dl2311/demo")
