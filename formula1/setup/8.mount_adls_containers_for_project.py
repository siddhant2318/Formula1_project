# Databricks notebook source
# MAGIC %md
# MAGIC #### Mount adls containers for project
# MAGIC
# MAGIC

# COMMAND ----------

def mount_adls(storage_account_name,container_name):
    #get secrets from key vault
    client_id = "80bf8428-e183-471b-8ba1-9d824d8db6e7"
    tenant_id ="80b42f78-1bff-4235-83c8-494826352bf9"
    client_secret="y6c8Q~08FCBpsf0.Yi_eC8gRgj.~4x1nXG3n7aCs"    
    #set spark configuration
    configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}
    #mount the storage account container
    dbutils.fs.mount(
        source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
        mount_point = f"/mnt/{storage_account_name}/{container_name}",
        extra_configs = configs)
    display(dbutils.fs.mounts())

# COMMAND ----------

# mount_adls('formula1dl2311','demo')
display(dbutils.fs.mounts())
# dbutils.fs.unmount('/mnt/{storage_account_name}/{container_name}')

# COMMAND ----------

mount_adls('formula1dl2311','raw')
# display(dbutils.fs.mounts())
# dbutils.fs.unmount('/mnt/{storage_account_name}/{container_name}')

# COMMAND ----------

mount_adls('formula1dl2311','processed')
display(dbutils.fs.mounts())
# dbutils.fs.unmount('/mnt/{storage_account_name}/{container_name}')

# COMMAND ----------

mount_adls('formula1dl2311','presentation')
display(dbutils.fs.mounts())
# dbutils.fs.unmount('/mnt/{storage_account_name}/{container_name}')
