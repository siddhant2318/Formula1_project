# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingest constructors.json file  

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1- Read the json file using the Sparkdataframe Reader

# COMMAND ----------

constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"
constructors_df =spark.read.option("header",True).schema(constructors_schema).json(f"{raw_folder_path}/constructors.json")
display(constructors_df)
constructors_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step 2- Drop the unwanted rows from the dataframe

# COMMAND ----------

selected_df = constructors_df.drop("url")
display(selected_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step 3- Rename the column names and add ingestion date column
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
final_constructors_df = selected_df.withColumnRenamed("constructorId","constructor_id")\
                                    .withColumnRenamed("constructorRef","constructor_ref")
final_constructors_df =add_ingestion_date(final_constructors_df)
display(final_constructors_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 4-write output to parquet file

# COMMAND ----------

final_constructors_df.write.mode("overwrite").parquet(f"{processed_folder_path}/constructors")
display(spark.read.parquet(f"{processed_folder_path}/constructors"))
