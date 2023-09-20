# Databricks notebook source
# MAGIC %md
# MAGIC #####Ingest drivers.json file

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md 
# MAGIC #####Step 1 -Read the JSON file using the spark dataframe API

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DateType

# COMMAND ----------

name_schema = StructType(fields=[StructField("forename",StringType(),True),
                                 StructField("surname",StringType(),True)])

drivers_schema = StructType(fields=[StructField("driverId",IntegerType(),False),
                                 StructField("driverRef",StringType(),True),
                                 StructField("number",IntegerType(),True),
                                 StructField("code",StringType(),True),
                                 StructField("name",name_schema,True),
                                 StructField("nationality",StringType(),True),
                                 StructField("dob",DateType(),True)
                                 ])

# COMMAND ----------

drivers_df = spark.read.schema(drivers_schema).json(f"{raw_folder_path}/drivers.json")
display(drivers_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step 2- Rename columns and add new columns

# COMMAND ----------

from pyspark.sql.functions import col,current_timestamp,concat,lit

# COMMAND ----------

drivers_renamed_col = drivers_df.withColumnRenamed("driverId","driver_id") \
                                .withColumnRenamed("driverRef","driver_Ref")\
                                .withColumn("name",concat(col("name.forename"),lit(" "),col("name.surname")) )
drivers_renamed_col = add_ingestion_date(drivers_renamed_col)                                   
display(drivers_renamed_col)                                

# COMMAND ----------

# MAGIC %md 
# MAGIC #####Step 3- Drop the unwanted columns

# COMMAND ----------

drivers_final_df = drivers_renamed_col.drop("url")
display(drivers_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step 4- load the final df to processed folder in parquet format

# COMMAND ----------

drivers_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/drivers")

# COMMAND ----------

display(spark.read.parquet(f"{processed_folder_path}/drivers"))
