# Databricks notebook source
# MAGIC %md 
# MAGIC ###Ingest circuits.csv file
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 1- Read the CSV file using the spark dataframe Reader

# COMMAND ----------

dbutils.widgets.help()

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")


# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

from pyspark.sql.types import StructField,StructType,IntegerType,StringType,DoubleType

# COMMAND ----------

circuits_schema = StructType(fields = [StructField("circuitId",IntegerType(),False),
                                       StructField("circuitRef",StringType(),False),
                                       StructField("name",StringType(),False),
                                       StructField("location",StringType(),False),
                                       StructField("country",StringType(),False),
                                       StructField("lat",DoubleType(),False),
                                       StructField("lng",DoubleType(),False),
                                       StructField("alt",IntegerType(),False),
                                       StructField("url",StringType(),False)                    
                                    ])

# COMMAND ----------

circuits_df=spark.read.option("header",True).schema(circuits_schema).csv(f"{raw_folder_path}/circuits.csv")

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

# %fs
# ls /mnt/formula1dl2311/raw


# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - select only the required columns
# MAGIC

# COMMAND ----------

circuits_selected_df = circuits_df.select("circuitId","circuitRef","name","location","country","lat","lng")
display(circuits_selected_df);

# COMMAND ----------

# MAGIC %md
# MAGIC #### some other ways to use select 

# COMMAND ----------

# circuits_selected_df = circuits_df.select(circuits_df.circuitId,circuits_df.circuitRef,circuits_df.name,circuits_df.location,circuits_df.country)
circuits_selected_df = circuits_df.select(circuits_df["circuitId"],circuits_df["circuitRef"],circuits_df["name"],circuits_df["location"],circuits_df["country"],circuits_df["lat"],circuits_df["lng"],circuits_df["alt"])
# circuits_selected_df = circuits_df.select(circuits_df["circuitId"],circuits_df["circuitRef"],circuits_df["name"],circuits_df["location"],circuits_df["country"])
# from pyspark.sql.functions import col

# circuits_selected_df = circuits_df.select(col("circuitId"),col("circuitRef"),col("name").alias("raceName"),col("location"))

display(circuits_selected_df);



# COMMAND ----------

# MAGIC %md
# MAGIC #####Step 3 - Rename the columns as required

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

circuits_renamed_df = circuits_selected_df.withColumnRenamed("circuitid","circuit_id")\
    .withColumnRenamed("circuitRef","circuit_Ref")\
    .withColumnRenamed("lat","latitude")\
    .withColumnRenamed("lng","longitude")\
    .withColumnRenamed("alt","altitude")\
    .withColumn("data_source",lit(v_data_source))    
  

# COMMAND ----------

display(circuits_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ####step 4 - add ingestion date to the dataframe

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit,to_timestamp,concat,col

# COMMAND ----------

circuits_final_df = add_ingestion_date(circuits_renamed_df)
    
    # .withColumn("env",lit("production"))
display(circuits_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ####step 5 - Write data to datalake as parquet
# MAGIC

# COMMAND ----------

circuits_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/circuits")

# COMMAND ----------

display(spark.read.parquet(f"{processed_folder_path}/circuits"))
