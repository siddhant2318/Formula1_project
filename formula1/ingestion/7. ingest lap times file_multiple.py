# Databricks notebook source
# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,TimestampType,StringType

# COMMAND ----------

lap_times_schema = StructType(fields =[StructField("raceId",IntegerType(),False),
                                       StructField("driverId",IntegerType(),False),
                                       StructField("lap",IntegerType(),False),
                                       StructField("positon",IntegerType(),True),
                                       StructField("time",StringType(),True),                                       
                                       StructField("milliseconds",IntegerType(),True) ])

# COMMAND ----------

lap_times_df = spark.read.option("header",False).schema(lap_times_schema).csv(f"{raw_folder_path}/lap_times/")
display(lap_times_df)

# COMMAND ----------

lap_times_df.count()

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
lap_times_final_df = lap_times_df.withColumnRenamed("raceId","race_id")\
                                    .withColumnRenamed("driverId","driver_id")
lap_times_final_df = add_ingestion_date(lap_times_final_df)                                    
display(lap_times_final_df)

# COMMAND ----------

lap_times_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/lap_times")

# COMMAND ----------

display(spark.read.parquet(f"{processed_folder_path}/lap_times"))
