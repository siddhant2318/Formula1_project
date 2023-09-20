# Databricks notebook source
# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

from pyspark.sql.types import IntegerType,StringType,StructField,StringType,TimestampType,StructType
from pyspark.sql.functions import current_timestamp

# COMMAND ----------

pit_stops_schema = StructType(fields=[StructField("raceId",IntegerType(),False),
                                 StructField("driverId",IntegerType(),False),
                                 StructField("stop",IntegerType(),False),
                                 StructField("lap",StringType(),False),
                                 StructField("time",TimestampType(),False),
                                 StructField("duration",StringType(),True),
                                 StructField("milliseconds",IntegerType(),True)
                                 ])

# COMMAND ----------

pit_stops_df = spark.read.schema(pit_stops_schema).option("multiLine",True).json(f"{raw_folder_path}/pit_stops.json")
display(pit_stops_df)

# COMMAND ----------

pit_stops_final_df = pit_stops_df.withColumnRenamed("raceId","race_id")\
                                    .withColumnRenamed("driverId","driver_id")\
                                    
pit_stops_final_df = add_ingestion_date(pit_stops_final_df)                                    
display(pit_stops_final_df)

# COMMAND ----------

pit_stops_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/pit_stops")
display(spark.read.parquet(f"{processed_folder_path}/pit_stops"))
