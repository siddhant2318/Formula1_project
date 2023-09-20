# Databricks notebook source
# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DateType,FloatType

# COMMAND ----------



results_schema = StructType(fields=[StructField("resultid",IntegerType(),False),
                                      StructField("raceId",IntegerType(),False),
                                      StructField("driverId",IntegerType(),False),
                                      StructField("constructorId",IntegerType(),False),
                                      StructField("number",IntegerType(),True),
                                      StructField("grid",IntegerType(),False),
                                      StructField("position",IntegerType(),True),
                                      StructField("positionText",StringType(),False),
                                      StructField("positionOrder",IntegerType(),False),
                                      StructField("points",FloatType(),True),
                                      StructField("laps",IntegerType(),True),
                                      StructField("time",StringType(),True),
                                      StructField("milliseconds",IntegerType(),True),
                                      StructField("fastestlap",IntegerType(),True),
                                      StructField("rank",IntegerType(),True),
                                      StructField("fastestLaptime",StringType(),True),
                                      StructField("fastestLapSpeed",StringType(),True),
                                      StructField("statusId",IntegerType(),False)])

# COMMAND ----------

results_df = spark.read.option("header",True).schema(results_schema).json(f"{raw_folder_path}/results.json")
display(results_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### step 2 - Rename the file
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

results_df_renamed = results_df.withColumnRenamed("resultId","result_id")\
                               .withColumnRenamed("raceId","race_id")\
                               .withColumnRenamed("driverId","driver_id")\
                               .withColumnRenamed("constructorId","constructor_id")\
                               .withColumnRenamed("positionText","position_text")\
                               .withColumnRenamed("positionOrder","Position_order")\
                               .withColumnRenamed("fastestLapTime","fastest_lap_time")\
                               .withColumnRenamed("fastestLapSpeed","fastest_lap_speed")\
                               .withColumnRenamed("fastestLap","fastest_lap")
                               
results_df_renamed = add_ingestion_date(results_df_renamed) 
display(results_df_renamed)                                


# COMMAND ----------

results_df_final = results_df_renamed

# COMMAND ----------

results_df_final.write.mode("overwrite").parquet(f"{processed_folder_path}/results")

# COMMAND ----------

display(spark.read.parquet(f"{processed_folder_path}/results"))
