# Databricks notebook source
# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DateType,FloatType

# COMMAND ----------

qualifying_schema = StructType(fields =[StructField("qualifyId",IntegerType(),False),
                                       StructField("raceId",IntegerType(),False),
                                       StructField("driverId",IntegerType(),False),
                                       StructField("constructorId",IntegerType(),False),
                                       StructField("number",IntegerType(),False),                                       
                                       StructField("position",IntegerType(),True),
                                       StructField("q1",StringType(),True),
                                       StructField("q2",StringType(),True),
                                       StructField("q3",StringType(),True) ])

# COMMAND ----------

qualifying_df = spark.read.option("multiline",True).schema(qualifying_schema).json(f"{raw_folder_path}/qualifying")
display(qualifying_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
qualifying_final_df = qualifying_df.withColumnRenamed("raceId","race_id")\
                                    .withColumnRenamed("driverId","driver_id")\
                                    .withColumnRenamed("qualifyId","qualify_id")\
                                    .withColumnRenamed("constructorId","constructor_id")
qualifying_final_df = add_ingestion_date(qualifying_final_df)                                    
                                    
display(qualifying_final_df)                                    


# COMMAND ----------

qualifying_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/qualifying")
display(spark.read.parquet(f"{processed_folder_path}/qualifying"))
