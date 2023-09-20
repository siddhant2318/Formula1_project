# Databricks notebook source
# MAGIC %md 
# MAGIC ##### Step 1-Read the CSV file using the spark dataframe Reader
# MAGIC

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

races_df=spark.read.option("header",True).csv("/mnt/formula1dl2311/raw/races.csv")
display(races_df)

# COMMAND ----------

from pyspark.sql.types import StructField,StructType,IntegerType,StringType,DoubleType,DateType

races_schema = StructType(fields = [StructField("raceId",IntegerType(),False),
                                 StructField("year",IntegerType(),True),
                                 StructField("round",IntegerType(),True),
                                 StructField("circuitId",IntegerType(),True),
                                 StructField("name",StringType(),True),
                                 StructField("date",DateType(),True),
                                 StructField("time",StringType(),True),
                                 StructField("url",StringType(),True)
                                 ])


# COMMAND ----------

races_df=spark.read.option("header",True).schema(races_schema).csv(f"{raw_folder_path}/races.csv")
races_df.printSchema()
display(races_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC #####Step 2-select only the required columns

# COMMAND ----------

from pyspark.sql.functions import col

races_selected_df = races_df.select(races_df.raceId,races_df.year,races_df.round,races_df.circuitId,races_df.name,races_df.date,races_df.time)
display(races_selected_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### step 3 - rename the columns as per requirement

# COMMAND ----------

races_renamed_df = races_selected_df.withColumnRenamed("raceId","race_id")\
    .withColumnRenamed("year","race_year")\
    .withColumnRenamed("circuitId","circuit_id")\
    .withColumnRenamed("raceId","race_id")
display(races_renamed_df)    
    


# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 4 - add ingestion date to dataframe

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit,to_timestamp

# COMMAND ----------

races_final_df = add_ingestion_date(races_renamed_df)
races_final_df =races_final_df.withColumn("race_timestamp",to_timestamp(concat(col('date'),lit(' '),col('time')),'yyyy-MM-dd HH:mm:ss'))
    # .withColumn("env",lit("production"))
races_final_df = races_final_df.select(races_final_df.race_id,races_final_df.race_year,races_final_df.round,races_final_df.circuit_id,races_final_df.name,races_final_df.ingestion_date,races_final_df.race_timestamp)
display(races_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step 5 - Write final data to datalake as parquet

# COMMAND ----------

races_final_df.write.mode('overwrite').partitionBy('race_year').parquet(f"{processed_folder_path}/races")
# display(spark.read.parquet("/mnt/formula1dl2311/processed/races"))
