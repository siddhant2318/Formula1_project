# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/races")
races_df.show(2)


# COMMAND ----------

final_races_df = races_df.where(races_df["circuit_id"] == 21)
display(final_races_df)
