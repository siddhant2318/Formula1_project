# Databricks notebook source
display(dbutils.fs.ls('/FileStore/'))
# display(dbutils.fs.ls('/databricks-datasets/COVID/'))
display(spark.read.csv('/FileStore/circuits.csv'))


# COMMAND ----------


