# Databricks notebook source
# MAGIC %run /Workspace/Users/naval_1724213487060@npupgradassessment.onmicrosoft.com/day8/includes

# COMMAND ----------

df=spark.read.csv(f"{input_path}products.csv",header=True,inferSchema=True)
df1=add_ingestion_col(df)
df1.write.mode("overwrite").saveAsTable("bronze.products_bronze")
