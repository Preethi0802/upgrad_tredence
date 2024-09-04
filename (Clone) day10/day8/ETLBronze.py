# Databricks notebook source
# MAGIC %run /Workspace/Users/preethi_1724384753078@npupgradassessment.onmicrosoft.com/day10/day8/includes

# COMMAND ----------

df=spark.read.csv(f"{input_path}products.csv",header=True,inferSchema=True)
df1=add_ingestion_col(df)
df1.write.mode("overwrite").saveAsTable("bronze.products_bronze")

# COMMAND ----------

# MAGIC %run /Workspace/Users/preethi_1724384753078@npupgradassessment.onmicrosoft.com/day10/day8/includes

# COMMAND ----------

input_path="/Workspace/Users/preethi_1724384753078@npupgradassessment.onmicrosoft.com/day10/day8/includes"

# COMMAND ----------

dbutils.widgets.text("environment","dev")
v=dbutils.widgets.get("environment")

# COMMAND ----------

df=spark.read.csv(f"{input_path}products.csv",header=True,inferSchema=True)
df1=add_ingestion_col(df)
df2=df1.withColumn("environment",lit(v))
df2.write.mode("overwrite").option("mergeSchema", "true").saveAsTable("bronze.products_bronze")
