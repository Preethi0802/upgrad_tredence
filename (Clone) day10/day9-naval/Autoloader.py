# Databricks notebook source
# MAGIC %run /Workspace/Users/naval_1724213487060@npupgradassessment.onmicrosoft.com/day8/includes

# COMMAND ----------

# DBTITLE 1,Stream
# MAGIC %sql
# MAGIC spark\
# MAGIC .readStream\
# MAGIC .schema(users_schema)\
# MAGIC .csv(f"{input_path}streaminput",header=True)\
# MAGIC .writeStream\
# MAGIC .option("checkpointLocation","dbfs:/mnt/upgradtrendenceadls/delta/files/output/checkpoint")\
# MAGIC .trigger(once=True)\
# MAGIC .table("stream")

# COMMAND ----------

input_path

# COMMAND ----------

# DBTITLE 1,Autoloader
(spark
 .readStream
 .format("cloudFiles")
 .option("cloudFiles.format","csv")
 .option("cloudFiles.inferColumnTypes","true")
 .load(f"{input_path}streaminput")
 .writeStream
 .option("checkpointLocation","dbfs:/mnt/upgradtrendenceadls/delta/files/output/checkpoint_autoloader")
 .table("autoloader")
)

# COMMAND ----------

(spark
 .readStream
 .format("cloudFiles")
 .option("cloudFiles.format","csv")
 .option("cloudFiles.inferColumnTypes","true")
 .option("cloudFiles.schemaLocation","dbfs:/mnt/upgradtrendenceadls/delta/files/output/schema_location")
 .load(f"{input_path}streaminput")
 .writeStream
 .option("checkpointLocation","dbfs:/mnt/upgradtrendenceadls/delta/files/output/checkpoint_autoloader")
 .trigger(once=True)
 .table("autoloader")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from autoloader

# COMMAND ----------


