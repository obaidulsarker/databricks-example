# Databricks notebook source
# MAGIC %md
# MAGIC ##  Configure Auto Loader to ingest data to Delta Lake

# COMMAND ----------

# Import functions
from pyspark.sql.functions import col, current_timestamp

# Define variables used in the code below
catalog_name = "data_pipelines"
schema_name = "basics"
tbl_name ="events_raw_data"

# Define variables used in code below
file_path = "/databricks-datasets/structured-streaming/events"
username = spark.sql("SELECT regexp_replace(current_user(), '[^a-zA-Z0-9]', '_')").first()[0]
table_name = f"{catalog_name}.{schema_name}.{tbl_name}"
#table_name = f"{username}_etl_quickstart"
checkpoint_path = f"/tmp/{username}/_checkpoint/{tbl_name}"

# Clear out data from previous demo execution
spark.sql(f"DROP TABLE IF EXISTS {table_name}")
dbutils.fs.rm(checkpoint_path, True)

# Configure Auto Loader to ingest JSON data to a Delta table
(spark.readStream
  .format("cloudFiles")
  .option("cloudFiles.format", "json")
  .option("cloudFiles.schemaLocation", checkpoint_path)
  .load(file_path)
  .select("*", col("_metadata.file_path").alias("source_file"), current_timestamp().alias("processing_time"))
  .writeStream
  .option("checkpointLocation", checkpoint_path)
  .trigger(availableNow=True)
  .toTable(table_name))


# COMMAND ----------

# MAGIC %md
# MAGIC ## Process and interact with data 

# COMMAND ----------

df = spark.read.table(table_name)
display(df)
