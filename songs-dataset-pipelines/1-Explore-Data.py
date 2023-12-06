# Databricks notebook source
# MAGIC %md
# MAGIC ### Step-1:  Explore the source data 

# COMMAND ----------

# MAGIC %fs
# MAGIC ls "/databricks-datasets/songs/data-001"

# COMMAND ----------

# MAGIC %fs 
# MAGIC head --maxBytes=10000 "/databricks-datasets/songs/README.md"

# COMMAND ----------

# MAGIC %fs
# MAGIC head --maxBytes=10000 "/databricks-datasets/songs/data-001/part-00000"

# COMMAND ----------

df = spark.read.format('csv').option("sep", "\t").load('dbfs:/databricks-datasets/songs/data-001/part-00000')
df.display()
df.printSchema()
