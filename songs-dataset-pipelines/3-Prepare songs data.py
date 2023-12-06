# Databricks notebook source
# MAGIC %md
# MAGIC ### Step-3: Prepare Songs Data

# COMMAND ----------

# MAGIC %md
# MAGIC catalog_name = "data_pipelines" <br>
# MAGIC schema_name = "songs" <br>
# MAGIC tbl_name ="song_raw_data"

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE
# MAGIC   data_pipelines.songs.prepared_song_data (
# MAGIC     artist_id STRING,
# MAGIC     artist_name STRING,
# MAGIC     duration DOUBLE,
# MAGIC     release STRING,
# MAGIC     tempo DOUBLE,
# MAGIC     time_signature DOUBLE,
# MAGIC     title STRING,
# MAGIC     year DOUBLE,
# MAGIC     processed_time TIMESTAMP
# MAGIC   );
# MAGIC
# MAGIC INSERT INTO
# MAGIC   data_pipelines.songs.prepared_song_data
# MAGIC SELECT
# MAGIC   artist_id,
# MAGIC   artist_name,
# MAGIC   duration,
# MAGIC   release,
# MAGIC   tempo,
# MAGIC   time_signature,
# MAGIC   title,
# MAGIC   year,
# MAGIC   current_timestamp()
# MAGIC FROM
# MAGIC   data_pipelines.songs.song_raw_data
# MAGIC
