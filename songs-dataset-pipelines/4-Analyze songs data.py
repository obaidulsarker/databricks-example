# Databricks notebook source
# MAGIC %md
# MAGIC ### Step 4: Query the transformed data

# COMMAND ----------

# MAGIC %md
# MAGIC catalog_name = data_pipelines <br>
# MAGIC schema_name = songs <br>
# MAGIC tbl_name = prepared_song_data

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Which artists released the most songs each year?
# MAGIC SELECT
# MAGIC   artist_name,
# MAGIC   count(artist_name) AS num_songs,
# MAGIC   year
# MAGIC FROM
# MAGIC   data_pipelines.songs.prepared_song_data
# MAGIC WHERE
# MAGIC   year > 0
# MAGIC GROUP BY
# MAGIC   artist_name,
# MAGIC   year
# MAGIC ORDER BY
# MAGIC   num_songs DESC,
# MAGIC   year DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC  -- Find songs for your DJ list
# MAGIC  SELECT
# MAGIC    artist_name,
# MAGIC    title,
# MAGIC    tempo
# MAGIC  FROM
# MAGIC    data_pipelines.songs.prepared_song_data
# MAGIC  WHERE
# MAGIC    time_signature = 4
# MAGIC    AND
# MAGIC    tempo between 100 and 140;
# MAGIC
