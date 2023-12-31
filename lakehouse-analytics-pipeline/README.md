#Lakehouse Analytics Pipeline

##Steps:
    1. Launching a Unity Catalog enabled compute cluster.

    2. Creating a Databricks notebook.

    3. Writing and reading data from a Unity Catalog external location.

    4. Configuring incremental data ingestion to a Unity Catalog table with Auto Loader.

    5. Executing notebook cells to process, query, and preview data.

    6. Scheduling a notebook as a Databricks job.

    7. Querying Unity Catalog tables from Databricks SQL

##Step 1: Create a cluster
To do exploratory data analysis and data engineering, create a cluster to provide the compute resources needed to execute commands.

    1.1 Click compute icon Compute in the sidebar.

    1.2 Click New Icon New in the sidebar, then select Cluster. This opens the New Cluster/Compute page.

    1.3 Specify a unique name for the cluster.

    1.4 Select the Single node radio button.

    1.5 Select Single User from the Access mode dropdown.

    1.6 Make sure your email address is visible in the Single User field.

    1.7 Select the desired Databricks runtime version, 11.1 or above to use Unity Catalog.

    1.8 Click Create compute to create the cluster.


##Step 2: Create a Databricks notebook
To get started writing and executing interactive code on Databricks, create a notebook.

    2.1 Click New Icon New in the sidebar, then click Notebook.

    2.2 On the Create Notebook page:

        - Specify a unique name for your notebook.

        - Make sure the default language is set to Python.

        - Use the Connect dropdown menu to select the cluster you created in step 1 from the Cluster dropdown.

The notebook opens with one empty cell.

##Step 3: Write and read data from an external location managed by Unity Catalog 
Databricks recommends using Auto Loader for incremental data ingestion. Auto Loader automatically detects and processes new files as they arrive in cloud object storage.

Use Unity Catalog to manage secure access to external locations. Users or service principals with READ FILES permissions on an external location can use Auto Loader to ingest data.

Normally, data will arrive in an external location due to writes from other systems. In this demo, you can simulate data arrival by writing out JSON files to an external location.

Copy the code below into a notebook cell. Replace the string value for catalog with the name of a catalog with CREATE CATALOG and USE CATALOG permissions. Replace the string value for external_location with the path for an external location with READ FILES, WRITE FILES, and CREATE EXTERNAL TABLE permissions.

External locations can be defined as an entire storage container, but often point to a directory nested in a container.

The correct format for an external location path is "s3://bucket-name/path/to/external_location".

```

 external_location = "<your-external-location>"
 catalog = "<your-catalog>"

 dbutils.fs.put(f"{external_location}/filename.txt", "Hello world!", True)
 display(dbutils.fs.head(f"{external_location}/filename.txt"))
 dbutils.fs.rm(f"{external_location}/filename.txt")

 display(spark.sql(f"SHOW SCHEMAS IN {catalog}"))
```

The Python code below uses your email address to create a unique database in the catalog provided and a unique storage location in external location provided.

```

from pyspark.sql.functions import col

# Set parameters for isolation in workspace and reset demo
username = spark.sql("SELECT regexp_replace(current_user(), '[^a-zA-Z0-9]', '_')").first()[0]
database = f"{catalog}.e2e_lakehouse_{username}_db"
source = f"{external_location}/e2e-lakehouse-source"
table = f"{database}.target_table"
checkpoint_path = f"{external_location}/_checkpoint/e2e-lakehouse-demo"

spark.sql(f"SET c.username='{username}'")
spark.sql(f"SET c.database={database}")
spark.sql(f"SET c.source='{source}'")

spark.sql("DROP DATABASE IF EXISTS ${c.database} CASCADE")
spark.sql("CREATE DATABASE ${c.database}")
spark.sql("USE ${c.database}")

# Clear out data from previous demo execution
dbutils.fs.rm(source, True)
dbutils.fs.rm(checkpoint_path, True)


# Define a class to load batches of data to source
class LoadData:

    def __init__(self, source):
        self.source = source

    def get_date(self):
        try:
            df = spark.read.format("json").load(source)
        except:
            return "2016-01-01"
        batch_date = df.selectExpr("max(distinct(date(tpep_pickup_datetime))) + 1 day").first()[0]
        if batch_date.month == 3:
            raise Exception("Source data exhausted")
        return batch_date

    def get_batch(self, batch_date):
        return (
            spark.table("samples.nyctaxi.trips")
            .filter(col("tpep_pickup_datetime").cast("date") == batch_date)
        )

    def write_batch(self, batch):
        batch.write.format("json").mode("append").save(self.source)

    def land_batch(self):
        batch_date = self.get_date()
        batch = self.get_batch(batch_date)
        self.write_batch(batch)

RawData = LoadData(source)

```

You can now land a batch of data by copying the following code into a cell and executing it. You can manually execute this cell up to 60 times to trigger new data arrival.

```
RawData.land_batch()
```

### Configure Auto Loader to ingest data to Unity Catalog
Databricks recommends storing data with Delta Lake. Delta Lake is an open source storage layer that provides ACID transactions and enables the data lakehouse. Delta Lake is the default format for tables created in Databricks.

To configure Auto Loader to ingest data to a Unity Catalog table, copy and paste the following code into an empty cell in your notebook:

```
# Import functions
from pyspark.sql.functions import col, current_timestamp

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
  .option("mergeSchema", "true")
  .toTable(table))
```

### Process and interact with data
To query the table you’ve just created, copy and paste the following code into an empty cell, then press SHIFT+ENTER to run the cell.

```
df = spark.read.table(table_name)
```

To preview the data in your DataFrame, copy and paste the following code into an empty cell, then press SHIFT+ENTER to run the cell.

```
display(df)
```

Reference:
https://docs.databricks.com/en/getting-started/lakehouse-e2e.html