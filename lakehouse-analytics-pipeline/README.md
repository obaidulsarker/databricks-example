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
