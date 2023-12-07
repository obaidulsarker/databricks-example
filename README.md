# databricks-example
Databricks example codes

https://docs.databricks.com/en/getting-started/data-pipeline-get-started.html


Project:

Example: Million Song dataset

## Step 1: Create a cluster
To perform the data processing and analysis in this example, create a cluster to provide the compute resources needed to run commands.


    1.1 Click Compute in the sidebar.

    1.2 On the Compute page, click Create Cluster.

    1.3 On the New Cluster page, enter a unique name for the cluster.

    1.4 In Access mode, select Single User.

    1.5 In Single user or service principal access, select your user name.

    1.6 Leave the remaining values in their default state, and click Create Cluster.

## Step 2: Explore the source data
  To learn how to use the Databricks interface to explore the raw source data, see Explore the source data for a data pipeline. If you want to go directly to ingesting and preparing the data, continue to Step 3: Ingest the raw data.
  https://docs.databricks.com/en/getting-started/data-pipeline-explore-data.html


## Step 3: Ingest the raw data
  In this step, you load the raw data into a table to make it available for further processing. To manage data assets on the Databricks platform such as tables, Databricks recommends Unity Catalog. However, if you don’t have permissions to create the required catalog and schema to publish tables to Unity Catalog, you can still complete the following steps by publishing tables to the Hive metastore.

  To ingest data, Databricks recommends using Auto Loader. Auto Loader automatically detects and processes new files as they arrive in cloud object storage.

  You can configure Auto Loader to automatically detect the schema of loaded data, allowing you to initialize tables without explicitly declaring the data schema and evolve the table schema as new columns are introduced. This eliminates the need to manually track and apply schema changes over time. Databricks recommends schema inference when using Auto Loader. However, as seen in the data exploration step, the songs data does not contain header information. Because the header is not stored with the data, you’ll need to explicitly define the schema, as shown in the next example.

  3.1 In the sidebar, click New Icon New and select Notebook from the menu. The Create Notebook dialog appears.

  3.2 Enter a name for the notebook, for example, Ingest songs data. By default:

    Python is the selected language.

    The notebook is attached to the last cluster you used. In this case, the cluster you created in Step 1: Create a cluster.

  3.3 Click Create.

  3.4 Enter the following into the first cell of the notebook:
  ```
    from pyspark.sql.types import DoubleType, IntegerType, StringType, StructType, StructField

    # Define variables used in the code below
    file_path = "/databricks-datasets/songs/data-001/"
    table_name = "<table-name>"
    checkpoint_path = "/tmp/pipeline_get_started/_checkpoint/song_data"

    schema = StructType(
      [
        StructField("artist_id", StringType(), True),
        StructField("artist_lat", DoubleType(), True),
        StructField("artist_long", DoubleType(), True),
        StructField("artist_location", StringType(), True),
        StructField("artist_name", StringType(), True),
        StructField("duration", DoubleType(), True),
        StructField("end_of_fade_in", DoubleType(), True),
        StructField("key", IntegerType(), True),
        StructField("key_confidence", DoubleType(), True),
        StructField("loudness", DoubleType(), True),
        StructField("release", StringType(), True),
        StructField("song_hotnes", DoubleType(), True),
        StructField("song_id", StringType(), True),
        StructField("start_of_fade_out", DoubleType(), True),
        StructField("tempo", DoubleType(), True),
        StructField("time_signature", DoubleType(), True),
        StructField("time_signature_confidence", DoubleType(), True),
        StructField("title", StringType(), True),
        StructField("year", IntegerType(), True),
        StructField("partial_sequence", IntegerType(), True)
      ]
    )

    (spark.readStream
      .format("cloudFiles")
      .schema(schema)
      .option("cloudFiles.format", "csv")
      .option("sep","\t")
      .load(file_path)
      .writeStream
      .option("checkpointLocation", checkpoint_path)
      .trigger(availableNow=True)
      .toTable(table_name)
    )
  ```
  If you are using Unity Catalog, replace <table-name> with a catalog, schema, and table name to contain the ingested records (for example, data_pipelines.songs_data.raw_song_data). Otherwise, replace <table-name> with the name of a table to contain the ingested records, for example, raw_song_data.

  Replace <checkpoint-path> with a path to a directory in DBFS to maintain checkpoint files, for example, /tmp/pipeline_get_started/_checkpoint/song_data.

  3.5 Click Run Menu, and select Run Cell. This example defines the data schema using the information from the README, ingests the songs data from all of the files contained in file_path, and writes the data to the table specified by table_name.


## Step 4: Prepare the raw data

    To prepare the raw data for analysis, the following steps transform the raw songs data by filtering out unneeded columns and adding a new field containing a timestamp for the creation of the new record.

    4.1 In the sidebar, click New Icon New and select Notebook from the menu. The Create Notebook dialog appears.

    4.2 Enter a name for the notebook. For example, Prepare songs data. Change the default language to SQL.

    4.3 Click Create.

    4.4 Enter the following in the first cell of the notebook:
  
      CREATE OR REPLACE TABLE
        <table-name> (
          artist_id STRING,
          artist_name STRING,
          duration DOUBLE,
          release STRING,
          tempo DOUBLE,
          time_signature DOUBLE,
          title STRING,
          year DOUBLE,
          processed_time TIMESTAMP
        );

      INSERT INTO
        <table-name>
      SELECT
        artist_id,
        artist_name,
        duration,
        release,
        tempo,
        time_signature,
        title,
        year,
        current_timestamp()
      FROM
        <raw-songs-table-name>
    
    If you are using Unity Catalog, replace <table-name> with a catalog, schema, and table name to contain the filtered and transformed records (for example, data_pipelines.songs_data.prepared_song_data). Otherwise, replace <table-name> with the name of a table to contain the filtered and transformed records (for example, prepared_song_data).

    Replace <raw-songs-table-name> with the name of the table containing the raw songs records ingested in the previous step.

    4.5 Click Run Menu, and select Run Cell.

## Step 5: Query the transformed data 

    5.1 In this step, you extend the processing pipeline by adding queries to analyze the songs data. These queries use the prepared records created in the previous step.

    5.2 In the sidebar, click New Icon New and select Notebook from the menu. The Create Notebook dialog appears.

    5.3 Enter a name for the notebook. For example, Analyze songs data. Change the default language to SQL.

    5.4 Click Create.

    5.5 Enter the following in the first cell of the notebook:
  ```
      -- Which artists released the most songs each year?
      SELECT
        artist_name,
        count(artist_name)
      AS
        num_songs,
        year
      FROM
        <prepared-songs-table-name>
      WHERE
        year > 0
      GROUP BY
        artist_name,
        year
      ORDER BY
        num_songs DESC,
        year DESC
  ```
    Replace <prepared-songs-table-name> with the name of the table containing prepared data. For example, data_pipelines.songs_data.prepared_song_data.

    5.5 Click Down Caret in the cell actions menu, select Add Cell Below and enter the following in the new cell:

      ```
         -- Find songs for your DJ list
        SELECT
          artist_name,
          title,
          tempo
        FROM
          <prepared-songs-table-name>
        WHERE
          time_signature = 4
          AND
          tempo between 100 and 140;
      ```
      Replace <prepared-songs-table-name> with the name of the prepared table created in the previous step. For example, data_pipelines.songs_data.prepared_song_data.
    5.6 To run the queries and view the output, click Run all.

## Step 6: Create a Databricks job to run the pipeline

  You can create a workflow to automate running the data ingestion, processing, and analysis steps using a Databricks job.

    6.1 In your Data Science & Engineering workspace, do one of the following:

        Click Jobs Icon Workflows in the sidebar and click Create Job Button.

        In the sidebar, click New Icon New and select Job.
    
    6.2 In the task dialog box on the Tasks tab, replace Add a name for your job… with your job name. For example, “Songs workflow”.

    6.3 In Task name, enter a name for the first task, for example, Ingest_songs_data.

    6.4 In Type, select the Notebook task type.

    6.5 In Source, select Workspace.

    6.6 Use the file browser to find the data ingestion notebook, click the notebook name, and click Confirm.

    6.7 In Cluster, select Shared_job_cluster or the cluster you created in the Create a cluster step.

    6.8 Click Create.

    6.9 Click Add Task Button below the task you just created and select Notebook.

    6.10 In Task name, enter a name for the task, for example, Prepare_songs_data.

    6.11 In Type, select the Notebook task type.

    6.12 In Source, select Workspace.

    6.13 Use the file browser to find the data preparation notebook, click the notebook name, and click Confirm.

    6.14 In Cluster, select Shared_job_cluster or the cluster you created in the Create a cluster step.

    6.15 Click Create.

    6.16 Click Add Task Button below the task you just created and select Notebook.

    6.17 In Task name, enter a name for the task, for example, Analyze_songs_data.

    6.18 In Type, select the Notebook task type.

    6.19 In Source, select Workspace.

    6.20 Use the file browser to find the data analysis notebook, click the notebook name, and click Confirm.

    6.21 In Cluster, select Shared_job_cluster or the cluster you created in the Create a cluster step.

    6.22 Click Create.

    6.23 To run the workflow, Click Run Now Button. To view details for the run, click the link in the Start time column for the run in the job runs view. Click each task to view details for the task run.

    6.24 To view the results when the workflow completes, click the final data analysis task. The Output page appears and displays the query results.


## Step 7: Schedule the data pipeline job

  A common requirement is to run a data pipeline on a scheduled basis. To define a schedule for the job that runs the pipeline:

    7.1 Click Jobs Icon Workflows in the sidebar.

    7.2 In the Name column, click the job name. The side panel displays the Job details.

    7.3 Click Add trigger in the Job details panel and select Scheduled in Trigger type.

    7.4 Specify the period, starting time, and time zone. Optionally select the Show Cron Syntax checkbox to display and edit the schedule in Quartz Cron Syntax.

    7.5 Click Save.

