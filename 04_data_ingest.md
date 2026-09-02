# Data Ingestion in Databricks

Until now, we've used the DataFrame Reader and DataStream Reader APIs to ingest data from files. However, there are more ways to ingest data that are built on top of these APIs allowing automation and abstraction over them

## The Data Ingestion Layered Stack

The Data Ingestion tools in Databricks can be represented as a layered stack with a trade-off between abstraction and controllability:

Spark Structured Streaming -> Lakeflow Spark Declarative Pipelines (SDP) -> Lakeflow Connect managed connectors
Low Abstraction, High Controllability -> High Abstraction, Low Controllability

Spark Streaming -> Write Spark code
Lakeflow Spark Declarative Pipelines -> Write declarations of target datasets only (scheduling, monitoring, error handling managed by Databricks)
Lakeflow Connect -> Configuration only, No code

Databricks recommends starting at the most managed/abstracted layer and dropping down only if it doesn't satisfy your requirements.

## Standard Connectors vs Managed Connectors

A Connector is an object that connects Spark to one external system's protocols and data schema.

**Lakeflow Connect Standard Connectors**: Connectors used directly with Spark and SDPs. Provides a data source to an external system - a way to read data into Spark's DataFrames; you build the pipeline around it.

Standard Connectors like Kafka and Auto Loaders can be used with both, Spark and Spark Declarative Pipelines, layers of the Data Ingestion Hierarchy. You can use them in either. Whereas, a Standard Connector like SFTP is only compatible with Spark Structured Streaming

**Lakeflow Connect Managed Connectors**: Connectors used for Lakeflow Connect. Provides a data pipeline to an external system like SaaS applications (Salesforce, Workday) and Database applications (MySQL, PostgreSQL, SQL Server). You do not write any of the ETL logic for ingestion - only configure destination, scheduling, permissions, etc.

## Auto Loader

Auto Loader is a Spark Structured Streaming source accessed using `cloudFiles` that is designed for data ingestion from cloud storage.

### Why Auto Loader?

Why use Auto Loader to ingest data from cloud storage if we already have the vanilla Structured Streaming DataStream Reader?

1) No Incremental Loading: Vanilla Structured Streaming performs full table scans on entire directories to detect new files. This is slow and inefficient when dealing with millions of files.

2) In-Memory File List Storage: Duplicate file detection via vanilla Structured Streaming is done by storing a list of all files in-memory on the the Driver node. This doesn't scale well since memory constraints arise as the file list increases.

3) No Schema Evolution: Vanilla Structured Streaming requires manually defining the schema before the streaming starts. Also, new column addition  results in either data loss or requires manual handling.

Auto Loader solves the above limitations of the traditional DataStream Reader API via:

1) Supports Incremental Loading: Enabling 'File Notification Mode' leverages cloud storage services like AWS S3 Event Notifications or Azure Event Grid to track new files. Instead of manually performing a full tabel scan of the directory, it leverages a Cloud Queue to detect new files

2) RocksDB: A distributed key-value store which supersedes storing the entire file list in-memory in the Driver node - enables infinite scalability

3) Supports Schema Evolution: Auto Loader supports automatic schema inference as well as schema evolution by automatically adding new data columns or unexpected data in a seperate 'rescue data' column

**Summary**: Auto Loader is more efficient for big data ingestion and provides schema change resilience

### Auto Loader Syntax

Using `.format('cloudFiles')` to use Auto Loader

```python
df = spark.readStream \
    .format('cloudFiles') # Selects Auto Loader as the streaming source
    .option('cloudFiles.format', 'json') # Required: Format of files being used - json, csv, parquet, text, binaryFile, etc.
    .option('cloudFiles.schemaLocation','/Volumes/gizmobox/raw/operational_data/customers_autoloader/_schema') # Directory for inferred schema
    .option('cloudFiles.inferColumnTypes', 'true') # Infer schema types. If not given, stores all column types as Strings
    .option('cloudFiles.schemaHints', 'created_timestamp TIMESTAMP, date_of_birth DATE, member_since DATE') # DDL types if inferred is incorrect
    .option('cloudFiles.useNotifications', 'true') # File Notification Mode (Azure Event Grid) to track new files
    .load('/Volumes/gizmobox/raw/operational_data/customers_autoloader/') # Directory to monitor for input
```

```python
df_transformed = df.withColumn('file_path', col('_metadata.file_path')).withColumn('ingestion_date', current_timestamp()) # Transforming
```

```python
streaming_query = df_transformed.writeStream \
    .format('delta') \
    .option('checkpointLocation', '/Volumes/gizmobox/raw/operational_data/customers_autoloader/_checkpoint_stream') # storage dir for checkpoints
    .toTable('gizmobox.bronze.customers_autoloader') # delta/managed tables to write to
```

### More Auto Loader Options

More Auto Loader options to be familiar with:

`.option('cloudFiles.modifiedBefore', Timestamp)`: Optional filter to ingest files having a modification timestamp before the given one
`.option('cloudFiles.modifiedAfter', Timestamp)`: Optional filter to ingest files having a modification timestamp before the given one
`.option('pathGlobFilter', 'customers_2024_*.json')`: Optional filter to ingest file names matching the given pattern

***Note***" These don't have the `cloudFiles.` prefix since these are inherited from Structured Streaming, not an Auto Loader invention

### Schema Evolution in Auto Loader

As seen above, Auto Loader supports inference of Schema as well as overriding of the inferred schema via 'schemaHints'.

Auto Loader can also detect addition of new columns while it processes the data. This is known as Schema Evolution. 
It performs schema inference on the latest micro-batch and updates the schema location with the latest schema by merging new columns to the end of the schema while keeping the data types of the existing columns unchanged.

The following modes are supported for Schema Evolution set by `.option('cloudFiles.schemaEvolutionMode', mode)`

1) addNewColumns (Default): Stream fails. New columns are added to the schema automatically after restarting.

2) failOnNewColumns: Stream fails. Stream does not restart unless the schema is manually updated or offending file is removed

3) rescue: Stream continues. New columns aren't evolved into the schema - instead they're added to the `_rescue_date` column

4) none: Stream continues. Schema does not evolve and new columns are ignored. New data is not rescurd either

***Note***: `cloudFiles.schemaEvolutionMode` and `mergeSchema` are similar but deal with opposite ends of the pipeline

`.option('cloudFiles.schemaEvolutionMode', mode)` → a READ option. What Auto Loader does when the source data has a column it didn't expect.
`.option('mergeSchema', true)` → a WRITE option. Whether the target Delta table is allowed to widen to accept new columns.

## Event Streaming

A database table stores the CURRNT STATE of data (customer 42 has address X). An event stream stores events which are essentially FACTS (customer 42 changed address to X at 14:03). A Fact stores both the CURRENT STATE and the LOG of the changes that lead to the current state. 

An event stream is immutable and append-only.

## Kafka

Kafka is a storage platform for event streaming - essentially an append-only log. 
Unlike a queue, reading of events is non-destructive. The data stays for a retention period until which each consumer can access it independently.

Initializing a Kafka Connector is similar to Auto Loader - accessed via the DataStream API as a source:

```python
df = (spark.readStream
        .format('kafka') # Use Kafka as the Data streaming source
        .option('kafka.bootstrap.servers', 'host:port') # Specify the Kafka server/cluster to connect to
        .option('subscribe', 'topic_name') # The topic in the server to subscribe to
        .option('startingOffsets', 'latest') # 'earliest' - ingest all events in the stream. 'latest' - ingest only new events as they arrive
        .load())

df_parsed = df.selectExpr("CAST(value AS STRING)") # Kafka returns data in BINARY which must be casted to STRING before further transformations
```
***Note***: The event streaming platform for Azure is 'Event Hubs'. The structure of the code remains almost identical as the above.
After applying the minimal transformations, the data is usually stored in a Delta table in the Bronze schema.

## Lakeflow Connect Managed Connectors

Managed Connectors are fully-managed solutions to ingest data provided by Databricks from Saas (Salesforce, Workday ) and Databases (MySQl, PostgreSQL).

- SaaS Applications exspose their data through APIs which is accessed via a Connection in Databricks.
- A Connection is defined as a UC Object within Databricks, allowing governance and reusability across workspaces.
- Under the hood, Databrick connects to the API via HTTPS
- Once connected, the Ingestion Pipeline runs on Serverless compute and is fully managed by Databricks handling incremental ingestion, managing failures, and writting data into the Lakehouse (Delta tables in the Bronze layer)

The 4-Component Architecture of Configuring a SaaS Managed Connector:

1) Source (SaaS Tables where the data is to be ingested from                            - Account and Contact tables in SF)
2) Connection (The UC Object that connects Databricks to the SaaS to access the Source  - lakeflow_man_conn_salesforce)
3) Ingestion Pipeline (The fully mangewd pipeline itself within Databricks              - managed_ingestion_pl_salesforce)
4) Destination (The Delta Table where the transformed data from the Pipeline is stored  - databricks_learning_ws.bronze.tables)

The 6-Component Architecture of Configuring a Database Managed Connector:

