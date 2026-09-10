# Lakeflow Spark Declarative Pipelines

### Recent Changes to Naming:
`Delta Live Tables` are now now called `Lakeflow Spark Declarative Pipelines` 
`import dlt` package has been replaced with `import dp`
`@dlt` decorator has been replaced with `@dp`
`@table` decorator is used to create streaming tables
`@materialized_view` decorator is used to create materialized views
`@view` decorator is now `@temporary_view`

## What is a Lakeflow Spark Declarative Pipeline?

Spark Declarative Pipelines are declarative ETL frameworks for building reliable data processing pipelines. You define the transformations to perform on your data and the SDP handles the task orchestration, cluster management, data quality checks, and error handling.

SDPs also abstract away the differences between Batch and Stream processing. The same pipeline can be used for both and can be easily switched between the two by configuration.

SDPs are automated declarative ETL pipelines. The traditional Databricks Jobs we've seen so far leverage SDPs by scheduling those pipelines within a workflow.

## SDP Architecture

1) Compute Layer: Spark

2) Storage Layer: Delta Lake

3) SDP User Interface:

Consists of the SDP Notebooks and SDP Pipelines:

- SDP Notebook: Consist of the Data Transformations and the Data Quality Expectations. Can be written in SQL or Python. SQL preferred for simplicity. However, Python is more flexible so allows implementation of advanced workloads. 

- SDP Pipelines: Puts together all the SDP Notebooks in a Directed Acyclic Graph (DAG) that represents the flow of data through the pipeline. The DAG is created automatically by identifying the dependencies between the notebooks. The SDP Pipeline then provisions the infrastructure for the processing of the pipeline (Serverless or Job clusters only) based on the configuration.

## Imperative vs Declarative Programming

Imperative Programming such as Python is like a Recipe. You explicitly tell the program step-by-step what to do. 

```python
adults = []
for user in users:
    if user.age >= 18
        adults.append(user)
```

Declarative Programming like SQL is like an Order. You tell the program what you want and the compiler figures out the most optimized way to do it.

```sql
SELECT *
FROM users
WHERE age > 18
```

## Structured Tables vs Materialized Views vs Views

In traditional Spark notebooks, we describe both the computation AND the configuration around it: where to write, which checkpoint directory to use, what output mode, which trigger to set, etc.

In SDP Pipelines, we still describe the computation via Definitions: `dataset X is the result of this query`. But, the engine infers the order of execution, checkpoints location, etc.

Two design decisions then arise for the resulting datasets:
1) Should the data be stored physically? Streaming Table: Yes. Materialized View: Yes. View: No.
2) How should the data be refreshed? Streaming Table: Incrementally. Materialized View: Overwrite. View: Compute ad-hoc

### Streaming Table

A streaming table is a Delta table that incrementally streams data.
The engine keeps a checkpoint recording exactly which input records it has already consumed. On the next run it picks up from that offset and appends only the new rows.

Exactly once guarantees: Each source record contributes to the table exactly once, even after failures. This is ensured by the Checkpoints (avoids 0 commit of records) and atomic commits in the Transactional Log (avoids more than 1 commit).

A Streaming Table never revisits history. Hence, the source has to be append-only such as Kafka, Event Hubs, streaming Cloud Files via Auto Loader.

Since it's a real table, it allows DML operations such as inserts, updates, and deletes.

### Materialized Views

A materialized view is also a Delta table, but its content is always what the query would return over the current source data, i.e, the Delta table is overwritten each time the pipeline runs

It doesn't allow DML operations since the data in the Delta Table is contingent upon the query - so, to change data in the tables, change the query and re-run.

Used for building aggregate tables for BI reports

### Views

No storage, no table, nothing published. Conceptually the same as Temporary Views - a named query that exists only in the pipeline and only survives the pipeline run instead of the Spark session.

Since it isn't created as a UC Object, nothing outside the pipeline can refer to it.

It is used to modularize and re-use logic within the pipeline that's be used multiple times such as a certain data-cleaning logic

# Spark Declarative Pipeline Project

- Customers, Orders: JSON
- Addresses: CSV

## Process Customers Data - Bronze Layer

The Landing layer will receive files of the format day1, day2,...
The files will only include data of records from a specific day and hence, must be incrementally loaded. Thus, using Auto Loader to incrementally ingest files from the cloud storage.

```sql
CREATE OR REFRESH STREAMING TABLE bronze_customers
    COMMENT 'Raw customer data ingested from the source volume operational data'
    TBLPROPERTIES ('quality' = 'bronze')
AS
SELECT *, _metadata.file_path AS file_path, current_timestamp() AS ingestion_timestamp
FROM cloud_files(
    '/Volumes/circuitbox/landing/operational_data/customers/',
    'json',
    map(
        'cloudFiles.inferColumnTypes', 'true'
    )
)
```

The `OR REFRESH` is what makes this an SDP Notebook rather than a Spark Notebook; first run of the pipeline creates the table, every subsequent run updates it incrementally.

To configure ETL Pipeline -> Pipeline Settings

***Note***: A Declarative Pipeline only accepts SDP notebooks written either in SQL or in Python, not both. The individual notebooks can be either but all the cells within a notebook must be of the same language

**Development vs Production Pipeline Mode**

1) Auto-termination: In the development mode, the Job Cluster is kept running even after the pipeline run finishes for a certain time duration (default = 2hrs) which can be changed via the TBLPROPERTIES key-value pair 'pipelines.clusterShutdown.delay'. This allows debugging between runs without needing to restart the cluster everytime. In the production mode, the cluster is terminated immediately after every run to minimize infrastructure costs.

2) Automatic retries: In development workloads, retries are disabled by default since errors are expected but enabled by default in production workloads.