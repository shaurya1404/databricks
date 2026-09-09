# Lakeflow Spark Declarative Pipelines

### Recent Changes to Naming:
`Delta Live Tables` are now now called `Lakeflow Spark Declarative Pipelines` 
`import dlt` package has been replace with `import dp`
`@dlt` decorator has been replaced with `@dp`
`@table` decorator is used to create streaming tables
`@materialized_view` decorator is used to create materialized views
`@view` decorator is now `@temporary_view`

## What is a Lakeflow Spark Declarative Pipeline?

Spark Declarative Pipelines are declarative ETL frameworks for building reliable data processing pipelines. You define the transformations to perform on your data and the SDP handles the task orchestration, cluster management, data quality checks, and error handling.

SDPs also abstract away the differences between Batch and Stream processing. The same pipeline can be used for both and can be easily switched just by configuration.

SDPs automate the complex task orchestration of an ETL pipeline. The traditional Databricks Jobs we've seen so far utilizes SDPs and schedules that pipeline within a workflow.

## SDP Architecture

1) Compute Layer: Spark

2) Storage Layer: Delta Lake

3) SDP User Interface

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

## Structured Tables vs Materialized Views vs Temporary Views

In traditional Spark Notebooks, we followed the pattern `read from here, transform into this, write there`. However, in a Declrative Pipelines, we write Definitions: `dataset X will be the result of this query`. The engine builds a DAG on the basis of these definitions to then work out the order of execution to decide how to bring uo each dataset when the pipeline runs.

Two design decisions then arise for the resulting datasets:
1) Should the data be stored physically? Streaming Table: Yes. Materialized View: Yes. View: No.
2) How should the data be refreshed? Streaming Table: Incrementally. Materialized View: Overwrite. View: Compute ad-hoc

### Streaming Table

A streaming table is a Delta table fed by a streaming read.
The engine keeps a checkpoint recording exactly which input records it has already consumed. On the next run it picks up from that offset and appends only the new rows.

Exactly once guarantees: Each source record contributes to the table exactly once, even after failures. This is ensured by the Checkpoints (avoids 0 commit of records) and atomic commits in the Transactional Log (avoids more than 1 commit).

A Streaming Table never revisits history. Hence, the source has to be append-only such as Kafka, Event Hubs, Streaming Cloud Files via Auto Loader.

Since it's a real table, it allows DML operations.

### Materialized Views

A materialized view is also a Delta table, but it's defined by an invariant: its contents always equal what the query would return over the current source data, i.e, the Delta table is overwritten each time the pipeline runs

It doesn't allow DML operations since the data in the Delta Table is contingent upon the query - so, to change data in the tables, change the query and re-run.

Used for building aggregate tables for BI reports

### Views

No storage, no table, nothing published. Conceptually the same as Temporary Views - a named query that exists only in the pipeline and only survives the pipeline run instead of the Spark session.

Since it isn't created as a UC Object, nothing outside the pipeline can refer to it.

It is used to modularize and re-use logic within the pipeline that may be used in multiple points such as a certain data-cleaning logic