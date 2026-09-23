# Lakeflow Spark Declarative Pipelines

### Recent Changes to Naming

`Delta Live Tables` are now now called `Lakeflow Spark Declarative Pipelines` 
`import dlt` package has been replaced with `import dp`
`@dlt` decorator has been replaced with `@dp`
`@table` decorator is used to create streaming tables
`@materialized_view` decorator is used to create materialized views
`@view` decorator is now `@temporary_view`

## What is a Lakeflow Spark Declarative Pipeline?

Spark Declarative Pipelines are declarative ETL frameworks for building reliable data processing pipelines. You define the transformations to perform on your data and the SDP handles the task orchestration, cluster management, data quality checks, and error handling.

SDPs also abstract away the differences between Batch and Stream processing. The same pipeline can be used for both and can be easily switched between the two by configuration.

SDPs are automated declarative ETL pipelines. The traditional Databricks Jobs we've seen so far leverage SDPs by scheduling those pipelines within a job/workflow.

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

A streaming table is a Delta table that only incrementally reads data and allows upserts data while writing.
The engine leverages Checkpoints recording exactly which input records it has already consumed. On the next run it picks up from that offset and appends only the new rows.

Exactly once guarantees: Each source record contributes to the table exactly once, even after failures. This is ensured by the Checkpoints (avoids 0 commit of records) and atomic commits in the Transactional Log (avoids more than 1 commit).

A Streaming Table never revisits history when reading the source. Hence, the source has to be incremental reading only such as Kafka, Event Hubs, streaming Cloud Files via Auto Loader.
However, since it's a real table, it allows DML operations such as inserts, updates, and deletes.

Thus, a Streaming Tables only reads new data from the source and allows upsert operations on the sink

### Materialized Views

A materialized view is also a Delta table, but its content is always what the query would return over the current source data, i.e, the Delta table is overwritten each time the pipeline runs

It doesn't allow DML operations since the data in the Delta Table is contingent upon the query - so, to change data in the tables, change the query and re-run.

Used for building tables having Aggregate functions

**Pattern**: Bronze, Silver -> Streaming Table; Gold -> Materialized Views

Streaming Table: Only allow incremental reading and perform upsertions while writing
Materialized View: Read the entire source and overwrite the complete sink

### Views

No storage, no table, nothing published. Conceptually the same as Temporary Views - a named query that exists only in the pipeline and only survives the pipeline run instead of the Spark session.

Since it isn't created as a UC Object, nothing outside the pipeline can refer to it.

It is used to modularize and re-use logic within the pipeline that's expected to be used multiple times such as a certain data-cleaning logic

# Spark Declarative Pipeline Project

- Customers, Orders: JSON
- Addresses: CSV

## Process Customers Data - SDP SQL

The Landing layer will receive files of the format day1, day2,...
The files will only include data of records from a specific day and hence, must be incrementally loaded. Thus, using Auto Loader to incrementally ingest files from the cloud storage.

***Note***: We can now pass the three-level namespace `circuitbox.bronze.customers` in both SDP SQL and SDP Python as multiple schemas are now supported in Declarative Pipelines instead of having to store all three bronze, silver, and gold layers in the same default 'lakehouse' schema that is configured in the Pipeline Settings.

To configure: ETL Pipeline -> Pipeline Setting

***Note***: A Declarative Pipeline only accepts SDP notebooks written either in SQL or in Python, not both. The individual notebooks can be either but all the cells within a notebook must be in the same language.

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

The `OR REFRESH` is what makes this statement idempotent; first run of the pipeline creates the table, every subsequent run appends to it incrementally via the checkpoints (streaming table) or re-writes the table (materialized view).

The `STREAMING TABLE` ensures that this is a streaming table by validating that the `AS SELECT` statement is a streaming query such as `cloud_files(...)` or `STREAM()`. A plain batch read will be rejected.

Hence `CREATE OR REFRESH STREAMING TABLE x` is an idempotent declaration that x is a Delta table fed by a streaming query (implying that it's checkpointed) — created on first run, incrementally appended on every run after, never rebuilt unless you explicitly force a Full Refresh via the Pipeline UI.

Notice that the Declarative Pipeline requires defining the dataset itself but abstracts away its management: `.outputMode(append)` implied by `STREAMING` (`.outputMode(update)` implied by Auto CDC in Silver layer). `.option("checkpointLocation", ...)` is managed by the pipeline. `.trigger(...)` moved to pipeline configuration. DAG ordering is handled as the `bronze_customers` appears in silver's `FROM` clause.

**Development vs Production Pipeline Mode**

1) Auto-termination: In development mode, the Job Cluster is kept running even after the pipeline run finishes (default = 2hrs) which can be altered via the `pipelines.clusterShutdown.delay` property in `TBLPROPERTIES`. This allows debugging between runs without needing to restart the cluster everytime. In production mode, the cluster is terminated immediately after every run to minimize infrastructure costs.

2) Auto retries: In development mode, retries are disabled by default since errors are expected but enabled by default in production mode.

### Process Customers Data - Silver Layer

`FROM bronze_customers` peforms a batch load, i.e, reads the full current state of the source table. Thus, the target can only be a materialized view. You can't build a streaming table from it — the pipeline will reject the definition.

`FROM STREAM(bronze_customers)` enables stream read on tables, i.e, incremental loading of data from the source table. The engine keeps a checkpoint recording how far into bronze_customers' transaction log it has read. On each run it picks up from that offset and processes only the rows appended since.

`cloud_files(...) vs STREAM(...)`: The Auto Loader enables incremental loading from FILES. Whereas, STREAM() enables incremental loading from TABLES.

**Expectations**: The Data Quality layer of SDPs. Written via the `CONSTRAINT` clause in materialized view, streaming table, or view creation statements that apply predicate expressions on each record from the source.

```sql
CONSTRAINT expectation_name                   -- name
EXPECT (predicate_expression)                 -- predicate that must be TRUE
[ON VIOLATION (FAIL UPDATE | DROP ROW)]       -- optional: WARN is the default
```

Transforming (CASTS) and performing data quality checks (CONSTRAINTS) on the data from the `bronze_customers` table and storing in the `silver_customers_cleaned` table. Both are Streaming Delta Tables btw.

```sql
CREATE OR REFRESH STREAMING TABLE silver_customers_clean (
    CONSTRAINT valid_customer_id EXPECT (customer_id IS NOT NULL) ON VIOLATION FAIL UPDATE,
    CONSTRAINT valid_customer_name EXPECT (customer_name IS NOT NULL) ON VIOLATION DROP ROW,
    CONSTRAINT valid_telephone EXPECT (LENGTH(telephone) >= 10),
    CONSTRAINT valid_email EXPECT (email IS NOT NULL),
    CONSTRAINT valid_date_of_birth EXPECT (date_of_birth >= '1920-01-01')
)
COMMENT 'Cleaned data with data quality checks for the silver layer'
TBLPROPERTIES ('quality' = 'silver')
AS 
SELECT customer_id, customer_name, CAST(date_of_birth AS DATE), telephone, email, CAST(created_date AS DATE)
FROM STREAM(bronze_customers)
```

### SCD Type 1 vs Type 2

Slowly Changing Dimensions (SCD) answers one question: When a record in a Dimensions Table changes, what happens to the old record?

- Type 1: One row per key. Overwrite. No History maintained
- Type 2: Add New Row. Close Old Row. History maintained using columnns such as 'valid_from' and 'valid_to'.
- Type 3: Rarely used. One row perkey. Add new column that retains only the one prior value from the current one such as 'previous_city'

SDP only supports Type 1 and Type 2

### AUTO CDC INTO (Formerly APPLY CHANGES)

Performs insert/update/delete operations like `MERGE INTO` but allows defining recency based on specific columns (SEQUENCE BY clause) instead of naively updating with the most recent value it sees.

```sql
CREATE OR REFRESH STREAMING TABLE target_table; -- AUTO CDC doesn't create the table

AUTO CDC INTO target_table
FROM source_table
KEYS (columns) -- Column(s) used to uniquely identify an entity
[APPLY AS DELETE WHEN condition] -- Optional expression that identifies a row as a delete event
[APPLY AS TRUNCATE WHEN condition] -- Type 1 Only (Type 2 preserves history): Optional expression for full-table truncation
SEQUENCE BY sequence_column -- Column(s) defining recency for the latest record
[COLUMNS {column_list | * EXCEPT (except_column_list)}] -- Columns to include/exclude in the target table
[STORED AS {SCD TYPE 1 | SCD TYPE 2}] -- SCD Type Declaration (Default: Type 1)
[TRACK HISTORY ON {column_list | * EXCEPT (except_column_list)}] -- Type 2 Only: Which columns trigger a new record to be created (Default: All)
```

Applying changes and updating the Customer's data from the cleaned records that passed the Data Quality Checks.

```sql
CREATE OR REFRESH STREAMING TABLE silver_customers
    COMMENT 'Upserting the cleaned data to update the Customers data'
    TBLPROPERTIES ('quality' = 'silver');

AUTO CDC INTO silver_customers
FROM STREAM(silver_customers_clean) -- STREAM() since the target_table is a STREAMING TABLE
KEYS (customer_id)
SEQUENCE BY created_date
STORED AS SCD TYPE 1;
```

## Process Addresses Data - SDP Python

SDP is implemented using Python via the `pipelines` module in PySpark which is conventionally aliased as `dp`
`from pyspark import pipelines as dp`

One decorator + One function = One dataset

### Dataset Decorators

Decorators declare the dataset type and ensure their corresponding method for readng from the source is used:
1) Streaming Table = `@dp.table` + `spark.readStream`
2) Materialized View = `@dp.materialized_view` + `spark.read`
3) Temporary View = `@dp.temporary_view` + `spark.read`

- The name of the function that the decorator wraps is assumed as the target table's name if a 'name' argument is not passed in the decorator.
- The function must always RETURN A DATAFRAME

```python
from pyspark import pipelines as dp

@dp.table()
def function_name():
    **PySpark Code to Read Source Data & Apply Transformations**
    return <dataframe>
```

### Ingest Addresses Data - Bronze Layer

Incrementally ingesting data from cloud files via Auto Loader and writing in the 'bronze_addresses' streaming table

```python
from pyspark import pipelines as dp
from pyspark.sql.functions import *

@dp.table(
    name = 'bronze_addresses', # New Update: Multiple Schemas supported via the three-level namespace instead of just the default one configured
    comment = 'This table ingests data from the cloud files to the bronze layer',
    table_properties = {'quality': 'bronze'}
)

def created_bronze_addresses():
    return (
        spark.readStream \
        .format('cloudFiles') \
        .option('cloudFiles.format', 'csv') \
        .option('cloudFiles.inferColumnTypes', 'true') \
        .load('/Volumes/circuitbox/landing/operational_data/addresses/') # .load() returns a DataFrame from the Reader API; DF methods go after it
        .withColumn('file_path', col('_metadata.file_path')) \
        .withColumn('ingestion_timestamp', current_timestamp())
    )
```

### Expectation Decorators

The decorator type declares which action occurs if a row fails. 
The key-value pair passed as an argument within the decorator defines the name and the constraint of the expectation.

1) WARN: `@dp.expect`, `@dp.expect_all`
2) DROP: `@dp.expect_or_drop`, `@dp.expect_all_or_drop`
3) FAIL: `@dp.expect_or_fail`, `@dp.expect_all_or_fail`

The multiple-contraints syntaxes take a Dictionary as an argument

```python
from pyspark import pipelines as dp

@dp.table()
@dp.expect_or_fail('valid_customer_id', 'customer_id IS NOT NULL')
@dp.expect_all_or_fail({ 'valid_customer_id': 'customer_id IS NOT NULL', 'valid_order_id': 'order_id IS NOT NULL'})
```

### Perform Data Quality Checks - Silver Layer

```python
from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *

@dp.table(
    name = 'silver_addresses_clean',
    comment = 'Performing data qulaity checks before applying changes to addresses data'
    table_properties = {'quality': 'silver'}
)
@dp.expect_or_fail({'valid_customer_id', 'customer_id IS NOT NULL'})
@dp.expect_or_drop({'valid_address_line_1', 'address_line_1 IS NOT NULL'})
@dp.expect({'valid_postcode', 'LENGTH(postcode) = 5'})

def create_silver_addresses_clean():
    return (
        spark.readStream.table('bronze_addresses') \
        .withColumn('created_date', col('created_date').cast(DateType()))
    )
```

### Applying Changes via AUTO CDC

Performing insert/update/delete operations using the Auto CDC and maintaining the final silver streaming table as a SCD Type 2 (maintains history)

AUTO CDC doesn't create the table, so its definition is a two-part declaration:

```python
from pyspark import pipelines as dp

# Step 1: declare the target table (empty shell)
dp.create_streaming_table("target_table")

# Step 2: declare a flow that writes changes into it
dp.create_auto_cdc_flow( # Formerly dlt.apply_changes()
    target="target_table",
    source="source_table",
    keys=["key1", "key2", "keyN"],
    sequence_by="<ordering_column>",
    stored_as_scd_type= 1 | 2
)
```
- Optional Auto CDC arguments:
`apply_as_deletes` — an expression that identifies a row as a delete event
`apply_as_truncates` — same, for full-table truncation (SCD Type 1 only)
`column_list` / `except_column_list` — columns to include/exclude in the target table
`track_history_column_list` / `track_history_except_column_list` — which columns actually trigger a new version (SCD Type 2 only)

Performing insert/update/delete using Auto CDC to update Addresses data for the Silver Streaming Table maintained as a SCD Type 2

```python
dp.create_streaming_table (
    name = 'silver_addresses'.
    comment = 'This table stores the updated addresses data from the cleaned source'
    table_properties = {'quality': 'silver'}
)

dp.create_auto_cdc_flow (
    target = 'silver_addresses',
    source = 'silver_addresses_cleaned',
    keys = ['customer_id'],
    sequenced_by 'created_date',
    stored_as_scd_type = 2
)
```

## Process Orders Data

1) Ingest into Bronze Layer as a Streaming Table using Auto Loader

```python
from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp

@dp.table(
    name='bronze_orders'
    comment='Raw orders data incrementally ingested via Auto Loader'
    table_properties={'quality' = 'bronze'}
)

def bronze_orders():
    return (
        spark.readStream \
        .format('cloudFiles')
        .option('cloudFiles.format', 'json')
        .option('cloudFiles.inferColumnTypes', 'true')
        .load('/Volumes/circuitbox/landing/operational_data/orders/')
        .withColumn('file_path', col('_metadata.file_path'))
        .withColumn('ingestion_time', current_timestamp())
    )
```

2) Peforming Data Quality Checks via Expectations

```python
from pyspark import pipelines as dp
from pyspark.sql.types import *
from pyspark.sql.functions import *

@dp.table(
    name='silver_orders',
    comment='Orders data passed through Data Quality Checks',
    table_properties={'quality': 'silver'}
)
@dp.expect_all_or_fail({
    'valid_customer_id': 'customer_id IS NOT NULL',
    'valid_order_id': 'order_id IS NOT NULL'
})
@dp.expect_all({
    'valid_order_status': '''order_status IN ('Pending', 'Shipped', 'Cancelled', 'Completed')''',
    'valid_payment_method': '''payment_method IN ('Credit Card', 'PayPal', 'Bank Transfer')'''
})

def silver_orders():
    return (
        spark.readStream.table('bronze_orders') \
        .withColumn('order_timestamp', col('order_timestamp').cast(TimestampType()))
        .withColumn('item', explode(col('items')))
        .select('order_id', 'order_timestamp', 'customer_id', 'order_status', 'payment_method','item.product_id', 'item.quantity', 'item.price', 'item.category')
    )
```

No Auto CDC required since the Orders Data is not maintained as a SCD Table - Orders are expected to not change over time

## Create Customer Order Summary - Gold Layer

The final gold layer joins all three tables to yield every customer, their current address, and a summary of their orders. The summary of the orders will be calculated using aggregate functions - hence, a Materialized View will be used since it allows Aggregations. 

Materilaized Views make use of the optimizer called Enzyme to determine whether the current pipeline run requires incremental loading or a Full Refresh of the table.

Created using the same 

```sql
CREATE OR REFRESH MATERIALIZED VIEW gold_customer_order_summary
COMMENT 'Customer-level order summary stored in Delta Tables via Materialized Views'
TBLPROPERTIES ('quality' = 'gold')
AS
SELECT c.customer_id, c.customer_name, c.date_of_birth, c.telephone, c.email, a.address_line_1, a.city, a.state, a.postcode,
        COUNT(DISTINCT order_id) AS total_orders, -- DISTINCT since we exploded the items array - order IDs may appear more than once
        SUM(item.quantity) AS total_items, SUM(item.quantity * item.price) AS total_amount
FROM customers c 
    JOIN addresses a ON c.customer_id = a.customer_id
    JOIN orders o ON c.customer_id = o.customer_id
WHERE a.__END_AT IS NULL -- since Addresses is a SCD Type 2, we want the most recent one
GROUP BY ALL -- shorthand syntax in Spark SQL to GROUP BY all columns in SELECT except Aggregated ones
```
