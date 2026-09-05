# Delta Lakes

Delta Lake is the optimized storage layer defined in the Lakehouse architecture.

Delta Lake File Format = Parquet File Format + Transaction Log

## Delta Lake Characteristics

1) Supports ACID Transactions 

Using Delta Lake format enables ACID Transaction capabilities in the storage layer. 
ACID Transactions imply that concurrent transactions can be carried out without affecting the integrity of the data.

Data files are written first, THEN made visible by one atomic log commit. The commit isn't made on partial transactions. That one commit is the atomic switch:
- Atomicity: Until the commit, none of the new files are visible. Commit made only AFTER the entire operation is successful
- Consistency: Schema is recorded in the log and enforced on every write operation
- Isolation: Readers see distinct versions of the storage layer such as N or N+1 - never a transient state
- Durability: Log is stored in durable cloud storage

2) Scalability via Metadata: Transactions logs allow scaling tables with Petabytes of data with ease

3) Time Travel: Transaction log enables querying previous versions of the table

4) Unified Solution: Delta Lake enables a single solution of data for both Batch and Stream processing as they exist on the same `spark` API.

5) Support for DML Operations: Traditional Data Lakes were not efficient with ingesting incremental data nor updating existing records. Delta Lakes support all DML operations such as Insert, Update, and Delete

## Delta Lake Architecture

1) Data Storage = Parquet Files + Delta/Transaction Log

Parquet - Columnar binary file format
Transaction Log - JSON file that records every transaction performed on the file

The transaction log differentiates Delta Lake from Data lake as it enables ACID transactions, time travel, and versioning

2) Unity Catalog Delta Table: UC Object built on top of the Data Storage layer that enables data governance and security

3) Delta Engine: Spark-compatible query engine optimized for performance on Delta Lakes

4) Spark Compute: Peforms ingestions, transformations, and data processing on the Delta Lakes

## Transaction Log

1) Creating the Catalog and Schema where the Delta Table will live

```sql
CREATE CATALOG IF NOT EXISTS demo
MANAGED LOCATION 'abfss://demo@databrickslearningextadl.dfs.core.windows.net/'

CREATE SCHEMA IF NOT EXISTS demo.delta_lake
MANAGED LOCATION 'abfss://demo@databrickslearningextadl.dfs.core.windows.net/delta_lake'
```

2) Creating the Delta/Managed Table

```sql
CREATE TABLE IF NOT EXISTS demo.delta_lake.companies
(
    company_name STRING,
    founded_date DATE,
    country STRING
)
```

Since no location is specified while creating the table, Spark knows to create a Delta Table in the cloud directory pointed by the closest Managed Location which is `schema: delta_lake`

Thus, Delta Lake table storage location: 
`abfss://demo@databrickslearningextadl.dfs.core.windows.net/delta_lake/__unitystorage/schemas/5786e7b6-09cd-4485-9f81-d31c3417ced4/tables/3ea675ff-9647-4924-81cd-2815898f8e15`

Every operation on the Managed Table, i.e, the Parquet files is logged as a new JSON file in the `_delta_log` transaction log directory

### History and Time Travel

1) Query Delta Lake Table History

```sql
DESCRIBE HISTORY demo.delta_lake.companies
```

2) Query Data from a Specific Version

```sql
SELECT * FROM demo.delta_lake.companies
VERSION AS OF 1
```

3) Query Data from a Specific Timestamp

```sql
SELECT * FROM demo.delta_lake.companies
TIMESTAMP AS OF '2026-09-03T09:54:45.000+00:00'
```

4) Restore Data from a Specific Version

```sql
RESTORE TABLE demo.delta_lake.companies
VERSION AS OF 1
```

Restoring to a previous version creates a new transaction log file (`00000000000000000003.json`) which describes the current version of the Delta Table as having EXCLUDED the Parquet files that weren't in the version that we restored to rather than rewritting any of the data

## ACID Transactions

Important Characteristics of a Delta Table that enable ACID Transactions:

1) Transaction log files are written only at the end of the transaction
2) The transaction log is the single source of truth for all readers of the Delta lake that describes which files are available to read

### Scenario 1 - Concurrent Write and Read Operations (Isolation)

Current File in Delta Lake: A.parquet
Writer: In-progress of B.parquet
Transaction Log: Only displays A.parquet
Reader: Only sees A.parquet

### Scenario 2 - Failed Write Operation (Atomicity & Consistency)

Current File in Delta Lake: A.parquet
Writer: Partilly Writes B.parquet and Fails
Transaction Log: Only records A.parquet
Writer: retries and succeeds in C.parquet
Transaction Log: Commits A and C
Reader: Only sees A and C and not the corrupted file B 

## Create Table - Table and Column Properties

Some properties that can be defined for the table/columns when creating a Delta Table

1) Table Properties

- COMMENT: Documentation describing the table
- TBLPROPERTIES: Used to specify metadata and/or configuration parameters

```sql
CREATE TABLE IF NOT EXISTS demo.delta_lake.companies (
    company_name STRING NOT NULL,
    founded_date DATE,
    country STRING
)
COMMENT 'This table consists of data about some of the tech giants'
TBLPROPERTIES ('sensitive' = 'true', 'delta.enableDeletionVectors'= 'false')
```

2) Column Properties

- NOT NULL: NULLable by default
- COMMENT: per-column comments

```sql
CREATE TABLE IF NOT EXISTS demo.delta_lake.companies (
    company_name STRING NOT NULL,
    founded_date DATE COMMENT 'The year the company was founded in',
    country STRING
)
```

3) Generated Columns

Columns whose values are generated at the time of insertion of the record

- Generated Identity Column: Used to uniquely identify rows - usually set as the PK

`GENERATE [ALWAYS | BY DEFAULT] AS IDENTITY ([START WITH start] | [INCREMENT BY step])`

- Generated Computed Column: A column derived as an expression of other column values

`GENERATE ALWAYS AS ( expr )`

```sql
CREATE TABLE IF NOT EXISTS demo.delta_lake.companies (
    company_id BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
    company_name STRING NOT NULL,
    founded_date DATE COMMENT 'Date when the company was founded',
    founded_year STRING GENERATED ALWAYS AS (date_format(founded_date, 'yyyy')),
    country STRING
)
```

## Create Or Replace vs Drop and Create

The difference between `CREATE OR REPLACE` and `DROP - CREATE` lies in whether the transaction log is cleared or not. In the former, the table history is preserved. In the latter, it's reset every time.

```sql
DROP TABLE IF EXISTS demo.delta_lake.companies;
CREATE TABLE demo.delta_lake.companies (
    company_name STRING,
    founded_date DATE,
    country STRING
);

INSERT INTO demo.delta_lake.companies
VALUES ('Databricks', '2012-07-01', 'USA'),
    ('Microsoft', '1975-04-01', 'USA'),
    ('Google', '1998-09-04', 'USA'),
    ('Amazon', '1994-05-05', 'USA');
```

Re-executing the above returns the same two operations (CREATE and WRITE) in the table's history

```sql
CREATE OR REPLACE TABLE demo.delta_lake.companies (
    company_name STRING,
    founded_date DATE,
    country STRING
);

INSERT INTO demo.delta_lake.companies
VALUES ('Databricks', '2012-07-01', 'USA'),
    ('Microsoft', '1975-04-01', 'USA'),
    ('Google', '1998-09-04', 'USA'),
    ('Amazon', '1994-05-05', 'USA');
```

Re-executing the above appends two rows (CREATE OR REPLACE and WRITE) in the history each time and does not reset it

## CTAS

Create Table As Select (CTAS) allows creating a new table based on a `SELECT` query

```sql
CREATE TABLE demo.delta_lake.companies_china
AS
SELECT *
FROM demo.delta_lake.companies
WHERE country = 'China'
```

### Limitations of CTAS Statements

CTAS Statements do not allow setting Column Properties directly such as Casting Data Type, NOT NULL contraints, and Comments. Here are the work-arounds:

1) No Casting Data Type - CAST() in SELECT

```sql
CREATE TABLE demo.delta_lake.companies_china
AS
SELECT *
FROM demo.delta_lake.companies
WHERE country = 'China'
```

2) No NOT NULL Constraint - ALTER TABLE

```sql
ALTER TABLE demo.delta_lake.companies_china
ALTER COLUMN company_name SET NOT NULL
```

3) No Column Comments - ALTER TABLE

```sql
ALTER TABLE demo.delta_lake.companies_china
ALTER COLUMN founded_date COMMENT 'The year the company was founded'
```

## Insert Overwrite

INSERT INTO - Append new data
INSERT OVERWRITE - Replace existing data in a table or a specific partition with new data

### Overwrite in a Table

```sql
INSERT OVERWRITE demo.delta_lake.gold_companies
SELECT * FROM demo.delta_lake.bronze_companies
```

Overwriting the existing the data in the gold schema table with the data currently sitting in the bronze schema. The transaction log tracks this as a new version.

### Overwrite in a specific Partition

Partitioning splits the table's storage by column value. In the Delta table, each partitioned value gets its own directory which contains parquet files pertaining to that column's value only

`/gold_companies_partitoned/country=China/part-0000.parquet`
`/gold_companies_partitoned/event_date=USA/part-0000.parquet`

Partitioning relsults in better query performance when filtering on the column partitioned by as the engine can overlook the directories that don't match the column's values we filtered on.
An index is a separate B-tree structure that maps values to individual records. Whereas, partitioning is breaking the the large table into smaller distinct tables based on a column's values

Low-Cardinality Columns - PARTIONED BY
High-Cardinality Columns - INDEX

A table can be both PARTITIONED and have a Clustered Index (both lead to physical re-arrangement of rows in the storage) since the partition decides which partition the row goes inside, while the index decides the arrangement of rows within the partitions; N B-Trees will exist - one for each partition.

```sql
DROP TABLE IF EXISTS demo.delta_lake.gold_companies_partitioned;

CREATE TABLE IF NOT EXISTS demo.delta_lake.gold_companies_partitioned (
    company_name STRING,
    founded_date DATE,
    country STRING
)
PARTITIONED BY (country);

INSERT INTO demo.delta_lake.gold_companies_partitioned
VALUES
('Microsoft', '1975-04-04', 'USA'),
('Alibaba', '1999-07-01', 'China');

SELECT * FROM demo.delta_lake.gold_companies_partitioned
```

Now, overwriting the partition for country='USA' without affecting data in 'China'

```sql
INSERT OVERWRITE demo.delta_lake.gold_companies_partitioned
PARTITION (country='USA')
SELECT company_name, founded_date -- PARTITIONED column's value is implicitly added
FROM demo.delta_lake.bronze_companies_usa;
```

***Note***: Use `CREATE OR REPLACE` when overwriting data in which the schema has changed. The new data has to have the same schema for INSERT OVERWRITE to work

## COPY INTO

Similar to INSERT INTO, it loads data from Cloud Storage into a Delta Table.
It differs from INSERT INTO in the sense that COPY INTO loads incrementally - only new files are appended; already existing ones are ignored

It's an alternative to Auto Loader (performs Stream Ingestion) for Batch Ingestion

```sql
COPY INTO demo.delta_lake.bronze_stock_prices
FROM 'abfss://demo@databrickslearningextadl.dfs.core.windows.net/raw/stock_prices'
FILEFORMAT = JSON
FORMAT_OPTIONS ('inferSchema' = 'true') -- infer schema when reading
COPY_OPTIONS ('mergeSchema' = 'true') -- evolve schema when writing
```

FORMAT_OPTIONS: How to read the source data
COPY_OPTIONS: How to write to the destination

### Caveat of COPY INTO

`COPY INTO` only compares which files have been loaded previously for incremental loading. It compares the files in the source directory against the files listed in the transaction log of the destination - it does NOT compare the individual records inside the files.

Therefore:
1) Same records in two different files -> duplicates
2) Renaming/modifying (modificationTime updates in _delta_log) a file -> identified as new file

## MERGE

MERGE takes a SOURCE of incoming rows and a TARGET table, matches them on a key you supply, and then lets you INSERT / UPDATE / DELETE in each of three cases:
1) the rows matched - `WHEN MATCHED`
2) in the source but not the target - `WHEN NOT MATCHED`
3) in the target but not the source - `WHEN NOT MATCHED BY SOURCE` (almost never used - just know it exists)

```sql
MERGE INTO target AS t          -- the table being changed
USING source  AS s              -- the incoming rows
ON t.id = s.id                  -- how to JOIN

WHEN MATCHED THEN 
    UPDATE SET *
WHEN NOT MATCHED THEN
    INSERT *
```

***Note***: Refer to the Notebook on MERGE for the practical example

