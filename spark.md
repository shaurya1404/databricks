# Data Lakehouse Project Overview

1) Operational Data

Customers, Orders - JSON
Addresses - CSV
Membership -Images

2) External Data

Payments - CSV
Refunds - Azure SQL Database

## Cloud Storage Definitions

- **Blob**: Binary Large Object (Blob) is Azure's analogous for a file. Parquet, PNG, CSV files are all Blobs - Azure stores and retrieves the bytes; interpreting them is entirely your problem. This is why the service is Azure Blob Storage, and the role names are Storage Blob Data Contributor and Storage Blob Data Reader.

- **Storage Account**: A top-level Azure resource that holds your data. It's the unit at which you choose the region, the redundancy, the performance, and the encryption. It's the unit that gets a globally unique name across all of Azure since it becomes a host name:

    https://acmedata.blob.core.windows.net/                      ← Blob endpoint
    abfss://container_name@storage_name.dfs.core.windows.net/    ← Azure Blob File System Secure URL

- **Containers**: A top-level grouping of BLOBs inside a Storage Account. Analogous to a Bucket in Amazon S3. You create containers like raw, bronze, silver, gold, sales, marketing, etc. It can contain nested sub-directories

## Query to Create External Location

The URL cannot be accessed until we create an External Location since that is what attaches our Credential to the path whenever we attempt to access it via the Databricks workspace.

```sql
CREATE EXTERNAL LOCATION IF NOT EXISTS databricks_learning_ext_adl_gizmobox
    URL 'abfss://gizmobox@databrickslearningextadl.dfs.core.windows.net/'
    WITH (STORAGE CREDENTIAL databricks_learning_ext_sc)
    COMMENT 'External Location for GizmoBox'
```

## Creating Unity Catalog Objects

We need Unity Catalog Objects to identify and access specific resources within the Databricks Workspace.

Every Metastore has a three-level namespace to categorize UC objects: `catalog.schema.object`
- Catalog: GizmoBox
- Schema: Raw, Bronze, Silver, Gold
- Volume: Operational

UC Objects include: 
1) Tables — tabular data
2) Views — saved queries, including dynamic views for row/column-level security
3) Volumes — non-tabular data, i.e. files
4) Functions — UDFs

### Creating the Catalog

Creating the top-level container in the Metastore by the name of GizmoBox.

View all Catalogs in the Metastore: `SHOW CATALOGS`

```sql
CREATE CATALOG IF NOT EXISTS gizmobox
  MANAGED LOCATION 'abfss://gizmobox@databrickslearningextadl.dfs.core.windows.net/' -- Use s3:// for AWS or gs:// for GCP
  COMMENT 'This is the catalog for the GizmoBox Data Lakehouse';
```

If a Managed Location is not specified, all objects will be created under the Metastore's root storage -  an isolated cloud storage container created on your behalf within Databrick's own cloud provider.

### Creating the Schemas

The top-level logical containers within a Catalog.

Ensure talog before creating the schemas:
View Current Catalog: `SELECT current_catalog()`
Switch Catalog: `USE CATALOG gizmobox`

```sql
CREATE SCHEMA IF NOT EXISTS raw
    COMMENT 'This is the schema for the Raw layer in GizmoBox'
    MANAGED LOCATION 'abfss://gizmobox@databrickslearningextadl.dfs.core.windows.net/raw';
CREATE SCHEMA IF NOT EXISTS bronze
    COMMENT 'This is the schema for the Bronze layer in GizmoBox'
    MANAGED LOCATION 'abfss://gizmobox@databrickslearningextadl.dfs.core.windows.net/bronze';
CREATE SCHEMA IF NOT EXISTS silver
    COMMENT 'This is the schema for the Silver layer in GizmoBox'
    MANAGED LOCATION 'abfss://gizmobox@databrickslearningextadl.dfs.core.windows.net/silver';
CREATE SCHEMA IF NOT EXISTS gold
    COMMENT 'This is the schema for the Gold layer in GizmoBox'
    MANAGED LOCATION 'abfss://gizmobox@databrickslearningextadl.dfs.core.windows.net/gold'
```

If Managed Location is not specified, the UC will check to see if a Managed Location has been specific for the Catalog of this Schema. If yes, all objects will be created directly inside there (the gizmobox container in this case). If not, back to the Metastore's root storage

### Creating the Volumes

A volume is a directory-like logical container that holds non-tabular data (files). Similar to how a table holds rows, a Volume holds files and folders inside it.

```sql
USE CATALOG gizmobox;
USE SCHEMA raw;

CREATE EXTERNAL VOLUME IF NOT EXISTS operational_data
    LOCATION 'abfss://gizmobox@databrickslearningextadl.dfs.core.windows.net/raw/operational/'
    COMMENT 'This is a Volume for the Operational data we have in the Raw Schema'
```

Once created with the location, the UC now automatically sees all the files and folders stored inside the directory that the Volume points to in the Cloud Storage. 
The UC now enables data governance via grants/revokes on the Volume for the data in the cloud.

Creating a Volume also enables accessing the data using the relative UC filepath which internally references to the absolute ABFSS URL.
Filepath format: `/Volumes/catalog/schema/volume`

```sql
%fs ls /Volumes/gizmobox/raw/operational_data
```

# Querying Data

Querying structured/semi-structured data from file in the the raw/landing layer to be displayed as a structured table. 

## Querying JSON Files Using Spark SQL

1) Query Single JSON File

```sql
SELECT *
FROM json.`/Volumes/gizmobox/raw/operational_data/customers/customers_2024_10.json`
```

2) Querying Multiple JSON Files

```sql
SELECT *
FROM json.`/Volumes/gizmobox/raw/operational_data/customers/customers_2024_*.json`
```

3) Querying All Files in a Folder

```sql
SELECT *
FROM json.`/Volumes/gizmobox/raw/operational_data/customers`
```

4) Querying Files with Metadata

Enables auditing capabilities to trace data lineage

```sql
SELECT input_file_name() AS depr_file_path, -- Deprecated
    _metadata.file_name AS file_path,
    *
FROM json.`/Volumes/gizmobox/raw/operational_data/customers`
```

## Registering Data into the Bronze Schema

There are two ways to register the data extracted from the raw layer into the Bronze schema of the Unity Catalog
1) Create a View
2) Create a Table

A View is virtually a table created based on the result set of a SELECT Query

Unlike a table, a View is a saved query, not saved data. A table is static - doesn't update with data if the source updates unless done explicitly. A View is dynamic - it runs the SQL query every time the View is called resulting in an updated result set at time of execution.

```sql 
CREATE OR REPLACE VIEW catalog.schema.view
AS
SELECT *
FROM json.`/Volumes/gizmobox/raw/operational_data/customers`
```

## Temporary Views

A View is created within the Unity Catalog. But, Spark also allows us to create Temporary Views for the current session:

1) View: A permanent object registered in the metastore, sitting in the namespace exactly where a table would: `catalog.schema.view`
2) Temporary View: Lasts the Spark Session. Invisible to other notebooks in the same cluster
3) Global Temporary Views: Lasts the Spark Application. Visible to every notebook attached to the cluster.

- Spark Session: A notebook attached to a cluster - ends on detatching the notebook or restarting the cluster 
- Spark Application: A cluster - ends on restarting the cluster

Two notebooks inside one cluster are two sessions in one application.

```sql
CREATE OR REPLACE TEMPORARY VIEW tv_customers
AS
SELECT *, _metadata.file_name AS source_path
FROM json.`/Volumes/gizmobox/raw/operational_data/customers`
```

```sql
CREATE OR REPLACE GLOBAL TEMPORARY VIEW gtv_customers
AS
SELECT *, _metadata.file_name AS source_path
FROM json.`/Volumes/gizmobox/raw/operational_data/customers`
```

***Note***: Global Temporary Views are legacy. Use a Temp View for single notebook access or a persisted view for multi notebook access.

## Complex JSONs

- Complex JSONs: The JSON objects within the file contain nested structures rather than just scalar values
- Single-line JSONs (Default): Also called JSONL. Each JSON object occupies a single-line. No commas. No wrapping array

Single-line JSON:
    {"id":1,"name":"Alice","city":"SF"}
    {"id":2,"name":"Bob","city":"NYC"}

Single-line Complex JSON:
    {"id":1,"user":{"name":"Alice","email":"alice@x.com"},"city":"SF","tags":["vip","beta"]}
    {"id":2,"user":{"name":"Bob","email":"bob@x.com"},"city":"NYC","tags":["trial"]}

Multi-line JSON: [
    {"id":1,"name":"Alice","city":"SF"},
    {"id":2,"name":"Bob","city":"NYC"}
]
Multi-line Complex JSON: [
    {"id":1,"profile":{"name":"Alice","contact":{"email":"alice@x.com"}},"tags":["vip","beta"]},
    {"id":2,"profile":{"name":"Bob","contact":{"email":"bob@x.com"}},"tags":["standard"]}
]

## Querying Orders Folder - Complex JSON

The Orders table has data inconsistencies such as type mismatches in some of the records. Hence, the JSON parser fails to qualify all the rows and returns the corrupted rows in the `_corrupt_record` column while leaving NULL in the actual columns.

Hence, to avoid data loss, loading the file as a 'text' file format into the Bronze layer. We will then fix the issues and then load it using the JSON parser into the Silver layer

```sql
CREATE OR REPLACE VIEW gizmobox.bronze.v_orders
AS
SELECT *
FROM text.`/Volumes/gizmobox/raw/operational_data/orders`
```

## Binary File Format

A Binary File Format is used to process unstructured data in Databricks such as PDFs, PNGs, MP3s, MP4s, or any other file format

It is not a conventional file format like Parquet or CSV. Spark knows what a CSV is and how to handle it. A Binary File is a conversion of any file format into raw bytes without interpreting them - pure storage.

When processing Binary File formats, every file becomes one row in a four-column schema:
- path: `string`. Full path to the file
- modificationTime: `timestamp`. Last modified
- length: `long`: Size in bytes
- content: `binary`. The file's raw bytes

## Querying Memberships Folder - PNG

```sql
CREATE OR REPLACE VIEW gizmobox.bronze.v_memberships
AS
SELECT *
FROM binaryFile.`/Volumes/gizmobox/raw/operational_data/memberships/*/*.png` -- Query all PNG files in all the folders of the membership folder
```

## CSV Files 

The straight-forward `SELECT * FROM <file_format>.<file_path>` we've been using until now is good for default parsing behaviour for files such as ',' as the delimiter and having no header in CSV file; but, it can't take arguments.

However, the Addresses file uses tab (`\t`) as the delimiter to delineate columns and has a header. Hence, the minimal SELECT statement incorrectly parses the data. Hence, there are two ways of resolving this:

1) read_files()

read_files() is a table-valued function, i.e, a function that produces a table and, as a corllary, is used in the FROM clause. It reads files from the cloud storage while also allowing arguments.

**Syntax**: `read_files(path, [, option_key => option_value] [...])`

### Querying Addresses Folder - CSV

```sql
CREATE OR REPLACE VIEW gizmobox.bronze.v_addresses
AS
SELECT *
FROM read_files('/Volumes/gizmobox/raw/operational_data/addresses', format => 'csv', delimiter => '\t', header => true)
```

2) External Tables

Unlike 'operational_data', a UC Volume has not been created on the 'external' directory but it can still be accessed directly via the ABFSS protocol.

An External Table is a UC object that's useful when only reading data from an external source. It doesn't move or copy the data itself but just stores metadata describing the data in the metastore - hence, dropping the table only deletes the metadata without touching the actual data.

An External Table and a Volume cannot co-exist on the same external resource since we would then have two governance objects governing the same data leading to contradictions. The External Table can only be created if an External Location has already been created on the path that the External Table points to since the Storage Credential is stored in the External Location which the External Table needs to access the path.

### Creating An External Table on Payments

```sql
CREATE TABLE IF NOT EXISTS gizmobox.bronze.payments(
    payment_id INT,
    order_id INT,
    payment_timestamp TIMESTAMP,
    payment_status STRING,
    payment_method STRING
)
USING CSV
OPTIONS (
    delimter=','
)
LOCATION 'abfss://gizmobox@databrickslearningextadl.dfs.core.windows.net/raw/external/payments' -- Specifying the location makes it External
```

When data gets added/updated/deleted in the cloud directory/file that the External table points to, Databricks doesn't automatically update the data until the following command is run:

```sql
REFRESH TABLE gizmobox.bronze.payments
```

## Lakehouse Federation

Lakehouse Federation is a capability in Databricks that allows you to access and govern data stored outside the Lakehouse without having to move or copy it into the Lakehouse.

In many organizations, data lives outside the data lakehouse in external databases (MySQL, PostgreSQL, SQL Server, etc.) or external catalogs (Snowflake, AWS Glue, Hive Metastore). The Lakehouse Federation connects to these external sources and allows data access and governance as if the data was a part of the Databricks Lakehouse itself.

Federating (uniting for shared access) data from an external database is called a **Query Federation**
Federating data from an external catalog is called a **Catalog Federation**

External tables and Catalog Federation both read storage directly and compute locally — they differ only in whether we or a foreign catalog owns the data. In case of Query Federations, we send the query via JDBC but the compute takes place in the database itself

1) Query Federation

Used for External Databases such as MySQL, PostgreSQL, and SQL Server (Azure SQL)

Connection: To External Database via JDBC
Point of Execution: Query is optimized in our Spark engine but executed in the database's engine - partial dependence on database for cost and performance
Distributed Compute: External database may not running query using a distributed compute engine like Spark

2) Catalog Federation

A "Catalog" from the perspective of the industry is any system that knows what tables exists, what the schema of the table is, and where it is stored. Unity Catalog is a Catalog. Hive Metastore is a Catalog. Snowflake has itsown. AWS Glue has its own.

Connection: Directly to Storage Object via External Catalog
Point of Execution: Query executes in the databricks compute itself - cost-effective and performance-optimized

Both: CREATE CONNECTION → CREATE FOREIGN CATALOG. No data copied. UC-governed. Three-namespace: `foreign_cat.schema.table`
Both allow joining the external data with the Lakehouse data via the query
Both give you one namespace (catalog) and one governance model over data you never moved — what differs is only where the query executes.

## Lakehouse Federation Implementation

1) Query Federation

Pre-requisite: Unity Catalog must be enabled
- Create Connection (For the server)
- Create Foreign Catalog (For the database)
- Grant Privileges
- Run Queries (pushed to external database)

2) Catalog Federation

Pre-requisite: Unity Catalog must be enabled
- Create Connection (JDBC + Credentials)
- Create Storage Credential + External Location
- Create Foreign Catalog
- Grant Privileges
- Run Queries (executed in Databricks compute directly on the foreign object storage)

### Querying Refunds Table from Azure SQL via Query Federation

1) Create Connection

```sql
CREATE CONNECTION asql_gizmobox_db_conn_sql TYPE sqlserver
OPTIONS (
  host 'gizmobox-dbsrvr.database.windows.net', -- Connection to the Server
  port '1433',
  user 'gizmoboxadmin',
  password 'Gizmobox@123'
)
```

2) Create Foreign Catalog

```sql
CREATE FOREIGN CATALOG IF NOT EXISTS asl_gizmobox_db_catalog_sql USING CONNECTION asql_gizmobox_db_conn_sql
OPTIONS (database 'gizmobox-db'); -- Catalog to the Database in the Server
```

# Transform Data

1) Data Profiling using dbutils

`dbutils` cannot be executed in a SQL cell. The `dbutils.data.summarize()` takes a DataFrame as its arguement, not the table name directly

```python
df = spark.table('gizmobox.bronze.v_customers')
dbutils.data.summarize(df)
```

2) Count NULLs in Customers Data

```sql
SELECT COUNT(*), COUNT(customer_id), COUNT(email), COUNT(telephone)
FROM gizmobox.bronze.v_customers
```

OR

```sql
SELECT COUNT_IF(customer_id IS NULL), COUNT_IF(email IS NULL), COUNT_IF(telephone IS NULL)
FROM gizmobox.bronze.v_customers
```

3) Count Exact Duplicates

```sql
SELECT COUNT(customer_id), COUNT (DISTINCT customer_id)
FROM gizmobox.bronze.v_customers
```

4) Find Records with Duplicates

```sql
SELECT customer_id, COUNT(*) AS duplicate_count
FROM gizmobox.bronze.v_customers
GROUP BY customer_id
HAVING COUNT(*) > 1
```

## Transform Customers Data

1. - 4. Check notebook 'Transofrm Customer Data' for removing NULL, deduping, and casting transofrmation via temp views

5) Create Delta (Managed) Table In Silver Schema

```sql
CREATE TABLE gizmobox.silver.customers AS 
SELECT *
FROM casted
```