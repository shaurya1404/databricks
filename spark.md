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

- **Containers**: A top-level grouping of BLOBs inside a Storage Account. Analogous to a Bucket in Amazon S3. You create containers like raw, bronze, silver, gold, sales, marketing, etc. It can contain nested subdirectories

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
    MANAGED LOCATION 'abfss://gizmobox@databrickslearningextadl.dfs.core.windows.net/raw';
CREATE SCHEMA IF NOT EXISTS silver
    COMMENT 'This is the schema for the Silver layer in GizmoBox'
    MANAGED LOCATION 'abfss://gizmobox@databrickslearningextadl.dfs.core.windows.net/raw';
CREATE SCHEMA IF NOT EXISTS gold
    COMMENT 'This is the schema for the Gold layer in GizmoBox'
    MANAGED LOCATION 'abfss://gizmobox@databrickslearningextadl.dfs.core.windows.net/raw'
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

A View is a virtual table created based on the result set of a SELECT Query

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