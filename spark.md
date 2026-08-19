# Data Lakehouse Project Overview

1) Operational Data

Customers, Orders - JSON
Addresses - CSV
Membership -Images

2) External Data

Payments - CSV
Refunds - Azure SQL Database

## Cloud Storage Definitions

- **Blob**: Binary Large Object (Blob) is Azure's analogous for a file. A Parquet, PNG, CSV file are all Blobs, Azure stores and retrieves the bytes; interpreting them is entirely your problem. This is why the service is Azure Blob Storage, and the role names are Storage Blob Data Contributor, Storage Blob Data Reader.

- **Storage Account**: A top-level Azure resource that holds your data. It's the unit at whch you choose the region, the redundancy, the performance, and the encryption. It's the unit that gets a globally unique name across all of Azure since it becomes a host name:

    https://acmedata.blob.core.windows.net/       ← Blob endpoint
    abfss://container_name@storage_name.dfs.core.windows.net/    ← Azure Blob File System Secure URL

- **Containers**: A top-level grouping of BLOBs inside a Storage Account. Analogous to a Bucket in Amazon S3. You create containers like raw, bronze, silver, gold, and files (blobs) go inside them

## Query to Create External Location

The URL cannot be accessed until we create an External Location since that is what attched our Credential to the path access everytime granting us permssions

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

### Creating the Catalog

Creating the top-level container in the Metastore by the name of GizmoBox.

```sql
CREATE CATALOG IF NOT EXISTS cloud_catalog
  MANAGED LOCATION 'abfss://gizmobox@databrickslearningextadl.dfs.core.windows.net/' -- Use s3:// for AWS or gs:// for GCP
  COMMENT 'This is the catalog for the GizmoBox Data Lakehouse';
```

