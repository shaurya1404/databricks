# Data Warehouses

Introduced in the 1980s
**Definition**: structured, subject-oriented, time-invariant, and non-volatile storage of data

A Data Warehouse performs ETL operations in order to reconcile data from various sources in the organization.
The data received in a data warehouse is either structured such as SQL tables or semi-structured such as CSV or JSON format.
Data Warehouses cannot handle unstructured data such as PDFs, images, or videos.

Analysts and Businesses access the clean, transformed and enhanced data from Data Warehouses for BI reports.

## Limitations of DWs

1) With the boom of the Internet, unstructured data became important as well for decision making
2) With ETL operations, ingestion time became a constraint for development time as new data would have to be transformed first to meet quality
3) Data Warehouses ran on Massive Parallel Processing engines (MPPs) which used proprietary file formats. Thus, organizations would be locked to one vendor
4) Traditional Data Warehouses are not suitable for Data Science and ML development which need unstructured data.

# Data Lakes

Introduced in 2011
Designed to handle structured, semi-structured, and unstructured data.

In a Data Lake architecture, raw data is ingested directly into the data lake without any initial cleansing or transformation which allowed faster ingestion times and as a corollary, faster solution development times.

Data Lakes were built on cheap storage solutions such as Amazon S3 Buckets and Azure Data Lake Storage, which made them cost-effective.

Data Lakes used open source file formats such as Parquet, ORC, and Avro allowing for an eclectic range of tools and libraries to be used for processing.

Data Lakes were suitable for DS and ML workloads since it provided access to both structured and unstructured data.

However, Data Lakes weren't suitable for BI analysis and reports. So, as a stopgap solution, companies would pull the structured data out of the Data Lakes and into Data Warehouses via ETL operations. This lead to a complex architecture and two sources of truth.

## Limitations of DLs - Data Swamps

1) No support for ACID Transactions: Atomicity, Consistency, Isolation, and Durability

- Atomicity: No partial transactions - it either fully applies or leaves no trace. For example: debit account X, credit account Y. A non-atomic operation would lead to money destroyed in case of a crash. In Data lakes, a table write is hundreds of files with no moment where they all appear; failed jobs leave orphans - Data Swamp.

- Consistency: Data Consistency implies strict rules on data implying a transaction takes the database from one 'valid' state to another, otherwise, it is rejected.

- Isolation: Concurrent transactions must perform state-locking to avoid data read/write contradictions

- Durability: Once commited, the change survives power loss, failures, and crashes

2) GDPR Challenges: Under the GDPR rule, users have the right to be forgotten. Data Lakes didn't support deletions well so all the data had to be re-written just to delete data.

3) No Version Control: Made it hard to track changes, perform rollbacks, or ensure data governance

4) Poor BI Support: Struggled to perform fast query performance.

# Databricks - Data Lakehouses

Databricks enables one to build a modern Data Lakehouse platform.

A Data Lakehouse is a new data management architecture that combines the flexibility and cost-efficiency of Data Lakes with the data management and ACID transactions of Data Warehouses, enabling Business Intellgience and AI/ML/DS on a unified platform.

Data Lakehouses can be used for Data Science annd AI/ML workloads while also integrating with BI tools such as Power BI and Tableau

Data Lakehouse = Data Lake + ACID Transactions (Delta Lake Files) + Data Governance (Unity Catalog)

Handles structured, semi-structured, and unstructured data.
Uses cheap cloud storage platforms such as Amazon S3 and Azure Data Lake Storage.
Uses open source file formart called Delta files.
Provide support for ACID transactions, data versioning, and history.

## Medallion Architecture

The Data architecture used in Data Lakehouses is the Medallion architecture.

Bronze -> Silver -> Gold
Quality of data improves in each layer.

- Bronze: Raw ingested data with minimal changes
- Silver: Cleansed, transformed and standarized data. Suitable for AI/ML
- Gold: Further aggregated and enriched with context using patterns like Star Schema. Suitable for BI reports.

# Databricks Architecture

Databricks splits the platform into a **control layer** (Web UI, Compute Orchestration, Unity Catalog) that manages and orchestrates the cluster runs and a **compute layer** (Classical or Serverless) that runs the actual workloads.

- Web UI: Allows interation with workloads, notebooks, queries, etc. through the browser
- Compute Orchestration: Cluster or Job launch, number and type of workers, Autoscaling
- Unity Catalog: Data Governance and Lineage
- Classical Compute: Complex but high controlability/configurability clusters managed by the Cloud Platform
- Serverless Compute: Simple/Abstracted but low controlability clusters managewd by Databricks

**Note**: The words compute and cluster are used interchangyably in pratical use cases

## Classical Compute

Allow low-level configuration and maximum control over the cluster

Can be divided into 2 types: 

1) All Purpose Cluster
- Created for ad hoc workloads
- Can be persisted even after job finishes
- Can be shared among users
- Costlier

2) Job Clusters
- Created for regular and recurring jobs
- Automatically terminated after each job finishes
- Isolated only to the job being executed
- Cheaper

### Cluster Config Options in Classical Compute

1) Single vs Multi Node: Driver has one or more worker nodes OR a single node is both the driver and the worker. Unlike multi-node clusters, single node clusters do not support process isolation and not intended for sharing of cluster with other users. 

2) Access Mode (Dedicated vs Standard): Who can access the cluster? Dedicated allows only one user to access the cluster. Standard clusters allows sharing the cluster among multiple users and support process isolation.

3) Databricks Runtime(Databricks Runtime vs Databricks Runtime ML): The software specs & core libraries that run uniformaly in a cluster.
- Databricks Runtime = Apache Spark + Supporting libraries + Photon
- Databricks Runtime ML = Apache Spark + ML Libraries (PyTorch, Keras, TensorFlow) + Supporting Libraries + Photon

**Note**: LTS stands for Long-Term Support - supported for three years vs six months for a regular release. Use LTS for Production workloads
**Note**: Photon is an optional vectorized engine that replaces parts of the regular Spark engine for faster processing - more expensive per unit time but save costs on larger workloads since queries finish sooner

4) Auto Termination: Time after which cluster is automatically terminated to avoid unnecessary costs

5) Auto Scaling: Min/Max worker nodes between which auto scaling takes place. Spot instances are allowed for Worker nodes (Not driver)
Spot instances: Unused VMs in the Cloud that are offered at cheaper price but can be pre-empted by a customer paying regular price.

6) VM Type: The hardware specs - Memory-optimized, Compute-optimized, Storage-optimized, General Purpose, GPU Accelerated

7) Cluster Policy: Can be set by admins to restrict or pre-configure the above settings for clusters for specific users

## Notebooks

Notebook: A Jupyter-style interactive document made of cells, attached to compute. 
You run a cell, the code goes to the cluster, the result comes back and is displayed inline. The notebook itself is a control-layer object; the execution happens on the cluster it's attached to.

Volume: Unity Catalog objects that govern non-tabular data — files. Tables govern your rows and columns; volumes govern your PDFs, images, CSVs, JSON drops, ML model artifacts

Workspace: An isolated Databricks instance that has its own users, notebooks, clusters, jobs, etc.

## Magic Commands

**%python, %sql, %scala, %r**: Override the default language executed in a cell
**%md**: To write markdown text
**%fs**: Run file system commands
**%sh**: Run shell commands (Driver node only)
**%pip**: Install Python libraries
**%run**: Import code from other notebooks into the current one - allow us to modularize the code

## Databricks Utilities (dbutils)

A built-in object that allows you to execute different types of operations within the code itself such as touch the file system, read secrets, and paramterize notebooks.

These utilities can only be run from Python, Scala, or R cells but not SQL cells

### File System Utilities (dbutils.fs)

`%fs` is syntactic sugar around `dbutil.fs`

Use `%fs` for ad hoc queries
Use `dbutils.fs` for production scripts since it integrates well with Python to perform powerful queries

```python
items = dbutils.fs.ls('/databricks-datasets/') # .ls returns a LIST of the contents in a folder

# Count number of files and folders in a given folder
folder_count = len([item for item in items if item.name.endswith('/')])
file_count = len([item for item in items if not item.name.endswith('/')])

print(f'File Count: {file_count}')
print(f'Folder Count {folder_count}')
```

## Databricks Git Folders (Formerly: Repos)

A Git folder (formerly called Repos) is a clone of a remote git repository.

An ordinary folder has revision history for notebooks, but that history is workspace-local — it can't be reviewed, branched, or shared. A Git folder makes your notebooks and files real repository contents, so the pull, branching, pushing, and CI/CD pipelines are supported. 

1. DEV WORKSPACE (Databricks)
   - Git folder = clone of the remote repo; each user gets their own clone, so two people can sit on different branches without interfering.
   - Create a feature branch, develop, commit, push to remote.
   - Regularly pull main into the feature branch to stay current. If that pull conflicts, resolve it HERE in the Git folder UI.

2. GIT PROVIDER (GitHub / Azure DevOps) — outside Databricks
   - Dev opens a PR: feature → main.
   - CI runs ON THE PR, BEFORE the merge: tests, linting, sometimes integration tests against a dev/staging workspace.
   - Branch protection (required approvals + required checks) blocks the merge until CI passes — this is what enforces "manager must approve".
   - Reviewer approves and merges. Merge happens here, never in Databricks.

# Unity Catalog

It replaces the old Hive Metastore + DBFS architecture

## Hive Metastore (Legacy)

The original metastore - a database containing table names, schemas, views, functions and the storage paths in the cloud where they're stored. Used for structured and semi-structured data giving them structure to allow SQL queries to run on them.

### Limtations of Hive Metastore

1) Workspace-scoped: Unlike Unity Catalog where all workspaces share one metastore for the entire region, connecting multiple workspaces within a single catalog via `catalog.schema.table`, the Hive Metastore is independent for each workspace and provides only a two-level access via `database.table`

2) No fine-grained security: No row level or column level security

3) No data lineage or data governance 

## Databricks File System (Legacy)

A filesystem abstraction layer over the cloud storage such as Azure Data Lake Storage or Amazon S3. It enables references to data stored in the cloud directly from the Databricks notebooks or a cluster or a job - usually used for unstructured data

  - DBFS root (dbfs:/): the workspace's own bucket. NOT for production data.
  - Mounts (/mnt/...): path alias + stored credential.
  - Core flaw: Mount grants access to anyone with the path + credentials - so, anyone in the cluster. No per-user data governance

## Unity Catalog

A unified solution for accessing table objects (structured/semi-structured) as well as files (unstructured). Allows creation of (1) tables, views, and functions as objects for structured/semi-structured data as well as (2) Volume objects as an abstraction layer over cloud object storage for unstructured data.

The UC Metastore is the top-level container that lives in the account-level (unlike Hive metastore - workspace-level) and only one can be created per Azure region. It connects all the workspaces within that region.

Catalog is just a logical container within the metastore. Usually one per business level (finance/sales/tech) or one per development environment (dev/staging/prod) 

Schemas (formerly databases) are also logical containers within catalogs. Each schema contains one or more volumes, tables, views, or functions

- Storage Credentials & External Locations: How UC references to cloud storage other than the default Metastore
- Connections: Refers to read-only access to databases in an external database system suchas as MysSQL or PostgreSQL via Lakehouse Federation
- Share, Recipient, Provider: Handle Delta Sharing

Databricks allows 100% backward compatability with the Hive Metastore in the form of a reserved pseudo-catalog called `hive_metastore`

### Managed vs External

Managed Volumes: Created via the Unity Catalog which then decides the location of where it's stored in the cloud platform storage - manages access control as well as lifecycle of the object.

`DROP VOLUME` deletes the files

External Volumes: A path to the volume already exists in your cloud storage and then we create a Volume to point to it - manages access control but not life cycle of the object.

`DROP VOLUME` removes only the registration — the files stay exactly where they are

**External vs Managed Tables**: The same distinction as Volumes. Managed tables only allow Delta file format whereas, external tables can be Delta, Parquet, CSV, JSON, etc.

Storage Credentials: An authentication and authorization mechanism for accessing data in the cloud storage. Created on top of a Managed Identity which is a way to autheticate and authorize Azure resources without needing to manage credentials manually.

External Location: An object that combines a Storage Credential to an Azure Data Lake Storage (ADLS) Container. So, when a user tries to access an the specific path, the UC knows which Storage Credential to use for it

## Configure Unity Catalog to Access the Cloud Storage

1) Create an Access Connector

An Access Connector is an Azure resource that's a wrapper for a Managed Identity. It connects that Managed Identity to the Unity Catalog for the purpose of authentication to access data registered inside the Unity Catalog but stored in the Cloud Platform.

The managed identity carries NO permissions on its own; it is only "I'm the Unity Catalog". You separately grant it a role on your storage account, typically Storage Blob Data Contributor. Azure now knows: this identity may read and write this container. Databricks isn't involved yet.

It is a SERVICE identity, not specific to a user. It represents Unity Catalog to Azure Storage — one identity serving the whole metastore.

TWO AUTHORIZATION CHECKS, TWO IDENTITIES:
  1. UC checks YOUR identity against the grant (e.g. SELECT on the table).
  2. Azure checks the MANAGED IDENTITY's (proxy for the Unity Catalog) role assignment on the storage.
This is why an analyst can query a governed table while holding zero Azure permissions — and why revoking their UC grant blocks them even though the managed identity's role assignment never changed.

Why not store the Managed Identity in the Storage Credential directly? Why do we need the Access Connector? Because a Managed Identity is a property of a resource. It is not a resource in and of itself. Hence, the resource we attach it to in order to proxy the UC is the Access Connector.

2) Create an Azure Data Lake Storage Gen2 Account

Creating a Cloud Storage Account which will hold all our structured (tables) and unstructured (files) data. Azure Storage includes Azure Blobs (objects), Azure Data Lake Storage Gen2, Azure Files, and Azure Tables. We are using ADLS Gen2

3) Give the Access Connector the role of Storage Blob Contributor

Giving read/write permissions to the Access Connector on the Storage Account via the Access Control (IAM) in the Storage Account

4) Create a Storage Credential in the Databricks UI

A Storage Credential is a Unity Catalog Object that wraps the Access Connector that we've created in Azure. Essentially, it's a double-wrapper around the managed identity. You give the UC the Access Connector's resource ID, and UC now has a governed handle on the cloud identity.

The Storage Credential is what allows the Unity Catalog to govern access on the Storage Account. The cloud-native identity wrapper of the credential is an Access Connector on Azure, or an IAM role on AWS.

The Access Connector's managed identity gives Unity Catalog the capability to reach the storage account. That capability is registered as a storage credential in the Unity Catalog. By default, no one has access to any of it. Users are then granted privileges on external locations, and UC checks those grants at query time.

5) Create an External Location

External Location = Path + Storage Credential

The storage container is what makes the UC capable to govern the data on the cloud. The external locations are what the UC enforces checks on in order to allow users access to resources.

Storage credential = CAPABILITY (what UC can reach). Not a grant. Default deny.
External location  = PERMISSION SURFACE (what a user may reach). Grantable.

```sql
CREATE EXTERNAL LOCATION IF NOT EXISTS databricks_learning_ext_adl_gizmobox
    URL 'abfss://gizmobox@databrickslearningextadl.dfs.core.windows.net/'
    WITH (STORAGE CREDENTIAL databricks_learning_ext_sc)
    COMMENT 'External Location for GizmoBox'
```