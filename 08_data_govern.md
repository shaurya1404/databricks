# Data Governance

Having a functional pipeline is just the first half of the equation. Data Governance provides control and accountability over the data ensuring that it's a trusted source for decision-making.

Data Governance is a set of rules and mechanisms that answer the question "who" gets access to "what" data and "why". Regulations such as GDPR mandate organizations to store data securely from foreign entities. Additionally, via mechanisms like role-based access control, data is ensured to only be accesed and manipulated only by authorized identities within the organization ensuring accuracy and reliability of data.

## Data Governance in Databricks

Databricks ensures Data Governance via a set of tools:
1) Unity Catalog: The core governance tools that provides centralized access control and data lineage across all workspaces
2) Delta Lake: Ensures data reliability via ACID transactions, schema enforcement, and audit trails via transaction logs
3) Delta Sharing: Enables governed data sharing across cloud platforms
4) Cluster and Workspace Logging: Configurations for audit logging

## Unity Catalog

A unified solution for implementing Data Governance in Data Lakehouses offered by Databricks.

UC implements Data Goverance using 4 components:

1) Data Access Control: Resticting access to files and tables in the Delta Lake, notebooks, dashboards, and ML models to certain users and groups
2) Data Audit: Enables auditing capabilities as to how the authorized user is using the data
3) Data Lineage: Ensures authenticity of data and root-cause analysis of issues by allowing tracing back data to its source
4) Data Discoverability: A unified searchable data catalog to discover data within the platform

### Account, Metastore, Workspaces, and Users

Account-scoped -> Workspace-scoped

Accounts are the top-level container; one per organization and tied to the cloud provider (AWS/Azure/GCP) that handles billing, users, and the Unity Catalog metastore(s).

A Metastore is a top-level container for data. There can only exist one per region per account. It holds the catalogs, schemas, volumes, tables, and views. Metastores are account-scoped

One metastore per region, and a workspace attaches to exactly one metastore, but a metastore can serve many workspaces in that region.

A user is an account-level identity with an email that's assigned to one or more workspaces. Users and workspaces have a many-to-many relationship and they're both account-scoped.

A workspace is a deployment of Databricks having its own URL that users can access. It isolates its own compute-layer and control-layer resources. 

**Account-scoped**: Metastore Data (Catalogs, Schemas, Volumes, Tables, Views), Users, Storage credentials, External locations.
**Workspace-scoped** : Cluster configurations, Notebooks, Git folders, Jobs, SDP Pipelines, Dashboards, and Secrets.

***Note***: Hive metastore data (two-level namespaced `schema.object`) and DBFS data (`/FileStore`) are both workspace-scoped.