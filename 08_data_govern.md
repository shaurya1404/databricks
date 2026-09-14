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

UC has the following 4 components:

1) Data Access Control: User-based centralized access control on files stored on the cloud via Volumes (F`1 iles) and Tables (Metadata + Files)
2) Data Audit: Enables audit logs that describe whether autherized users are using the data in the expected manner
3) Data Lineage: Ensures authenticity of data and root-cause analysis of issues by tracking datasets both upstream and downstream in a pipeline
4) Data Discoverability: A searchable data catalog to discover data within the metastore

### Account, Metastore, Workspaces, and Users

Account-scoped -> Workspace-scoped

Accounts are the top-level container; one per organization and tied to the cloud provider (AWS/Azure/GCP) that handles billing, users, and the Unity Catalog metastore(s).

A Metastore is a top-level container for data. There can only exist one per region per account. It holds the catalogs, schemas, volumes, tables, and views. Metastores are account-scoped

One metastore per region, and a workspace attaches to exactly one metastore, but a metastore can serve many workspaces in that region.

A user is an account-level identity with an email that's assigned to one or more workspaces. Users and workspaces have a many-to-many relationship and they're both account-scoped.

A workspace is a deployment of Databricks having its own URL that users can access. It isolates its own compute-layer and control-layer resources. A worksapce attaches to a Metastore making all the UC Objects in it reachable (not necessarily readable; they need access control privileges)

**Account-scoped**: UC Metastore data (Catalogs, Schemas, Volumes, Tables, Views), Users, Storage credentials, External locations.
**Workspace-scoped** : Cluster configurations, Notebooks, Git folders, Jobs, SDP Pipelines, Dashboards, and Secrets.

***Note***: Hive metastore data (two-level namespaced `schema.object`) and DBFS data (`/FileStore`) are both workspace-scoped.

## Unity Catalog Security Model

UC Catalog allows us to govern every single UC object that the Metastore holds against Principals via Role-Based Access Control and the Access Control List.

### Users, Service Principals, and Groups

These three are collectively called Principals — identities that can be granted permissions.

1) User: An account-level identity that uniquely identifies a human via their email. A user logs in and gets a personal Home folder in each workspace they're assigned to. A user is tied to a person's employment. When they leave, the account is deprovisioned and everything they had access to is forsaken.

2) Service Principal: An account-level, non-human identity used by automation — jobs, pipelines, CI/CD, external apps. It's identified by an application ID rather than an email, can't log into the UI, and receives its own Unity Catalog grants and workspace permissions independently of any person.

If a production pipeline runs as a User, it erroneously gets all permissions of that User (even ones out of the scope of the pipeline) and the pipeline becmoes contingent upon that User's account - if they leave the organization and their account is deleted, the pipeline breaks.

Hence, service principals are used to give non-human entities their own permissions decoupling them from Users entirely.

3) Group: A collection of principals — users, service principals, or other groups. Unlike the other two, groups are not something that uniquely identifies an entity. They exist purely as a unit of permission assignment.

Without groups, a team of 50 data engineers in the organization having 30 UC Grants would require 1500 distinct operations by the admin.
With a DE group, each User can be collectively enlisted in that group which holds the 30 privileges.

