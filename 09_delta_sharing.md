# Delta Sharing

Every organization eventually needs to give data to someone outside it: a partner, a vendor, a government regulator; another business unit on a different cloud.

The traditional answers all amount to making a copy - using SFTP, APIs or Email to transfer data. These have limitations:
1) Stale Data: Copied data will become outdated very soon
2) High Egress and Ingress Costs: High costs of transferring data across the network
3) Data Duplication: Extra storage costs for copying existing data

Delta Sharing is an open-source protocol for sharing live data across organizations, clouds, and platforms without copying it. It was created by Databricks and donated to the Linux Foundation - it's not a proprietary feature. The recipient isn't restricted to Databricks, Spark, or any specific cloud platform. It allows sharing with any cloud platform such as AWS, Azure, GCP as well as any other non-databricks clients such as Snowflake, Power BI, and Tableu.

Delta Sharing eliminates the need for manual file transfers, reduces costs, and ensures that data is always up-to-date.

## Important Terminology

1) Share: A read-only container that defines what data is being shared - a logical container of references to the data on your cloud, not a copy. 

In case of Open Sharing, it can only consist of tabular data in Delta tables. 
In case of D2D, it can include other Databricks assets such as notebooks and volumes (unstructured data) too.

2) Provider: The organization or the Databricks workspace that owns the data and creates the Share.

3) Recipient: The organization or Databricks workspace that receives access to the shared data. Depending on the sharing protocol, the recipient accesses the data either within Databricks Catalog or via a secure credential file.

The UC manages all three of these objects when using either the D2D protocol or the Open Sharing protocol

## The Delta Sharing Protocols

Depending on the platform that the Provider and Recipient are on, Delta Sharing offers three different protocols for sharing data:

1) Databricks-to-Databricks (D2D): Both parties are using UC-enabled Databricks workspaces. It allows sharing of tabular data as Delta tables from the Data Lakehouse as well as Databricks assets such as notebooks, volumes (unstructured data), dashborads, ML models. It also allows fine-grained access control and auditing.

2) Open Sharing: Allows sharing tabular data as Delta tables from the Data Lakehouse via the Provider being a UC-enabled Databricks workspace with a Recipient outside of Databricks. It only allows sharing of tabular data from the Lakehouse but not other Databricks assets such as notebooks, volumes (unstructured data), dashboards, ML models. It also allows fine-grained access control and auditing.

3) Customer-Managed Open-Source: Allows data in Delta Lake format from any platform to any platform. Only know it exists