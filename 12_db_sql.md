# Databricks SQL

A tool part of the Databricks Platform that enables data analytics and reporting on the data stored in the Data Lakehouse
It is a staple tool used by Data Analysts but very occsionally used by Data Engineers. 

As we are already familiar with the first two layers:
- Data Storage: Cloud Storage (ADLS Gen2, AWS S3, GCS)
- Unity Catalog: Metadata and Data Governance
- Compute (for Databricks SQL): SQL Warehouse - Compute optimized for BI and SQL workloads. Serverless vs Provisioned (Same as Classical)
- Databricks SQL UI: SQL Editor, Dashboard UI, Alerts

## Alerts

Allows setting up notifications for users that get triggered when a specified query meets a certain condition.
Can only be set up if we've saved a query in the 'Query' section since the output from a query (which is scheduled to run periodically to re-assess the threshold value) is used to define the condition.