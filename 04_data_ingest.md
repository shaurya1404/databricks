# Data Ingestion in Databricks

Until now, we've used the DataFrame Reader and DataStream Reader APIs to ingest data from files. However, there are more ways to ingest data that are built on top of these APIs allowing automation and abstraction over the same APIs

## The Data Ingestion Layered Stack

The Data Ingestion tools in Databricks can be represented as a layered stack with a trade-off between abstraction and controllability:

Spark Structured Streaming + Auto Loader -> Lakeflow Spark Declarative Pipelines (SDP) -> Lakeflow Connect managed connectors
Low Abstraction, High Controllability -> High Abstraction, Low Controllability

Structured Streaming + Auto Loader -> Write Spark code
Lakeflow Spark Declarative Pipelines -> Write declarations of target datasets only (scheduling, monitoring, error handling manged by Databricks)
Lakeflow Connect -> Configuration only, No code

Databricks recommends starting at the most managed/abstracted layer and dropping down only if it doesn't satisfy your requirements.

## Standard Connectors vs Managed Connectors

A Connector is an object that connects Spark to one external system's protocols and schema.

**Standard Connectors**: Connectors for Structured Streaming and SDP. Provides a data source to an external system - a way to read data into Spark's DataFrames; you build the pipeline around it.

Standard Connectors like Kafka and Auto Loaders sit in both, Structured Streaming and Spark Declarative Pipelines, layers of the Data Ingestion Hierarchy. You can use them in either. Whereas, a Standard Connector like SFTP is only compatible with Strcutured Streaming

**Managed Connectors**: Connectors for Lakeflow Connect. Provides a data pipeline to an external system like SaaS applications (Salesforce, Workday) and Database applications (MySQL, PostgreSQL, SQL Server). You do not write any of the ETL logic for ingestion - only configure destination, scheduling, permissions, etc.