# Data Warehouses

Introduced in the 1980s
**Definition**: structured, subject-oriented, time-invariant, and non-volatile storage of data

A Data Warehouse performs ETL operations in order to reconcile data from various sources in the organization.
The data received in a data warehouse is either structured such as SQL tables or semi-structured such as CSV or JSON format.
Data Warehouses cannot handle unstructured data such as PDFs, images, or videos.

Analysts and Business could access the clean, transformed and enhanced data from Data Warehouses for BI reports.

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

- Web UI: Allows interation with worloads, notebooks, queries, etc. through the browser
- Compute Orchestration: Cluster or Job launch, number and type of workers, Autoscaling
- Unity Catalog: Data Governance and Lineage
- Classical Compute: Complex but high controlable/configurable clusters managed by the Cloud Platform
- Serverless Compute: Simple/Abstracted but less controlable clusters managewd by Databricks

## Classical Compute

Allow low-level configuration and maximum control over the cluster

Can be divided into 2 types: 

1) All Purpose Clsuter
- Created for ad hoc workloads
- Can be persisted even after job finishes
- Can be shared among users
- Costlier

2) Job Clusters
- Created for regular and recurring jobs
- Automatically terminated after each job finishes
- Isloated only to the job being executed
- Cheaper

### Cluster Config Options in Classical Compute

1) Single vs Multi Node: Driver has one or more worker nodes OR a single node is both the driver and the worker. Unlike multi-node clusters, single node clusters do not support process isolation and not intended for sharing of cluster with other users. 

2) Access Mode (Dedicated vs Standard): "Who" can access "What" data? Dedicated allows only one user to access the cluster. Standard clusters allows sdharing the cluster among multiple users and support process isolation.

3) Databricks Runtime(Databricks Runtime vs Databricks Runtime ML): The set of core libraries that run uniformaly in a cluster.
Databricks Runtime = Apache Spark + Supporting libraries + Photon (Vectorized engine to boost Apache Spark)
Databricks Runtime ML = Apache Spark + ML Libraries (PyTorch, Keras, TensorFlow) + Supporting Libraries + Photon

4) Auto Termination: Time after which cluster is automatically terminated to avoid unnecessary costs

5) Auto Scaling: Min/Max worker nodes between which auto scaling takes place. Spot instances are allowed for Worker nodes (Not driver)
Spot instances: Unused VMs in the Cloud that are offered at cheaper price but can be preempted by a customer paying regular price.

6) VM Type: Memory-optimized, Compute-optimized, Storage-optimized, General Purpose, GPU Accelerated

7) Cluster Policy: Can be set by admins to restrict or pre-configure the above setting for clusters for specific users