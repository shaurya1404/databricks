# Databricks Lakehouse

Databricks enables one to build a modern Data Lakehouse platform.

A Data Lakehouse is a new data management architecture that combines the flexibility and cost-efficiency of Data Lakes with the data management and ACID transactions of Data Warehouses, enabling Business Intellgience and AI/ML/DS on a unified platform.

## Data Warehouses

Introduced in the 1980s
**Definition**: structured, subject-oriented, time-invariant, and non-volatile storage of data

A Data Warehouse performs ETL operations in order to reconcile data from various sources in the organization.
The data received in a data warehouse is either structured such as SQL tables or semi-structured such as CSV or JSON format.
Data Warehouses cannot handle unstructured data such as PDFs, images, or videos.

Analysts and Business could access the clean, transformed and enhanced data from Data Warehouses for BI reports.

### Limitations of DWs

1) With the boom of the Internet, unstructured data became impotant as well for decision making
2) With ETL operations, ingestion time for new data was unsatisfactory
3) Data Warehouses used Massive Parallel Processing engines (MPPs) which had proprietary file formats. Thus, data would be locked-in to vendors
4) Traditional Data Warehouses are not suitable for Data Science and ML development which need unstructured data.

## Data Lakes

Introduced in 2011
Designed to handle structured, semi-structured, and unstructured data.

In a Data Lake architecture, raw data is ingested directly into the data lake without any initial cleansing or transformation which allowed faster ingestion times and as a corollary, faster solution development times.

Data Lakes were built on cheap storage solutions such as Amazon S3 Buckets and Azure Data Lake Storage, which made them cost-effective.

Data Lakes used open source file formats such as Parquet, ORC, and Avro allowing for an eclectic range of tools and libraries to be used for processing.

Data Lakes were suitable for DS and ML workloads since it provided access to both structured and unstructured data.

However, Data Lakes weren't suitable for BI analysis and reports. So, as a stopgap solution, companies would pull the structured data out of the Data Lakes and into Data Warehouses via ETL operations. This lead to a complex architecture and two sources of truth.

### Limitations of DLs - Data Swamps

1) No support for ACID Transations: Atomicity, Consistency, Isolation, and Durability

- Atomicity: No partial transactions - it either fully applies or leaves no trace. For example: debit account X, credit account Y. A non-atomic operation would lead to money destroyed in case of a crash. In Data lakes, a table write is hundreds of files with no moment where they all appear; failed jobs leave orphans - Data Swamp.

- Consistency: Data Consistency implies strict rules on data implying a transaction takes the database from one 'valid' state to another, otherwise, it is rejected.

- Isolation: Concurrent transactions must perform state-locking to avoid data read/write contradictions

- Durability: Once commited, the change survives power loss, failures, and crashes

2) GDPR Challenges: Under GDPR rule, users have the right to be forgotten. Data Lakes didn't support deletions well so all the data had to be re-written just to delete data.

3) No Version Control: Made it hard to track changes, perform rollbacks, or ensure data governance

4) Poor BI Support: Strugled to perform fast query performance.