# Delta Lakes

Delta Lake is the optimized storage layer that defines the foundational tables in a Lakehouse architecture.
A Delta Table is an instance of the Delta Lake format.

Delta Lake File Format = Parquet File Format + Transaction Log

## Delta Lake Characteristics

1) Supports ACID Transactions 

Using Delta Lake format enables ACID Transaction capabilities in the storage layer. 
ACID Transactions imply that concurrent transactions can be carried out without affecting the integrity of the data.

Data files are written first, THEN made visible by one atomic log commit. The commit isn't made on partial transactions. That one commit is the atomic switch:
- Atomicity: Until the commit, none of the new files are visible. Commit made only AFTER the entire operation is successful
- Consistency: Schema is recorded in the log and enforced on every write operation
- Isolation: Readers see distinct versions of the storage layer such as N or N+1 - never a transient state
- Durability: Log is stored in durable cloud storage

2) Scalability via Metadata: Transactions logs allow scaling tables with Petabytes of data with ease

3) Time Travel: Transaction log enables querying previous version of the table

4) Unified Solution: Delta Lake enables a single solution of data for both Batch and Stream processing as exist on the same `spark` API.

5) Support for DML Operations: Traditional Data Lakes were not efficient with ingesting incremental data nor updating existing records. Delta Lakes support all DML operations such as Insert

## Delta Lake Architecture

