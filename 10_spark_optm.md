# Spark Optimization

Operations in Spark can be split into two types:
1) Transformations (select, filter, join, groupBy, withColumn) are LAZY. None of these are executed immediately when they're read. They are all added in the Logical/Execution Plan. 
2) Actions (show, count, collect, write, take) are EAGER. They force the entire Logical Plan made up until their line of call to execute.

## Narrow vs Wide Transformations

The question that decides Narrow or Wide: to compute one output partition, is everything I need already sitting on this machine?

If yes, the operation is Narrow (`filter`, `map`, `select`, `withColumn`). A filter on partition 7 only needs the rows in partition 7. Every child partition only requires data from a single parent partition, so the work stays local to the executor.

If no, the operation is Wide (`groupBy`, `sum`, `count`, `distinct`). To compute "all rows where country = 'USA'" as one group, you need rows from every partition, because they're scattered across all of them. A single executor processing the child partition requires data from multiple machines that are holding subsets of the child partition, which means data has to move across the network. That movement is a Shuffle.

Essentially, a Narrow transformation is a one-step/stage process wherein the output partition is computed exactly from a single input partition. Whereas, a Wide transformation is a two-step/stage process - first, every machine compute fragments of the output partitions on the basis of the input partition they received; second, a single machine in the next stage coalesces all the fragmented data from each partition in the previous

- How Spark executes the execution plan:
1) Job: A complete unit of work triggered by an Action
2) Stages: A major phase within the Job that runs a collection of Narrow transformations and splits at a Wide one, i.e, Shuffle boundaries.
3) Tasks: The smallest unit of work; stages are divided into N tasks. Usually, one task per partition, executed by a single core in an executor.

## Spark Perfomance Bottlenecks

1) Excessive Data Scanning

Caused due to Spark reading unnecessary data than what the query requires. It leads to additional processing time, I/O operations, and costs.

Spark UI Indicator: Large Input Size - `Input: 4.25 TB`

Solution:
- Filtering data earlier
- Selecting only the required columns reduces I/O
- Partition data efficiently so that fewer files are read

2) Small Files Problem

Spark jobs are usually limited by I/O, not by CPU. The engine is fast at transforming rows; what it's slow at is getting data into executor memory. As a corollary, Small Files problem is caused due to data being spread across a large number of smaller files instead of a small number of larger files.

Spark UI Indicatior: Large No. of Tasks - `Tasks: 50132`

Solution:
- Compacting files periodically (e.g OPTIMIZE)
- Avoid excessive partitioning
- Write larger batches (since Delta tables store every write as a new parquet file)

3) Shuffle Operations

Data movement across executors due to Wide transformations leading the network and I/O overhead.

Spark UI Indicator: High Shuffle Read/Write - `Shuffle Read: 26.8 GB`

Solution:
- Filtering data earlier
- Reduce unnecessary transformations
- Use broadcast joins for small tables

4) Data Skew

Occurs when some executors end up processing significantly more data than others. The next stage only begins when the slowest task from the previous stage finishes. Usually occurs during Wide transformations where some partitions are significantly larger than others.

Spark UI Indicator: Task(s) taking much more time than other: `Task Duration: 12 mins`

Solutions:
- Filter data earlier
- Avoid heavily skewed partitions
- Adaptive Query Execution

5) Data Spilling

Memory-based operations are much faster than disk-based operations; so executors compute data in the memory. Data Spilling occurs when executors run out of memory leading to data being processed in-disk leading to slower processing.

Spark UI Indicator: High Disk Spilled - `312.26 GB`

Solutions:
- Even workload distribution among executors

## Delta Lake Optimization

As Delta tables grow, their layouts can be become less efficient over time.
Data is continuously being added via Batch and Stream workloads. Small files accumulate and related data may get spread across multiple files. This leads to Spark needing to read more files and perform more operations than what is needed. Thus, reducing query performance.

Hence, Delta Lake provides several optimization techniques to ensure data is efficiently stores in Delta tables:

1) OPTIMIZE: Consolidates many smaller files into fewer larger files
2) Z-ORDER: Organizes data to speed up filter operations
3) VACUUM: Frees up storage by deleting obsolete files
4) Liquid Clustering: Automatically adapts layout as query patterns change over time
5) Predictive Optimization: Automatic maintenance runs 

### OPTIMIZE and ZORDER

`OPTIMIZE` reads the many small Parquet files that make up a Delta table and rewrites them into fewer, larger ones. Fewer larger files allow much faster read operations and less metadata storage for each file.

Delta tables maintain a transaction log that creates a new file for every write, append, and merge operation regardless of how much data the new file carries leading to the number of files to grow over time.

**Syntax**:`OPTIMIZE sales ZORDER BY (customer_id);`

***Note***: OPTIMIZE doesn't delete anything. It writes new files and marks the old ones as removed in the transaction log. The old files stay on storage until you run VACUUM. So right after OPTIMIZE your table physically takes more space, and time travel to older versions still works.

`OPTIMIZE sales WHERE year = '2026` is valid too only if the WHERE clause consists of PARTITIONED BY columns. This is because OPTIMIZE rewrites whole files, so it needs to select whole directories, not rows.

`ZORDER BY` physically reorders rows during compaction via OPTIMIZE ensuring that similar values of the ZORDER BY clause columns are kept together in the same files. This allows overlooking a lot of files when filtering on the basis of these columns.

### Liquid Clustering

Liquid Clustering is a Delta Lake data-layout technique declare which columns your queries filter on, and the platform takes responsibility for physically organizing the files to match — and without directories.

It replaces both table partitioning and ZORDER which were both manual and rigid.

1) Manual -> Automatic

When performing OPTIMIZE, files are automatically compacted AND clustered if Liquid Clustering is enabled. So, the new files created will implicitly be arranged on the current clustering columns mentioned in the transaction log while leaving the old files untouched (removed via VACUUM). Hence, incremental - no full-rewrites

2) Rigid -> Flexible

With table partitioning, the engine relies on directory names for data skipping. This made it rigid since changing the PARTITIONED BY columns is not possible after the first time as it would require re-writing all directory names and content.

Liquid Clustering keeps files with no directory hierarchy at all. The organization lives entirely in the transaction log, which records which key ranges each file covers. Data skipping then works based on those file-level metadata rather than directory names. New data is just clustered according to the new cluster columns/keys while leaving older files as they are (they will be re-arranged in newer files when we run OPTIMIZE).

```sql
-- For a new table
CREATE TABLE table1 (col1 INT, col2 STRING)
CLUSTER BY (col1);  -- Upto 4 columns allowed
```

```sql
-- For an existing table
ALTER TABLE table1
CLUSTER BY (col1)
```