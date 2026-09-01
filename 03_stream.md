# Stream Processing

The difference between Batch and Stream Processing fundamentally comes down to whether the input data is bounded or unbounded.

## Bounded vs Unbounded Processing

Batch Processing operates on data collected over a period of time, and then processed in bulk at scheduled intervals. The dataset is static/bounded, i.e, it has a definitive start and end.

However, batch processing is not suited for real-time solutions such as ad-hoc decision making for fraud detection and algorithm recommendations, or live processing for IoT devices.

Stream Processing allows processing of data continuously as it arrives, enabling real-time and near real-time solutions. Stream Processing operates on a dynamic/unbounded dataset - one that has a start but no end. There is no final output, only a continuously revised one.

## Spark Structured Streaming API

The SparkSession `spark` exposes an API for optimized and fault-tolerant for stream processing that's nearly identical to the Batch processing API.

### How Spark Streaming Optimizes for Stream Processing
- Spark asks you to imagine stream as a dynamic table which keeps having rows appended to the bottom. Spark processes the stream as a series of small batch jobs called 'micro-batches'. 
- Spark provides 'once guarantees' that ensures no data processed is duplicated or missing.
- Spark streaming also ensures fault-tolerance for stream by using 'Checkpoints'

**Summary**: In batch, each run starts from nothing. In streaming, each micro-batch inherits two things from the last one: how far it has read (offsets), and what it has computed so far (state).

## Structured Streaming Example

1) Read Stream via DataStream Reader API

A schema must be explicitly defined as the Streaming API does not support infering schema in order to improve performance and avoid data inconsistencies between different micro-batches

```python
struct_schema = StructType([
    StructField('customer_id', IntegerType()),
    StructField('customer_name', StringType()),
    StructField('date_of_birth', DateType()),
    StructField('telephone', StringType()),
    StructField('email', StringType()),
    StructField('member_since', DateType()),
    StructField('created_timestamp', StringType())
])

df = spark.readStream \
    .format('json') \
    .schema(struct_schema) \
    .load('/Volumes/gizmobox/raw/operational_data/customers_stream/*.json')
# We always load the data from an external table (raw) and transform and write back to managed/delta tables (bronze/silver/gold)
```

2) Transform Data

Adding two new columns befrore writing back to the Sink - `file_path` and `ingestion_time`

```python
df_transformed = df.withColumn('file_path', col('_metadata.file_path')).withColumn('ingestion_date', current_timestamp())
```

3) Write Stream via DataStream Writer API

`.format('delta')`: Writing to a managed/delta table
`.option('checkpointLocation', path)`: Required. The location where the checkpoints will be stored. Must be unique - two streams cannot share
`.toTable(catalog.schema.table)`: Writing the managed/delta table

```python
streaming_query = df_transformed.writeStream \
    .format('delta') \
    .option('checkpointLocation', '/Volumes/gizmobox/raw/operational_data/customers_stream/_checkpoint_stream') \
    .toTable('gizmobox.bronze.customers_stream')
```

The streaming query runs indefinitely unless manually stopped: `streaming_query.stop()`

## Trigger

`.trigger()` is a method in the DataStream Writer API that answers 'when does the next micro-batch start?'

1) No trigger (Default)

Next batch starts as soon as the previous finishes; polls source every few ms when idle
Lowest latency, highest cost — constant cloud storage API calls

2) `.trigger(processingTime="2 minutes")`

Micro-batches run on a fixed interval. Finihses early -> idle. Overruns -> Wait until current finishes; then start next immediately.
Most effective latency vs cost option.

3) `.trigger(once=True)` — deprecated

Everything available in ONE micro-batch, then stop

4) `.trigger(availableNow=True)`

Superseded 'once=True' trigger - Everything available across MULTIPLE micro-batches, then stops; honors rate limits making it optimized for large workloads

5) `trigger(continuous="2 seconds")` — experimental

No micro-batches at all. Allows ultra-low latency (~ms) by processing per-row. The time-interval argument is how often the CHECKPOINT is written; not how often data is processed.
Should not be used for Production workloads.

## Output Mode

`.outputMode()` is a method on the DataStream Writer API which controls how the processed data is written to the sink

1) `.outMode(append)` - Default

Writes the new rows only that have arrived since the last micro-batch. Used well with operations like `filter`. 
Does not allow aggregate functions since they require updating previous rows as wel.

2) `.outMode(update)`: Writes rows that have changed since the last micro-batch

3) `.outMode(complete)`: Re-writes the entire table; enables aggregate functions

## Checkpoints

Checkpointing is a fault-tolerance mechanism that allows a streaming query to recover from failures and resume processing from where it left off while avoiding data duplication or loss.

Checkpoint storage locations must be unique - The same checkpoint location cannot be specified for multiple streams.

1) Offset Log (Write-Ahead Log) & Commit Log

BEFORE a micro-batch starts, Spark records the offset of the current batch and writes it to the Write-Ahead Log. AFTER the batch has fully executed and the sink write succeeds, Spark writes to the Commit log. This file means one thing: batch N is done, never redo it.

Ona subsequent micro-batch, Spark first reads the Write-Ahead log and the Commit log of the previous batch; if they match, data processing begins from the END of the previous batch. If they don't match, data processing begins from the START of the previous batch.

2) Idempotent Sinks

Such sinks include Delta, Lake, and Kafka. They enable 'once guarantees' as it ensures data written multiple times to the sink by ignoring the duplicates. In non-idempotent sinks, dedup must be handled manually