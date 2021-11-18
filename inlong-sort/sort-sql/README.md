
# Description

## overview
Inlong-sort-sql is used to extract data from different source systems, then transforms the data and finally loads the data into different storage systems.

## features
### isolation
Each sort workflow is an independent application based on flink sql. 

### rich connectors
Flink’s Table API & SQL programs can be connected to other external systems for reading and writing both batch and streaming tables.
A table source provides access to data which is stored in external systems (such as a database, key-value store, message queue, or file system).
A table sink emits a table to an external storage system. Depending on the type of source and sink, they support different formats such as CSV, Avro, Parquet, or ORC.

If you want to implement your own custom table source or sink, have a look at [the user-defined sources & sinks page](https://nightlies.apache.org/flink/flink-docs-master/docs/dev/table/sourcessinks/).

### more
We will support customized udf, dimension table lookup in the future.

## limitations
Currently, we just support basic transform framework.

## workflow demo
```sql
CREATE TABLE events (
  f_type INT,
  f_uid INT,
  ts AS localtimestamp,
  WATERMARK FOR ts AS ts
) WITH (
  'connector' = 'datagen',
  'rows-per-second'='5',
  'fields.f_type.min'='1',
  'fields.f_type.max'='5',
  'fields.f_uid.min'='1',
  'fields.f_uid.max'='1000'
);

CREATE TABLE print_table (
  type INT,
  uid INT,
  lstmt TIMESTAMP
) WITH (
  'connector' = 'print',
  'sink.parallelism' = '2'
);

INSERT INTO print_table SELECT * FROM events where f_type = 1;
```