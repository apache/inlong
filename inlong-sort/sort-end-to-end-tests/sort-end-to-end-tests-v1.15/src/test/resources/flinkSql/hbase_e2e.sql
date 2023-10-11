CREATE TABLE MyHBaseSource (
 rowkey STRING,
 family1 ROW<f1c1 STRING>,
 family2 ROW<f2c1 STRING, f2c2 STRING>
) WITH (
 'connector' = 'hbase-2.2-inlong',
 'table-name' = 'sourceTable',
 'zookeeper.quorum' = 'hbase1:2181'
);

CREATE TABLE MyHBaseSink
(
    rowkey STRING,
    family1 ROW<f1c1 STRING>,
    family2 ROW<f2c1 STRING, f2c2 STRING>
) WITH (
      'connector' = 'hbase-2.2-inlong',
      'table-name' = 'sinkTable',
      'zookeeper.quorum' = 'hbase2:2181',
      'sink.buffer-flush.max-rows' = '1',
      'sink.buffer-flush.interval' = '2s'
      );

INSERT INTO MyHBaseSink
SELECT rowkey,
       ROW(a),
       ROW(b, c)
FROM (
    SELECT rowkey,
           REGEXP_REPLACE(family1.f1c1, 'v', 'value') as a,
           family2.f2c1 as b,
           family2.f2c2 as c
    FROM MyHBaseSource) source;