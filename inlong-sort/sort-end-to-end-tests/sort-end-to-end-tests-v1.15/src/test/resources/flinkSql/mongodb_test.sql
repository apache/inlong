SET 'execution.checkpointing.interval' = '3s';
CREATE TABLE test_input1 (
    _id INT primary key,
    name STRING,
    description STRING
) WITH (
    'connector' = 'mongodb-cdc-inlong',
    'hosts' = 'mongo:27017',
    'database' = 'test',
    'collection' = 'test_input1',
    'connection.options' = 'connectTimeoutMS=30000&maxIdleTimeMS=20000'
);

CREATE TABLE test_output1 (
    _id INT primary key,
    name STRING,
    description STRING
) WITH (
    'connector' = 'starrocks-inlong',
    'jdbc-url' = 'jdbc:mysql://starrocks:9030',
    'load-url'='starrocks:8030',
    'database-name'='test',
    'table-name' = 'test_output1',
    'username' = 'inlong',
    'password' = 'inlong',
    'sink.properties.format' = 'json',
    'sink.properties.strip_outer_array' = 'true',
    'sink.buffer-flush.interval-ms' = '1000'
);

INSERT INTO test_output1 select * from test_input1;