CREATE TABLE test_input1
(
    `id` INT,
    name STRING,
    description STRING
)
WITH (
    'connector' = 'pulsar-inlong',
    'topic' = 'persistent://public/default/test-topic',
    'service-url' = 'pulsar://pulsar:6650',
    'admin-url' = 'http://pulsar:8080',
    'format' = 'json',
    'scan.startup.mode' = 'earliest'
);


CREATE TABLE test_output1
(
    id INT,
    name STRING,
    description STRING
)
     WITH (
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


INSERT INTO test_output1
SELECT id, name, description
FROM test_input1;
