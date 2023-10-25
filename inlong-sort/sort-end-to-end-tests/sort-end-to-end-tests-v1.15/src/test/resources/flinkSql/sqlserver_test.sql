CREATE TABLE test_input1 (
    `id` INT,
    name STRING,
    description STRING,
    PRIMARY KEY(`id`) NOT ENFORCED
) WITH (
    'connector' = 'sqlserver-cdc-inlong',
    'hostname' = 'sqlserver',
    'port' = '1433',
    'username' = 'sa',
    'password' = 'A_Str0ng_Required_Password',
    'database-name' = 'test',
    'table-name' = 'test_input1',
    'schema-name' = 'dbo'
);
CREATE TABLE test_output1 (
    `id` INT,
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



