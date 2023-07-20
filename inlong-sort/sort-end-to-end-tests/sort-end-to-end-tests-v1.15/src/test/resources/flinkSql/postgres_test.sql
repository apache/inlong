CREATE TABLE test_input1 (
    `id` INT primary key,
    name STRING,
    description STRING
) WITH (
    'connector' = 'postgres-cdc-inlong',
    'hostname' = 'postgres',
    'port' = '5432',
    'username' = 'inlong',
    'password' = 'inlong',
    'database-name' = 'test',
    'table-name' = 'test_input1',
    'schema-name' = 'public',
    'decoding.plugin.name' = 'pgoutput',
    'slot.name' = 'inlong_slot',
    'debezium.slot.name' = 'inlong_slot'
);

CREATE TABLE test_output1 (
    `id` INT primary key,
    name STRING,
    description STRING
) WITH (
    'connector' = 'starrocks-inlong',
    'jdbc-url' = 'jdbc:mysql://%STRAROCKS_HOSTNAME%:9030',
    'load-url'='%STRAROCKS_HOSTNAME%:8030',
    'database-name'='test',
    'table-name' = 'test_output1',
    'username' = 'inlong',
    'password' = 'inlong',
    'sink.buffer-flush.interval-ms' = '5000',
    'sink.properties.column_separator' = '\x01',
    'sink.properties.row_delimiter' = '\x02'
);

INSERT INTO test_output1 select * from test_input1;



