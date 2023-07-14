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
    'debezium.slot.name' = 'inlong_slot'
);

CREATE TABLE test_output1 (
    `id` INT primary key,
    name STRING,
    description STRING
) WITH (
    'connector' = 'jdbc-inlong',
    'url' = 'jdbc:mysql://mysql:3306/test',
    'table-name' = 'test_output1',
    'username' = 'inlong',
    'password' = 'inlong'
);

INSERT INTO test_output1 select * from test_input1;



