CREATE TABLE test_input1 (
    `id` INT,
    name STRING,
    description STRING
) WITH (
      'connector' = 'postgres-cdc-inlong',
      'hostname' = 'postgres',
      'port' = '5432',
      'username' = 'flinkuser',
      'password' = 'flinkpw',
      'database-name' = 'test',
      'table-name' = 'test_input1',
      'schema-name' = 'public',
      'decoding.plugin.name' = 'pgoutput',
      'slot.name' = 'inlong_slot',
      'debezium.slot.name' = 'inlong_slot'
);
CREATE TABLE test_output1 (
    `id` INT,
    name STRING,
    description STRING
) WITH (
      'connector' = 'elasticsearch7-inlong',
      'hosts' = 'http://elasticsearch:9200',
      'index' = 'test_index',
      'format' = 'json'
);
INSERT INTO test_output1 select * from test_input1;



