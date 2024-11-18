CREATE TABLE test_input1 (
    `id` INT primary key,
    name STRING,
    description STRING
) WITH (
    'connector' = 'mysql-cdc-inlong',
    'hostname' = 'mysql',
    'port' = '3306',
    'username' = 'root',
    'password' = 'inlong',
    'database-name' = 'test',
    'table-name' = 'test_input1',
    'scan.incremental.snapshot.enabled' = 'true',
    'jdbc.properties.useSSL' = 'false',
    'jdbc.properties.allowPublicKeyRetrieval' = 'true'
    );

CREATE TABLE test_output1 (
      `id` INT primary key,
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



