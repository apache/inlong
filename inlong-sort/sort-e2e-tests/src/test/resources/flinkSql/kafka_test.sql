-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--   http://www.apache.org/licenses/LICENSE-2.0
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the License is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
-- KIND, either express or implied.  See the License for the
-- specific language governing permissions and limitations
-- under the License.

CREATE TABLE test_input (
    `id` INT NOT NULL,
    name STRING,
    description STRING,
    weight DECIMAL(10,3),
    enum_c STRING,
    json_c STRING,
    point_c STRING
) WITH (
    'connector' = 'mysql-cdc-inlong',
    'hostname' = 'mysql',
    'port' = '3306',
    'username' = 'inlong',
    'password' = 'inlong',
    'database-name' = 'test',
    'table-name' = 'test_input',
    'append-mode' = 'true',
    'scan.incremental.snapshot.chunk.size' = '4',
    'scan.incremental.snapshot.enabled' = 'false'
);

CREATE TABLE kafka_load (
    `id` INT NOT NULL,
    name STRING,
    description STRING,
    weight DECIMAL(10,3),
    enum_c STRING,
    json_c STRING,
    point_c STRING
) WITH (
    'connector' = 'kafka-inlong',
    'topic' = 'test-topic',
    'properties.bootstrap.servers' = 'kafka:9092',
    'format' = 'csv'
);

CREATE TABLE kafka_extract (
    `id` INT NOT NULL,
    name STRING,
    description STRING,
    weight DECIMAL(10,3),
    enum_c STRING,
    json_c STRING,
    point_c STRING
) WITH (
    'connector' = 'kafka-inlong',
    'topic' = 'test-topic',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.group.id' = 'testGroup',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'csv'
);

CREATE TABLE test_output (
    `id` INT NOT NULL,
    name STRING,
    description STRING,
    weight DECIMAL(10,3),
    enum_c STRING,
    json_c STRING,
    point_c STRING
) WITH (
    'connector' = 'jdbc-inlong',
    'url' = 'jdbc:mysql://mysql:3306/test',
    'table-name' = 'test_output',
    'username' = 'inlong',
    'password' = 'inlong'
);

INSERT INTO kafka_load select * from test_input;
INSERT INTO test_output select * from kafka_extract




