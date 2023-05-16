# Description

# Overview

InLong-Sort is used to extract data from different source systems, then transforms the data and finally loads the data
into diffrent storage systems.
InLong-Sort is simply a Flink Application, and relys on InLong-Manager to manage meta data(such as the source
informations and storage informations).

# Features

## Supported Extract Node

- Pulsar
- MySQL
- Kafka
- MongoDB
- PostgreSQL
- HDFS

## Supported Transform

- String Split
- String Regular Replace
- String Regular Replace First Matched Value
- Data Filter
- Data Distinct
- Regular Join

## Supported Load Node

- Hive
- Kafka
- HBase
- ClickHouse
- Iceberg
- Hudi 
- PostgreSQL
- HDFS
- TDSQL Postgres
- Redis 

## Future Plans

### More kinds of Extract Node

Oracle, SqlServer, and etc.

### More kinds of Transform

Time window aggregation, Content extraction, Type conversion, Time format conversion, and etc.

### More kinds of Load Node

Elasticsearch, and etc.

# Compile Multi-version of Flink
1. The packaging of different versions does not conflict with each other, and the dependent versions are managed by themselves.
2. The dependencies related to flink under the Sort module only need to introduce sort-flink-dependencies-xxx.
3. To switch the Flink version under the Sort module, you need to modify the relevant conflicts to ensure compatibility. Looking forward to your PR.
4. How to switch Flink version？
- By default, it will only pack: profile id is v1.13
- If you need to package all modules: mvn clean install -DskipTests -P v1.15,v1.13