# Description

## Overview
InLong Sort is used to extract data from different source systems, then transforms the data and finally loads the data into different storage systems.

InLong Sort can be used together with the Manager to manage metadata, or it can run independently in the Flink environment.

## Features
### Supports a variety of data nodes

| Type         | Service                                    |
|--------------|--------------------------------------------|
| Extract Node | Pulsar                                     | 
|              | MySQL                                      | 
|              | Kafka                                      | 
|              | MongoDB                                    | 
|              | PostgreSQL                                 | 
| Transform    | String Split                               | 
|              | String Regular Replace                     | 
|              | String Regular Replace First Matched Value | 
|              | Data Filter                                |
|              | Data Distinct                              | 
|              | Regular Join                               | 
| Load Node    | Hive                                       | 
|              | Kafka                                      | 
|              | HBase                                      | 
|              | ClickHouse                                 | 
|              | Iceberg                                    | 
|              | PostgreSQL                                 | 
|              | HDFS                                       | 
|              | TDSQL Postgres                             | 
|              | Hudi                                       | 

## Compile Multi-version of Flink

How to switch Flink version？
- By default, it will only pack: profile id is v1.13
- Update Modify the variables of the root pom sort.flink.version
- mvn clean install -DskipTests -P v1.15 