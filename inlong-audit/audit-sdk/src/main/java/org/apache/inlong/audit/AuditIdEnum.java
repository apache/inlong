/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.inlong.audit;

/**
 * Audit item management, each module is assigned two baseline audit item IDs, namely receiving and sending.
 */
public enum AuditIdEnum {

    SDK_INPUT(1, "Audit for SDK to receive data"),
    SDK_OUTPUT(2, "Audit for SDK to send data"),

    AGENT_INPUT(3, "Audit for Agent to receive data"),
    AGENT_OUTPUT(4, "Audit for Agent to send data"),

    DATA_PROXY_INPUT(5, "Audit for DataProxy to receive data"),
    DATA_PROXY_OUTPUT(6, "Audit for DataProxy to send data"),

    SORT_HIVE_INPUT(7, "Audit for Sort Hive to receive data"),
    SORT_HIVE_OUTPUT(8, "Audit for Sort Hive to send data"),

    SORT_CLICKHOUSE_INPUT(9, "Audit for Sort ClickHouse to receive data"),
    SORT_CLICKHOUSE_OUTPUT(10, "Audit for Sort ClickHouse to send data"),

    SORT_ELASTICSEARCH_INPUT(11, "Audit for Sort ElasticSearch to receive data"),
    SORT_ELASTICSEARCH_OUTPUT(12, "Audit for Sort ElasticSearch to send data"),

    SORT_STARROCKS_INPUT(13, "Audit for Sort StarRocks to receive data"),
    SORT_STARROCKS_OUTPUT(14, "Audit for Sort StarRocks to send data"),

    SORT_HUDI_INPUT(15, "Audit for Sort HuDi to receive data"),
    SORT_HUDI_OUTPUT(16, "Audit for Sort HuDi to send data"),

    SORT_ICEBERG_INPUT(17, "Audit for Sort Iceberg to receive data"),
    SORT_ICEBERG_OUTPUT(18, "Audit for Sort Iceberg to send data"),

    SORT_HBASE_INPUT(19, "Audit for Sort HBase to receive data"),
    SORT_HBASE_OUTPUT(20, "Audit for Sort HBase to send data"),

    SORT_DORIS_INPUT(21, "Audit for Sort Doris to receive data"),
    SORT_DORIS_OUTPUT(22, "Audit for Sort Doris to send data"),

    SORT_KUDU_INPUT(25, "Audit for Sort Kudu to receive data"),
    SORT_KUDU_OUTPUT(26, "Audit for Sort Kudu to send data"),

    SORT_POSTGRES_INPUT(27, "Audit for Sort Postgres to receive data"),
    SORT_POSTGRES_OUTPUT(28, "Audit for Sort Postgres to send data"),

    SORT_BINLOG_INPUT(29, "Audit for Sort Binlog to receive data"),
    SORT_BINLOG_OUTPUT(30, "Audit for Sort Binlog to send data"),

    SORT_TUBE_INPUT(33, "Audit for Sort Tube to receive data"),
    SORT_TUBE_OUTPUT(34, "Audit for Sort Tube to send data"),

    SORT_MYSQL_INPUT(35, "Audit for Sort MySQL to receive data"),
    SORT_MYSQL_OUTPUT(36, "Audit for Sort MySQL to send data");

    private final int auditId;
    private final String description;

    AuditIdEnum(int auditId, String description) {
        this.auditId = auditId;
        this.description = description;
    }

    public int getValue() {
        return auditId;
    }

    public String getDescription() {
        return description;
    }
}
