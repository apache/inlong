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

    SDK_INPUT(1, "Received Audit Metrics for SDK"),
    SDK_OUTPUT(2, "Sent Audit Metrics for SDK"),

    AGENT_INPUT(3, "Received Audit Metrics for Agent"),
    AGENT_OUTPUT(4, "Sent Audit Metrics for Agent"),

    DATA_PROXY_INPUT(5, "Received Audit Metrics for DataProxy"),
    DATA_PROXY_OUTPUT(6, "Sent Audit Metrics for DataProxy"),

    SORT_HIVE_INPUT(7, "Received Audit Metrics for Sort Hive"),
    SORT_HIVE_OUTPUT(8, "Sent Audit Metrics for Sort Hive"),

    SORT_CLICKHOUSE_INPUT(9, "Received Audit Metrics for Sort ClickHouse"),
    SORT_CLICKHOUSE_OUTPUT(10, "Sent Audit Metrics for Sort ClickHouse"),

    SORT_ELASTICSEARCH_INPUT(11, "Received Audit Metrics for Sort ElasticSearch"),
    SORT_ELASTICSEARCH_OUTPUT(12, "Sent Audit Metrics for Sort ElasticSearch"),

    SORT_STARROCKS_INPUT(13, "Received Audit Metrics for Sort StarRocks"),
    SORT_STARROCKS_OUTPUT(14, "Sent Audit Metrics for Sort StarRocks"),

    SORT_HUDI_INPUT(15, "Received Audit Metrics for Sort HuDi"),
    SORT_HUDI_OUTPUT(16, "Sent Audit Metrics for Sort HuDi"),

    SORT_ICEBERG_INPUT(17, "Received Audit Metrics for Sort Iceberg"),
    SORT_ICEBERG_OUTPUT(18, "Sent Audit Metrics for Sort Iceberg"),

    SORT_HBASE_INPUT(19, "Received Audit Metrics for Sort HBase"),
    SORT_HBASE_OUTPUT(20, "Sent Audit Metrics for Sort HBase"),

    SORT_DORIS_INPUT(21, "Received Audit Metrics for Sort Doris"),
    SORT_DORIS_OUTPUT(22, "Sent Audit Metrics for Sort Doris"),

    SORT_KUDU_INPUT(25, "Received Audit Metrics for Sort Kudu"),
    SORT_KUDU_OUTPUT(26, "Sent Audit Metrics for Sort Kudu"),

    SORT_POSTGRES_INPUT(27, "Received Audit Metrics for Sort Postgres"),
    SORT_POSTGRES_OUTPUT(28, "Sent Audit Metrics for Sort Postgres"),

    SORT_BINLOG_INPUT(29, "Received Audit Metrics for Sort Binlog"),
    SORT_BINLOG_OUTPUT(30, "Sent Audit Metrics for Sort Binlog"),

    SORT_TUBE_INPUT(33, "Received Audit Metrics for Sort Tube"),
    SORT_TUBE_OUTPUT(34, "Sent Audit Metrics for Sort Tube"),

    SORT_MYSQL_INPUT(35, "Received Audit Metrics for Sort MySQL"),
    SORT_MYSQL_OUTPUT(36, "Sent Audit Metrics for Sort MySQL");

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
