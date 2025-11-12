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

import org.apache.inlong.audit.entity.AuditType;
import org.apache.inlong.audit.entity.FlowType;
import org.apache.inlong.audit.exceptions.AuditTypeNotExistException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.inlong.audit.entity.AuditType.AGENT;
import static org.apache.inlong.audit.entity.AuditType.BINLOG;
import static org.apache.inlong.audit.entity.AuditType.BIFANG;
import static org.apache.inlong.audit.entity.AuditType.CLICKHOUSE;
import static org.apache.inlong.audit.entity.AuditType.DATAPROXY;
import static org.apache.inlong.audit.entity.AuditType.DORIS;
import static org.apache.inlong.audit.entity.AuditType.ELASTICSEARCH;
import static org.apache.inlong.audit.entity.AuditType.HBASE;
import static org.apache.inlong.audit.entity.AuditType.HDFS;
import static org.apache.inlong.audit.entity.AuditType.HIVE;
import static org.apache.inlong.audit.entity.AuditType.HUDI;
import static org.apache.inlong.audit.entity.AuditType.ICEBERG;
import static org.apache.inlong.audit.entity.AuditType.ICEBERG_AO;
import static org.apache.inlong.audit.entity.AuditType.KUDU;
import static org.apache.inlong.audit.entity.AuditType.MYSQL;
import static org.apache.inlong.audit.entity.AuditType.POSTGRES;
import static org.apache.inlong.audit.entity.AuditType.SDK;
import static org.apache.inlong.audit.entity.AuditType.STARROCKS;
import static org.apache.inlong.audit.entity.AuditType.TDSQL_MYSQL;
import static org.apache.inlong.audit.entity.AuditType.TUBEMQ;
import static org.apache.inlong.audit.entity.FlowType.INPUT;
import static org.apache.inlong.audit.entity.FlowType.OUTPUT;

/**
 * Audit item management, each module is assigned two baseline audit item IDs, namely receiving and sending.
 */
public enum AuditIdEnum {

    SDK_INPUT(1, INPUT, SDK, "Received Audit Metrics for SDK"),
    SDK_OUTPUT(2, OUTPUT, SDK, "Sent Audit Metrics for SDK"),

    AGENT_INPUT(3, INPUT, AGENT, "Received Audit Metrics for Agent"),
    AGENT_OUTPUT(4, OUTPUT, AGENT, "Sent Audit Metrics for Agent"),

    DATA_PROXY_INPUT(5, INPUT, DATAPROXY, "Received Audit Metrics for DataProxy"),
    DATA_PROXY_OUTPUT(6, OUTPUT, DATAPROXY, "Sent Audit Metrics for DataProxy"),

    SORT_HIVE_INPUT(7, INPUT, HIVE, "Received Audit Metrics for Sort Hive"),
    SORT_HIVE_OUTPUT(8, OUTPUT, HIVE, "Sent Audit Metrics for Sort Hive"),

    SORT_CLICKHOUSE_INPUT(9, INPUT, CLICKHOUSE, "Received Audit Metrics for Sort ClickHouse"),
    SORT_CLICKHOUSE_OUTPUT(10, OUTPUT, CLICKHOUSE, "Sent Audit Metrics for Sort ClickHouse"),

    SORT_ELASTICSEARCH_INPUT(11, INPUT, ELASTICSEARCH, "Received Audit Metrics for Sort ElasticSearch"),
    SORT_ELASTICSEARCH_OUTPUT(12, OUTPUT, ELASTICSEARCH, "Sent Audit Metrics for Sort ElasticSearch"),

    SORT_STARROCKS_INPUT(13, INPUT, STARROCKS, "Received Audit Metrics for Sort StarRocks"),
    SORT_STARROCKS_OUTPUT(14, OUTPUT, STARROCKS, "Sent Audit Metrics for Sort StarRocks"),

    SORT_HUDI_INPUT(15, INPUT, HUDI, "Received Audit Metrics for Sort HuDi"),
    SORT_HUDI_OUTPUT(16, OUTPUT, HUDI, "Sent Audit Metrics for Sort HuDi"),

    SORT_ICEBERG_INPUT(17, INPUT, ICEBERG, "Received Audit Metrics for Sort Iceberg"),
    SORT_ICEBERG_OUTPUT(18, OUTPUT, ICEBERG, "Sent Audit Metrics for Sort Iceberg"),

    SORT_HBASE_INPUT(19, INPUT, HBASE, "Received Audit Metrics for Sort HBase"),
    SORT_HBASE_OUTPUT(20, OUTPUT, HBASE, "Sent Audit Metrics for Sort HBase"),

    SORT_DORIS_INPUT(21, INPUT, DORIS, "Received Audit Metrics for Sort Doris"),
    SORT_DORIS_OUTPUT(22, OUTPUT, DORIS, "Sent Audit Metrics for Sort Doris"),

    SORT_KUDU_INPUT(25, INPUT, KUDU, "Received Audit Metrics for Sort Kudu"),
    SORT_KUDU_OUTPUT(26, OUTPUT, KUDU, "Sent Audit Metrics for Sort Kudu"),

    SORT_POSTGRES_INPUT(27, INPUT, POSTGRES, "Received Audit Metrics for Sort Postgres"),
    SORT_POSTGRES_OUTPUT(28, OUTPUT, POSTGRES, "Sent Audit Metrics for Sort Postgres"),

    SORT_BINLOG_INPUT(35, INPUT, BINLOG, "Received Audit Metrics for Sort Binlog"),
    SORT_BINLOG_OUTPUT(36, OUTPUT, BINLOG, "Sent Audit Metrics for Sort Binlog"),

    SORT_TUBE_INPUT(33, INPUT, TUBEMQ, "Received Audit Metrics for Sort TubeMQ"),
    SORT_TUBE_OUTPUT(34, OUTPUT, TUBEMQ, "Sent Audit Metrics for Sort TubeMQ"),

    SORT_MYSQL_INPUT(35, INPUT, MYSQL, "Received Audit Metrics for Sort MySQL"),
    SORT_MYSQL_OUTPUT(36, OUTPUT, MYSQL, "Sent Audit Metrics for Sort MySQL"),

    SORT_HDFS_INPUT(37, INPUT, HDFS, "Received Audit Metrics for Sort HDFS"),
    SORT_HDFS_OUTPUT(38, OUTPUT, HDFS, "Sent Audit Metrics for Sort HDFS"),

    SORT_TDSQL_MYSQL_INPUT(39, INPUT, TDSQL_MYSQL, "Received Audit Metrics for TDSQL MYSQL"),
    SORT_TDSQL_MYSQL_OUTPUT(40, OUTPUT, TDSQL_MYSQL, "Sent Audit Metrics for TDSQL MYSQL"),

    BIFANG_INPUT(41, INPUT, BIFANG, "Received Audit Metrics for BIFANG"),
    BIFANG_OUTPUT(42, OUTPUT, BIFANG, "Sent Audit Metrics for BIFANG"),

    ICEBERG_AO_INPUT(43, INPUT, ICEBERG_AO, "Received Audit Metrics for ICEBERG AO"),
    ICEBERG_AO_OUTPUT(44, OUTPUT, ICEBERG_AO, "Sent Audit Metrics for ICEBERG AO");

    private static final Logger LOGGER = LoggerFactory.getLogger(AuditIdEnum.class);
    private final int auditId;
    private final String description;
    private final FlowType flowType;
    private final AuditType auditType;

    AuditIdEnum(int auditId, FlowType flowType, AuditType auditType, String description) {
        this.auditId = auditId;
        this.description = description;
        this.flowType = flowType;
        this.auditType = auditType;
    }

    public int getValue() {
        return auditId;
    }

    public String getDescription() {
        return description;
    }

    public FlowType getFlowType() {
        return flowType;
    }

    public AuditType getAuditType() {
        return auditType;
    }

    public static AuditIdEnum getAuditId(String auditType, FlowType flowType) {
        for (AuditIdEnum auditIdEnum : AuditIdEnum.values()) {
            if (auditIdEnum.getFlowType() == flowType &&
                    auditType.equalsIgnoreCase(auditIdEnum.getAuditType().value())) {
                return auditIdEnum;
            }
        }
        LOGGER.error("Error Audit type: {}, flow type {}: ", auditType, flowType);
        throw new AuditTypeNotExistException(String.format("Audit type %s does not exist", auditType));
    }
}
