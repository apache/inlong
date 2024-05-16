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

    SDK_INPUT(1),
    SDK_OUTPUT(2),

    AGENT_INPUT(3),
    AGENT_OUTPUT(4),

    DATA_PROXY_INPUT(5),
    DATA_PROXY_OUTPUT(6),

    SORT_HIVE_INPUT(7),
    SORT_HIVE_OUTPUT(8),

    SORT_CLICKHOUSE_INPUT(9),
    SORT_CLICKHOUSE_OUTPUT(10),

    SORT_ELASTICSEARCH_INPUT(11),
    SORT_ELASTICSEARCH_OUTPUT(12),

    SORT_STARROCKS_INPUT(13),
    SORT_STARROCKS_OUTPUT(14),

    SORT_HUDI_INPUT(15),
    SORT_HUDI_OUTPUT(16),

    SORT_ICEBERG_INPUT(17),
    SORT_ICEBERG_OUTPUT(18),

    SORT_HBASE_INPUT(19),
    SORT_HBASE_OUTPUT(20),

    SORT_DORIS_INPUT(21),
    SORT_DORIS_OUTPUT(22),

    SORT_KUDU_INPUT(25),
    SORT_KUDU_OUTPUT(26),

    SORT_POSTGRES_INPUT(27),
    SORT_POSTGRES_OUTPUT(28),

    SORT_BINLOG_INPUT(29),
    SORT_BINLOG_OUTPUT(30),

    SORT_TUBE_INPUT(33),
    SORT_TUBE_OUTPUT(34),

    SORT_MYSQL_INPUT(35),
    SORT_MYSQL_OUTPUT(36);

    private final int auditId;

    AuditIdEnum(int auditId) {
        this.auditId = auditId;
    }

    public int getValue() {
        return auditId;
    }
}
