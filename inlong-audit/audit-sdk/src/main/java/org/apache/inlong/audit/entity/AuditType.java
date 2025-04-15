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

package org.apache.inlong.audit.entity;

public enum AuditType {

    SDK("SDK"),
    AGENT("Agent"),
    DATAPROXY("DataProxy"),
    HIVE("Hive"),
    CLICKHOUSE("ClickHouse"),
    ELASTICSEARCH("ElasticSearch"),
    STARROCKS("StarRocks"),
    HUDI("HuDi"),
    ICEBERG("Iceberg"),
    HBASE("HBase"),
    DORIS("Doris"),
    KUDU("Kudu"),
    POSTGRES("Postgres"),
    BINLOG("MYSQL_BINLOG"),
    TUBEMQ("TubeMQ"),
    MYSQL("MYSQL"),
    HDFS("HDFS"),
    TDSQL_MYSQL("TDSQL_MYSQL"),
    MQ_PULSAR("MQ_PULSAR");

    private final String auditType;

    AuditType(String auditType) {
        this.auditType = auditType;
    }
    public String value() {
        return auditType;
    }
}
