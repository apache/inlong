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

package org.apache.inlong.manager.common.consts;

import com.google.common.collect.Sets;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Constants of sink type.
 */
public class SinkType extends StreamType {

    public static final String HIVE = "HIVE";
    public static final String CLICKHOUSE = "CLICKHOUSE";
    public static final String HBASE = "HBASE";
    public static final String ELASTICSEARCH = "ES";
    public static final String HDFS = "HDFS";
    public static final String GREENPLUM = "GREENPLUM";
    public static final String MYSQL = "MYSQL";
    public static final String TDSQLPOSTGRESQL = "TDSQLPOSTGRESQL";
    public static final String DORIS = "DORIS";
    public static final String STARROCKS = "STARROCKS";
    public static final String KUDU = "KUDU";
    public static final String REDIS = "REDIS";
    /**
     * Tencent cloud log service
     * Details: <a href="https://www.tencentcloud.com/products/cls">CLS</a>
     */
    public static final String CLS = "CLS";

    public static final Set<String> SORT_FLINK_SINK = new HashSet<>();
    public static final Set<String> SORT_STANDALONE_SINK = new HashSet<>();

    static {
        SORT_FLINK_SINK.addAll(
                Sets.newHashSet(HIVE, CLICKHOUSE, HBASE, HDFS, GREENPLUM, MYSQL, TDSQLPOSTGRESQL, DORIS,
                        STARROCKS, KUDU, REDIS, KAFKA, HUDI, POSTGRESQL, SQLSERVER, ORACLE, ICEBERG));
        SORT_STANDALONE_SINK.addAll(Sets.newHashSet(CLS, ELASTICSEARCH, PULSAR));
    }

    public static boolean containSortFlinkSink(List<String> sinkTypes) {
        for (String sinkType : sinkTypes) {
            if (SinkType.SORT_FLINK_SINK.contains(sinkType)) {
                return true;
            }
        }
        return false;
    }
}
