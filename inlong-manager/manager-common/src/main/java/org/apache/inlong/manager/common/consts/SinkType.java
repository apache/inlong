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

import org.apache.inlong.manager.common.enums.ClusterType;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Constants of sink type.
 */
public class SinkType extends StreamType {

    @SupportSortType(sortType = SortType.SORT_FLINK)
    public static final String HIVE = "HIVE";

    @SupportSortType(sortType = SortType.SORT_FLINK)
    public static final String CLICKHOUSE = "CLICKHOUSE";

    @SupportSortType(sortType = SortType.SORT_FLINK)
    public static final String HBASE = "HBASE";

    @SupportSortType(sortType = SortType.SORT_STANDALONE)
    public static final String ES = "ES";

    @SupportSortType(sortType = SortType.SORT_FLINK)
    public static final String HDFS = "HDFS";

    @SupportSortType(sortType = SortType.SORT_FLINK)
    public static final String GREENPLUM = "GREENPLUM";

    @SupportSortType(sortType = SortType.SORT_FLINK)
    public static final String MYSQL = "MYSQL";

    @SupportSortType(sortType = SortType.SORT_FLINK)
    public static final String TDSQLPOSTGRESQL = "TDSQLPOSTGRESQL";

    @SupportSortType(sortType = SortType.SORT_FLINK)
    public static final String DORIS = "DORIS";

    @SupportSortType(sortType = SortType.SORT_FLINK)
    public static final String STARROCKS = "STARROCKS";

    @SupportSortType(sortType = SortType.SORT_FLINK)
    public static final String KUDU = "KUDU";

    @SupportSortType(sortType = SortType.SORT_FLINK)
    public static final String REDIS = "REDIS";

    @SupportSortType(sortType = SortType.SORT_FLINK)
    public static final String TUBEMQ = "TUBEMQ";

    @SupportSortType(sortType = SortType.SORT_STANDALONE)
    public static final String HTTP = "HTTP";

    @SupportSortType(sortType = SortType.SORT_FLINK)
    public static final String OCEANBASE = "OCEANBASE";

    /**
     * Tencent cloud log service
     * Details: <a href="https://www.tencentcloud.com/products/cls">CLS</a>
     */
    @SupportSortType(sortType = SortType.SORT_STANDALONE)
    public static final String CLS = "CLS";

    public static final Map<String, String> SINK_TO_CLUSTER = new HashMap<>();

    public static final Set<String> SORT_FLINK_SINK = new HashSet<>();

    public static final Set<String> SORT_STANDALONE_SINK = new HashSet<>();

    static {
        SINK_TO_CLUSTER.put(CLS, ClusterType.SORT_CLS);
        SINK_TO_CLUSTER.put(ES, ClusterType.SORT_ES);
        SINK_TO_CLUSTER.put(PULSAR, ClusterType.SORT_PULSAR);
        SINK_TO_CLUSTER.put(KAFKA, ClusterType.SORT_KAFKA);
    }

    static {
        SinkType obj = new SinkType();
        Class<? extends SinkType> clazz = obj.getClass();
        Field[] fields = clazz.getFields();
        for (Field field : fields) {
            if (field.isAnnotationPresent(SupportSortType.class)) {
                SupportSortType annotation = field.getAnnotation(SupportSortType.class);
                if (Objects.equals(annotation.sortType(), SortType.SORT_STANDALONE)) {
                    SORT_STANDALONE_SINK.add(field.getName());
                } else {
                    SORT_FLINK_SINK.add(field.getName());
                }
            }
        }
    }

    public static boolean containSortFlinkSink(List<String> sinkTypes) {
        return sinkTypes.stream().anyMatch(SORT_FLINK_SINK::contains);
    }

    public static String relatedSortClusterType(String sinkType) {
        return SINK_TO_CLUSTER.get(sinkType);
    }
}
