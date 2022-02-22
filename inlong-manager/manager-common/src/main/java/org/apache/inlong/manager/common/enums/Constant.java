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

package org.apache.inlong.manager.common.enums;

/**
 * Constant for system
 */
public class Constant {

    public static final String SOURCE_FILE = "FILE";

    public static final String SOURCE_DB_SQL = "DB_SQL";

    public static final String SOURCE_DB_BINLOG = "DB_BINLOG";

    public static final String SOURCE_KAFKA = "KAFKA";

    public static final String SINK_HIVE = "HIVE";

    public static final String SINK_KAFKA = "KAFKA";

    public static final String SINK_CLICKHOUSE = "CLICKHOUSE";

    public static final String SINK_ICEBERG = "ICEBERG";

    public static final String DATA_SOURCE_DB = "DB";

    public static final String DATA_SOURCE_FILE = "FILE";

    public static final String DATA_SOURCE_AUTO_PUSH = "AUTO_PUSH";

    public static final String DATA_TYPE_TEXT = "TEXT";

    public static final String DATA_TYPE_KEY_VALUE = "KEY-VALUE";

    public static final String FILE_FORMAT_TEXT = "TextFile";

    public static final String FILE_FORMAT_ORC = "OrcFile";

    public static final String FILE_FORMAT_SEQUENCE = "SequenceFile";

    public static final String FILE_FORMAT_PARQUET = "Parquet";

    public static final String MIDDLEWARE_TUBE = "TUBE";

    public static final String MIDDLEWARE_PULSAR = "PULSAR";

    public static final String SCHEMA_M0_DAY = "m0_day";

    public static final String CLUSTER_HIVE_TOPO = "HIVE_TOPO";

    public static final String ID_IS_EMPTY = "primary key is empty";

    public static final String GROUP_ID_IS_EMPTY = "data group id is empty";

    public static final String STREAM_ID_IS_EMPTY = "inlong stream id is empty";

    public static final String REQUEST_IS_EMPTY = "request is empty";

    public static final String SOURCE_TYPE_IS_EMPTY = "sourceType is empty";

    public static final String SOURCE_TYPE_NOT_SAME = "Expected sourceType is %s, but found %s";

    public static final String SINK_TYPE_IS_EMPTY = "sinkType is empty";

    public static final String SINK_TYPE_NOT_SAME = "Expected sinkType is %s, but found %s";

    public static final String PULSAR_TOPIC_TYPE_SERIAL = "SERIAL";

    public static final String PULSAR_TOPIC_TYPE_PARALLEL = "PARALLEL";

    public static final String SYSTEM_USER = "SYSTEM"; // system user

    public static final String PREFIX_DLQ = "dlq"; // prefix of the Topic of the dead letter queue

    public static final String PREFIX_RLQ = "rlq"; // prefix of the Topic of the retry letter queue

    public static final Integer ENABLE_CREATE_RESOURCE = 1; // Enable create resource

    public static final Integer DISABLE_CREATE_RESOURCE = 0; // Disable create resource

}
