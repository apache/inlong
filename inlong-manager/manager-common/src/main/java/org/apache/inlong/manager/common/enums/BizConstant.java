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
 * Constant for business
 */
public class BizConstant {

    public static final String STORAGE_HIVE = "HIVE";

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

    public static final String ID_IS_EMPTY = "id cannot empty during the update/delete operation";

    public static final String GROUP_ID_IS_EMPTY = "business group id is empty";

    public static final String STREAM_ID_IS_EMPTY = "data stream id is empty";

    public static final String PULSAR_TOPIC_TYPE_SERIAL = "SERIAL";

    public static final String PULSAR_TOPIC_TYPE_PARALLEL = "PARALLEL";

    public static final String SYSTEM_USER = "SYSTEM"; // system user

    public static final String PREFIX_DLQ = "dlq"; // prefix of the Topic of the dead letter queue

    public static final String PREFIX_RLQ = "rlq"; // prefix of the Topic of the retry letter queue

    public static final Integer ENABLE_CREATE_TABLE = 1; // Enable create table

    public static final Integer DISABLE_CREATE_TABLE = 0; // Disable create table
}
