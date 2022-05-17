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
@Deprecated
public class Constant {

    public static final String URL_SPLITTER = ",";
    public static final String HOST_SPLITTER = ":";

    public static final String DATA_SOURCE_DB = "DB";

    public static final String DATA_TYPE_TEXT = "TEXT";

    public static final String DATA_TYPE_KEY_VALUE = "KEY-VALUE";

    public static final String CLUSTER_TUBE = "TUBE";
    public static final String CLUSTER_PULSAR = "PULSAR";
    public static final String CLUSTER_TDMQ_PULSAR = "TDMQ_PULSAR";

    public static final String PULSAR_TOPIC_TYPE_SERIAL = "SERIAL";

    public static final String PULSAR_TOPIC_TYPE_PARALLEL = "PARALLEL";

    public static final String SYSTEM_USER = "SYSTEM"; // system user

    public static final String PREFIX_DLQ = "dlq"; // prefix of the Topic of the dead letter queue

    public static final String PREFIX_RLQ = "rlq"; // prefix of the Topic of the retry letter queue


}
