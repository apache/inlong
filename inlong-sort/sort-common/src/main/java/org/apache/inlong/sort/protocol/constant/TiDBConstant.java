/*
 *   Licensed to the Apache Software Foundation (ASF) under one
 *   or more contributor license agreements.  See the NOTICE file
 *   distributed with this work for additional information
 *   regarding copyright ownership.  The ASF licenses this file
 *   to you under the Apache License, Version 2.0 (the
 *   "License"); you may not use this file except in compliance
 *   with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package org.apache.inlong.sort.protocol.constant;

/**
 * TiDB options constant
 */
public class TiDBConstant {

    public static final String CONNECTOR_NAME = "tidb";

    public static final String CONNECTOR = "connector";

    public static final String URL = "tidb.database.url";

    public static final String USERNAME = "tidb.username";

    public static final String PASSWORD = "tidb.password";

    public static final String DATABASE_NAME = "tidb.database.name";

    public static final String TABLE_NAME = "tidb.table.name";

    public static final String STREAMING_SOURCE = "tidb.streaming.source";

    public static final String STREAMING_CODEC = "tidb.streaming.codec";

    public static final String STREAMING_SOURCE_KAFKA = "kafka";

    public static final String BOOTSTRAP_SERVERS = "tidb.streaming.kafka.bootstrap.servers";

    public static final String TOPIC_NAME = "tidb.streaming.kafka.topic";

    public static final String GROUP_ID = "tidb.streaming.kafka.group.id";

    public static final String OFFSET_RESET = "tidb.streaming.kafka.auto.offset.reset";
}
