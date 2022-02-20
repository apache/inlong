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

package org.apache.inlong.agent.constants;

/**
 * Basic config for a single job
 */
public class JobConstants extends CommonConstants {

    // job id
    public static final String JOB_ID = "job.id";
    public static final String JOB_INSTANCE_ID = "job.instance.id";
    public static final String JOB_IP = "job.ip";
    public static final String JOB_RETRY = "job.retry";

    public static final String JOB_SOURCE = "job.source";
    public static final String JOB_SINK = "job.sink";
    public static final String JOB_CHANNEL = "job.channel";
    public static final String JOB_TRIGGER = "job.trigger";
    public static final String JOB_NAME = "job.name";

    public static final String DEFAULT_JOB_NAME = "default";
    public static final String JOB_DESCRIPTION = "job.description";
    public static final String DEFAULT_JOB_DESCRIPTION = "default job description";
    public static final String DEFAULT_JOB_LINE_FILTER = "";

    //File job
    public static final String JOB_LINE_FILTER_PATTERN = "job.pattern";
    public static final String JOB_DIR_FILTER_PATTERN = "job.dir.pattern";
    public static final String JOB_FILE_TIME_OFFSET = "job.timeOffset";
    public static final String JOB_FILE_MAX_WAIT = "job.file.max.wait";
    public static final String JOB_ADDITION_STR = "job.additionStr";
    public static final String JOB_CYCLE_UNIT = "job.cycleUnit";

    public static final String JOB_DIR_FILTER_PATH = "job.dir.path";

    //Binlog job
    private static final String JOB_DATABASE_USER = "job.database.user";
    private static final String JOB_DATABASE_PASSWORD = "job.database.password";
    private static final String JOB_DATABASE_HOSTNAME = "job.database.hostname";
    private static final String JOB_DATABASE_WHITELIST = "job.database.tableWhiteList";
    private static final String JOB_DATABASE_SERVER_TIME_ZONE = "job.database.serverTimezone";
    private static final String JOB_DATABASE_STORE_OFFSET_INTERVAL_MS = "offset.flush.interval.ms";
    private static final String JOB_DATABASE_STORE_HISTORY_FILENAME = "job.database.history.file.filename";
    private static final String JOB_DATABASE_SNAPSHOT_MODE = "job.database.snapshot.mode";
    private static final  String JOB_DATABASE_OFFSET = "job.database.offset";

    //Kafka job
    private static final  String SOURCE_KAFKA_TOPIC = "source_kafka_topic";
    private static final  String SOURCE_KAFKA_KEY_DESERIALIZER = "source_kafka_key_deserializer";
    private static final  String SOURCE_KAFKA_VALUE_DESERIALIZER = "source_kafka_value_deserializer";
    private static final  String SOURCE_KAFKA_BOOTSTRAP_SERVERS = "source_kafka_bootstrap_servers";
    private static final  String SOURCE_KAFKA_GROUP_ID = "source_kafka_group_id";
    private static final  String SOURCE_KAFKA_RECORD_SPEED = "source_kafka_record_speed";
    private static final  String SOURCE_KAFKA_BYTE_SPEED_LImIT = "source_kafka_byte_speed_limit";
    private static final  String SOURCE_KAFKA_MIN_INTERVAL = "source_kafka_min_interval";
    private static final  String SOURCE_KAFKA_OFFSET = "source_kafka_offset";

    // job type, delete/add
    public static final String JOB_TYPE = "job.type";

    public static final String JOB_CHECKPOINT = "job.checkpoint";

    public static final String DEFAULT_JOB_FILE_TIME_OFFSET = "0d";

    // time in min
    public static final int DEFAULT_JOB_FILE_MAX_WAIT = 1;

    public static final String JOB_READ_WAIT_TIMEOUT = "job.file.read.wait";

    public static final int DEFAULT_JOB_READ_WAIT_TIMEOUT = 100;

    public static final String JOB_ID_PREFIX = "job_";

    public static final String SQL_JOB_ID = "sql_job_id";

    public static final String JOB_STORE_TIME = "job.store.time";

    public static final String JOB_OP = "job.op";

    public static final String TRIGGER_ONLY_ONE_JOB = "job.standalone";

    // field splitter
    public static final String JOB_FIELD_SPLITTER = "job.splitter";

    // job delivery time
    public static final String JOB_DELIVERY_TIME = "job.deliveryTime";

    // job time reading file
    public static final String JOB_DATA_TIME = "job.dataTime";

    /**
     * when job is retried, the retry time should be provided
     */
    public static final String JOB_RETRY_TIME = "job.retryTime";

}
