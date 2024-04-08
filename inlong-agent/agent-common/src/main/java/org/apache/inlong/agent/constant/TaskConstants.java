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

package org.apache.inlong.agent.constant;

/**
 * Basic config for a single job
 */
public class TaskConstants extends CommonConstants {

    // job id
    // public static final String JOB_ID = "job.id";
    public static final String TASK_ID = "task.id";
    public static final String INSTANCE_ID = "instance.id";
    public static final String JOB_INSTANCE_ID = "job.instance.id";
    public static final String INSTANCE_CREATE_TIME = "instance.createTime";
    public static final String INSTANCE_MODIFY_TIME = "instance.modifyTime";
    public static final String JOB_IP = "job.ip";
    public static final String JOB_RETRY = "job.retry";
    public static final String JOB_UUID = "job.uuid";
    public static final String TASK_GROUP_ID = "task.groupId";
    public static final String TASK_STREAM_ID = "task.streamId";
    public static final String RESTORE_FROM_DB = "task.restoreFromDB";

    public static final String TASK_SOURCE = "task.source";
    public static final String JOB_SOURCE_TYPE = "job.sourceType";

    public static final String TASK_CHANNEL = "task.channel";
    public static final String JOB_NAME = "job.name";
    public static final String JOB_LINE_FILTER_PATTERN = "job.pattern";

    public static final String DEFAULT_JOB_NAME = "default";
    public static final String JOB_DESCRIPTION = "job.description";
    public static final String DEFAULT_JOB_DESCRIPTION = "default job description";
    public static final String DEFAULT_JOB_LINE_FILTER = "";
    public static final String TASK_CLASS = "task.taskClass";
    public static final String INSTANCE_CLASS = "task.instance.class";
    public static final String JOB_FILE_TRIGGER = "job.fileTask.trigger";

    // sink config
    public static final String TASK_SINK = "task.sink";
    public static final String JOB_PROXY_SEND = "job.proxySend";
    public static final boolean DEFAULT_JOB_PROXY_SEND = false;
    public static final String JOB_MQ_ClUSTERS = "job.mqClusters";
    public static final String JOB_MQ_TOPIC = "job.topicInfo";
    public static final String OFFSET = "offset";
    public static final Long DEFAULT_OFFSET = -1L;
    public static final String INODE_INFO = "inodeInfo";

    // File job
    public static final String TASK_DIR_FILTER_PATTERN = "task.fileTask.dir.pattern"; // deprecated
    public static final String FILE_DIR_FILTER_PATTERNS = "task.fileTask.dir.patterns";
    public static final String TASK_FILE_TIME_OFFSET = "task.fileTask.timeOffset";
    public static final String TASK_TIME_ZONE = "task.timeZone";
    public static final String TASK_FILE_MAX_WAIT = "task.fileTask.file.max.wait";
    public static final String TASK_CYCLE_UNIT = "task.cycleUnit";
    public static final String FILE_TASK_CYCLE_UNIT = "task.fileTask.cycleUnit";
    public static final String TASK_FILE_TRIGGER_TYPE = "task.fileTask.collectType";
    public static final String JOB_FILE_LINE_END_PATTERN = "job.fileTask.line.endPattern";
    public static final String JOB_FILE_CONTENT_COLLECT_TYPE = "job.fileTask.contentCollectType";
    public static final String JOB_FILE_META_ENV_LIST = "job.fileTask.envList";
    public static final String JOB_FILE_META_FILTER_BY_LABELS = "job.fileTask.filterMetaByLabels";
    public static final String JOB_FILE_PROPERTIES = "job.fileTask.properties";
    public static final String SOURCE_DATA_CONTENT_STYLE = "task.fileTask.dataContentStyle";
    public static final String SOURCE_DATA_SEPARATOR = "task.fileTask.dataSeparator";
    public static final String JOB_FILE_MONITOR_INTERVAL = "job.fileTask.monitorInterval";
    public static final String JOB_FILE_MONITOR_STATUS = "job.fileTask.monitorStatus";
    public static final String JOB_FILE_MONITOR_EXPIRE = "job.fileTask.monitorExpire";
    public static final String TASK_RETRY = "task.fileTask.retry";
    public static final String TASK_START_TIME = "task.fileTask.startTime";
    public static final String TASK_END_TIME = "task.fileTask.endTime";
    public static final String FILE_MAX_NUM = "task.fileTask.maxFileCount";
    public static final String PREDEFINE_FIELDS = "task.predefinedFields";
    public static final String FILE_SOURCE_EXTEND_CLASS = "task.fileTask.extendedClass";
    public static final String DEFAULT_FILE_SOURCE_EXTEND_CLASS =
            "org.apache.inlong.agent.plugin.sources.file.extend.ExtendedHandler";

    // Binlog job
    public static final String JOB_DATABASE_USER = "job.binlogJob.user";
    public static final String JOB_DATABASE_PASSWORD = "job.binlogJob.password";
    public static final String JOB_DATABASE_HOSTNAME = "job.binlogJob.hostname";
    public static final String JOB_TABLE_WHITELIST = "job.binlogJob.tableWhiteList";
    public static final String JOB_DATABASE_WHITELIST = "job.binlogJob.databaseWhiteList";
    public static final String JOB_DATABASE_OFFSETS = "job.binlogJob.offsets";
    public static final String JOB_DATABASE_OFFSET_FILENAME = "job.binlogJob.offset.filename";

    public static final String JOB_DATABASE_SERVER_TIME_ZONE = "job.binlogJob.serverTimezone";
    public static final String JOB_DATABASE_STORE_OFFSET_INTERVAL_MS = "job.binlogJob.offset.intervalMs";

    public static final String JOB_DATABASE_STORE_HISTORY_FILENAME = "job.binlogJob.history.filename";
    public static final String JOB_DATABASE_INCLUDE_SCHEMA_CHANGES = "job.binlogJob.schema";
    public static final String JOB_DATABASE_SNAPSHOT_MODE = "job.binlogJob.snapshot.mode";
    public static final String JOB_DATABASE_HISTORY_MONITOR_DDL = "job.binlogJob.ddl";
    public static final String JOB_DATABASE_PORT = "job.binlogJob.port";

    // Kafka job
    public static final String TASK_KAFKA_TOPIC = "task.kafkaJob.topic";
    public static final String TASK_KAFKA_BOOTSTRAP_SERVERS = "task.kafkaJob.bootstrap.servers";
    public static final String TASK_KAFKA_GROUP_ID = "task.kafkaJob.group.id";
    public static final String JOB_KAFKA_RECORD_SPEED_LIMIT = "job.kafkaJob.recordSpeed.limit";
    public static final String JOB_KAFKA_BYTE_SPEED_LIMIT = "job.kafkaJob.byteSpeed.limit";
    public static final String TASK_KAFKA_OFFSET = "task.kafkaJob.partition.offset";
    public static final String JOB_KAFKA_READ_TIMEOUT = "job.kafkaJob.read.timeout";
    public static final String TASK_KAFKA_AUTO_COMMIT_OFFSET_RESET = "task.kafkaJob.autoOffsetReset";

    // Pulsar task
    public static final String TASK_PULSAR_TENANT = "task.pulsarTask.tenant";
    public static final String TASK_PULSAR_NAMESPACE = "task.pulsarTask.namespace";
    public static final String TASK_PULSAR_TOPIC = "task.pulsarTask.topic";
    public static final String TASK_PULSAR_SUBSCRIPTION = "task.pulsarTask.subscription";
    public static final String TASK_PULSAR_SUBSCRIPTION_TYPE = "task.pulsarTask.subscriptionType";
    public static final String TASK_PULSAR_SERVICE_URL = "task.pulsarTask.serviceUrl";
    public static final String TASK_PULSAR_SUBSCRIPTION_POSITION = "task.pulsarTask.subscriptionPosition";
    public static final String TASK_PULSAR_RESET_TIME = "task.pulsarTask.resetTime";

    public static final String JOB_MONGO_HOSTS = "job.mongoJob.hosts";
    public static final String JOB_MONGO_USER = "job.mongoJob.user";
    public static final String JOB_MONGO_PASSWORD = "job.mongoJob.password";
    public static final String JOB_MONGO_DATABASE_INCLUDE_LIST = "job.mongoJob.databaseIncludeList";
    public static final String JOB_MONGO_DATABASE_EXCLUDE_LIST = "job.mongoJob.databaseExcludeList";
    public static final String JOB_MONGO_COLLECTION_INCLUDE_LIST = "job.mongoJob.collectionIncludeList";
    public static final String JOB_MONGO_COLLECTION_EXCLUDE_LIST = "job.mongoJob.collectionExcludeList";
    public static final String JOB_MONGO_FIELD_EXCLUDE_LIST = "job.mongoJob.fieldExcludeList";
    public static final String JOB_MONGO_SNAPSHOT_MODE = "job.mongoJob.snapshotMode";
    public static final String JOB_MONGO_CAPTURE_MODE = "job.mongoJob.captureMode";
    public static final String JOB_MONGO_QUEUE_SIZE = "job.mongoJob.queueSize";
    public static final String JOB_MONGO_STORE_HISTORY_FILENAME = "job.mongoJob.history.filename";
    public static final String JOB_MONGO_OFFSET_SPECIFIC_OFFSET_FILE = "job.mongoJob.offset.specificOffsetFile";
    public static final String JOB_MONGO_OFFSET_SPECIFIC_OFFSET_POS = "job.mongoJob.offset.specificOffsetPos";
    public static final String JOB_MONGO_OFFSETS = "job.mongoJob.offsets";
    public static final String JOB_MONGO_CONNECT_TIMEOUT_MS = "job.mongoJob.connectTimeoutInMs";
    public static final String JOB_MONGO_CURSOR_MAX_AWAIT = "job.mongoJob.cursorMaxAwaitTimeInMs";
    public static final String JOB_MONGO_SOCKET_TIMEOUT = "job.mongoJob.socketTimeoutInMs";
    public static final String JOB_MONGO_SELECTION_TIMEOUT = "job.mongoJob.selectionTimeoutInMs";
    public static final String JOB_MONGO_FIELD_RENAMES = "job.mongoJob.fieldRenames";
    public static final String JOB_MONGO_MEMBERS_DISCOVER = "job.mongoJob.membersAutoDiscover";
    public static final String JOB_MONGO_CONNECT_MAX_ATTEMPTS = "job.mongoJob.connectMaxAttempts";
    public static final String JOB_MONGO_BACKOFF_MAX_DELAY = "job.mongoJob.connectBackoffMaxDelayInMs";
    public static final String JOB_MONGO_BACKOFF_INITIAL_DELAY = "job.mongoJob.connectBackoffInitialDelayInMs";
    public static final String JOB_MONGO_INITIAL_SYNC_MAX_THREADS = "job.mongoJob.initialSyncMaxThreads";
    public static final String JOB_MONGO_SSL_INVALID_HOSTNAME_ALLOWED = "job.mongoJob.sslInvalidHostnameAllowed";
    public static final String JOB_MONGO_SSL_ENABLE = "job.mongoJob.sslEnabled";
    public static final String JOB_MONGO_POLL_INTERVAL = "job.mongoJob.pollIntervalInMs";

    public static final Long JOB_KAFKA_DEFAULT_OFFSET = 0L;

    // job type, delete/add
    public static final String JOB_TYPE = "job.type";

    public static final String JOB_CHECKPOINT = "job.checkpoint";

    public static final String DEFAULT_JOB_FILE_TIME_OFFSET = "0d";

    // time in min
    public static final int DEFAULT_JOB_FILE_MAX_WAIT_TIME = 1;

    public static final String JOB_READ_WAIT_TIMEOUT = "job.file.read.wait";

    public static final String JOB_ID_PREFIX = "job_";

    public static final String SQL_JOB_ID = "sql_job_id";

    public static final String JOB_STORE_TIME = "job.store.time";

    public static final String JOB_OP = "job.op";

    public static final String JOB_STATE = "job.state";

    public static final String TASK_STATE = "task.state";

    public static final String INSTANCE_STATE = "instance.state";

    public static final String FILE_UPDATE_TIME = "fileUpdateTime";

    public static final String LAST_UPDATE_TIME = "lastUpdateTime";

    public static final String TRIGGER_ONLY_ONE_JOB = "job.standalone"; // TODO:delete it

    // field splitter
    public static final String JOB_FIELD_SPLITTER = "job.splitter";

    // job delivery time
    public static final String JOB_DELIVERY_TIME = "job.deliveryTime";

    // data time reading file
    public static final String SOURCE_DATA_TIME = "source.dataTime";

    // data time for sink
    public static final String SINK_DATA_TIME = "sink.dataTime";

    // job of the number of seconds to wait before starting the task
    public static final String JOB_TASK_BEGIN_WAIT_SECONDS = "job.taskWaitSeconds";

    /**
     * when job is retried, the retry time should be provided
     */
    public static final String JOB_RETRY_TIME = "job.retryTime";

    /**
     * delimiter to split offset for different task
     */
    public static final String JOB_OFFSET_DELIMITER = "_";

    /**
     * delimiter to split all partition offset for all kafka tasks
     */
    public static final String JOB_KAFKA_PARTITION_OFFSET_DELIMITER = "#";

    /**
     * sync send data when sending to DataProxy
     */
    public static final int SYNC_SEND_OPEN = 1;

    public static final String INTERVAL_MILLISECONDS = "1000";

    /**
     * monitor switch, 1 true and 0 false
     */
    public static final String JOB_FILE_MONITOR_DEFAULT_STATUS = "1";

    /**
     * monitor expire time and the time in milliseconds.
     * default value is -1 and stand for not expire time.
     */
    public static final String JOB_FILE_MONITOR_DEFAULT_EXPIRE = "-1";

}
