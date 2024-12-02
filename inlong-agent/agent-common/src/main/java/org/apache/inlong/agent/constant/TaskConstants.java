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
 * Basic config for a single task
 */
public class TaskConstants extends CommonConstants {

    // job id
    // public static final String JOB_ID = "job.id";
    public static final String TASK_ID = "task.id";
    public static final String INSTANCE_ID = "instance.id";
    public static final String JOB_INSTANCE_ID = "job.instance.id";
    public static final String INSTANCE_CREATE_TIME = "instance.createTime";
    public static final String INSTANCE_MODIFY_TIME = "instance.modifyTime";
    public static final String TASK_GROUP_ID = "task.groupId";
    public static final String TASK_STREAM_ID = "task.streamId";
    public static final String RESTORE_FROM_DB = "task.restoreFromDB";

    public static final String TASK_SOURCE = "task.source";

    public static final String TASK_CHANNEL = "task.channel";

    public static final String TASK_CLASS = "task.taskClass";
    public static final String INSTANCE_CLASS = "task.instance.class";
    public static final String TASK_FILE_TRIGGER = "task.fileTask.trigger";

    // sink config
    public static final String TASK_SINK = "task.sink";
    public static final String TASK_PROXY_SEND = "task.proxySend";
    public static final boolean DEFAULT_TASK_PROXY_SEND = false;
    public static final String TASK_MQ_CLUSTERS = "task.mqClusters";
    public static final String TASK_MQ_TOPIC = "task.topicInfo";
    public static final String OFFSET = "offset";
    public static final String DEFAULT_OFFSET = "-1L";
    public static final String INODE_INFO = "inodeInfo";

    // File task
    public static final String TASK_DIR_FILTER_PATTERN = "task.fileTask.dir.pattern"; // deprecated
    public static final String FILE_DIR_FILTER_PATTERNS = "task.fileTask.dir.patterns";
    public static final String TASK_FILE_TIME_OFFSET = "task.fileTask.timeOffset";
    public static final String TASK_TIME_ZONE = "task.timeZone";
    public static final String TASK_CYCLE_UNIT = "task.cycleUnit";
    public static final String FILE_TASK_CYCLE_UNIT = "task.fileTask.cycleUnit";
    public static final String TASK_FILE_CONTENT_COLLECT_TYPE = "task.fileTask.contentCollectType";
    public static final String FILE_CONTENT_STYLE = "task.fileTask.dataContentStyle";
    public static final String FILE_DATA_SEPARATOR = "task.fileTask.dataSeparator";
    public static final String FILE_FILTER_STREAMS = "task.fileTask.filterStreams";
    public static final String TASK_RETRY = "task.retry";
    public static final String FILE_TASK_TIME_FROM = "task.fileTask.dataTimeFrom";
    public static final String FILE_TASK_TIME_TO = "task.fileTask.dataTimeTo";
    public static final String FILE_MAX_NUM = "task.fileTask.maxFileCount";
    public static final String PREDEFINE_FIELDS = "task.predefinedFields";
    public static final String TASK_AUDIT_VERSION = "task.auditVersion";

    // Kafka task
    public static final String TASK_KAFKA_TOPIC = "task.kafkaTask.topic";
    public static final String TASK_KAFKA_BOOTSTRAP_SERVERS = "task.kafkaTask.bootstrap.servers";
    public static final String TASK_KAFKA_OFFSET = "task.kafkaTask.partition.offset";
    public static final String TASK_KAFKA_AUTO_COMMIT_OFFSET_RESET = "task.kafkaTask.autoOffsetReset";

    // COS task
    public static final String COS_TASK_CYCLE_UNIT = "task.cosTask.cycleUnit";
    public static final String COS_CONTENT_STYLE = "task.cosTask.contentStyle";
    public static final String COS_MAX_NUM = "task.cosTask.maxFileCount";
    public static final String COS_TASK_PATTERN = "task.cosTask.pattern";
    public static final String TASK_COS_TIME_OFFSET = "task.cosTask.timeOffset";
    public static final String COS_TASK_RETRY = "task.cosTask.retry";
    public static final String COS_TASK_TIME_FROM = "task.cosTask.dataTimeFrom";
    public static final String COS_TASK_TIME_TO = "task.cosTask.dataTimeTo";
    public static final String COS_TASK_BUCKET_NAME = "task.cosTask.bucketName";
    public static final String COS_TASK_SECRET_ID = "task.cosTask.secretId";
    public static final String COS_TASK_SECRET_KEY = "task.cosTask.secretKey";
    public static final String COS_TASK_REGION = "task.cosTask.region";
    public static final String COS_DATA_SEPARATOR = "task.cosTask.dataSeparator";
    public static final String COS_FILTER_STREAMS = "task.cosTask.filterStreams";

    /**
     * delimiter to split offset for different task
     */
    public static final String TASK_KAFKA_OFFSET_DELIMITER = "_";

    /**
     * delimiter to split all partition offset for all kafka tasks
     */
    public static final String TASK_KAFKA_PARTITION_OFFSET_DELIMITER = "#";

    // Pulsar task
    public static final String TASK_PULSAR_TENANT = "task.pulsarTask.tenant";
    public static final String TASK_PULSAR_NAMESPACE = "task.pulsarTask.namespace";
    public static final String TASK_PULSAR_TOPIC = "task.pulsarTask.topic";
    public static final String TASK_PULSAR_SUBSCRIPTION = "task.pulsarTask.subscription";
    public static final String TASK_PULSAR_SUBSCRIPTION_TYPE = "task.pulsarTask.subscriptionType";
    public static final String TASK_PULSAR_SERVICE_URL = "task.pulsarTask.serviceUrl";
    public static final String TASK_PULSAR_SUBSCRIPTION_POSITION = "task.pulsarTask.subscriptionPosition";
    public static final String TASK_PULSAR_RESET_TIME = "task.pulsarTask.resetTime";

    // Mongo task
    public static final String TASK_MONGO_HOSTS = "task.mongoTask.hosts";
    public static final String TASK_MONGO_USER = "task.mongoTask.user";
    public static final String TASK_MONGO_PASSWORD = "task.mongoTask.password";
    public static final String TASK_MONGO_DATABASE_INCLUDE_LIST = "task.mongoTask.databaseIncludeList";
    public static final String TASK_MONGO_DATABASE_EXCLUDE_LIST = "task.mongoTask.databaseExcludeList";
    public static final String TASK_MONGO_COLLECTION_INCLUDE_LIST = "task.mongoTask.collectionIncludeList";
    public static final String TASK_MONGO_COLLECTION_EXCLUDE_LIST = "task.mongoTask.collectionExcludeList";
    public static final String TASK_MONGO_FIELD_EXCLUDE_LIST = "task.mongoTask.fieldExcludeList";
    public static final String TASK_MONGO_SNAPSHOT_MODE = "task.mongoTask.snapshotMode";
    public static final String TASK_MONGO_CAPTURE_MODE = "task.mongoTask.captureMode";
    public static final String TASK_MONGO_QUEUE_SIZE = "task.mongoTask.queueSize";
    public static final String TASK_MONGO_STORE_HISTORY_FILENAME = "task.mongoTask.history.filename";
    public static final String TASK_MONGO_OFFSET_SPECIFIC_OFFSET_FILE = "task.mongoTask.offset.specificOffsetFile";
    public static final String TASK_MONGO_OFFSET_SPECIFIC_OFFSET_POS = "task.mongoTask.offset.specificOffsetPos";
    public static final String TASK_MONGO_OFFSETS = "task.mongoTask.offsets";
    public static final String TASK_MONGO_CONNECT_TIMEOUT_MS = "task.mongoTask.connectTimeoutInMs";
    public static final String TASK_MONGO_CURSOR_MAX_AWAIT = "task.mongoTask.cursorMaxAwaitTimeInMs";
    public static final String TASK_MONGO_SOCKET_TIMEOUT = "task.mongoTask.socketTimeoutInMs";
    public static final String TASK_MONGO_SELECTION_TIMEOUT = "task.mongoTask.selectionTimeoutInMs";
    public static final String TASK_MONGO_FIELD_RENAMES = "task.mongoTask.fieldRenames";
    public static final String TASK_MONGO_MEMBERS_DISCOVER = "task.mongoTask.membersAutoDiscover";
    public static final String TASK_MONGO_CONNECT_MAX_ATTEMPTS = "task.mongoTask.connectMaxAttempts";
    public static final String TASK_MONGO_BACKOFF_MAX_DELAY = "task.mongoTask.connectBackoffMaxDelayInMs";
    public static final String TASK_MONGO_BACKOFF_INITIAL_DELAY = "task.mongoTask.connectBackoffInitialDelayInMs";
    public static final String TASK_MONGO_INITIAL_SYNC_MAX_THREADS = "task.mongoTask.initialSyncMaxThreads";
    public static final String TASK_MONGO_SSL_INVALID_HOSTNAME_ALLOWED = "task.mongoTask.sslInvalidHostnameAllowed";
    public static final String TASK_MONGO_SSL_ENABLE = "task.mongoTask.sslEnabled";
    public static final String TASK_MONGO_POLL_INTERVAL = "task.mongoTask.pollIntervalInMs";

    // Oracle task
    public static final String TASK_ORACLE_HOSTNAME = "task.oracleTask.hostname";
    public static final String TASK_ORACLE_PORT = "task.oracleTask.port";
    public static final String TASK_ORACLE_USER = "task.oracleTask.user";
    public static final String TASK_ORACLE_PASSWORD = "task.oracleTask.password";
    public static final String TASK_ORACLE_DBNAME = "task.oracleTask.dbname";
    public static final String TASK_ORACLE_SERVERNAME = "task.oracleTask.serverName";
    public static final String TASK_ORACLE_SCHEMA_INCLUDE_LIST = "task.oracleTask.schemaIncludeList";
    public static final String TASK_ORACLE_TABLE_INCLUDE_LIST = "task.oracleTask.tableIncludeList";
    public static final String TASK_ORACLE_SNAPSHOT_MODE = "task.oracleTask.snapshotMode";

    // PostgreSQL task
    public static final String TASK_POSTGRES_HOSTNAME = "task.postgreSQLTask.hostname";
    public static final String TASK_POSTGRES_PORT = "task.postgreSQLTask.port";
    public static final String TASK_POSTGRES_USER = "task.postgreSQLTask.user";
    public static final String TASK_POSTGRES_PASSWORD = "task.postgreSQLTask.password";
    public static final String TASK_POSTGRES_DBNAME = "task.postgreSQLTask.dbname";
    public static final String TASK_POSTGRES_SERVERNAME = "task.postgreSQLTask.servername";
    public static final String TASK_POSTGRES_SCHEMA_INCLUDE_LIST = "task.postgreSQLTask.schemaIncludeList";
    public static final String TASK_POSTGRES_TABLE_INCLUDE_LIST = "task.postgreSQLTask.tableIncludeList";
    public static final String TASK_POSTGRES_PLUGIN_NAME = "task.postgreSQLTask.pluginName";
    public static final String TASK_POSTGRES_SNAPSHOT_MODE = "task.postgreSQLTask.snapshotMode";

    // MQTT
    public static final String TASK_MQTT_USERNAME = "task.mqttTask.userName";
    public static final String TASK_MQTT_PASSWORD = "task.mqttTask.password";
    public static final String TASK_MQTT_SERVER_URI = "task.mqttTask.serverURI";
    public static final String TASK_MQTT_TOPIC = "task.mqttTask.topic";
    public static final String TASK_MQTT_CONNECTION_TIMEOUT = "task.mqttTask.connectionTimeOut";
    public static final String TASK_MQTT_KEEPALIVE_INTERVAL = "task.mqttTask.keepAliveInterval";
    public static final String TASK_MQTT_QOS = "task.mqttTask.qos";
    public static final String TASK_MQTT_CLEAN_SESSION = "task.mqttTask.cleanSession";
    public static final String TASK_MQTT_CLIENT_ID_PREFIX = "task.mqttTask.clientIdPrefix";
    public static final String TASK_MQTT_QUEUE_SIZE = "task.mqttTask.queueSize";
    public static final String TASK_MQTT_AUTOMATIC_RECONNECT = "task.mqttTask.automaticReconnect";
    public static final String TASK_MQTT_VERSION = "task.mqttTask.mqttVersion";

    // SQLServer task
    public static final String TASK_SQLSERVER_HOSTNAME = "task.sqlserverTask.hostname";
    public static final String TASK_SQLSERVER_PORT = "task.sqlserverTask.port";
    public static final String TASK_SQLSERVER_USER = "task.sqlserverTask.user";
    public static final String TASK_SQLSERVER_PASSWORD = "task.sqlserverTask.password";
    public static final String TASK_SQLSERVER_DB_NAME = "task.sqlserverTask.dbname";
    public static final String TASK_SQLSERVER_SNAPSHOT_MODE = "task.sqlserverTask.snapshot.mode";
    public static final String TASK_SQLSERVER_SERVER_NAME = "task.sqlserverTask.serverName";
    public static final String TASK_SQLSERVER_SCHEMA_NAME = "task.sqlserverTask.schemaName";
    public static final String TASK_SQLSERVER_TABLE_NAME = "task.sqlserverTask.tableName";
    public static final String TASK_SQLSERVER_UNIX_TIMESTAMP_FORMAT_ENABLE =
            "task.sqlserverTask.unixTimestampFormatEnable";

    public static final String TASK_REDIS_PORT = "task.redisTask.port";
    public static final String TASK_REDIS_HOSTNAME = "task.redisTask.hostname";
    public static final String TASK_REDIS_SSL = "task.redisTask.ssl";
    public static final String TASK_REDIS_AUTHUSER = "task.redisTask.authUser";
    public static final String TASK_REDIS_AUTHPASSWORD = "task.redisTask.authPassword";
    public static final String TASK_REDIS_READTIMEOUT = "task.redisTask.readTimeout";
    public static final String TASK_REDIS_REPLID = "task.redisTask.replId";
    public static final String TASK_REDIS_OFFSET = "task.redisTask.offset";
    public static final String TASK_REDIS_DB_NAME = "task.redisTask.dbName";
    public static final String TASK_REDIS_COMMAND = "task.redisTask.command";
    public static final String TASK_REDIS_KEYS = "task.redisTask.keys";
    public static final String TASK_REDIS_FIELD_OR_MEMBER = "task.redisTask.fieldOrMember";
    public static final String TASK_REDIS_IS_SUBSCRIBE = "task.redisTask.isSubscribe";
    public static final String TASK_REDIS_SUBSCRIPTION_OPERATION = "task.redisTask.subscriptionOperation";
    public static final String TASK_REDIS_SYNC_FREQ = "task.redisTask.syncFreq";

    public static final String TASK_STATE = "task.state";

    public static final String INSTANCE_STATE = "instance.state";

    public static final String FILE_UPDATE_TIME = "fileUpdateTime";

    public static final String LAST_UPDATE_TIME = "lastUpdateTime";

    // data time reading file
    public static final String SOURCE_DATA_TIME = "source.dataTime";

    // data time for sink
    public static final String SINK_DATA_TIME = "sink.dataTime";

    /**
     * when job is retried, the retry time should be provided
     */
    public static final String TASK_RETRY_TIME = "task.retryTime";

    /**
     * sync send data when sending to DataProxy
     */
    public static final int SYNC_SEND_OPEN = 1;

}
