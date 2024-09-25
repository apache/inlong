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

package org.apache.inlong.agent.pojo;

import org.apache.inlong.agent.conf.AgentConfiguration;
import org.apache.inlong.agent.conf.TaskProfile;
import org.apache.inlong.agent.constant.CycleUnitType;
import org.apache.inlong.agent.pojo.BinlogTask.BinlogTaskConfig;
import org.apache.inlong.agent.pojo.FileTask.FileTaskConfig;
import org.apache.inlong.agent.pojo.FileTask.Line;
import org.apache.inlong.agent.pojo.KafkaTask.KafkaTaskConfig;
import org.apache.inlong.agent.pojo.MongoTask.MongoTaskConfig;
import org.apache.inlong.agent.pojo.MqttTask.MqttConfig;
import org.apache.inlong.agent.pojo.OracleTask.OracleTaskConfig;
import org.apache.inlong.agent.pojo.PostgreSQLTask.PostgreSQLTaskConfig;
import org.apache.inlong.agent.pojo.PulsarTask.PulsarTaskConfig;
import org.apache.inlong.agent.pojo.RedisTask.RedisTaskConfig;
import org.apache.inlong.agent.pojo.SqlServerTask.SqlserverTaskConfig;
import org.apache.inlong.common.constant.MQType;
import org.apache.inlong.common.enums.TaskTypeEnum;
import org.apache.inlong.common.pojo.agent.DataConfig;

import com.google.gson.Gson;
import lombok.Data;

import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;
import static org.apache.inlong.agent.constant.FetcherConstants.AGENT_MANAGER_ADDR;
import static org.apache.inlong.agent.constant.TaskConstants.SYNC_SEND_OPEN;
import static org.apache.inlong.common.enums.DataReportTypeEnum.NORMAL_SEND_TO_DATAPROXY;

@Data
public class TaskProfileDto {

    public static final String DEFAULT_FILE_TASK = "org.apache.inlong.agent.plugin.task.file.LogFileTask";
    public static final String DEFAULT_KAFKA_TASK = "org.apache.inlong.agent.plugin.task.KafkaTask";
    public static final String DEFAULT_PULSAR_TASK = "org.apache.inlong.agent.plugin.task.PulsarTask";
    public static final String DEFAULT_MONGODB_TASK = "org.apache.inlong.agent.plugin.task.MongoDBTask";
    public static final String DEFAULT_ORACLE_TASK = "org.apache.inlong.agent.plugin.task.OracleTask";
    public static final String DEFAULT_POSTGRESQL_TASK = "org.apache.inlong.agent.plugin.task.PostgreSQLTask";
    public static final String DEFAULT_MQTT_TASK = "org.apache.inlong.agent.plugin.task.MqttTask";
    public static final String DEFAULT_SQLSERVER_TASK = "org.apache.inlong.agent.plugin.task.SQLServerTask";
    public static final String DEFAULT_CHANNEL = "org.apache.inlong.agent.plugin.channel.MemoryChannel";
    public static final String MANAGER_JOB = "MANAGER_JOB";
    public static final String DEFAULT_DATA_PROXY_SINK = "org.apache.inlong.agent.plugin.sinks.ProxySink";
    public static final String PULSAR_SINK = "org.apache.inlong.agent.plugin.sinks.PulsarSink";
    public static final String KAFKA_SINK = "org.apache.inlong.agent.plugin.sinks.KafkaSink";

    /**
     * file source
     */
    public static final String DEFAULT_SOURCE = "org.apache.inlong.agent.plugin.sources.LogFileSource";
    /**
     * binlog source
     */
    public static final String BINLOG_SOURCE = "org.apache.inlong.agent.plugin.sources.BinlogSource";
    /**
     * kafka source
     */
    public static final String KAFKA_SOURCE = "org.apache.inlong.agent.plugin.sources.KafkaSource";
    // pulsar source
    public static final String PULSAR_SOURCE = "org.apache.inlong.agent.plugin.sources.PulsarSource";
    /**
     * PostgreSQL source
     */
    public static final String POSTGRESQL_SOURCE = "org.apache.inlong.agent.plugin.sources.PostgreSQLSource";
    /**
     * mongo source
     */
    public static final String MONGO_SOURCE = "org.apache.inlong.agent.plugin.sources.MongoDBSource";
    /**
     * oracle source
     */
    public static final String ORACLE_SOURCE = "org.apache.inlong.agent.plugin.sources.OracleSource";
    /**
     * redis source
     */
    public static final String REDIS_SOURCE = "org.apache.inlong.agent.plugin.sources.RedisSource";
    /**
     * mqtt source
     */
    public static final String MQTT_SOURCE = "org.apache.inlong.agent.plugin.sources.MqttSource";
    /**
     * sqlserver source
     */
    public static final String SQLSERVER_SOURCE = "org.apache.inlong.agent.plugin.sources.SQLServerSource";

    private static final Gson GSON = new Gson();

    public static final String deafult_time_offset = "0";

    private static final String DEFAULT_AUDIT_VERSION = "0";

    private Task task;
    private Proxy proxy;

    private static BinlogTask getBinlogTask(DataConfig dataConfigs) {
        BinlogTaskConfig binlogTaskConfig = GSON.fromJson(dataConfigs.getExtParams(),
                BinlogTaskConfig.class);

        BinlogTask binlogTask = new BinlogTask();
        binlogTask.setHostname(binlogTaskConfig.getHostname());
        binlogTask.setPassword(binlogTaskConfig.getPassword());
        binlogTask.setUser(binlogTaskConfig.getUser());
        binlogTask.setTableWhiteList(binlogTaskConfig.getTableWhiteList());
        binlogTask.setDatabaseWhiteList(binlogTaskConfig.getDatabaseWhiteList());
        binlogTask.setSchema(binlogTaskConfig.getIncludeSchema());
        binlogTask.setPort(binlogTaskConfig.getPort());
        binlogTask.setOffsets(dataConfigs.getSnapshot());
        binlogTask.setDdl(binlogTaskConfig.getMonitoredDdl());
        binlogTask.setServerTimezone(binlogTaskConfig.getServerTimezone());

        BinlogTask.Offset offset = new BinlogTask.Offset();
        offset.setIntervalMs(binlogTaskConfig.getIntervalMs());
        offset.setFilename(binlogTaskConfig.getOffsetFilename());
        offset.setSpecificOffsetFile(binlogTaskConfig.getSpecificOffsetFile());
        offset.setSpecificOffsetPos(binlogTaskConfig.getSpecificOffsetPos());

        binlogTask.setOffset(offset);

        BinlogTask.Snapshot snapshot = new BinlogTask.Snapshot();
        snapshot.setMode(binlogTaskConfig.getSnapshotMode());

        binlogTask.setSnapshot(snapshot);

        BinlogTask.History history = new BinlogTask.History();
        history.setFilename(binlogTaskConfig.getHistoryFilename());

        binlogTask.setHistory(history);

        return binlogTask;
    }

    private static FileTask getFileTask(DataConfig dataConfig) {
        FileTask fileTask = new FileTask();
        fileTask.setId(dataConfig.getTaskId());

        FileTaskConfig taskConfig = GSON.fromJson(dataConfig.getExtParams(),
                FileTaskConfig.class);

        FileTask.Dir dir = new FileTask.Dir();
        dir.setPatterns(taskConfig.getPattern());
        dir.setBlackList(taskConfig.getBlackList());
        fileTask.setDir(dir);
        fileTask.setCollectType(taskConfig.getCollectType());
        fileTask.setContentCollectType(taskConfig.getContentCollectType());
        fileTask.setDataContentStyle(taskConfig.getDataContentStyle());
        fileTask.setDataSeparator(taskConfig.getDataSeparator());
        fileTask.setMaxFileCount(taskConfig.getMaxFileCount());
        fileTask.setRetry(taskConfig.getRetry());
        fileTask.setCycleUnit(taskConfig.getCycleUnit());
        fileTask.setStartTime(taskConfig.getStartTime());
        fileTask.setEndTime(taskConfig.getEndTime());
        if (taskConfig.getFilterStreams() != null) {
            fileTask.setFilterStreams(GSON.toJson(taskConfig.getFilterStreams()));
        }
        if (taskConfig.getTimeOffset() != null) {
            fileTask.setTimeOffset(taskConfig.getTimeOffset());
        } else {
            fileTask.setTimeOffset(deafult_time_offset + fileTask.getCycleUnit());
        }

        if (taskConfig.getAdditionalAttr() != null) {
            fileTask.setAddictiveString(taskConfig.getAdditionalAttr());
        }

        if (null != taskConfig.getLineEndPattern()) {
            Line line = new Line();
            line.setEndPattern(taskConfig.getLineEndPattern());
            fileTask.setLine(line);
        }

        if (null != taskConfig.getMonitorInterval()) {
            fileTask.setMonitorInterval(taskConfig.getMonitorInterval());
        }

        if (null != taskConfig.getMonitorStatus()) {
            fileTask.setMonitorStatus(taskConfig.getMonitorStatus());
        }
        return fileTask;
    }

    private static KafkaTask getKafkaTask(DataConfig dataConfigs) {

        KafkaTaskConfig kafkaTaskConfig = GSON.fromJson(dataConfigs.getExtParams(),
                KafkaTaskConfig.class);
        KafkaTask kafkaTask = new KafkaTask();

        KafkaTask.Bootstrap bootstrap = new KafkaTask.Bootstrap();
        bootstrap.setServers(kafkaTaskConfig.getBootstrapServers());
        kafkaTask.setBootstrap(bootstrap);
        KafkaTask.Partition partition = new KafkaTask.Partition();
        partition.setOffset(kafkaTaskConfig.getPartitionOffsets());
        kafkaTask.setPartition(partition);
        KafkaTask.Group group = new KafkaTask.Group();
        group.setId(kafkaTaskConfig.getGroupId());
        kafkaTask.setGroup(group);
        KafkaTask.RecordSpeed recordSpeed = new KafkaTask.RecordSpeed();
        recordSpeed.setLimit(kafkaTaskConfig.getRecordSpeedLimit());
        kafkaTask.setRecordSpeed(recordSpeed);
        KafkaTask.ByteSpeed byteSpeed = new KafkaTask.ByteSpeed();
        byteSpeed.setLimit(kafkaTaskConfig.getByteSpeedLimit());
        kafkaTask.setByteSpeed(byteSpeed);
        kafkaTask.setAutoOffsetReset(kafkaTaskConfig.getAutoOffsetReset());

        kafkaTask.setTopic(kafkaTaskConfig.getTopic());

        return kafkaTask;
    }

    private static PulsarTask getPulsarTask(DataConfig dataConfig) {
        PulsarTaskConfig pulsarTaskConfig = GSON.fromJson(dataConfig.getExtParams(),
                PulsarTaskConfig.class);
        PulsarTask pulsarTask = new PulsarTask();

        pulsarTask.setTenant(pulsarTaskConfig.getPulsarTenant());
        pulsarTask.setNamespace(pulsarTaskConfig.getNamespace());
        pulsarTask.setTopic(pulsarTaskConfig.getTopic());
        pulsarTask.setSubscription(pulsarTaskConfig.getSubscription());
        pulsarTask.setSubscriptionType(pulsarTaskConfig.getSubscriptionType());
        pulsarTask.setServiceUrl(pulsarTaskConfig.getServiceUrl());
        pulsarTask.setSubscriptionPosition(pulsarTaskConfig.getScanStartupMode());
        pulsarTask.setResetTime(pulsarTaskConfig.getResetTime());

        return pulsarTask;
    }

    private static PostgreSQLTask getPostgresTask(DataConfig dataConfigs) {
        PostgreSQLTaskConfig config = GSON.fromJson(dataConfigs.getExtParams(),
                PostgreSQLTaskConfig.class);
        PostgreSQLTask postgreSQLTask = new PostgreSQLTask();

        postgreSQLTask.setUser(config.getUsername());
        postgreSQLTask.setPassword(config.getPassword());
        postgreSQLTask.setHostname(config.getHostname());
        postgreSQLTask.setPort(config.getPort());
        postgreSQLTask.setDbname(config.getDatabase());
        postgreSQLTask.setSchemaIncludeList(config.getSchema());
        postgreSQLTask.setPluginName(config.getDecodingPluginName());
        // Each identifier is of the form schemaName.tableName and connected with ","
        postgreSQLTask.setTableIncludeList(
                config.getTableNameList().stream().map(tableName -> config.getSchema() + "." + tableName).collect(
                        Collectors.joining(",")));
        postgreSQLTask.setServerTimeZone(config.getServerTimeZone());
        postgreSQLTask.setSnapshotMode(config.getScanStartupMode());
        postgreSQLTask.setPrimaryKey(config.getPrimaryKey());

        return postgreSQLTask;
    }

    private static RedisTask getRedisTask(DataConfig dataConfig) {
        RedisTaskConfig config = GSON.fromJson(dataConfig.getExtParams(), RedisTaskConfig.class);
        RedisTask redisTask = new RedisTask();

        redisTask.setAuthUser(config.getUsername());
        redisTask.setAuthPassword(config.getPassword());
        redisTask.setHostname(config.getHostname());
        redisTask.setPort(config.getPort());
        redisTask.setSsl(config.getSsl());
        redisTask.setReadTimeout(config.getTimeout());
        redisTask.setQueueSize(config.getQueueSize());
        redisTask.setReplId(config.getReplId());

        return redisTask;
    }

    private static MongoTask getMongoTask(DataConfig dataConfigs) {

        MongoTaskConfig config = GSON.fromJson(dataConfigs.getExtParams(),
                MongoTaskConfig.class);
        MongoTask mongoTask = new MongoTask();

        mongoTask.setHosts(config.getHosts());
        mongoTask.setUser(config.getUsername());
        mongoTask.setPassword(config.getPassword());
        mongoTask.setDatabaseIncludeList(config.getDatabase());
        mongoTask.setCollectionIncludeList(config.getCollection());
        mongoTask.setSnapshotMode(config.getSnapshotMode());

        MongoTask.Offset offset = new MongoTask.Offset();
        offset.setFilename(config.getOffsetFilename());
        offset.setSpecificOffsetFile(config.getSpecificOffsetFile());
        offset.setSpecificOffsetPos(config.getSpecificOffsetPos());
        mongoTask.setOffset(offset);

        MongoTask.Snapshot snapshot = new MongoTask.Snapshot();
        snapshot.setMode(config.getSnapshotMode());
        mongoTask.setSnapshot(snapshot);

        MongoTask.History history = new MongoTask.History();
        history.setFilename(config.getHistoryFilename());
        mongoTask.setHistory(history);

        return mongoTask;
    }

    private static OracleTask getOracleTask(DataConfig dataConfigs) {
        OracleTaskConfig config = GSON.fromJson(dataConfigs.getExtParams(),
                OracleTaskConfig.class);
        OracleTask oracleTask = new OracleTask();

        oracleTask.setHostname(config.getHostname());
        oracleTask.setPort(config.getPort());
        oracleTask.setUser(config.getUsername());
        oracleTask.setPassword(config.getPassword());
        oracleTask.setSchemaIncludeList(config.getSchemaName());
        oracleTask.setDbname(config.getDatabase());
        oracleTask.setTableIncludeList(config.getTableName());

        OracleTask.Offset offset = new OracleTask.Offset();
        offset.setFilename(config.getOffsetFilename());
        offset.setSpecificOffsetFile(config.getSpecificOffsetFile());
        offset.setSpecificOffsetPos(config.getSpecificOffsetPos());
        oracleTask.setOffset(offset);

        OracleTask.Snapshot snapshot = new OracleTask.Snapshot();
        snapshot.setMode(config.getScanStartupMode());
        oracleTask.setSnapshot(snapshot);

        OracleTask.History history = new OracleTask.History();
        history.setFilename(config.getHistoryFilename());
        oracleTask.setHistory(history);

        return oracleTask;
    }

    private static SqlServerTask getSqlServerTask(DataConfig dataConfigs) {
        SqlserverTaskConfig config = GSON.fromJson(dataConfigs.getExtParams(),
                SqlserverTaskConfig.class);
        SqlServerTask sqlServerTask = new SqlServerTask();
        sqlServerTask.setUser(config.getUsername());
        sqlServerTask.setHostname(config.getHostname());
        sqlServerTask.setPassword(config.getPassword());
        sqlServerTask.setPort(config.getPort());
        sqlServerTask.setServerName(config.getSchemaName());
        sqlServerTask.setDbname(config.getDatabase());
        sqlServerTask.setSchemaName(config.getSchemaName());
        sqlServerTask.setTableName(config.getSchemaName() + "." + config.getTableName());
        sqlServerTask.setServerTimezone(config.getServerTimezone());
        sqlServerTask.setUnixTimestampFormatEnable(config.getUnixTimestampFormatEnable());

        SqlServerTask.Offset offset = new SqlServerTask.Offset();
        offset.setFilename(config.getOffsetFilename());
        offset.setSpecificOffsetFile(config.getSpecificOffsetFile());
        offset.setSpecificOffsetPos(config.getSpecificOffsetPos());
        sqlServerTask.setOffset(offset);

        SqlServerTask.Snapshot snapshot = new SqlServerTask.Snapshot();
        snapshot.setMode(config.getSnapshotMode());
        sqlServerTask.setSnapshot(snapshot);

        SqlServerTask.History history = new SqlServerTask.History();
        history.setFilename(config.getHistoryFilename());
        sqlServerTask.setHistory(history);

        return sqlServerTask;
    }

    public static MqttTask getMqttTask(DataConfig dataConfigs) {
        MqttConfig config = GSON.fromJson(dataConfigs.getExtParams(),
                MqttConfig.class);
        MqttTask mqttTask = new MqttTask();

        mqttTask.setServerURI(config.getServerURI());
        mqttTask.setUserName(config.getUsername());
        mqttTask.setPassword(config.getPassword());
        mqttTask.setTopic(config.getTopic());
        mqttTask.setConnectionTimeOut(config.getConnectionTimeOut());
        mqttTask.setKeepAliveInterval(config.getKeepAliveInterval());
        mqttTask.setQos(config.getQos());
        mqttTask.setCleanSession(config.getCleanSession());
        mqttTask.setClientIdPrefix(config.getClientId());
        mqttTask.setQueueSize(config.getQueueSize());
        mqttTask.setAutomaticReconnect(config.getAutomaticReconnect());
        mqttTask.setMqttVersion(config.getMqttVersion());

        return mqttTask;
    }

    private static Proxy getProxy(DataConfig dataConfigs) {
        Proxy proxy = new Proxy();
        Manager manager = new Manager();
        AgentConfiguration agentConf = AgentConfiguration.getAgentConf();
        manager.setAddr(agentConf.get(AGENT_MANAGER_ADDR));
        proxy.setInlongGroupId(dataConfigs.getInlongGroupId());
        proxy.setInlongStreamId(dataConfigs.getInlongStreamId());
        proxy.setManager(manager);
        if (null != dataConfigs.getSyncSend()) {
            proxy.setSync(dataConfigs.getSyncSend() == SYNC_SEND_OPEN);
        }
        if (null != dataConfigs.getSyncPartitionKey()) {
            proxy.setPartitionKey(dataConfigs.getSyncPartitionKey());
        }
        return proxy;
    }

    /**
     * convert DataConfig to TaskProfile
     */
    public static TaskProfile convertToTaskProfile(DataConfig dataConfig) {
        if (!dataConfig.isValid()) {
            throw new IllegalArgumentException("input dataConfig" + dataConfig + "is invalid please check");
        }

        TaskProfileDto profileDto = new TaskProfileDto();
        Proxy proxy = getProxy(dataConfig);
        profileDto.setProxy(proxy);
        Task task = new Task();

        // common attribute
        task.setId(String.valueOf(dataConfig.getTaskId()));
        task.setGroupId(dataConfig.getInlongGroupId());
        task.setStreamId(dataConfig.getInlongStreamId());
        task.setChannel(DEFAULT_CHANNEL);
        task.setIp(dataConfig.getIp());
        task.setOp(dataConfig.getOp());
        task.setDeliveryTime(dataConfig.getDeliveryTime());
        task.setUuid(dataConfig.getUuid());
        task.setVersion(dataConfig.getVersion());
        task.setState(dataConfig.getState());
        task.setPredefinedFields(dataConfig.getPredefinedFields());
        task.setCycleUnit(CycleUnitType.REAL_TIME);
        task.setTimeZone(dataConfig.getTimeZone());
        if (dataConfig.getAuditVersion() == null) {
            task.setAuditVersion(DEFAULT_AUDIT_VERSION);
        } else {
            task.setAuditVersion(dataConfig.getAuditVersion());
        }
        // set sink type
        if (dataConfig.getDataReportType() == NORMAL_SEND_TO_DATAPROXY.ordinal()) {
            task.setSink(DEFAULT_DATA_PROXY_SINK);
            task.setProxySend(false);
        } else if (dataConfig.getDataReportType() == 1) {
            task.setSink(DEFAULT_DATA_PROXY_SINK);
            task.setProxySend(true);
        } else {
            String mqType = dataConfig.getMqClusters().get(0).getMqType();
            task.setMqClusters(GSON.toJson(dataConfig.getMqClusters()));
            task.setTopicInfo(GSON.toJson(dataConfig.getTopicInfo()));
            if (mqType.equals(MQType.PULSAR)) {
                task.setSink(PULSAR_SINK);
            } else if (mqType.equals(MQType.KAFKA)) {
                task.setSink(KAFKA_SINK);
            } else {
                throw new IllegalArgumentException("invalid mq type " + mqType + " please check");
            }
        }
        TaskTypeEnum taskType = TaskTypeEnum.getTaskType(dataConfig.getTaskType());
        switch (requireNonNull(taskType)) {
            case SQL:
            case BINLOG:
                BinlogTask binlogTask = getBinlogTask(dataConfig);
                task.setBinlogTask(binlogTask);
                task.setSource(BINLOG_SOURCE);
                profileDto.setTask(task);
                break;
            case FILE:
                task.setTaskClass(DEFAULT_FILE_TASK);
                FileTask fileTask = getFileTask(dataConfig);
                task.setCycleUnit(fileTask.getCycleUnit());
                task.setFileTask(fileTask);
                task.setSource(DEFAULT_SOURCE);
                profileDto.setTask(task);
                break;
            case KAFKA:
                task.setTaskClass(DEFAULT_KAFKA_TASK);
                KafkaTask kafkaTask = getKafkaTask(dataConfig);
                task.setKafkaTask(kafkaTask);
                task.setSource(KAFKA_SOURCE);
                profileDto.setTask(task);
                break;
            case PULSAR:
                task.setTaskClass(DEFAULT_PULSAR_TASK);
                PulsarTask pulsarTask = getPulsarTask(dataConfig);
                task.setPulsarTask(pulsarTask);
                task.setSource(PULSAR_SOURCE);
                profileDto.setTask(task);
                break;
            case POSTGRES:
                task.setTaskClass(DEFAULT_POSTGRESQL_TASK);
                PostgreSQLTask postgreSQLTask = getPostgresTask(dataConfig);
                task.setPostgreSQLTask(postgreSQLTask);
                task.setSource(POSTGRESQL_SOURCE);
                profileDto.setTask(task);
                break;
            case ORACLE:
                task.setTaskClass(DEFAULT_ORACLE_TASK);
                OracleTask oracleTask = getOracleTask(dataConfig);
                task.setOracleTask(oracleTask);
                task.setSource(ORACLE_SOURCE);
                profileDto.setTask(task);
                break;
            case SQLSERVER:
                task.setTaskClass(DEFAULT_SQLSERVER_TASK);
                SqlServerTask sqlserverTask = getSqlServerTask(dataConfig);
                task.setSqlserverTask(sqlserverTask);
                task.setSource(SQLSERVER_SOURCE);
                profileDto.setTask(task);
                break;
            case MONGODB:
                task.setTaskClass(DEFAULT_MONGODB_TASK);
                MongoTask mongoTask = getMongoTask(dataConfig);
                task.setMongoTask(mongoTask);
                task.setSource(MONGO_SOURCE);
                profileDto.setTask(task);
                break;
            case REDIS:
                RedisTask redisTask = getRedisTask(dataConfig);
                task.setRedisTask(redisTask);
                task.setSource(REDIS_SOURCE);
                profileDto.setTask(task);
                break;
            case MQTT:
                task.setTaskClass(DEFAULT_MQTT_TASK);
                MqttTask mqttTask = getMqttTask(dataConfig);
                task.setMqttTask(mqttTask);
                task.setSource(MQTT_SOURCE);
                profileDto.setTask(task);
                break;
            case MOCK:
                profileDto.setTask(task);
                break;
            default:
        }
        return TaskProfile.parseJsonStr(GSON.toJson(profileDto));
    }

    @Data
    public static class Task {

        private String id;
        private String groupId;
        private String streamId;
        private String ip;
        private String source;
        private String sink;
        private String channel;
        private String name;
        private String op;
        private String retryTime;
        private String deliveryTime;
        private String uuid;
        private Integer version;
        private boolean proxySend;
        private String mqClusters;
        private String topicInfo;
        private String taskClass;
        private String predefinedFields;
        private Integer state;
        private String cycleUnit;
        private String timeZone;
        private String auditVersion;

        private FileTask fileTask;
        private BinlogTask binlogTask;
        private KafkaTask kafkaTask;
        private PulsarTask pulsarTask;
        private PostgreSQLTask postgreSQLTask;
        private OracleTask oracleTask;
        private MongoTask mongoTask;
        private RedisTask redisTask;
        private MqttTask mqttTask;
        private SqlServerTask sqlserverTask;
    }

    @Data
    public static class Manager {

        private String addr;
    }

    @Data
    public static class Proxy {

        private String inlongGroupId;
        private String inlongStreamId;
        private Manager manager;
        private Boolean sync;
        private String partitionKey;
    }

}