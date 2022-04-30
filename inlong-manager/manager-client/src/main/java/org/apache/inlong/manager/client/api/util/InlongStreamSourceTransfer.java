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

package org.apache.inlong.manager.client.api.util;

import com.google.common.base.Joiner;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.inlong.manager.client.api.auth.DefaultAuthentication;
import org.apache.inlong.manager.client.api.source.AgentFileSource;
import org.apache.inlong.manager.client.api.source.AutoPushSource;
import org.apache.inlong.manager.client.api.source.KafkaSource;
import org.apache.inlong.manager.client.api.source.MySQLBinlogSource;
import org.apache.inlong.manager.common.enums.DataFormat;
import org.apache.inlong.manager.common.enums.SourceType;
import org.apache.inlong.manager.common.pojo.source.SourceRequest;
import org.apache.inlong.manager.common.pojo.source.SourceResponse;
import org.apache.inlong.manager.common.pojo.source.autopush.AutoPushSourceRequest;
import org.apache.inlong.manager.common.pojo.source.autopush.AutoPushSourceResponse;
import org.apache.inlong.manager.common.pojo.source.binlog.BinlogSourceRequest;
import org.apache.inlong.manager.common.pojo.source.binlog.BinlogSourceResponse;
import org.apache.inlong.manager.common.pojo.source.file.FileSourceRequest;
import org.apache.inlong.manager.common.pojo.source.file.FileSourceResponse;
import org.apache.inlong.manager.common.pojo.source.kafka.KafkaSourceRequest;
import org.apache.inlong.manager.common.pojo.source.kafka.KafkaSourceResponse;
import org.apache.inlong.manager.common.pojo.stream.InlongStreamInfo;
import org.apache.inlong.manager.common.pojo.stream.StreamSource;
import org.apache.inlong.manager.common.pojo.stream.StreamSource.State;
import org.apache.inlong.manager.common.pojo.stream.StreamSource.SyncType;

import java.util.Arrays;

/**
 * Transfer the inlong stream source.
 */
public class InlongStreamSourceTransfer {

    public static SourceRequest createSourceRequest(StreamSource streamSource, InlongStreamInfo streamInfo) {
        SourceType sourceType = streamSource.getSourceType();
        switch (sourceType) {
            case KAFKA:
                return createKafkaSourceRequest((KafkaSource) streamSource, streamInfo);
            case BINLOG:
                return createBinlogSourceRequest((MySQLBinlogSource) streamSource, streamInfo);
            case FILE:
                return createFileSourceRequest((AgentFileSource) streamSource, streamInfo);
            case AUTO_PUSH:
                return createAutoPushSourceRequest((AutoPushSource) streamSource, streamInfo);
            default:
                throw new RuntimeException(String.format("Unsupported source=%s for Inlong", sourceType));
        }
    }

    public static StreamSource parseStreamSource(SourceResponse sourceResponse) {
        String type = sourceResponse.getSourceType();
        SourceType sourceType = SourceType.forType(type);
        if (sourceType == SourceType.KAFKA && sourceResponse instanceof KafkaSourceResponse) {
            return parseKafkaSource((KafkaSourceResponse) sourceResponse);
        }
        if (sourceType == SourceType.BINLOG && sourceResponse instanceof BinlogSourceResponse) {
            return parseMySQLBinlogSource((BinlogSourceResponse) sourceResponse);
        }
        if (sourceType == SourceType.FILE && sourceResponse instanceof FileSourceResponse) {
            return parseAgentFileSource((FileSourceResponse) sourceResponse);
        }
        if (sourceType == SourceType.AUTO_PUSH && sourceResponse instanceof AutoPushSourceResponse) {
            return parseAutoPushSource((AutoPushSourceResponse) sourceResponse);
        }
        throw new IllegalArgumentException(String.format("Unsupported source type : %s for Inlong", sourceType));
    }

    private static KafkaSource parseKafkaSource(KafkaSourceResponse response) {
        KafkaSource kafkaSource = new KafkaSource();
        kafkaSource.setSourceName(response.getSourceName());
        kafkaSource.setConsumerGroup(response.getGroupId());
        DataFormat dataFormat = DataFormat.forName(response.getSerializationType());
        kafkaSource.setDataFormat(dataFormat);
        kafkaSource.setState(State.parseByStatus(response.getStatus()));
        kafkaSource.setAgentIp(response.getAgentIp());
        kafkaSource.setTopic(response.getTopic());
        kafkaSource.setBootstrapServers(response.getBootstrapServers());
        kafkaSource.setByteSpeedLimit(response.getByteSpeedLimit());
        kafkaSource.setTopicPartitionOffset(response.getTopicPartitionOffset());
        kafkaSource.setRecordSpeedLimit(response.getRecordSpeedLimit());
        kafkaSource.setSyncType(SyncType.FULL);
        kafkaSource.setDatabasePattern(response.getDatabasePattern());
        kafkaSource.setTablePattern(response.getTablePattern());
        kafkaSource.setIgnoreParseErrors(response.isIgnoreParseErrors());
        kafkaSource.setTimestampFormatStandard(response.getTimestampFormatStandard());
        kafkaSource.setFields(InlongStreamTransfer.parseStreamFields(response.getFieldList()));
        return kafkaSource;
    }

    private static MySQLBinlogSource parseMySQLBinlogSource(BinlogSourceResponse response) {
        MySQLBinlogSource binlogSource = new MySQLBinlogSource();
        binlogSource.setSourceName(response.getSourceName());
        binlogSource.setHostname(response.getHostname());
        binlogSource.setDataFormat(DataFormat.NONE);
        binlogSource.setPort(response.getPort());
        binlogSource.setAgentIp(response.getAgentIp());
        binlogSource.setState(State.parseByStatus(response.getStatus()));
        binlogSource.setServerId(response.getServerId());
        DefaultAuthentication defaultAuthentication = new DefaultAuthentication(
                response.getUser(),
                response.getPassword());
        binlogSource.setAuthentication(defaultAuthentication);
        binlogSource.setIncludeSchema(response.getIncludeSchema());
        binlogSource.setServerTimezone(response.getServerTimezone());
        binlogSource.setMonitoredDdl(response.getMonitoredDdl());
        binlogSource.setTimestampFormatStandard(response.getTimestampFormatStandard());
        binlogSource.setAllMigration(response.isAllMigration());
        binlogSource.setPrimaryKey(response.getPrimaryKey());
        if (StringUtils.isNotBlank(response.getDatabaseWhiteList())) {
            binlogSource.setDbNames(Arrays.asList(response.getDatabaseWhiteList().split(",")));
        }
        if (StringUtils.isNotBlank(response.getTableWhiteList())) {
            binlogSource.setTableNames(Arrays.asList(response.getTableWhiteList().split(",")));
        }
        binlogSource.setFields(InlongStreamTransfer.parseStreamFields(response.getFieldList()));
        return binlogSource;
    }

    private static AgentFileSource parseAgentFileSource(FileSourceResponse response) {
        AgentFileSource fileSource = new AgentFileSource();
        fileSource.setSourceName(response.getSourceName());
        fileSource.setState(State.parseByStatus(response.getStatus()));
        fileSource.setDataFormat(DataFormat.NONE);
        fileSource.setPattern(response.getPattern());
        fileSource.setIp(response.getIp());
        fileSource.setTimeOffset(response.getTimeOffset());
        fileSource.setFields(InlongStreamTransfer.parseStreamFields(response.getFieldList()));
        return fileSource;
    }

    private static AutoPushSource parseAutoPushSource(AutoPushSourceResponse response) {
        AutoPushSource autoPushSource = new AutoPushSource();
        autoPushSource.setSourceName(response.getSourceName());
        autoPushSource.setState(State.parseByStatus(response.getStatus()));
        autoPushSource.setDataFormat(DataFormat.NONE);
        autoPushSource.setDataProxyGroup(response.getDataProxyGroup());
        autoPushSource.setFields(InlongStreamTransfer.parseStreamFields(response.getFieldList()));
        return autoPushSource;
    }

    private static KafkaSourceRequest createKafkaSourceRequest(KafkaSource kafkaSource, InlongStreamInfo streamInfo) {
        KafkaSourceRequest sourceRequest = new KafkaSourceRequest();
        sourceRequest.setSourceName(kafkaSource.getSourceName());
        sourceRequest.setInlongGroupId(streamInfo.getInlongGroupId());
        sourceRequest.setInlongStreamId(streamInfo.getInlongStreamId());
        sourceRequest.setSourceType(kafkaSource.getSourceType().getType());
        sourceRequest.setBootstrapServers(kafkaSource.getBootstrapServers());
        sourceRequest.setTopic(kafkaSource.getTopic());
        sourceRequest.setRecordSpeedLimit(kafkaSource.getRecordSpeedLimit());
        sourceRequest.setByteSpeedLimit(kafkaSource.getByteSpeedLimit());
        sourceRequest.setTopicPartitionOffset(kafkaSource.getTopicPartitionOffset());
        sourceRequest.setAutoOffsetReset(kafkaSource.getAutoOffsetReset().getName());
        sourceRequest.setGroupId(kafkaSource.getConsumerGroup());
        sourceRequest.setSerializationType(kafkaSource.getDataFormat().getName());
        sourceRequest.setDatabasePattern(kafkaSource.getDatabasePattern());
        sourceRequest.setTablePattern(kafkaSource.getTablePattern());
        sourceRequest.setIgnoreParseErrors(kafkaSource.isIgnoreParseErrors());
        sourceRequest.setTimestampFormatStandard(kafkaSource.getTimestampFormatStandard());
        sourceRequest.setFieldList(InlongStreamTransfer.createStreamFields(kafkaSource.getFields(), streamInfo));
        return sourceRequest;
    }

    private static BinlogSourceRequest createBinlogSourceRequest(MySQLBinlogSource binlogSource,
            InlongStreamInfo streamInfo) {
        BinlogSourceRequest sourceRequest = new BinlogSourceRequest();
        sourceRequest.setSourceName(binlogSource.getSourceName());
        sourceRequest.setInlongGroupId(streamInfo.getInlongGroupId());
        sourceRequest.setInlongStreamId(streamInfo.getInlongStreamId());
        sourceRequest.setSourceType(binlogSource.getSourceType().getType());
        DefaultAuthentication authentication = binlogSource.getAuthentication();
        sourceRequest.setUser(authentication.getUserName());
        sourceRequest.setPassword(authentication.getPassword());
        sourceRequest.setHostname(binlogSource.getHostname());
        sourceRequest.setPort(binlogSource.getPort());
        sourceRequest.setServerId(binlogSource.getServerId());
        sourceRequest.setIncludeSchema(binlogSource.getIncludeSchema());
        sourceRequest.setServerTimezone(binlogSource.getServerTimezone());
        sourceRequest.setMonitoredDdl(binlogSource.getMonitoredDdl());
        sourceRequest.setAllMigration(binlogSource.isAllMigration());
        sourceRequest.setPrimaryKey(binlogSource.getPrimaryKey());
        if (CollectionUtils.isNotEmpty(binlogSource.getDbNames())) {
            String dbNames = Joiner.on(",").join(binlogSource.getDbNames());
            sourceRequest.setDatabaseWhiteList(dbNames);
        }
        if (CollectionUtils.isNotEmpty(binlogSource.getTableNames())) {
            String tableNames = Joiner.on(",").join(binlogSource.getTableNames());
            sourceRequest.setTableWhiteList(tableNames);
        }
        sourceRequest.setSnapshotMode("initial");
        sourceRequest.setIntervalMs("500");
        sourceRequest.setTimestampFormatStandard(binlogSource.getTimestampFormatStandard());
        sourceRequest.setFieldList(InlongStreamTransfer.createStreamFields(binlogSource.getFields(), streamInfo));
        return sourceRequest;
    }

    private static FileSourceRequest createFileSourceRequest(AgentFileSource fileSource, InlongStreamInfo streamInfo) {
        FileSourceRequest sourceRequest = new FileSourceRequest();
        sourceRequest.setSourceName(fileSource.getSourceName());
        sourceRequest.setInlongGroupId(streamInfo.getInlongGroupId());
        sourceRequest.setInlongStreamId(streamInfo.getInlongStreamId());
        sourceRequest.setSourceType(fileSource.getSourceType().getType());
        if (StringUtils.isEmpty(fileSource.getIp())) {
            throw new IllegalArgumentException(
                    String.format("AgentIp should not be null for fileSource=%s", fileSource));
        }
        sourceRequest.setIp(fileSource.getIp());
        if (StringUtils.isEmpty(fileSource.getPattern())) {
            throw new IllegalArgumentException(
                    String.format("SourcePattern should not be null for fileSource=%s", fileSource));
        }
        sourceRequest.setPattern(fileSource.getPattern());
        sourceRequest.setTimeOffset(fileSource.getTimeOffset());
        sourceRequest.setFieldList(InlongStreamTransfer.createStreamFields(fileSource.getFields(), streamInfo));
        return sourceRequest;
    }

    private static AutoPushSourceRequest createAutoPushSourceRequest(AutoPushSource source,
            InlongStreamInfo streamInfo) {
        AutoPushSourceRequest sourceRequest = new AutoPushSourceRequest();
        sourceRequest.setSourceName(source.getSourceName());
        if (StringUtils.isEmpty(sourceRequest.getSourceName())) {
            sourceRequest.setSourceName(streamInfo.getName());
        }
        sourceRequest.setInlongGroupId(streamInfo.getInlongGroupId());
        sourceRequest.setInlongStreamId(streamInfo.getInlongStreamId());
        sourceRequest.setSourceType(source.getSourceType().getType());
        sourceRequest.setDataProxyGroup(source.getDataProxyGroup());
        sourceRequest.setFieldList(InlongStreamTransfer.createStreamFields(source.getFields(), streamInfo));
        return sourceRequest;
    }
}
