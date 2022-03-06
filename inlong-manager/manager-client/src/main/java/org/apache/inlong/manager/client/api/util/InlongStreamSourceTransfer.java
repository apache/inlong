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
import org.apache.commons.lang3.StringUtils;
import org.apache.inlong.manager.client.api.DataFormat;
import org.apache.inlong.manager.client.api.StreamSource;
import org.apache.inlong.manager.client.api.StreamSource.SyncType;
import org.apache.inlong.manager.client.api.auth.DefaultAuthentication;
import org.apache.inlong.manager.client.api.source.KafkaSource;
import org.apache.inlong.manager.client.api.source.MySQLBinlogSource;
import org.apache.inlong.manager.common.enums.SourceType;
import org.apache.inlong.manager.common.pojo.source.SourceListResponse;
import org.apache.inlong.manager.common.pojo.source.SourceRequest;
import org.apache.inlong.manager.common.pojo.source.SourceResponse;
import org.apache.inlong.manager.common.pojo.source.binlog.BinlogSourceListResponse;
import org.apache.inlong.manager.common.pojo.source.binlog.BinlogSourceRequest;
import org.apache.inlong.manager.common.pojo.source.binlog.BinlogSourceResponse;
import org.apache.inlong.manager.common.pojo.source.kafka.KafkaSourceListResponse;
import org.apache.inlong.manager.common.pojo.source.kafka.KafkaSourceRequest;
import org.apache.inlong.manager.common.pojo.source.kafka.KafkaSourceResponse;
import org.apache.inlong.manager.common.pojo.stream.InlongStreamInfo;
import org.springframework.util.CollectionUtils;

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
            default:
                throw new RuntimeException(String.format("Unsupport source=%s for Inlong", sourceType));
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
        throw new IllegalArgumentException(String.format("Unsupport source type : %s for Inlong", sourceType));
    }

    public static StreamSource parseStreamSource(SourceListResponse sourceListResponse) {
        String type = sourceListResponse.getSourceType();
        SourceType sourceType = SourceType.forType(type);
        if (sourceType == SourceType.KAFKA && sourceListResponse instanceof KafkaSourceListResponse) {
            return parseKafkaSource((KafkaSourceListResponse) sourceListResponse);
        }
        if (sourceType == SourceType.BINLOG && sourceListResponse instanceof BinlogSourceListResponse) {
            return parseMySQLBinlogSource((BinlogSourceListResponse) sourceListResponse);
        }
        throw new IllegalArgumentException(String.format("Unsupport source type : %s for Inlong", sourceType));
    }

    private static KafkaSource parseKafkaSource(KafkaSourceResponse kafkaSourceResponse) {
        KafkaSource kafkaSource = new KafkaSource();
        kafkaSource.setSourceName(kafkaSourceResponse.getSourceName());
        kafkaSource.setConsumerGroup(kafkaSourceResponse.getGroupId());
        DataFormat dataFormat = DataFormat.forName(kafkaSourceResponse.getSerializationType());
        kafkaSource.setDataFormat(dataFormat);
        kafkaSource.setTopic(kafkaSourceResponse.getTopic());
        kafkaSource.setBootstrapServers(kafkaSourceResponse.getBootstrapServers());
        kafkaSource.setByteSpeedLimit(kafkaSourceResponse.getByteSpeedLimit());
        kafkaSource.setTopicPartitionOffset(kafkaSourceResponse.getTopicPartitionOffset());
        kafkaSource.setRecordSpeedLimit(kafkaSourceResponse.getRecordSpeedLimit());
        kafkaSource.setSyncType(SyncType.FULL);
        return kafkaSource;
    }

    private static KafkaSource parseKafkaSource(KafkaSourceListResponse kafkaSourceResponse) {
        KafkaSource kafkaSource = new KafkaSource();
        kafkaSource.setSourceName(kafkaSourceResponse.getSourceName());
        kafkaSource.setConsumerGroup(kafkaSourceResponse.getGroupId());
        DataFormat dataFormat = DataFormat.forName(kafkaSourceResponse.getSerializationType());
        kafkaSource.setDataFormat(dataFormat);
        kafkaSource.setTopic(kafkaSourceResponse.getTopic());
        kafkaSource.setBootstrapServers(kafkaSourceResponse.getBootstrapServers());
        kafkaSource.setByteSpeedLimit(kafkaSourceResponse.getByteSpeedLimit());
        kafkaSource.setTopicPartitionOffset(kafkaSourceResponse.getTopicPartitionOffset());
        kafkaSource.setRecordSpeedLimit(kafkaSourceResponse.getRecordSpeedLimit());
        kafkaSource.setSyncType(SyncType.FULL);
        return kafkaSource;
    }

    private static MySQLBinlogSource parseMySQLBinlogSource(BinlogSourceResponse response) {
        MySQLBinlogSource binlogSource = new MySQLBinlogSource();
        binlogSource.setSourceName(response.getSourceName());
        binlogSource.setHostname(response.getHostname());
        binlogSource.setDataFormat(DataFormat.NONE);
        binlogSource.setPort(response.getPort());
        DefaultAuthentication defaultAuthentication = new DefaultAuthentication(
                response.getUser(),
                response.getPassword());
        binlogSource.setAuthentication(defaultAuthentication);
        binlogSource.setIncludeSchema(response.getIncludeSchema());
        binlogSource.setServerTimezone(response.getServerTimezone());
        binlogSource.setMonitoredDdl(response.getMonitoredDdl());
        binlogSource.setTimestampFormatStandard(response.getTimestampFormatStandard());
        binlogSource.setAllMigration(response.isAllMigration());

        if (StringUtils.isNotBlank(response.getDatabaseWhiteList())) {
            binlogSource.setDbNames(Arrays.asList(response.getDatabaseWhiteList().split(",")));
        }
        if (StringUtils.isNotBlank(response.getTableWhiteList())) {
            binlogSource.setTableNames(Arrays.asList(response.getTableWhiteList().split(",")));
        }
        return binlogSource;
    }

    private static MySQLBinlogSource parseMySQLBinlogSource(BinlogSourceListResponse response) {
        MySQLBinlogSource binlogSource = new MySQLBinlogSource();
        binlogSource.setSourceName(response.getSourceName());
        binlogSource.setHostname(response.getHostname());
        binlogSource.setDataFormat(DataFormat.NONE);
        binlogSource.setPort(response.getPort());
        DefaultAuthentication defaultAuthentication = new DefaultAuthentication(
                response.getUser(),
                response.getPassword());
        binlogSource.setAuthentication(defaultAuthentication);
        binlogSource.setIncludeSchema(response.getIncludeSchema());
        binlogSource.setServerTimezone(response.getServerTimezone());
        binlogSource.setMonitoredDdl(response.getMonitoredDdl());
        binlogSource.setTimestampFormatStandard(response.getTimestampFormatStandard());
        binlogSource.setAllMigration(response.isAllMigration());

        if (StringUtils.isNotBlank(response.getDatabaseWhiteList())) {
            binlogSource.setDbNames(Arrays.asList(response.getDatabaseWhiteList().split(",")));
        }
        if (StringUtils.isNotBlank(response.getTableWhiteList())) {
            binlogSource.setTableNames(Arrays.asList(response.getTableWhiteList().split(",")));
        }
        return binlogSource;
    }

    private static KafkaSourceRequest createKafkaSourceRequest(KafkaSource kafkaSource, InlongStreamInfo streamInfo) {
        KafkaSourceRequest sourceRequest = new KafkaSourceRequest();
        sourceRequest.setSourceName(kafkaSource.getSourceName());
        sourceRequest.setInlongGroupId(streamInfo.getInlongGroupId());
        sourceRequest.setInlongStreamId(streamInfo.getInlongStreamId());
        sourceRequest.setSourceType(kafkaSource.getSourceType().name());
        sourceRequest.setBootstrapServers(kafkaSource.getBootstrapServers());
        sourceRequest.setTopic(kafkaSource.getTopic());
        sourceRequest.setRecordSpeedLimit(kafkaSource.getRecordSpeedLimit());
        sourceRequest.setByteSpeedLimit(kafkaSource.getByteSpeedLimit());
        sourceRequest.setTopicPartitionOffset(kafkaSource.getTopicPartitionOffset());
        sourceRequest.setGroupId(kafkaSource.getConsumerGroup());
        sourceRequest.setSerializationType(kafkaSource.getDataFormat().getName());
        return sourceRequest;
    }

    private static BinlogSourceRequest createBinlogSourceRequest(MySQLBinlogSource binlogSource,
            InlongStreamInfo streamInfo) {
        BinlogSourceRequest sourceRequest = new BinlogSourceRequest();
        sourceRequest.setSourceName(binlogSource.getSourceName());
        sourceRequest.setInlongGroupId(streamInfo.getInlongGroupId());
        sourceRequest.setInlongStreamId(streamInfo.getInlongStreamId());
        sourceRequest.setSourceType(binlogSource.getSourceType().name());
        DefaultAuthentication authentication = binlogSource.getAuthentication();
        sourceRequest.setUser(authentication.getUserName());
        sourceRequest.setPassword(authentication.getPassword());
        sourceRequest.setHostname(binlogSource.getHostname());
        sourceRequest.setPort(binlogSource.getPort());
        sourceRequest.setIncludeSchema(binlogSource.getIncludeSchema());
        sourceRequest.setServerTimezone(binlogSource.getServerTimezone());
        sourceRequest.setMonitoredDdl(binlogSource.getMonitoredDdl());
        sourceRequest.setAllMigration(binlogSource.isAllMigration());
        if (!CollectionUtils.isEmpty(binlogSource.getDbNames())) {
            String dbNames = Joiner.on(",").join(binlogSource.getDbNames());
            sourceRequest.setDatabaseWhiteList(dbNames);
        }
        if (!CollectionUtils.isEmpty(binlogSource.getTableNames())) {
            String tableNames = Joiner.on(",").join(binlogSource.getTableNames());
            sourceRequest.setTableWhiteList(tableNames);
        }
        sourceRequest.setSnapshotMode("initial");
        sourceRequest.setIntervalMs("500");
        sourceRequest.setTimestampFormatStandard(binlogSource.getTimestampFormatStandard());
        return sourceRequest;
    }
}
