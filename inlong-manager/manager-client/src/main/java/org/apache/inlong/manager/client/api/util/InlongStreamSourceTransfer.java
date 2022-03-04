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
import com.google.common.base.Splitter;
import java.util.List;
import org.apache.inlong.manager.client.api.DataFormat;
import org.apache.inlong.manager.client.api.StreamSource;
import org.apache.inlong.manager.client.api.StreamSource.SyncType;
import org.apache.inlong.manager.client.api.auth.DefaultAuthentication;
import org.apache.inlong.manager.client.api.source.KafkaSource;
import org.apache.inlong.manager.client.api.source.MySQLBinlogSource;
import org.apache.inlong.manager.common.enums.SourceType;
import org.apache.inlong.manager.common.pojo.source.SourceListResponse;
import org.apache.inlong.manager.common.pojo.source.SourceRequest;
import org.apache.inlong.manager.common.pojo.source.binlog.BinlogSourceListResponse;
import org.apache.inlong.manager.common.pojo.source.binlog.BinlogSourceRequest;
import org.apache.inlong.manager.common.pojo.source.kafka.KafkaSourceListResponse;
import org.apache.inlong.manager.common.pojo.source.kafka.KafkaSourceRequest;
import org.apache.inlong.manager.common.pojo.stream.InlongStreamInfo;

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

    private static MySQLBinlogSource parseMySQLBinlogSource(BinlogSourceListResponse binlogSourceResponse) {
        MySQLBinlogSource binlogSource = new MySQLBinlogSource();
        binlogSource.setSourceName(binlogSourceResponse.getSourceName());
        binlogSource.setHostname(binlogSourceResponse.getHostname());
        binlogSource.setDataFormat(DataFormat.NONE);
        binlogSource.setPort(binlogSourceResponse.getPort());
        DefaultAuthentication defaultAuthentication = new DefaultAuthentication(
                binlogSourceResponse.getUser(),
                binlogSourceResponse.getPassword());
        binlogSource.setAuthentication(defaultAuthentication);
        binlogSource.setMigrationTransfer(binlogSourceResponse.isMigrationTransfer());
        binlogSource.setTimeZone(binlogSourceResponse.getTimeZone());
        binlogSource.setTimestampFormatStandard(binlogSourceResponse.getTimestampFormatStandard());
        List<String> dbs = Splitter.on(",").splitToList(binlogSourceResponse.getWhitelist());
        binlogSource.setDbNames(dbs);
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
        BinlogSourceRequest binlogSourceRequest = new BinlogSourceRequest();
        binlogSourceRequest.setSourceName(binlogSource.getSourceName());
        binlogSourceRequest.setInlongGroupId(streamInfo.getInlongGroupId());
        binlogSourceRequest.setInlongStreamId(streamInfo.getInlongStreamId());
        binlogSourceRequest.setSourceType(binlogSource.getSourceType().name());
        DefaultAuthentication authentication = binlogSource.getAuthentication();
        binlogSourceRequest.setUser(authentication.getUserName());
        binlogSourceRequest.setPassword(authentication.getPassword());
        binlogSourceRequest.setHostname(binlogSource.getHostname());
        binlogSourceRequest.setPort(binlogSource.getPort());
        binlogSourceRequest.setMigrationTransfer(binlogSource.isMigrationTransfer());
        String dbNames = Joiner.on(",").join(binlogSource.getDbNames());
        binlogSourceRequest.setWhitelist(dbNames);
        binlogSourceRequest.setTimestampFormatStandard(binlogSource.getTimestampFormatStandard());
        binlogSourceRequest.setTimeZone(binlogSource.getTimeZone());
        binlogSourceRequest.setSnapshotMode("initial");
        binlogSourceRequest.setIntervalMs("500");
        return binlogSourceRequest;
    }
}
