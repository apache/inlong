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

package org.apache.inlong.manager.service.sort.util;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.inlong.common.enums.DataTypeEnum;
import org.apache.inlong.manager.common.enums.SourceType;
import org.apache.inlong.manager.common.pojo.source.SourceResponse;
import org.apache.inlong.manager.common.pojo.source.binlog.BinlogSourceResponse;
import org.apache.inlong.manager.common.pojo.source.kafka.KafkaOffset;
import org.apache.inlong.manager.common.pojo.source.kafka.KafkaSourceResponse;
import org.apache.inlong.manager.common.pojo.stream.InlongStreamFieldInfo;
import org.apache.inlong.sort.protocol.FieldInfo;
import org.apache.inlong.sort.protocol.enums.ScanStartupMode;
import org.apache.inlong.sort.protocol.node.ExtractNode;
import org.apache.inlong.sort.protocol.node.extract.KafkaExtractNode;
import org.apache.inlong.sort.protocol.node.extract.MySqlExtractNode;
import org.apache.inlong.sort.protocol.node.format.AvroFormat;
import org.apache.inlong.sort.protocol.node.format.CanalJsonFormat;
import org.apache.inlong.sort.protocol.node.format.CsvFormat;
import org.apache.inlong.sort.protocol.node.format.DebeziumJsonFormat;
import org.apache.inlong.sort.protocol.node.format.Format;
import org.apache.inlong.sort.protocol.node.format.JsonFormat;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Parse SourceResponse to ExtractNode which sort needed
 */
@Slf4j
public class ExtractNodeUtils {

    public static List<ExtractNode> createExtractNodes(List<SourceResponse> sourceResponses) {
        if (CollectionUtils.isEmpty(sourceResponses)) {
            return Lists.newArrayList();
        }
        return sourceResponses.stream().map(sourceResponse -> createExtractNode(sourceResponse))
                .collect(Collectors.toList());
    }

    public static ExtractNode createExtractNode(SourceResponse sourceResponse) {
        SourceType sourceType = SourceType.forType(sourceResponse.getSourceType());
        switch (sourceType) {
            case BINLOG:
                return createExtractNode((BinlogSourceResponse) sourceResponse);
            case KAFKA:
                return createExtractNode((KafkaSourceResponse) sourceResponse);
            default:
                throw new IllegalArgumentException(
                        String.format("Unsupported sourceType=%s to create extractNode", sourceType));
        }
    }

    /**
     * Create MySqlExtractNode based on BinlogSourceResponse
     *
     * @param binlogSourceResponse
     * @return
     */
    public static MySqlExtractNode createExtractNode(BinlogSourceResponse binlogSourceResponse) {
        String id = binlogSourceResponse.getSourceName();
        String name = binlogSourceResponse.getSourceName();
        String database = binlogSourceResponse.getDatabaseWhiteList();
        String primaryKey = binlogSourceResponse.getPrimaryKey();
        String hostName = binlogSourceResponse.getHostname();
        String userName = binlogSourceResponse.getUser();
        String password = binlogSourceResponse.getPassword();
        Integer port = binlogSourceResponse.getPort();
        Integer serverId = null;
        if (binlogSourceResponse.getServerId() != null && binlogSourceResponse.getServerId() > 0) {
            serverId = binlogSourceResponse.getServerId();
        }
        String tables = binlogSourceResponse.getTableWhiteList();
        List<String> tableNames = Splitter.on(",").splitToList(tables);
        List<InlongStreamFieldInfo> streamFieldInfos = binlogSourceResponse.getFieldList();
        List<FieldInfo> fieldInfos = streamFieldInfos.stream()
                .map(streamFieldInfo -> FieldInfoUtils.parseStreamFieldInfo(streamFieldInfo, name))
                .collect(Collectors.toList());
        String serverTimeZone = binlogSourceResponse.getServerTimezone();
        boolean incrementalSnapshotEnabled = true;
        if (binlogSourceResponse.isAllMigration()) {
            incrementalSnapshotEnabled = false;
        }
        return new MySqlExtractNode(id,
                name,
                fieldInfos,
                null,
                Maps.newHashMap(),
                primaryKey,
                tableNames,
                hostName,
                userName,
                password,
                database,
                port,
                serverId,
                incrementalSnapshotEnabled,
                serverTimeZone);
    }

    /**
     * Create KafkaExtractNode based KafkaSourceResponse
     *
     * @param kafkaSourceResponse
     * @return
     */
    public static KafkaExtractNode createExtractNode(KafkaSourceResponse kafkaSourceResponse) {
        String id = kafkaSourceResponse.getSourceName();
        String name = kafkaSourceResponse.getSourceName();
        List<InlongStreamFieldInfo> streamFieldInfos = kafkaSourceResponse.getFieldList();
        List<FieldInfo> fieldInfos = streamFieldInfos.stream()
                .map(streamFieldInfo -> FieldInfoUtils.parseStreamFieldInfo(streamFieldInfo, name))
                .collect(Collectors.toList());
        String topic = kafkaSourceResponse.getTopic();
        String bootstrapServers = kafkaSourceResponse.getBootstrapServers();
        Format format = null;
        DataTypeEnum dataType = DataTypeEnum.forName(kafkaSourceResponse.getSerializationType());
        switch (dataType) {
            case CSV:
                format = new CsvFormat();
                break;
            case AVRO:
                format = new AvroFormat();
                break;
            case JSON:
                format = new JsonFormat();
                break;
            case CANAL:
                format = new CanalJsonFormat();
                break;
            case DEBEZIUM_JSON:
                format = new DebeziumJsonFormat();
                break;
            default:
                throw new IllegalArgumentException(String.format("Unsupported dataType=%s for kafka source", dataType));
        }
        KafkaOffset kafkaOffset = KafkaOffset.forName(kafkaSourceResponse.getAutoOffsetReset());
        ScanStartupMode startupMode = null;
        switch (kafkaOffset) {
            case EARLIEST:
                startupMode = ScanStartupMode.EARLIEST_OFFSET;
                break;
            case LATEST:
            default:
                startupMode = ScanStartupMode.LATEST_OFFSET;
        }

        return new KafkaExtractNode(id,
                name,
                fieldInfos,
                null,
                Maps.newHashMap(),
                topic,
                bootstrapServers,
                format,
                startupMode,
                null);
    }
}
