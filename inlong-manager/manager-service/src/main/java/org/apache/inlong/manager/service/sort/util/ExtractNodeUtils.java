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
import org.apache.commons.lang3.StringUtils;
import org.apache.inlong.common.enums.DataTypeEnum;
import org.apache.inlong.manager.common.enums.SourceType;
import org.apache.inlong.manager.common.pojo.source.SourceResponse;
import org.apache.inlong.manager.common.pojo.source.binlog.BinlogSourceResponse;
import org.apache.inlong.manager.common.pojo.source.kafka.KafkaOffset;
import org.apache.inlong.manager.common.pojo.source.kafka.KafkaSourceResponse;
import org.apache.inlong.manager.common.pojo.source.postgres.PostgresSourceResponse;
import org.apache.inlong.manager.common.pojo.source.pulsar.PulsarSourceResponse;
import org.apache.inlong.manager.common.pojo.stream.StreamField;
import org.apache.inlong.sort.protocol.FieldInfo;
import org.apache.inlong.sort.protocol.enums.KafkaScanStartupMode;
import org.apache.inlong.sort.protocol.enums.PulsarScanStartupMode;
import org.apache.inlong.sort.protocol.node.ExtractNode;
import org.apache.inlong.sort.protocol.node.extract.KafkaExtractNode;
import org.apache.inlong.sort.protocol.node.extract.MySqlExtractNode;
import org.apache.inlong.sort.protocol.node.extract.PostgresExtractNode;
import org.apache.inlong.sort.protocol.node.extract.PulsarExtractNode;
import org.apache.inlong.sort.protocol.node.format.AvroFormat;
import org.apache.inlong.sort.protocol.node.format.CanalJsonFormat;
import org.apache.inlong.sort.protocol.node.format.CsvFormat;
import org.apache.inlong.sort.protocol.node.format.DebeziumJsonFormat;
import org.apache.inlong.sort.protocol.node.format.Format;
import org.apache.inlong.sort.protocol.node.format.InLongMsgFormat;
import org.apache.inlong.sort.protocol.node.format.JsonFormat;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Parse SourceResponse to ExtractNode which sort needed
 */
@Slf4j
public class ExtractNodeUtils {

    /**
     * Create extract nodes from the source responses.
     */
    public static List<ExtractNode> createExtractNodes(List<SourceResponse> sourceResponses) {
        if (CollectionUtils.isEmpty(sourceResponses)) {
            return Lists.newArrayList();
        }
        return sourceResponses.stream().map(ExtractNodeUtils::createExtractNode)
                .collect(Collectors.toList());
    }

    public static ExtractNode createExtractNode(SourceResponse sourceResponse) {
        SourceType sourceType = SourceType.forType(sourceResponse.getSourceType());
        switch (sourceType) {
            case BINLOG:
                return createExtractNode((BinlogSourceResponse) sourceResponse);
            case KAFKA:
                return createExtractNode((KafkaSourceResponse) sourceResponse);
            case PULSAR:
                return createExtractNode((PulsarSourceResponse) sourceResponse);
            case POSTGRES:
                return createExtractNode((PostgresSourceResponse) sourceResponse);
            default:
                throw new IllegalArgumentException(
                        String.format("Unsupported sourceType=%s to create extractNode", sourceType));
        }
    }

    /**
     * Create MySqlExtractNode based on BinlogSourceResponse
     *
     * @param sourceResponse binlog source response info
     * @return MySql extract node info
     */
    public static MySqlExtractNode createExtractNode(BinlogSourceResponse sourceResponse) {
        final String id = sourceResponse.getSourceName();
        final String name = sourceResponse.getSourceName();
        final String database = sourceResponse.getDatabaseWhiteList();
        final String primaryKey = sourceResponse.getPrimaryKey();
        final String hostName = sourceResponse.getHostname();
        final String userName = sourceResponse.getUser();
        final String password = sourceResponse.getPassword();
        final Integer port = sourceResponse.getPort();
        Integer serverId = null;
        if (sourceResponse.getServerId() != null && sourceResponse.getServerId() > 0) {
            serverId = sourceResponse.getServerId();
        }
        String tables = sourceResponse.getTableWhiteList();
        final List<String> tableNames = Splitter.on(",").splitToList(tables);
        final List<StreamField> streamFields = sourceResponse.getFieldList();
        final List<FieldInfo> fieldInfos = streamFields.stream()
                .map(streamField -> FieldInfoUtils.parseStreamFieldInfo(streamField, name))
                .collect(Collectors.toList());
        final String serverTimeZone = sourceResponse.getServerTimezone();
        boolean incrementalSnapshotEnabled = true;

        // TODO Needs to be configurable for those parameters
        Map<String, String> properties = Maps.newHashMap();
        if (sourceResponse.isAllMigration()) {
            // Unique properties when migrate all tables in database
            incrementalSnapshotEnabled = false;
            properties.put("migrate-all", "true");
        }
        properties.put("append-mode", "true");
        if (StringUtils.isEmpty(primaryKey)) {
            incrementalSnapshotEnabled = false;
            properties.put("scan.incremental.snapshot.enabled", "false");
        }
        return new MySqlExtractNode(id,
                name,
                fieldInfos,
                null,
                properties,
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
     * @param kafkaSourceResponse kafka source response
     * @return kafka extract node info
     */
    public static KafkaExtractNode createExtractNode(KafkaSourceResponse kafkaSourceResponse) {
        String id = kafkaSourceResponse.getSourceName();
        String name = kafkaSourceResponse.getSourceName();
        List<StreamField> streamFields = kafkaSourceResponse.getFieldList();
        List<FieldInfo> fieldInfos = streamFields.stream()
                .map(streamFieldInfo -> FieldInfoUtils.parseStreamFieldInfo(streamFieldInfo, name))
                .collect(Collectors.toList());
        String topic = kafkaSourceResponse.getTopic();
        String bootstrapServers = kafkaSourceResponse.getBootstrapServers();
        Format format;
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
        KafkaScanStartupMode startupMode = null;
        switch (kafkaOffset) {
            case EARLIEST:
                startupMode = KafkaScanStartupMode.EARLIEST_OFFSET;
                break;
            case LATEST:
            default:
                startupMode = KafkaScanStartupMode.LATEST_OFFSET;
        }
        final String primaryKey = kafkaSourceResponse.getPrimaryKey();
        String groupId = kafkaSourceResponse.getGroupId();

        return new KafkaExtractNode(id,
                name,
                fieldInfos,
                null,
                Maps.newHashMap(),
                topic,
                bootstrapServers,
                format,
                startupMode,
                primaryKey,
                groupId);
    }

    /**
     * Create PulsarExtractNode based PulsarSourceResponse
     *
     * @param pulsarSourceResponse pulsar source response
     * @return pulsar extract node info
     */
    public static PulsarExtractNode createExtractNode(PulsarSourceResponse pulsarSourceResponse) {
        String id = pulsarSourceResponse.getSourceName();
        String name = pulsarSourceResponse.getSourceName();
        List<StreamField> streamFields = pulsarSourceResponse.getFieldList();
        List<FieldInfo> fieldInfos = streamFields.stream()
                .map(streamFieldInfo -> FieldInfoUtils.parseStreamFieldInfo(streamFieldInfo, name))
                .collect(Collectors.toList());
        String topic = pulsarSourceResponse.getTopic();

        Format format = null;
        DataTypeEnum dataType = DataTypeEnum.forName(pulsarSourceResponse.getSerializationType());
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
        if (pulsarSourceResponse.isInlongComponent()) {
            Format innerFormat = format;
            format = new InLongMsgFormat(innerFormat, false);
        }
        PulsarScanStartupMode startupMode = PulsarScanStartupMode.forName(pulsarSourceResponse.getScanStartupMode());
        final String primaryKey = pulsarSourceResponse.getPrimaryKey();
        final String serviceUrl = pulsarSourceResponse.getServiceUrl();
        final String adminUrl = pulsarSourceResponse.getAdminUrl();

        return new PulsarExtractNode(id,
                name,
                fieldInfos,
                null,
                Maps.newHashMap(),
                topic,
                adminUrl,
                serviceUrl,
                format,
                startupMode.getValue(),
                primaryKey);
    }

    /**
     * Create PostgresExtractNode based PostgresSourceResponse
     *
     * @param postgresSourceResponse postgres source response
     * @return postgres extract node info
     */
    public static PostgresExtractNode createExtractNode(PostgresSourceResponse postgresSourceResponse) {
        List<StreamField> streamFields = postgresSourceResponse.getFieldList();
        String id = postgresSourceResponse.getSourceName();
        String name = postgresSourceResponse.getSourceName();
        List<FieldInfo> fields = streamFields.stream()
                .map(streamFieldInfo -> FieldInfoUtils.parseStreamFieldInfo(streamFieldInfo, name))
                .collect(Collectors.toList());
        return new PostgresExtractNode(id, name, fields, null, null,
                postgresSourceResponse.getPrimaryKey(), postgresSourceResponse.getTableNameList(),
                postgresSourceResponse.getHostname(), postgresSourceResponse.getUsername(),
                postgresSourceResponse.getPassword(), postgresSourceResponse.getDatabase(),
                postgresSourceResponse.getSchema(), postgresSourceResponse.getPort(),
                postgresSourceResponse.getDecodingPluginName());
    }
}
