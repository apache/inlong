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

import com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.inlong.common.enums.DataTypeEnum;
import org.apache.inlong.manager.common.enums.SinkType;
import org.apache.inlong.manager.common.pojo.sink.SinkField;
import org.apache.inlong.manager.common.pojo.sink.SinkResponse;
import org.apache.inlong.manager.common.pojo.sink.ck.ClickHouseSinkResponse;
import org.apache.inlong.manager.common.pojo.sink.hbase.HbaseSinkResponse;
import org.apache.inlong.manager.common.pojo.sink.hive.HiveSinkResponse;
import org.apache.inlong.manager.common.pojo.sink.kafka.KafkaSinkResponse;
import org.apache.inlong.manager.common.pojo.sink.postgres.PostgresSinkResponse;
import org.apache.inlong.sort.protocol.FieldInfo;
import org.apache.inlong.sort.protocol.node.LoadNode;
import org.apache.inlong.sort.protocol.node.format.AvroFormat;
import org.apache.inlong.sort.protocol.node.format.CanalJsonFormat;
import org.apache.inlong.sort.protocol.node.format.CsvFormat;
import org.apache.inlong.sort.protocol.node.format.DebeziumJsonFormat;
import org.apache.inlong.sort.protocol.node.format.Format;
import org.apache.inlong.sort.protocol.node.format.JsonFormat;
import org.apache.inlong.sort.protocol.node.load.ClickHouseLoadNode;
import org.apache.inlong.sort.protocol.node.load.HbaseLoadNode;
import org.apache.inlong.sort.protocol.node.load.HiveLoadNode;
import org.apache.inlong.sort.protocol.node.load.KafkaLoadNode;
import org.apache.inlong.sort.protocol.node.load.PostgresLoadNode;
import org.apache.inlong.sort.protocol.transformation.FieldRelationShip;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Util for load node info.
 */
public class LoadNodeUtils {

    /**
     * Create nodes of data load.
     */
    public static List<LoadNode> createLoadNodes(List<SinkResponse> sinkResponses) {
        if (CollectionUtils.isEmpty(sinkResponses)) {
            return Lists.newArrayList();
        }
        return sinkResponses.stream().map(LoadNodeUtils::createLoadNode).collect(Collectors.toList());
    }

    /**
     * Create node of data load.
     */
    public static LoadNode createLoadNode(SinkResponse sinkResponse) {
        SinkType sinkType = SinkType.forType(sinkResponse.getSinkType());
        switch (sinkType) {
            case KAFKA:
                return createLoadNode((KafkaSinkResponse) sinkResponse);
            case HIVE:
                return createLoadNode((HiveSinkResponse) sinkResponse);
            case HBASE:
                return createLoadNode((HbaseSinkResponse) sinkResponse);
            case POSTGRES:
                return createLoadNode((PostgresSinkResponse) sinkResponse);
            case CLICKHOUSE:
                return createLoadNode((ClickHouseSinkResponse) sinkResponse);
            default:
                throw new IllegalArgumentException(
                        String.format("Unsupported sinkType=%s to create loadNode", sinkType));
        }
    }

    /**
     * Create node of data load about kafka.
     */
    public static KafkaLoadNode createLoadNode(KafkaSinkResponse kafkaSinkResponse) {
        String id = kafkaSinkResponse.getSinkName();
        String name = kafkaSinkResponse.getSinkName();
        String topicName = kafkaSinkResponse.getTopicName();
        String bootstrapServers = kafkaSinkResponse.getBootstrapServers();
        List<SinkField> fieldList = kafkaSinkResponse.getFieldList();
        List<FieldInfo> fieldInfos = fieldList.stream()
                .map(field -> FieldInfoUtils.parseSinkFieldInfo(field, name))
                .collect(Collectors.toList());
        List<FieldRelationShip> fieldRelationships = parseSinkFields(fieldList, name);
        Map<String, String> properties = kafkaSinkResponse.getProperties().entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().toString()));
        Integer sinkParallelism = null;
        if (StringUtils.isNotEmpty(kafkaSinkResponse.getPartitionNum())) {
            sinkParallelism = Integer.parseInt(kafkaSinkResponse.getPartitionNum());
        }
        DataTypeEnum dataType = DataTypeEnum.forName(kafkaSinkResponse.getSerializationType());
        Format format;
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
        String primaryKey = kafkaSinkResponse.getPrimaryKey();
        return new KafkaLoadNode(id,
                name,
                fieldInfos,
                fieldRelationships,
                Lists.newArrayList(),
                null,
                topicName,
                bootstrapServers,
                format,
                sinkParallelism,
                properties,
                primaryKey);
    }

    /**
     * Create node of data load about hive.
     */
    public static HiveLoadNode createLoadNode(HiveSinkResponse hiveSinkResponse) {
        String id = hiveSinkResponse.getSinkName();
        String name = hiveSinkResponse.getSinkName();
        String database = hiveSinkResponse.getDbName();
        String tableName = hiveSinkResponse.getTableName();
        String hiveConfDir = hiveSinkResponse.getHiveConfDir();
        String hiveVersion = hiveSinkResponse.getHiveVersion();
        List<SinkField> fieldList = hiveSinkResponse.getFieldList();
        List<FieldInfo> fields = fieldList.stream()
                .map(sinkField -> FieldInfoUtils.parseSinkFieldInfo(sinkField, name))
                .collect(Collectors.toList());
        List<FieldRelationShip> fieldRelationships = parseSinkFields(fieldList, name);
        Map<String, String> properties = hiveSinkResponse.getProperties().entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().toString()));
        List<FieldInfo> partitionFields = Lists.newArrayList();
        if (CollectionUtils.isNotEmpty(hiveSinkResponse.getPartitionFieldList())) {
            partitionFields = hiveSinkResponse.getPartitionFieldList().stream()
                    .map(hivePartitionField -> new FieldInfo(hivePartitionField.getFieldName(), name,
                            FieldInfoUtils.convertFieldFormat(hivePartitionField.getFieldType(),
                                    hivePartitionField.getFieldFormat()))).collect(Collectors.toList());
        }
        return new HiveLoadNode(
                id,
                name,
                fields,
                fieldRelationships,
                Lists.newArrayList(),
                null,
                null,
                properties,
                null,
                database,
                tableName,
                hiveConfDir,
                hiveVersion,
                null,
                partitionFields
        );
    }

    /**
     * Create hbase load node from response.
     */
    public static HbaseLoadNode createLoadNode(HbaseSinkResponse sinkResponse) {
        String id = sinkResponse.getSinkName();
        String name = sinkResponse.getSinkName();
        List<SinkField> fieldList = sinkResponse.getFieldList();
        List<FieldInfo> fields = fieldList.stream()
                .map(sinkField -> FieldInfoUtils.parseSinkFieldInfo(sinkField, name))
                .collect(Collectors.toList());
        List<FieldRelationShip> fieldRelationships = parseSinkFields(fieldList, name);
        Map<String, String> properties = sinkResponse.getProperties().entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().toString()));
        return new HbaseLoadNode(
                id,
                name,
                fields,
                fieldRelationships,
                Lists.newArrayList(),
                null,
                null,
                properties,
                sinkResponse.getTableName(),
                sinkResponse.getNamespace(),
                sinkResponse.getZkQuorum(),
                sinkResponse.getRowKey(),
                sinkResponse.getBufferFlushMaxSize(),
                sinkResponse.getZkNodeParent(),
                sinkResponse.getBufferFlushMaxRows(),
                sinkResponse.getBufferFlushInterval()
        );
    }

    /**
     * Create postgres load node
     */
    public static PostgresLoadNode createLoadNode(PostgresSinkResponse postgresSinkResponse) {
        List<SinkField> fieldList = postgresSinkResponse.getFieldList();
        String name = postgresSinkResponse.getSinkName();
        List<FieldInfo> fields = fieldList.stream()
                .map(sinkField -> FieldInfoUtils.parseSinkFieldInfo(sinkField, name))
                .collect(Collectors.toList());
        List<FieldRelationShip> fieldRelationships = parseSinkFields(fieldList, name);
        return new PostgresLoadNode(postgresSinkResponse.getSinkName(),
                postgresSinkResponse.getSinkName(),
                fields, fieldRelationships, null, null, 1,
                null, postgresSinkResponse.getJdbcUrl(), postgresSinkResponse.getUsername(),
                postgresSinkResponse.getPassword(),
                postgresSinkResponse.getDbName() + "." + postgresSinkResponse.getTableName(),
                postgresSinkResponse.getPrimaryKey());
    }

    /**
     * Create ClickHouse load node
     */
    public static ClickHouseLoadNode createLoadNode(ClickHouseSinkResponse clickHouseSinkResponse) {
        List<SinkField> sinkFieldResponses = clickHouseSinkResponse.getFieldList();
        String name = clickHouseSinkResponse.getSinkName();
        List<FieldInfo> fields = sinkFieldResponses.stream()
                .map(sinkFieldResponse -> FieldInfoUtils.parseSinkFieldInfo(sinkFieldResponse,
                        name))
                .collect(Collectors.toList());
        List<FieldRelationShip> fieldRelationShips = parseSinkFields(sinkFieldResponses, name);
        return new ClickHouseLoadNode(clickHouseSinkResponse.getSinkName(),
                clickHouseSinkResponse.getSinkName(),
                fields, fieldRelationShips, null, null, 1,
                null, clickHouseSinkResponse.getTableName(),
                clickHouseSinkResponse.getJdbcUrl(),
                clickHouseSinkResponse.getUsername(),
                clickHouseSinkResponse.getPassword());
    }

    /**
     * Parse information field of data sink.
     */
    public static List<FieldRelationShip> parseSinkFields(List<SinkField> fieldList, String sinkName) {
        if (CollectionUtils.isEmpty(fieldList)) {
            return Lists.newArrayList();
        }
        return fieldList.stream()
                .filter(sinkField -> StringUtils.isNotEmpty(sinkField.getSourceFieldName()))
                .map(field -> {
                    String fieldName = field.getFieldName();
                    String fieldType = field.getFieldType();
                    String fieldFormat = field.getFieldFormat();
                    FieldInfo sinkField = new FieldInfo(fieldName, sinkName,
                            FieldInfoUtils.convertFieldFormat(fieldType, fieldFormat));
                    String sourceFieldName = field.getSourceFieldName();
                    String sourceFieldType = field.getSourceFieldType();
                    FieldInfo sourceField = new FieldInfo(sourceFieldName, sinkName,
                            FieldInfoUtils.convertFieldFormat(sourceFieldType));
                    return new FieldRelationShip(sourceField, sinkField);
                }).collect(Collectors.toList());
    }
}
