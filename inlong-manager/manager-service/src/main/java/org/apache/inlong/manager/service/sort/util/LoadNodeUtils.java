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
import org.apache.inlong.manager.common.pojo.sink.SinkFieldResponse;
import org.apache.inlong.manager.common.pojo.sink.SinkResponse;
import org.apache.inlong.manager.common.pojo.sink.hive.HiveSinkResponse;
import org.apache.inlong.manager.common.pojo.sink.kafka.KafkaSinkResponse;
import org.apache.inlong.sort.protocol.FieldInfo;
import org.apache.inlong.sort.protocol.node.LoadNode;
import org.apache.inlong.sort.protocol.node.format.AvroFormat;
import org.apache.inlong.sort.protocol.node.format.CanalJsonFormat;
import org.apache.inlong.sort.protocol.node.format.CsvFormat;
import org.apache.inlong.sort.protocol.node.format.DebeziumJsonFormat;
import org.apache.inlong.sort.protocol.node.format.Format;
import org.apache.inlong.sort.protocol.node.format.JsonFormat;
import org.apache.inlong.sort.protocol.node.load.HiveLoadNode;
import org.apache.inlong.sort.protocol.node.load.KafkaLoadNode;
import org.apache.inlong.sort.protocol.transformation.FieldRelationShip;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class LoadNodeUtils {

    public static List<LoadNode> createLoadNodes(List<SinkResponse> sinkResponses) {
        if (CollectionUtils.isEmpty(sinkResponses)) {
            return Lists.newArrayList();
        }
        return sinkResponses.stream().map(sourceResponse -> createLoadNode(sourceResponse))
                .collect(Collectors.toList());
    }

    public static LoadNode createLoadNode(SinkResponse sinkResponse) {
        SinkType sinkType = SinkType.forType(sinkResponse.getSinkType());
        switch (sinkType) {
            case KAFKA:
                return createLoadNode((KafkaSinkResponse) sinkResponse);
            case HIVE:
                return createLoadNode((HiveSinkResponse) sinkResponse);
            default:
                throw new IllegalArgumentException(
                        String.format("Unsupported sinkType=%s to create loadNode", sinkType));
        }
    }

    public static KafkaLoadNode createLoadNode(KafkaSinkResponse kafkaSinkResponse) {

        String id = kafkaSinkResponse.getSinkName();
        String name = kafkaSinkResponse.getSinkName();
        String topicName = kafkaSinkResponse.getTopicName();
        String bootstrapServers = kafkaSinkResponse.getBootstrapServers();
        List<SinkFieldResponse> sinkFieldResponses = kafkaSinkResponse.getFieldList();
        List<FieldInfo> fieldInfos = sinkFieldResponses.stream()
                .map(sinkFieldResponse -> new FieldInfo(sinkFieldResponse.getFieldName(), name,
                        FieldInfoUtils.convertFieldFormat(sinkFieldResponse.getFieldType(),
                                sinkFieldResponse.getFieldFormat()))).collect(Collectors.toList());
        List<FieldRelationShip> fieldRelationShips = parseSinkFields(sinkFieldResponses, name);
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
                fieldRelationShips,
                Lists.newArrayList(),
                topicName,
                bootstrapServers,
                format,
                sinkParallelism,
                properties,
                primaryKey);
    }

    public static HiveLoadNode createLoadNode(HiveSinkResponse hiveSinkResponse) {
        String id = hiveSinkResponse.getSinkName();
        String name = hiveSinkResponse.getSinkName();
        String database = hiveSinkResponse.getDbName();
        String tableName = hiveSinkResponse.getTableName();
        String hiveConfDir = hiveSinkResponse.getHiveConfDir();
        String hiveVersion = hiveSinkResponse.getHiveVersion();
        List<SinkFieldResponse> sinkFieldResponses = hiveSinkResponse.getFieldList();
        List<FieldInfo> fields = sinkFieldResponses.stream()
                .map(sinkFieldResponse -> new FieldInfo(sinkFieldResponse.getFieldName(), name,
                        FieldInfoUtils.convertFieldFormat(sinkFieldResponse.getFieldType(),
                                sinkFieldResponse.getFieldFormat()))).collect(Collectors.toList());
        List<FieldRelationShip> fieldRelationShips = parseSinkFields(sinkFieldResponses, name);
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
                fieldRelationShips,
                Lists.newArrayList(),
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

    public static List<FieldRelationShip> parseSinkFields(List<SinkFieldResponse> sinkFieldResponses, String sinkName) {
        if (CollectionUtils.isEmpty(sinkFieldResponses)) {
            return Lists.newArrayList();
        }
        return sinkFieldResponses.stream().map(sinkFieldResponse -> {
            String fieldName = sinkFieldResponse.getFieldName();
            String fieldType = sinkFieldResponse.getFieldType();
            String fieldFormat = sinkFieldResponse.getFieldFormat();
            FieldInfo sinkField = new FieldInfo(fieldName, sinkName,
                    FieldInfoUtils.convertFieldFormat(fieldType, fieldFormat));
            String sourceFieldName = sinkFieldResponse.getSourceFieldName();
            String sourceFieldType = sinkFieldResponse.getSourceFieldType();
            FieldInfo sourceField = new FieldInfo(sourceFieldName, sinkName,
                    FieldInfoUtils.convertFieldFormat(sourceFieldType));
            return new FieldRelationShip(sourceField, sinkField);
        }).collect(Collectors.toList());
    }
}
