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
import org.apache.commons.lang3.StringUtils;
import org.apache.inlong.common.enums.DataTypeEnum;
import org.apache.inlong.manager.common.enums.ErrorCodeEnum;
import org.apache.inlong.manager.common.enums.SinkType;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.pojo.sink.SinkField;
import org.apache.inlong.manager.common.pojo.sink.StreamSink;
import org.apache.inlong.manager.common.pojo.sink.ck.ClickHouseSink;
import org.apache.inlong.manager.common.pojo.sink.hbase.HBaseSink;
import org.apache.inlong.manager.common.pojo.sink.hive.HivePartitionField;
import org.apache.inlong.manager.common.pojo.sink.hive.HiveSink;
import org.apache.inlong.manager.common.pojo.sink.iceberg.IcebergSink;
import org.apache.inlong.manager.common.pojo.sink.kafka.KafkaSink;
import org.apache.inlong.manager.common.pojo.sink.postgres.PostgresSink;
import org.apache.inlong.manager.common.pojo.sink.sqlserver.SqlServerSink;
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
import org.apache.inlong.sort.protocol.node.load.IcebergLoadNode;
import org.apache.inlong.sort.protocol.node.load.KafkaLoadNode;
import org.apache.inlong.sort.protocol.node.load.PostgresLoadNode;
import org.apache.inlong.sort.protocol.node.load.SqlServerLoadNode;
import org.apache.inlong.sort.protocol.transformation.FieldRelation;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Util for load node info.
 */
public class LoadNodeUtils {

    private static final String DEFAULT_HIVE_VERSION = "2.3.6";

    /**
     * Create nodes of data load.
     */
    public static List<LoadNode> createLoadNodes(List<StreamSink> streamSinks) {
        if (CollectionUtils.isEmpty(streamSinks)) {
            return Lists.newArrayList();
        }
        return streamSinks.stream().map(LoadNodeUtils::createLoadNode).collect(Collectors.toList());
    }

    /**
     * Create node of data load.
     */
    public static LoadNode createLoadNode(StreamSink streamSink) {
        SinkType sinkType = SinkType.forType(streamSink.getSinkType());
        switch (sinkType) {
            case KAFKA:
                return createLoadNode((KafkaSink) streamSink);
            case HIVE:
                return createLoadNode((HiveSink) streamSink);
            case HBASE:
                return createLoadNode((HBaseSink) streamSink);
            case POSTGRES:
                return createLoadNode((PostgresSink) streamSink);
            case CLICKHOUSE:
                return createLoadNode((ClickHouseSink) streamSink);
            case ICEBERG:
                return createLoadNode((IcebergSink) streamSink);
            case SQLSERVER:
                return createLoadNode((SqlServerSink) streamSink);
            default:
                throw new IllegalArgumentException(
                        String.format("Unsupported sinkType=%s to create loadNode", sinkType));
        }
    }

    /**
     * Create node of data load about kafka.
     */
    public static KafkaLoadNode createLoadNode(KafkaSink kafkaSink) {
        String id = kafkaSink.getSinkName();
        String name = kafkaSink.getSinkName();
        String topicName = kafkaSink.getTopicName();
        String bootstrapServers = kafkaSink.getBootstrapServers();
        List<SinkField> fieldList = kafkaSink.getFieldList();
        List<FieldInfo> fieldInfos = fieldList.stream()
                .map(field -> FieldInfoUtils.parseSinkFieldInfo(field, name))
                .collect(Collectors.toList());
        List<FieldRelation> fieldRelations = parseSinkFields(fieldList, name);
        Map<String, String> properties = kafkaSink.getProperties().entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().toString()));
        Integer sinkParallelism = null;
        if (StringUtils.isNotEmpty(kafkaSink.getPartitionNum())) {
            sinkParallelism = Integer.parseInt(kafkaSink.getPartitionNum());
        }
        DataTypeEnum dataType = DataTypeEnum.forName(kafkaSink.getSerializationType());
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
        String primaryKey = kafkaSink.getPrimaryKey();
        return new KafkaLoadNode(id,
                name,
                fieldInfos,
                fieldRelations,
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
    public static HiveLoadNode createLoadNode(HiveSink hiveSink) {
        String id = hiveSink.getSinkName();
        String name = hiveSink.getSinkName();
        String database = hiveSink.getDbName();
        String tableName = hiveSink.getTableName();
        String hiveConfDir = hiveSink.getHiveConfDir();
        String hiveVersion = hiveSink.getHiveVersion();
        List<SinkField> fieldList = hiveSink.getFieldList();
        List<FieldInfo> fields = fieldList.stream()
                .map(sinkField -> FieldInfoUtils.parseSinkFieldInfo(sinkField, name))
                .collect(Collectors.toList());
        List<FieldRelation> fieldRelations = parseSinkFields(fieldList, name);
        Map<String, String> properties = hiveSink.getProperties().entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().toString()));
        List<FieldInfo> partitionFields = Lists.newArrayList();
        if (CollectionUtils.isNotEmpty(hiveSink.getPartitionFieldList())) {
            partitionFields = hiveSink.getPartitionFieldList().stream()
                    .map(hivePartitionField -> new FieldInfo(hivePartitionField.getFieldName(), name,
                            FieldInfoUtils.convertFieldFormat(hivePartitionField.getFieldType(),
                                    hivePartitionField.getFieldFormat()))).collect(Collectors.toList());
        }
        if (StringUtils.isEmpty(hiveVersion)) {
            hiveVersion = DEFAULT_HIVE_VERSION;
        }
        return new HiveLoadNode(
                id,
                name,
                fields,
                fieldRelations,
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
    public static HbaseLoadNode createLoadNode(HBaseSink hbaseSink) {
        String id = hbaseSink.getSinkName();
        String name = hbaseSink.getSinkName();
        List<SinkField> fieldList = hbaseSink.getFieldList();
        List<FieldInfo> fields = fieldList.stream()
                .map(sinkField -> FieldInfoUtils.parseSinkFieldInfo(sinkField, name))
                .collect(Collectors.toList());
        List<FieldRelation> fieldRelations = parseSinkFields(fieldList, name);
        Map<String, String> properties = hbaseSink.getProperties().entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().toString()));
        return new HbaseLoadNode(
                id,
                name,
                fields,
                fieldRelations,
                Lists.newArrayList(),
                null,
                null,
                properties,
                hbaseSink.getTableName(),
                hbaseSink.getNamespace(),
                hbaseSink.getZkQuorum(),
                hbaseSink.getRowKey(),
                hbaseSink.getBufferFlushMaxSize(),
                hbaseSink.getZkNodeParent(),
                hbaseSink.getBufferFlushMaxRows(),
                hbaseSink.getBufferFlushInterval()
        );
    }

    /**
     * Create postgres load node
     */
    public static PostgresLoadNode createLoadNode(PostgresSink postgresSink) {
        List<SinkField> fieldList = postgresSink.getFieldList();
        String name = postgresSink.getSinkName();
        List<FieldInfo> fields = fieldList.stream()
                .map(sinkField -> FieldInfoUtils.parseSinkFieldInfo(sinkField, name))
                .collect(Collectors.toList());
        List<FieldRelation> fieldRelations = parseSinkFields(fieldList, name);
        return new PostgresLoadNode(postgresSink.getSinkName(),
                postgresSink.getSinkName(),
                fields, fieldRelations, null, null, 1,
                null, postgresSink.getJdbcUrl(), postgresSink.getUsername(),
                postgresSink.getPassword(),
                postgresSink.getDbName() + "." + postgresSink.getTableName(),
                postgresSink.getPrimaryKey());
    }

    /**
     * Create ClickHouse load node
     */
    public static ClickHouseLoadNode createLoadNode(ClickHouseSink ckSink) {
        List<SinkField> sinkFields = ckSink.getFieldList();
        String name = ckSink.getSinkName();
        List<FieldInfo> fields = sinkFields.stream()
                .map(sinkField -> FieldInfoUtils.parseSinkFieldInfo(sinkField, name))
                .collect(Collectors.toList());
        List<FieldRelation> fieldRelations = parseSinkFields(sinkFields, name);
        return new ClickHouseLoadNode(name, name,
                fields, fieldRelations, null, null, 1,
                null, ckSink.getTableName(),
                ckSink.getJdbcUrl(),
                ckSink.getUsername(),
                ckSink.getPassword());
    }

    /**
     * Create iceberg load node
     */
    public static IcebergLoadNode createLoadNode(IcebergSink icebergSink) {
        String id = icebergSink.getSinkName();
        String name = icebergSink.getSinkName();
        String dbName = icebergSink.getDbName();
        String tableName = icebergSink.getTableName();
        String uri = icebergSink.getCatalogUri();
        String warehouse = icebergSink.getWarehouse();

        List<SinkField> sinkFields = icebergSink.getFieldList();
        List<FieldInfo> fields = sinkFields.stream()
                .map(sinkField -> FieldInfoUtils.parseSinkFieldInfo(sinkField, name))
                .collect(Collectors.toList());
        List<FieldRelation> fieldRelationShips = parseSinkFields(sinkFields, name);
        Map<String, String> properties = icebergSink.getProperties().entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().toString()));

        return new IcebergLoadNode(id, name, fields, fieldRelationShips, null, null, 1, properties,
                dbName, tableName, null, null, uri, warehouse);
    }

    /**
     * Create SqlServer load node based on SqlServerSink
     *
     * @param sqlServerSink SqlServer sink info
     * @return SqlServer load node info
     */
    public static SqlServerLoadNode createLoadNode(SqlServerSink sqlServerSink) {
        final String id = sqlServerSink.getSinkName();
        final String name = sqlServerSink.getSinkName();
        final String primaryKey = sqlServerSink.getPrimaryKey();
        final String jdbcUrl = sqlServerSink.getJdbcUrl();
        final String userName = sqlServerSink.getUsername();
        final String password = sqlServerSink.getPassword();
        final String schemaName = sqlServerSink.getSchemaName();
        final String tablename = sqlServerSink.getTableName();
        final List<SinkField> fieldList = sqlServerSink.getFieldList();
        List<FieldInfo> fields = fieldList.stream()
                .map(sinkField -> FieldInfoUtils.parseSinkFieldInfo(sinkField, name))
                .collect(Collectors.toList());
        List<FieldRelation> fieldRelations = parseSinkFields(fieldList, name);
        Map<String, String> properties = sqlServerSink.getProperties().entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().toString()));
        return new SqlServerLoadNode(
                id,
                name,
                fields,
                fieldRelations,
                null,
                null,
                null,
                properties,
                jdbcUrl,
                userName,
                password,
                schemaName,
                tablename,
                primaryKey
        );
    }

    /**
     * Parse information field of data sink.
     */
    public static List<FieldRelation> parseSinkFields(List<SinkField> fieldList, String sinkName) {
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
                    return new FieldRelation(sourceField, sinkField);
                }).collect(Collectors.toList());
    }

    /**
     * Check the validation of Hive partition field.
     */
    public static void checkPartitionField(List<SinkField> fieldList, List<HivePartitionField> partitionList) {
        if (CollectionUtils.isEmpty(partitionList)) {
            return;
        }
        if (CollectionUtils.isEmpty(fieldList)) {
            throw new BusinessException(ErrorCodeEnum.SINK_FIELD_LIST_IS_EMPTY);
        }

        Map<String, SinkField> sinkFieldMap = new HashMap<>(fieldList.size());
        fieldList.forEach(field -> sinkFieldMap.put(field.getFieldName(), field));

        for (HivePartitionField partitionField : partitionList) {
            String fieldName = partitionField.getFieldName();
            if (StringUtils.isBlank(fieldName)) {
                throw new BusinessException(ErrorCodeEnum.PARTITION_FIELD_NAME_IS_EMPTY);
            }

            SinkField sinkField = sinkFieldMap.get(fieldName);
            if (sinkField == null) {
                throw new BusinessException(
                        String.format(ErrorCodeEnum.PARTITION_FIELD_NOT_FOUND.getMessage(), fieldName));
            }
            if (StringUtils.isBlank(sinkField.getSourceFieldName())) {
                throw new BusinessException(
                        String.format(ErrorCodeEnum.PARTITION_FIELD_NO_SOURCE_FIELD.getMessage(), fieldName));
            }
        }
    }

}
