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

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.inlong.manager.common.enums.ErrorCodeEnum;
import org.apache.inlong.manager.common.enums.FieldType;
import org.apache.inlong.manager.common.enums.FileFormat;
import org.apache.inlong.manager.common.enums.SinkType;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.pojo.sink.SinkFieldBase;
import org.apache.inlong.manager.common.pojo.sink.SinkResponse;
import org.apache.inlong.manager.common.pojo.sink.ck.ClickHouseSinkResponse;
import org.apache.inlong.manager.common.pojo.sink.es.ElasticsearchSinkResponse;
import org.apache.inlong.manager.common.pojo.sink.hbase.HbaseSinkResponse;
import org.apache.inlong.manager.common.pojo.sink.hive.HivePartitionField;
import org.apache.inlong.manager.common.pojo.sink.hive.HiveSinkResponse;
import org.apache.inlong.manager.common.pojo.sink.iceberg.IcebergSinkResponse;
import org.apache.inlong.manager.common.pojo.sink.kafka.KafkaSinkResponse;
import org.apache.inlong.manager.common.pojo.source.SourceResponse;
import org.apache.inlong.sort.protocol.FieldInfo;
import org.apache.inlong.sort.protocol.serialization.SerializationInfo;
import org.apache.inlong.sort.protocol.sink.ClickHouseSinkInfo;
import org.apache.inlong.sort.protocol.sink.ClickHouseSinkInfo.PartitionStrategy;
import org.apache.inlong.sort.protocol.sink.ElasticsearchSinkInfo;
import org.apache.inlong.sort.protocol.sink.HbaseSinkInfo;
import org.apache.inlong.sort.protocol.sink.HiveSinkInfo;
import org.apache.inlong.sort.protocol.sink.HiveSinkInfo.HiveFieldPartitionInfo;
import org.apache.inlong.sort.protocol.sink.HiveSinkInfo.HiveFileFormat;
import org.apache.inlong.sort.protocol.sink.HiveSinkInfo.HivePartitionInfo;
import org.apache.inlong.sort.protocol.sink.HiveSinkInfo.HiveTimePartitionInfo;
import org.apache.inlong.sort.protocol.sink.IcebergSinkInfo;
import org.apache.inlong.sort.protocol.sink.KafkaSinkInfo;
import org.apache.inlong.sort.protocol.sink.SinkInfo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Utils for create sink info, such as kafka sink, clickhouse sink, etc.
 */
public class SinkInfoUtils {

    private static final String DATA_FORMAT = "yyyyMMddHH";
    private static final String TIME_FORMAT = "HHmmss";
    private static final String DATA_TIME_FORMAT = "yyyyMMddHHmmss";

    /**
     * Create sink info for DataFlowInfo.
     */
    public static SinkInfo createSinkInfo(SourceResponse sourceResponse, SinkResponse sinkResponse,
            List<FieldInfo> sinkFields) {
        String sinkType = sinkResponse.getSinkType();
        SinkInfo sinkInfo;
        switch (SinkType.forType(sinkType)) {
            case HIVE:
                sinkInfo = createHiveSinkInfo((HiveSinkResponse) sinkResponse, sinkFields);
                break;
            case KAFKA:
                sinkInfo = createKafkaSinkInfo(sourceResponse, (KafkaSinkResponse) sinkResponse, sinkFields);
                break;
            case ICEBERG:
                sinkInfo = createIcebergSinkInfo((IcebergSinkResponse) sinkResponse, sinkFields);
                break;
            case CLICKHOUSE:
                sinkInfo = createClickhouseSinkInfo((ClickHouseSinkResponse) sinkResponse, sinkFields);
                break;
            case HBASE:
                sinkInfo = createHbaseSinkInfo((HbaseSinkResponse) sinkResponse, sinkFields);
                break;
            case ELASTICSEARCH:
                sinkInfo = createElasticsearchSinkInfo((ElasticsearchSinkResponse) sinkResponse, sinkFields);
                break;
            default:
                throw new BusinessException(String.format("Unsupported SinkType {%s}", sinkType));
        }
        return sinkInfo;
    }

    private static ClickHouseSinkInfo createClickhouseSinkInfo(ClickHouseSinkResponse sinkResponse,
            List<FieldInfo> sinkFields) {
        if (StringUtils.isEmpty(sinkResponse.getJdbcUrl())) {
            throw new BusinessException(String.format("ClickHouse={%s} server url cannot be empty", sinkResponse));
        } else if (CollectionUtils.isEmpty(sinkResponse.getFieldList())) {
            throw new BusinessException(String.format("ClickHouse={%s} fields cannot be empty", sinkResponse));
        } else if (StringUtils.isEmpty(sinkResponse.getTableName())) {
            throw new BusinessException(String.format("ClickHouse={%s} table name cannot be empty", sinkResponse));
        } else if (StringUtils.isEmpty(sinkResponse.getDbName())) {
            throw new BusinessException(String.format("ClickHouse={%s} database name cannot be empty", sinkResponse));
        }

        Integer isDistributed = sinkResponse.getIsDistributed();
        if (isDistributed == null) {
            throw new BusinessException(String.format("ClickHouse={%s} isDistributed cannot be null", sinkResponse));
        }

        // Default partition strategy is RANDOM
        ClickHouseSinkInfo.PartitionStrategy partitionStrategy = PartitionStrategy.RANDOM;
        boolean distributedTable = isDistributed == 1;
        if (distributedTable) {
            if (PartitionStrategy.BALANCE.name().equalsIgnoreCase(sinkResponse.getPartitionStrategy())) {
                partitionStrategy = PartitionStrategy.BALANCE;
            } else if (PartitionStrategy.HASH.name().equalsIgnoreCase(sinkResponse.getPartitionStrategy())) {
                partitionStrategy = PartitionStrategy.HASH;
            }
        }

        // TODO Add keyFieldNames instead of `new String[0]`
        return new ClickHouseSinkInfo(sinkResponse.getJdbcUrl(), sinkResponse.getDbName(),
                sinkResponse.getTableName(), sinkResponse.getUsername(), sinkResponse.getPassword(),
                distributedTable, partitionStrategy, sinkResponse.getPartitionFields(),
                sinkFields.toArray(new FieldInfo[0]), new String[0],
                sinkResponse.getFlushInterval(), sinkResponse.getFlushRecord(),
                sinkResponse.getRetryTimes());
    }

    // TODO Need set more configs for IcebergSinkInfo
    private static IcebergSinkInfo createIcebergSinkInfo(IcebergSinkResponse sinkResponse, List<FieldInfo> sinkFields) {
        if (StringUtils.isEmpty(sinkResponse.getDataPath())) {
            throw new BusinessException(String.format("Iceberg={%s} data path cannot be empty", sinkResponse));
        }

        return new IcebergSinkInfo(sinkFields.toArray(new FieldInfo[0]), sinkResponse.getDataPath());
    }

    private static KafkaSinkInfo createKafkaSinkInfo(SourceResponse sourceResponse, KafkaSinkResponse sinkResponse,
            List<FieldInfo> sinkFields) {
        String addressUrl = sinkResponse.getBootstrapServers();
        String topicName = sinkResponse.getTopicName();
        SerializationInfo serializationInfo = SerializationUtils.createSerialInfo(sourceResponse,
                sinkResponse);
        return new KafkaSinkInfo(sinkFields.toArray(new FieldInfo[0]), addressUrl, topicName, serializationInfo);
    }

    /**
     * Create Hive sink info.
     */
    private static HiveSinkInfo createHiveSinkInfo(HiveSinkResponse hiveInfo, List<FieldInfo> sinkFields) {
        if (hiveInfo.getJdbcUrl() == null) {
            throw new BusinessException(String.format("HiveSink={%s} server url cannot be empty", hiveInfo));
        }
        if (CollectionUtils.isEmpty(hiveInfo.getFieldList())) {
            throw new BusinessException(String.format("HiveSink={%s} fields cannot be empty", hiveInfo));
        }
        // Use the field separator in Hive, the default is TextFile
        Character separator = (char) Integer.parseInt(hiveInfo.getDataSeparator());
        HiveFileFormat fileFormat;
        FileFormat format = FileFormat.forName(hiveInfo.getFileFormat());

        if (format == FileFormat.ORCFile) {
            fileFormat = new HiveSinkInfo.OrcFileFormat(1000);
        } else if (format == FileFormat.SequenceFile) {
            fileFormat = new HiveSinkInfo.SequenceFileFormat(separator, 100);
        } else if (format == FileFormat.Parquet) {
            fileFormat = new HiveSinkInfo.ParquetFileFormat();
        } else {
            fileFormat = new HiveSinkInfo.TextFileFormat(separator);
        }

        // Handle hive partition list
        List<HivePartitionInfo> partitionList = new ArrayList<>();
        List<HivePartitionField> partitionFieldList = hiveInfo.getPartitionFieldList();
        if (CollectionUtils.isNotEmpty(partitionFieldList)) {
            SinkInfoUtils.checkPartitionField(hiveInfo.getFieldList(), partitionFieldList);
            partitionList = partitionFieldList.stream().map(s -> {
                HivePartitionInfo partition;
                String fieldFormat = s.getFieldFormat();
                switch (FieldType.forName(s.getFieldType())) {
                    case TIMESTAMP:
                        fieldFormat = StringUtils.isNotBlank(fieldFormat) ? fieldFormat : DATA_TIME_FORMAT;
                        partition = new HiveTimePartitionInfo(s.getFieldName(), fieldFormat);
                        break;
                    case DATE:
                        fieldFormat = StringUtils.isNotBlank(fieldFormat) ? fieldFormat : DATA_FORMAT;
                        partition = new HiveTimePartitionInfo(s.getFieldName(), fieldFormat);
                        break;
                    default:
                        partition = new HiveFieldPartitionInfo(s.getFieldName());
                }
                return partition;
            }).collect(Collectors.toList());
        }

        // dataPath = dataPath + / + tableName
        StringBuilder dataPathBuilder = new StringBuilder();
        String dataPath = hiveInfo.getDataPath();
        if (!dataPath.endsWith("/")) {
            dataPathBuilder.append(dataPath).append("/");
        }
        dataPath = dataPathBuilder.append(hiveInfo.getTableName()).toString();

        return new HiveSinkInfo(sinkFields.toArray(new FieldInfo[0]), hiveInfo.getJdbcUrl(),
                hiveInfo.getDbName(), hiveInfo.getTableName(), hiveInfo.getUsername(), hiveInfo.getPassword(),
                dataPath, partitionList.toArray(new HiveSinkInfo.HivePartitionInfo[0]), fileFormat);
    }

    /**
     * Check the validation of Hive partition field.
     */
    public static void checkPartitionField(List<? extends SinkFieldBase> fieldList,
            List<HivePartitionField> partitionList) {
        if (CollectionUtils.isEmpty(partitionList)) {
            return;
        }

        if (CollectionUtils.isEmpty(fieldList)) {
            throw new BusinessException(ErrorCodeEnum.SINK_FIELD_LIST_IS_EMPTY);
        }

        Map<String, SinkFieldBase> sinkFieldMap = new HashMap<>(fieldList.size());
        fieldList.forEach(field -> sinkFieldMap.put(field.getFieldName(), field));

        for (HivePartitionField partitionField : partitionList) {
            String fieldName = partitionField.getFieldName();
            if (StringUtils.isBlank(fieldName)) {
                throw new BusinessException(ErrorCodeEnum.PARTITION_FIELD_NAME_IS_EMPTY);
            }

            SinkFieldBase sinkField = sinkFieldMap.get(fieldName);
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

    /**
     * Creat HBase sink info.
     */
    private static HbaseSinkInfo createHbaseSinkInfo(HbaseSinkResponse sinkResponse, List<FieldInfo> sinkFields) {
        if (StringUtils.isEmpty(sinkResponse.getZookeeperQuorum())) {
            throw new BusinessException(String.format("HBase={%s} zookeeper quorum url cannot be empty", sinkResponse));
        } else if (StringUtils.isEmpty(sinkResponse.getZookeeperZnodeParent())) {
            throw new BusinessException(String.format("HBase={%s} zookeeper node cannot be empty", sinkResponse));
        } else if (StringUtils.isEmpty(sinkResponse.getTableName())) {
            throw new BusinessException(String.format("HBase={%s} table name cannot be empty", sinkResponse));
        }

        return new HbaseSinkInfo(sinkFields.toArray(new FieldInfo[0]), sinkResponse.getZookeeperQuorum(),
                sinkResponse.getZookeeperZnodeParent(), sinkResponse.getNamespace(), sinkResponse.getTableName(),
                sinkResponse.getSinkBufferFlushMaxSize(), sinkResponse.getSinkBufferFlushMaxSize(),
                sinkResponse.getSinkBufferFlushInterval());

    }

    /**
     * Creat Elasticsearch sink info.
     */
    private static ElasticsearchSinkInfo createElasticsearchSinkInfo(ElasticsearchSinkResponse sinkResponse,
            List<FieldInfo> sinkFields) {
        if (StringUtils.isEmpty(sinkResponse.getHost())) {
            throw new BusinessException(String.format("ClickHouse={%s} server host cannot be empty", sinkResponse));
        } else if (StringUtils.isEmpty(sinkResponse.getIndexName())) {
            throw new BusinessException(String.format("ClickHouse={%s} indexName cannot be empty", sinkResponse));
        }

        return new ElasticsearchSinkInfo(sinkResponse.getHost(), sinkResponse.getPort(),
                sinkResponse.getIndexName(), sinkResponse.getUsername(), sinkResponse.getPassword(),
                sinkFields.toArray(new FieldInfo[0]), new String[0],
                sinkResponse.getFlushInterval(), sinkResponse.getFlushRecord(),
                sinkResponse.getRetryTimes());
    }

}
