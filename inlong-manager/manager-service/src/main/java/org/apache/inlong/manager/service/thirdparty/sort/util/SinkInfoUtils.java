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

package org.apache.inlong.manager.service.thirdparty.sort.util;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.inlong.manager.common.enums.FileFormat;
import org.apache.inlong.manager.common.enums.SinkType;
import org.apache.inlong.manager.common.pojo.sink.SinkResponse;
import org.apache.inlong.manager.common.pojo.sink.ck.ClickHouseSinkResponse;
import org.apache.inlong.manager.common.pojo.sink.hive.HiveSinkResponse;
import org.apache.inlong.manager.common.pojo.sink.kafka.KafkaSinkResponse;
import org.apache.inlong.manager.common.pojo.source.SourceResponse;
import org.apache.inlong.sort.protocol.FieldInfo;
import org.apache.inlong.sort.protocol.serialization.SerializationInfo;
import org.apache.inlong.sort.protocol.sink.ClickHouseSinkInfo;
import org.apache.inlong.sort.protocol.sink.ClickHouseSinkInfo.PartitionStrategy;
import org.apache.inlong.sort.protocol.sink.HiveSinkInfo;
import org.apache.inlong.sort.protocol.sink.HiveSinkInfo.HiveFileFormat;
import org.apache.inlong.sort.protocol.sink.HiveSinkInfo.HivePartitionInfo;
import org.apache.inlong.sort.protocol.sink.HiveSinkInfo.HiveTimePartitionInfo;
import org.apache.inlong.sort.protocol.sink.KafkaSinkInfo;
import org.apache.inlong.sort.protocol.sink.SinkInfo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class SinkInfoUtils {

    private static final Map<String, String> PARTITION_TIME_FORMAT_MAP = new HashMap<>();

    private static final Map<String, TimeUnit> PARTITION_TIME_UNIT_MAP = new HashMap<>();

    static {
        PARTITION_TIME_FORMAT_MAP.put("D", "yyyyMMdd");
        PARTITION_TIME_FORMAT_MAP.put("H", "yyyyMMddHH");
        PARTITION_TIME_FORMAT_MAP.put("I", "yyyyMMddHHmm");

        PARTITION_TIME_UNIT_MAP.put("D", TimeUnit.DAYS);
        PARTITION_TIME_UNIT_MAP.put("H", TimeUnit.HOURS);
        PARTITION_TIME_UNIT_MAP.put("I", TimeUnit.MINUTES);
    }

    /**
     * Create sink info for DataFlowInfo.
     */
    public static SinkInfo createSinkInfo(SourceResponse sourceResponse, SinkResponse sinkResponse,
            List<FieldInfo> sinkFields) {
        String sinkType = sinkResponse.getSinkType();
        SinkInfo sinkInfo;
        if (SinkType.forType(sinkType) == SinkType.HIVE) {
            sinkInfo = createHiveSinkInfo((HiveSinkResponse) sinkResponse, sinkFields);
        } else if (SinkType.forType(sinkType) == SinkType.KAFKA) {
            sinkInfo = createKafkaSinkInfo(sourceResponse, (KafkaSinkResponse) sinkResponse, sinkFields);
        } else if (SinkType.forType(sinkType) == SinkType.CLICKHOUSE) {
            sinkInfo = createClickhouseSinkInfo((ClickHouseSinkResponse) sinkResponse, sinkFields);
        } else {
            throw new RuntimeException(String.format("Unsupported SinkType {%s}", sinkType));
        }
        return sinkInfo;
    }

    private static ClickHouseSinkInfo createClickhouseSinkInfo(ClickHouseSinkResponse sinkResponse,
            List<FieldInfo> sinkFields) {
        if (StringUtils.isEmpty(sinkResponse.getJdbcUrl())) {
            throw new RuntimeException(String.format("ClickHouse={%s} server url cannot be empty", sinkResponse));
        } else if (CollectionUtils.isEmpty(sinkResponse.getFieldList())) {
            throw new RuntimeException(String.format("ClickHouse={%s} fields cannot be empty", sinkResponse));
        } else if (StringUtils.isEmpty(sinkResponse.getTableName())) {
            throw new RuntimeException(String.format("ClickHouse={%s} table name cannot be empty", sinkResponse));
        } else if (StringUtils.isEmpty(sinkResponse.getDatabaseName())) {
            throw new RuntimeException(String.format("ClickHouse={%s} database name cannot be empty", sinkResponse));
        }
        if (sinkResponse.getDistributedTable() == null) {
            throw new RuntimeException(String.format("ClickHouse={%s} distribute cannot be empty", sinkResponse));
        }

        ClickHouseSinkInfo.PartitionStrategy partitionStrategy;
        if (PartitionStrategy.BALANCE.name().equalsIgnoreCase(sinkResponse.getPartitionStrategy())) {
            partitionStrategy = PartitionStrategy.BALANCE;
        } else if (PartitionStrategy.HASH.name().equalsIgnoreCase(sinkResponse.getPartitionStrategy())) {
            partitionStrategy = PartitionStrategy.HASH;
        } else if (PartitionStrategy.RANDOM.name().equalsIgnoreCase(sinkResponse.getPartitionStrategy())) {
            partitionStrategy = PartitionStrategy.RANDOM;
        } else {
            partitionStrategy = PartitionStrategy.RANDOM;
        }

        return new ClickHouseSinkInfo(sinkResponse.getJdbcUrl(), sinkResponse.getDatabaseName(),
                sinkResponse.getTableName(), sinkResponse.getUsername(), sinkResponse.getPassword(),
                sinkResponse.getDistributedTable(), partitionStrategy, sinkResponse.getPartitionKey(),
                sinkFields.toArray(new FieldInfo[0]), sinkResponse.getKeyFieldNames(),
                sinkResponse.getFlushInterval(), sinkResponse.getFlushRecordNumber(),
                sinkResponse.getWriteMaxRetryTimes());
    }

    private static KafkaSinkInfo createKafkaSinkInfo(SourceResponse sourceResponse, KafkaSinkResponse sinkResponse,
            List<FieldInfo> sinkFields) {
        String addressUrl = sinkResponse.getAddress();
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
            throw new RuntimeException(String.format("HiveSink={%s} server url cannot be empty", hiveInfo));
        }
        if (CollectionUtils.isEmpty(hiveInfo.getFieldList())) {
            throw new RuntimeException(String.format("HiveSink={%s} fields cannot be empty", hiveInfo));
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

        // The primary partition field, in Sink must be HiveTimePartitionInfo
        List<HivePartitionInfo> partitionList = new ArrayList<>();
        String primary = hiveInfo.getPrimaryPartition();
        if (StringUtils.isNotEmpty(primary)) {
            // Hive partitions are by day, hour, and minute
            String unit = hiveInfo.getPartitionUnit();
            HiveTimePartitionInfo timePartitionInfo = new HiveTimePartitionInfo(
                    primary, PARTITION_TIME_FORMAT_MAP.get(unit));
            partitionList.add(timePartitionInfo);
        }
        // For the secondary partition field, the sink is temporarily encapsulated as HiveFieldPartitionInfo,
        // TODO the type be set according to the type of the field itself.
        if (StringUtils.isNotEmpty(hiveInfo.getSecondaryPartition())) {
            partitionList.add(new HiveSinkInfo.HiveFieldPartitionInfo(hiveInfo.getSecondaryPartition()));
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

}
