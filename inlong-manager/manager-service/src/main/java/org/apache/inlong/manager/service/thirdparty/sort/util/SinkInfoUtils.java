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

import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.inlong.manager.common.enums.Constant;
import org.apache.inlong.manager.common.enums.SinkType;
import org.apache.inlong.manager.common.enums.SourceType;
import org.apache.inlong.manager.common.pojo.sink.SinkFieldResponse;
import org.apache.inlong.manager.common.pojo.sink.SinkResponse;
import org.apache.inlong.manager.common.pojo.sink.hive.HiveSinkResponse;
import org.apache.inlong.manager.common.pojo.sink.kafka.KafkaSinkResponse;
import org.apache.inlong.manager.common.pojo.source.SourceResponse;
import org.apache.inlong.manager.common.pojo.stream.InlongStreamInfo;
import org.apache.inlong.sort.formats.common.FormatInfo;
import org.apache.inlong.sort.formats.common.StringFormatInfo;
import org.apache.inlong.sort.formats.common.TimestampFormatInfo;
import org.apache.inlong.sort.protocol.BuiltInFieldInfo;
import org.apache.inlong.sort.protocol.BuiltInFieldInfo.BuiltInField;
import org.apache.inlong.sort.protocol.FieldInfo;
import org.apache.inlong.sort.protocol.serialization.SerializationInfo;
import org.apache.inlong.sort.protocol.sink.HiveSinkInfo;
import org.apache.inlong.sort.protocol.sink.HiveSinkInfo.HiveFileFormat;
import org.apache.inlong.sort.protocol.sink.HiveSinkInfo.HivePartitionInfo;
import org.apache.inlong.sort.protocol.sink.HiveSinkInfo.HiveTimePartitionInfo;
import org.apache.inlong.sort.protocol.sink.KafkaSinkInfo;
import org.apache.inlong.sort.protocol.sink.SinkInfo;

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

    public static SinkInfo createSinkInfo(SinkResponse sinkResponse) {
        return createSinkInfo(null, null, sinkResponse);
    }

    public static SinkInfo createSinkInfo(SourceResponse sourceResponse, SinkResponse sinkResponse) {
        return createSinkInfo(null, sourceResponse, sinkResponse);
    }

    public static SinkInfo createSinkInfo(InlongStreamInfo inlongStreamInfo, SourceResponse sourceResponse,
            SinkResponse sinkResponse) {
        String sinkType = sinkResponse.getSinkType();
        if (SinkType.forType(sinkType) == SinkType.HIVE) {
            return createHiveSinkInfo(sourceResponse, (HiveSinkResponse) sinkResponse);
        } else if (SinkType.forType(sinkType) == SinkType.KAFKA) {
            return createKafkaSinkInfo(inlongStreamInfo, sourceResponse, (KafkaSinkResponse) sinkResponse);
        } else {
            //todo clickhouse and iceberg is wait to support
            throw new RuntimeException(
                    String.format("SinkType:{} not support in CreateSortConfigListener", sinkType));
        }
    }

    private static KafkaSinkInfo createKafkaSinkInfo(InlongStreamInfo inlongStreamInfo, SourceResponse sourceResponse,
            KafkaSinkResponse kafkaSinkResponse) {
        List<FieldInfo> fieldInfoList = Lists.newArrayList();
        if (SourceType.DATABASE_MIGRATION.getType().equalsIgnoreCase(sourceResponse.getSourceType())) {
            fieldInfoList.add(new BuiltInFieldInfo("DATABASE_MIGRATION", StringFormatInfo.INSTANCE, BuiltInField.DATA));
        }else {
            fieldInfoList = getSinkFields(kafkaSinkResponse.getFieldList(), null);
        }
        String addressUrl = kafkaSinkResponse.getAddress();
        String topicName = kafkaSinkResponse.getTopicName();
        SerializationInfo serializationInfo = SerializationUtils.createSerializationInfo(sourceResponse,
                kafkaSinkResponse, inlongStreamInfo);
        return new KafkaSinkInfo(fieldInfoList.toArray(new FieldInfo[0]), addressUrl, topicName, serializationInfo);
    }

    private static HiveSinkInfo createHiveSinkInfo(SourceResponse sourceResponse, HiveSinkResponse hiveInfo) {
        if (hiveInfo.getJdbcUrl() == null) {
            throw new RuntimeException(String.format("hiveSink={} server url cannot be empty", hiveInfo));
        }
        if (CollectionUtils.isEmpty(hiveInfo.getFieldList())) {
            throw new RuntimeException(String.format("hiveSink={} fields cannot be empty", hiveInfo));
        }
        // Use the field separator in Hive, the default is TextFile
        Character separator = (char) Integer.parseInt(hiveInfo.getDataSeparator());
        HiveFileFormat fileFormat;
        String format = hiveInfo.getFileFormat();

        if (Constant.FILE_FORMAT_ORC.equalsIgnoreCase(format)) {
            fileFormat = new HiveSinkInfo.OrcFileFormat(1000);
        } else if (Constant.FILE_FORMAT_SEQUENCE.equalsIgnoreCase(format)) {
            fileFormat = new HiveSinkInfo.SequenceFileFormat(separator, 100);
        } else if (Constant.FILE_FORMAT_PARQUET.equalsIgnoreCase(format)) {
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

        // dataPath = hdfsUrl + / + warehouseDir + / + dbName + .db/ + tableName
        StringBuilder dataPathBuilder = new StringBuilder();
        String hdfsUrl = hiveInfo.getHdfsDefaultFs();
        String warehouseDir = hiveInfo.getWarehouseDir();
        if (hdfsUrl.endsWith("/")) {
            dataPathBuilder.append(hdfsUrl, 0, hdfsUrl.length() - 1);
        } else {
            dataPathBuilder.append(hdfsUrl);
        }
        if (warehouseDir.endsWith("/")) {
            dataPathBuilder.append(warehouseDir, 0, warehouseDir.length() - 1);
        } else {
            dataPathBuilder.append(warehouseDir);
        }
        String dataPath = dataPathBuilder.append("/").append(hiveInfo.getDbName())
                .append(".db/").append(hiveInfo.getTableName()).toString();

        // Get the sink field, if there is no partition field in the source field, add the partition field to the end
        List<FieldInfo> fieldInfoList = Lists.newArrayList();
        if (SourceType.DATABASE_MIGRATION.getType().equalsIgnoreCase(sourceResponse.getSourceType())) {
            fieldInfoList.add(new BuiltInFieldInfo("DATABASE_MIGRATION", StringFormatInfo.INSTANCE, BuiltInField.DATA));
        }else {
            fieldInfoList = getSinkFields(hiveInfo.getFieldList(), hiveInfo.getPrimaryPartition());
        }

        return new HiveSinkInfo(fieldInfoList.toArray(new FieldInfo[0]), hiveInfo.getJdbcUrl(),
                hiveInfo.getDbName(), hiveInfo.getTableName(), hiveInfo.getUsername(), hiveInfo.getPassword(),
                dataPath, partitionList.toArray(new HiveSinkInfo.HivePartitionInfo[0]), fileFormat);
    }

    private static List<FieldInfo> getSinkFields(List<SinkFieldResponse> fieldList, String partitionField) {
        boolean duplicate = false;
        List<FieldInfo> fieldInfoList = new ArrayList<>();
        for (SinkFieldResponse field : fieldList) {
            String fieldName = field.getFieldName();
            if (fieldName.equals(partitionField)) {
                duplicate = true;
            }

            FormatInfo formatInfo = SortFieldFormatUtils.convertFieldFormat(field.getFieldType().toLowerCase());
            FieldInfo fieldInfo = new FieldInfo(fieldName, formatInfo);
            fieldInfoList.add(fieldInfo);
        }

        // There is no partition field in the ordinary field, you need to add the partition field to the end
        if (!duplicate && StringUtils.isNotEmpty(partitionField)) {
            FieldInfo fieldInfo = new FieldInfo(partitionField, new TimestampFormatInfo("MILLIS"));
            fieldInfoList.add(0, fieldInfo);
        }
        return fieldInfoList;
    }

}
