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

package org.apache.inlong.sort.flink.hive;

import com.google.common.annotations.VisibleForTesting;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.apache.inlong.sort.configuration.Configuration;
import org.apache.inlong.sort.flink.hive.formats.orc.OrcBulkWriterFactory;
import org.apache.inlong.sort.flink.hive.formats.parquet.ParquetRowWriterBuilder;
import org.apache.inlong.sort.flink.hive.formats.TextRowWriter;
import org.apache.inlong.sort.formats.base.TableFormatUtils;
import org.apache.inlong.sort.protocol.FieldInfo;
import org.apache.inlong.sort.protocol.sink.HiveSinkInfo;
import org.apache.inlong.sort.protocol.sink.HiveSinkInfo.HiveFileFormat;
import org.apache.inlong.sort.protocol.sink.HiveSinkInfo.HivePartitionInfo;
import org.apache.inlong.sort.protocol.sink.HiveSinkInfo.OrcFileFormat;
import org.apache.inlong.sort.protocol.sink.HiveSinkInfo.ParquetFileFormat;
import org.apache.inlong.sort.protocol.sink.HiveSinkInfo.TextFileFormat;

/**
 * Helper for hive sink.
 */
public class HiveSinkHelper {

    public static BulkWriter.Factory<Row> createBulkWriterFactory(
            HiveSinkInfo hiveSinkInfo,
            Configuration config) {
        final FieldInfo[] fieldInfos = getNonPartitionFields(hiveSinkInfo);
        String[] fieldNames = getFieldNames(fieldInfos);
        LogicalType[] fieldTypes = getFieldLogicalTypes(fieldInfos);
        RowType rowType = RowType.of(fieldTypes, fieldNames);
        HiveFileFormat hiveFileFormat = hiveSinkInfo.getHiveFileFormat();
        if (hiveFileFormat instanceof ParquetFileFormat) {
            return ParquetRowWriterBuilder.createWriterFactory(
                    rowType, (ParquetFileFormat) hiveFileFormat);
        } else if (hiveFileFormat instanceof TextFileFormat) {
            return new TextRowWriter.Factory((TextFileFormat) hiveFileFormat, fieldTypes, config);
        } else if (hiveFileFormat instanceof OrcFileFormat) {
            return OrcBulkWriterFactory.createWriterFactory(rowType, fieldTypes, config);
        } else {
            throw new IllegalArgumentException("Unsupported hive file format " + hiveFileFormat.getClass().getName());
        }
    }

    @VisibleForTesting
    static FieldInfo[] getNonPartitionFields(HiveSinkInfo hiveSinkInfo) {
        final List<String> partitionFields =
                Arrays.stream(hiveSinkInfo.getPartitions())
                        .map(HivePartitionInfo::getFieldName)
                        .collect(Collectors.toList());
        return Arrays.stream(hiveSinkInfo.getFields())
                .filter(fieldInfo -> !partitionFields.contains(fieldInfo.getName()))
                .toArray(FieldInfo[]::new);
    }

    @VisibleForTesting
    static String[] getFieldNames(FieldInfo[] fieldInfos) {
        return Arrays.stream(fieldInfos)
                .map(FieldInfo::getName)
                .toArray(String[]::new);
    }

    @VisibleForTesting
    static LogicalType[] getFieldLogicalTypes(FieldInfo[] fieldInfos) {
        return Arrays.stream(fieldInfos)
                .map(fieldInfo -> TableFormatUtils.deriveLogicalType(fieldInfo.getFormatInfo()))
                .toArray(LogicalType[]::new);
    }
}
