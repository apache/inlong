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

import java.util.ArrayList;
import java.util.List;
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
        String[] fieldNames = getFieldNames(hiveSinkInfo).toArray(new String[0]);
        LogicalType[] fieldTypes = getFieldLogicalTypes(hiveSinkInfo).toArray(new LogicalType[0]);
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

    private static List<String> getFieldNames(HiveSinkInfo hiveSinkInfo) {
        FieldInfo[] fieldInfos = hiveSinkInfo.getFields();
        List<String> fieldNames = new ArrayList<>();
        for (FieldInfo fieldInfo : fieldInfos) {
            fieldNames.add(fieldInfo.getName());
        }

        return fieldNames;
    }

    private static List<LogicalType> getFieldLogicalTypes(HiveSinkInfo hiveSinkInfo) {
        FieldInfo[] fieldInfos = hiveSinkInfo.getFields();
        List<LogicalType> fieldLogicalTypes = new ArrayList<>();
        for (FieldInfo fieldInfo : fieldInfos) {
            fieldLogicalTypes.add(TableFormatUtils.deriveLogicalType(fieldInfo.getFormatInfo()));
        }

        return fieldLogicalTypes;
    }
}
