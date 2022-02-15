/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.inlong.sort.singletenant.flink.deserialization;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.formats.json.canal.CanalJsonDecodingFormat;
import org.apache.flink.formats.json.debezium.DebeziumJsonDecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.inlong.sort.protocol.FieldInfo;
import org.apache.inlong.sort.protocol.deserialization.CanalDeserializationInfo;
import org.apache.inlong.sort.protocol.deserialization.DebeziumDeserializationInfo;
import org.apache.inlong.sort.protocol.deserialization.RowDataDeserializationInfo;

import java.util.Arrays;
import java.util.List;

import static org.apache.flink.table.types.utils.DataTypeUtils.validateInputDataType;
import static org.apache.inlong.sort.singletenant.flink.utils.CommonUtils.convertFieldInfosToDataType;
import static org.apache.inlong.sort.singletenant.flink.utils.CommonUtils.getTimestampFormatStandard;

public class RowDataDeserializationSchemaFactory {

    private static final List<String> ALL_SUPPORTED_METADATA_KEYS =
            Arrays.asList("database", "table", "sql-type", "pk-names", "ingestion-timestamp", "event-timestamp");

    public static DeserializationSchema<RowData> build(
            FieldInfo[] fieldInfos,
            RowDataDeserializationInfo deserializationInfo
    ) {
        if (deserializationInfo instanceof CanalDeserializationInfo) {
            return buildCanalDeserializationSchema(fieldInfos, (CanalDeserializationInfo) deserializationInfo);
        } else if (deserializationInfo instanceof DebeziumDeserializationInfo) {
            return buildDebeziumDeserializationSchema(fieldInfos, (DebeziumDeserializationInfo) deserializationInfo);
        }
        throw new IllegalArgumentException("Unsupported deserialization info: " + deserializationInfo);
    }

    private static DeserializationSchema<RowData> buildCanalDeserializationSchema(
            FieldInfo[] fieldInfos,
            CanalDeserializationInfo deserializationInfo
    ) {
        CanalJsonDecodingFormat canalJsonDecodingFormat = createCanalJsonDecodingFormat(
                deserializationInfo.getDatabase(),
                deserializationInfo.getTable(),
                deserializationInfo.isIgnoreParseErrors(),
                deserializationInfo.getTimestampFormatStandard(),
                deserializationInfo.isIncludeMetadata()
        );

        return canalJsonDecodingFormat.createRuntimeDecoder(
                new DynamicTableSource.Context() {
                    @Override
                    public <T> TypeInformation<T> createTypeInformation(DataType dataType) {
                        validateInputDataType(dataType);
                        return InternalTypeInfo.of(dataType.getLogicalType());
                    }

                    @Override
                    public DynamicTableSource.DataStructureConverter createDataStructureConverter(DataType dataType) {
                        return null;
                    }
                },
                convertFieldInfosToDataType(fieldInfos)
        );
    }

    private static DeserializationSchema<RowData> buildDebeziumDeserializationSchema(
            FieldInfo[] fieldInfos,
            DebeziumDeserializationInfo deserializationInfo
    ) {
        DebeziumJsonDecodingFormat debeziumJsonDecodingFormat = createDebeziumJsonDecodingFormat(
                false,
                deserializationInfo.isIgnoreParseErrors(),
                deserializationInfo.getTimestampFormatStandard()
        );

        return debeziumJsonDecodingFormat.createRuntimeDecoder(
                new DynamicTableSource.Context() {
                    @Override
                    public <T> TypeInformation<T> createTypeInformation(DataType dataType) {
                        validateInputDataType(dataType);
                        return InternalTypeInfo.of(dataType.getLogicalType());
                    }

                    @Override
                    public DynamicTableSource.DataStructureConverter createDataStructureConverter(DataType dataType) {
                        return null;
                    }
                },
                convertFieldInfosToDataType(fieldInfos)
        );
    }


    private static CanalJsonDecodingFormat createCanalJsonDecodingFormat(
            String database,
            String table,
            boolean ignoreParseErrors,
            String timestampFormatStandard,
            boolean includeMetadata
    ) {
        TimestampFormat timestampFormat = getTimestampFormatStandard(timestampFormatStandard);
        CanalJsonDecodingFormat canalJsonDecodingFormat =
                new CanalJsonDecodingFormat(database, table, ignoreParseErrors, timestampFormat);
        if (includeMetadata) {
            canalJsonDecodingFormat.applyReadableMetadata(ALL_SUPPORTED_METADATA_KEYS);
        }

        return canalJsonDecodingFormat;
    }

    private static DebeziumJsonDecodingFormat createDebeziumJsonDecodingFormat(
            boolean schemaInclude,
            boolean ignoreParseErrors,
            String timestampFormatStandard
    ) {
        TimestampFormat timestampFormat = getTimestampFormatStandard(timestampFormatStandard);
        return new DebeziumJsonDecodingFormat(schemaInclude, ignoreParseErrors, timestampFormat);
    }
}
