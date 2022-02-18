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

package org.apache.inlong.sort.singletenant.flink.deserialization;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.formats.json.canal.CanalJsonDecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.apache.inlong.sort.formats.common.ArrayFormatInfo;
import org.apache.inlong.sort.formats.common.IntFormatInfo;
import org.apache.inlong.sort.formats.common.LocalZonedTimestampFormatInfo;
import org.apache.inlong.sort.formats.common.MapFormatInfo;
import org.apache.inlong.sort.formats.common.StringFormatInfo;
import org.apache.inlong.sort.protocol.FieldInfo;
import org.apache.inlong.sort.protocol.deserialization.CanalDeserializationInfo;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.apache.flink.table.types.utils.DataTypeUtils.validateInputDataType;
import static org.apache.inlong.sort.singletenant.flink.utils.CommonUtils.convertDateToStringFormatInfo;
import static org.apache.inlong.sort.singletenant.flink.utils.CommonUtils.convertFieldInfosToDataType;
import static org.apache.inlong.sort.singletenant.flink.utils.CommonUtils.extractFormatInfos;
import static org.apache.inlong.sort.singletenant.flink.utils.CommonUtils.getTimestampFormatStandard;

public class CanalDeserializationSchemaBuilder {

    private static final List<String> ALL_SUPPORTED_METADATA_KEYS =
            Arrays.asList("database", "table", "sql-type", "pk-names", "ingestion-timestamp", "event-timestamp");

    public static DeserializationSchema<Row> build(
            FieldInfo[] fieldInfos,
            CanalDeserializationInfo deserializationInfo
    ) throws IOException, ClassNotFoundException {
        String timestampFormatStandard = deserializationInfo.getTimestampFormatStandard();
        boolean includeMetadata = deserializationInfo.isIncludeMetadata();
        CanalJsonDecodingFormat canalJsonDecodingFormat = createCanalJsonDecodingFormat(
                deserializationInfo.getDatabase(),
                deserializationInfo.getTable(),
                deserializationInfo.isIgnoreParseErrors(),
                timestampFormatStandard,
                includeMetadata
        );

        FieldInfo[] convertedInputFields = convertDateToStringFormatInfo(fieldInfos);
        DeserializationSchema<RowData> canalSchema = canalJsonDecodingFormat.createRuntimeDecoder(
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
                convertFieldInfosToDataType(convertedInputFields)
        );

        return wrapCanalDeserializationSchema(canalSchema, includeMetadata, fieldInfos, timestampFormatStandard);
    }

    private static DeserializationSchema<Row> wrapCanalDeserializationSchema(
            DeserializationSchema<RowData> canalSchema,
            boolean includeMetadata,
            FieldInfo[] origFieldInfos,
            String timestampFormatStandard
    ) throws IOException, ClassNotFoundException {
        FieldInfo[] allFields;
        if (includeMetadata) {
            allFields = new FieldInfo[origFieldInfos.length + ALL_SUPPORTED_METADATA_KEYS.size()];
            System.arraycopy(origFieldInfos, 0, allFields, 0, origFieldInfos.length);
            FieldInfo[] metadataFields = buildMetadataFields(timestampFormatStandard);
            System.arraycopy(metadataFields, 0, allFields, origFieldInfos.length, metadataFields.length);
        } else {
            allFields = origFieldInfos;
        }

        FieldInfo[] convertedAllFields = convertDateToStringFormatInfo(allFields);
        RowDataToRowDeserializationSchemaWrapper rowDataToRowSchema =
                new RowDataToRowDeserializationSchemaWrapper(canalSchema, convertedAllFields);
        return new CustomDateFormatDeserializationSchemaWrapper(rowDataToRowSchema, extractFormatInfos(allFields));
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

    private static FieldInfo[] buildMetadataFields(String timestampStandard) {
        return new FieldInfo[]{
                new FieldInfo(ALL_SUPPORTED_METADATA_KEYS.get(0), StringFormatInfo.INSTANCE),
                new FieldInfo(ALL_SUPPORTED_METADATA_KEYS.get(1), StringFormatInfo.INSTANCE),
                new FieldInfo(ALL_SUPPORTED_METADATA_KEYS.get(2),
                        new MapFormatInfo(StringFormatInfo.INSTANCE, IntFormatInfo.INSTANCE)),
                new FieldInfo(ALL_SUPPORTED_METADATA_KEYS.get(3), new ArrayFormatInfo(StringFormatInfo.INSTANCE)),
                new FieldInfo(ALL_SUPPORTED_METADATA_KEYS.get(4), new LocalZonedTimestampFormatInfo(timestampStandard)),
                new FieldInfo(ALL_SUPPORTED_METADATA_KEYS.get(5), new LocalZonedTimestampFormatInfo(timestampStandard))
        };
    }

}
