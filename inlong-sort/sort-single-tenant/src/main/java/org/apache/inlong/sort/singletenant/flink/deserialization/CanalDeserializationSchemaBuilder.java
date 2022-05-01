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

import static org.apache.flink.table.types.utils.DataTypeUtils.validateInputDataType;
import static org.apache.inlong.sort.singletenant.flink.utils.CommonUtils.convertDateToStringFormatInfo;
import static org.apache.inlong.sort.singletenant.flink.utils.CommonUtils.convertFieldInfosToDataType;
import static org.apache.inlong.sort.singletenant.flink.utils.CommonUtils.extractFormatInfos;
import static org.apache.inlong.sort.singletenant.flink.utils.CommonUtils.getProducedFieldInfos;
import static org.apache.inlong.sort.singletenant.flink.utils.CommonUtils.getTimestampFormatStandard;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.apache.inlong.sort.formats.common.BooleanFormatInfo;
import org.apache.inlong.sort.formats.common.LongFormatInfo;
import org.apache.inlong.sort.formats.common.StringFormatInfo;
import org.apache.inlong.sort.formats.json.canal.CanalJsonDecodingFormat;
import org.apache.inlong.sort.formats.json.canal.CanalJsonDecodingFormat.ReadableMetadata;
import org.apache.inlong.sort.protocol.BuiltInFieldInfo;
import org.apache.inlong.sort.protocol.BuiltInFieldInfo.BuiltInField;
import org.apache.inlong.sort.protocol.FieldInfo;
import org.apache.inlong.sort.protocol.deserialization.CanalDeserializationInfo;
import org.apache.inlong.sort.singletenant.flink.utils.CommonUtils;

public class CanalDeserializationSchemaBuilder {

    public static DeserializationSchema<Row> build(
            FieldInfo[] fieldInfos,
            CanalDeserializationInfo deserializationInfo
    ) throws IOException, ClassNotFoundException {
        String timestampFormatStandard = deserializationInfo.getTimestampFormatStandard();
        CanalJsonDecodingFormat canalJsonDecodingFormat = new CanalJsonDecodingFormat(
                deserializationInfo.getDatabase(),
                deserializationInfo.getTable(),
                deserializationInfo.isIgnoreParseErrors(),
                getTimestampFormatStandard(timestampFormatStandard)
        );

        // Extract required metadata
        FieldInfo[] metadataFieldInfos = getMetadataFieldInfos(fieldInfos);
        List<String> requiredMetadataKeys = Arrays.stream(metadataFieldInfos)
                .map(FieldInfo::getName)
                .collect(Collectors.toList());
        canalJsonDecodingFormat.applyReadableMetadata(requiredMetadataKeys);

        FieldInfo[] originPhysicalFieldInfos = CommonUtils.extractNonBuiltInFieldInfos(fieldInfos, false);
        FieldInfo[] convertedPhysicalFieldInfos = convertDateToStringFormatInfo(originPhysicalFieldInfos);
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
                convertFieldInfosToDataType(convertedPhysicalFieldInfos)
        );

        return wrapCanalDeserializationSchema(canalSchema, originPhysicalFieldInfos, convertedPhysicalFieldInfos);
    }

    private static DeserializationSchema<Row> wrapCanalDeserializationSchema(
            DeserializationSchema<RowData> canalSchema,
            FieldInfo[] originPhysicalFieldInfos,
            FieldInfo[] convertedPhysicalFieldInfos
    ) {

        RowDataToRowDeserializationSchemaWrapper rowDataToRowSchema = new RowDataToRowDeserializationSchemaWrapper(
                canalSchema,
                getProducedFieldInfos(convertedPhysicalFieldInfos));
        return new CustomDateFormatDeserializationSchemaWrapper(
                rowDataToRowSchema,
                extractFormatInfos(getProducedFieldInfos(originPhysicalFieldInfos)));
    }

    public static FieldInfo[] getMetadataFieldInfos(FieldInfo[] fieldInfos) {
        List<FieldInfo> metadataFieldInfos = new ArrayList<>();
        Arrays.stream(fieldInfos)
                .filter(fieldInfo -> fieldInfo instanceof BuiltInFieldInfo)
                .forEach(fieldInfo -> {
                    BuiltInFieldInfo builtInFieldInfo = (BuiltInFieldInfo) fieldInfo;
                    BuiltInField builtInField = builtInFieldInfo.getBuiltInField();
                    switch (builtInField) {
                        case MYSQL_METADATA_DATABASE:
                            metadataFieldInfos.add(new FieldInfo(
                                    ReadableMetadata.DATABASE.getKey(), StringFormatInfo.INSTANCE));
                            break;
                        case MYSQL_METADATA_TABLE:
                            metadataFieldInfos.add(new FieldInfo(
                                    ReadableMetadata.TABLE.getKey(), StringFormatInfo.INSTANCE));
                            break;
                        case MYSQL_METADATA_EVENT_TIME:
                            metadataFieldInfos.add(new FieldInfo(
                                    ReadableMetadata.EVENT_TIMESTAMP.getKey(), LongFormatInfo.INSTANCE));
                            break;
                        case MYSQL_METADATA_IS_DDL:
                            metadataFieldInfos.add(new FieldInfo(
                                    ReadableMetadata.IS_DDL.getKey(), BooleanFormatInfo.INSTANCE));
                            break;
                        case MYSQL_METADATA_EVENT_TYPE:
                        case MYSQL_METADATA_DATA:
                            break;
                        default:
                            throw new IllegalArgumentException(
                                    "Unsupported builtin field '" + builtInField + "' in debezium deserialization");
                    }
                });

        return metadataFieldInfos.toArray(new FieldInfo[0]);
    }
}
