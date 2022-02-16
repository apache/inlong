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
import org.apache.flink.formats.json.debezium.DebeziumJsonDecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.apache.inlong.sort.protocol.FieldInfo;
import org.apache.inlong.sort.protocol.deserialization.DebeziumDeserializationInfo;

import java.io.IOException;

import static org.apache.flink.table.types.utils.DataTypeUtils.validateInputDataType;
import static org.apache.inlong.sort.singletenant.flink.utils.CommonUtils.convertDateToStringFormatInfo;
import static org.apache.inlong.sort.singletenant.flink.utils.CommonUtils.convertFieldInfosToDataType;
import static org.apache.inlong.sort.singletenant.flink.utils.CommonUtils.extractFormatInfos;
import static org.apache.inlong.sort.singletenant.flink.utils.CommonUtils.getTimestampFormatStandard;

public class DebeziumDeserializationSchemaBuilder {

    public static DeserializationSchema<Row> build(
            FieldInfo[] fieldInfos,
            DebeziumDeserializationInfo deserializationInfo
    ) throws IOException, ClassNotFoundException {
        TimestampFormat timestampFormat = getTimestampFormatStandard(deserializationInfo.getTimestampFormatStandard());
        DebeziumJsonDecodingFormat debeziumJsonDecodingFormat =
                new DebeziumJsonDecodingFormat(false, deserializationInfo.isIgnoreParseErrors(), timestampFormat);

        FieldInfo[] convertedInputFields = convertDateToStringFormatInfo(fieldInfos);
        DeserializationSchema<RowData> debeziumSchema = debeziumJsonDecodingFormat.createRuntimeDecoder(
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

        RowDataToRowDeserializationSchemaWrapper rowDataToRowSchema =
                new RowDataToRowDeserializationSchemaWrapper(debeziumSchema, convertedInputFields);
        return new CustomDateFormatDeserializationSchemaWrapper(rowDataToRowSchema, extractFormatInfos(fieldInfos));
    }

}
