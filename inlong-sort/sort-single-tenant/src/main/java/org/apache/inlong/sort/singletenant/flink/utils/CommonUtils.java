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

package org.apache.inlong.sort.singletenant.flink.utils;

import org.apache.avro.Schema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.TableSchema.Builder;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.inlong.sort.formats.base.TableFormatUtils;
import org.apache.inlong.sort.formats.common.FormatInfo;
import org.apache.inlong.sort.formats.common.RowFormatInfo;
import org.apache.inlong.sort.formats.common.TypeInfo;
import org.apache.inlong.sort.protocol.FieldInfo;
import org.apache.inlong.sort.protocol.sink.SinkInfo;

import static org.apache.flink.formats.avro.typeutils.AvroSchemaConverter.convertToSchema;
import static org.apache.inlong.sort.formats.base.TableFormatUtils.deriveLogicalType;

public class CommonUtils {

    public static TableSchema getTableSchema(SinkInfo sinkInfo) {
        TableSchema.Builder builder = new Builder();
        FieldInfo[] fieldInfos = sinkInfo.getFields();

        for (FieldInfo fieldInfo : fieldInfos) {
            builder.field(
                    fieldInfo.getName(),
                    TableFormatUtils.getType(fieldInfo.getFormatInfo().getTypeInfo()));
        }

        return builder.build();
    }

    public static org.apache.flink.api.java.typeutils.RowTypeInfo convertFieldInfosToRowTypeInfo(
            FieldInfo[] fieldInfos
    ) {
        int length = fieldInfos.length;
        TypeInformation<?>[] typeInformationArray = new TypeInformation[length];
        String[] fieldNames = new String[length];
        for (int i = 0; i < length; i++) {
            FieldInfo fieldInfo = fieldInfos[i];
            fieldNames[i] = fieldInfo.getName();

            TypeInfo typeInfo = fieldInfo.getFormatInfo().getTypeInfo();
            typeInformationArray[i] = TableFormatUtils.getType(typeInfo);
        }

        return new org.apache.flink.api.java.typeutils.RowTypeInfo(typeInformationArray, fieldNames);
    }

    public static String buildAvroRecordSchemaInJson(FieldInfo[] fieldInfos) {
        int fieldLength = fieldInfos.length;
        String[] fieldNames = new String[fieldLength];
        FormatInfo[] fieldFormatInfos = new FormatInfo[fieldLength];
        for (int i = 0; i < fieldLength; i++) {
            fieldNames[i] = fieldInfos[i].getName();
            fieldFormatInfos[i] = fieldInfos[i].getFormatInfo();
        }

        RowFormatInfo rowFormatInfo = new RowFormatInfo(fieldNames, fieldFormatInfos);
        LogicalType logicalType = deriveLogicalType(rowFormatInfo);
        Schema schema = convertToSchema(logicalType);

        if (schema.isUnion()) {
            return schema.getTypes().get(1).toString();
        }
        return schema.toString();
    }
}
