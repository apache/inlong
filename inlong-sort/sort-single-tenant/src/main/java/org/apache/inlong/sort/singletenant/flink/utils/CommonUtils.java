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

import com.google.common.annotations.VisibleForTesting;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.SqlTimeTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.TableSchema.Builder;
import org.apache.inlong.sort.formats.base.TableFormatUtils;
import org.apache.inlong.sort.formats.common.ArrayTypeInfo;
import org.apache.inlong.sort.formats.common.BooleanTypeInfo;
import org.apache.inlong.sort.formats.common.ByteTypeInfo;
import org.apache.inlong.sort.formats.common.DateTypeInfo;
import org.apache.inlong.sort.formats.common.DecimalTypeInfo;
import org.apache.inlong.sort.formats.common.DoubleTypeInfo;
import org.apache.inlong.sort.formats.common.FloatTypeInfo;
import org.apache.inlong.sort.formats.common.IntTypeInfo;
import org.apache.inlong.sort.formats.common.LongTypeInfo;
import org.apache.inlong.sort.formats.common.MapTypeInfo;
import org.apache.inlong.sort.formats.common.RowTypeInfo;
import org.apache.inlong.sort.formats.common.ShortTypeInfo;
import org.apache.inlong.sort.formats.common.StringTypeInfo;
import org.apache.inlong.sort.formats.common.TimeTypeInfo;
import org.apache.inlong.sort.formats.common.TimestampTypeInfo;
import org.apache.inlong.sort.formats.common.TypeInfo;
import org.apache.inlong.sort.protocol.FieldInfo;
import org.apache.inlong.sort.protocol.sink.SinkInfo;

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
            typeInformationArray[i] = getTypeInformationFromTypeInfo(typeInfo);
        }

        return new org.apache.flink.api.java.typeutils.RowTypeInfo(typeInformationArray, fieldNames);
    }

    @VisibleForTesting
    static TypeInformation<?> getTypeInformationFromTypeInfo(TypeInfo typeInfo) {
        if (typeInfo instanceof StringTypeInfo) {
            return BasicTypeInfo.STRING_TYPE_INFO;
        } else if (typeInfo instanceof BooleanTypeInfo) {
            return BasicTypeInfo.BOOLEAN_TYPE_INFO;
        } else if (typeInfo instanceof ByteTypeInfo) {
            return BasicTypeInfo.BYTE_TYPE_INFO;
        } else if (typeInfo instanceof ShortTypeInfo) {
            return BasicTypeInfo.SHORT_TYPE_INFO;
        } else if (typeInfo instanceof IntTypeInfo) {
            return BasicTypeInfo.INT_TYPE_INFO;
        } else if (typeInfo instanceof LongTypeInfo) {
            return BasicTypeInfo.LONG_TYPE_INFO;
        } else if (typeInfo instanceof FloatTypeInfo) {
            return BasicTypeInfo.FLOAT_TYPE_INFO;
        } else if (typeInfo instanceof DoubleTypeInfo) {
            return BasicTypeInfo.DOUBLE_TYPE_INFO;
        } else if (typeInfo instanceof DecimalTypeInfo) {
            return BasicTypeInfo.BIG_DEC_TYPE_INFO;
        } else if (typeInfo instanceof TimeTypeInfo) {
            return SqlTimeTypeInfo.TIME;
        } else if (typeInfo instanceof DateTypeInfo) {
            return SqlTimeTypeInfo.DATE;
        } else if (typeInfo instanceof TimestampTypeInfo) {
            return SqlTimeTypeInfo.TIMESTAMP;
        } else if (typeInfo instanceof ArrayTypeInfo) {
            ArrayTypeInfo arrayTypeInfo = (ArrayTypeInfo) typeInfo;
            return ObjectArrayTypeInfo.getInfoFor(getTypeInformationFromTypeInfo(arrayTypeInfo.getElementTypeInfo()));
        } else if (typeInfo instanceof MapTypeInfo) {
            MapTypeInfo mapTypeInfo = (MapTypeInfo) typeInfo;
            return new org.apache.flink.api.java.typeutils.MapTypeInfo<>(
                    getTypeInformationFromTypeInfo(mapTypeInfo.getKeyTypeInfo()),
                    getTypeInformationFromTypeInfo(mapTypeInfo.getValueTypeInfo())
            );
        } else if (typeInfo instanceof RowTypeInfo) {
            RowTypeInfo rowTypeInfo = (RowTypeInfo) typeInfo;
            TypeInfo[] fieldTypeInfos = rowTypeInfo.getFieldTypeInfos();
            int length = fieldTypeInfos.length;
            TypeInformation<?>[] typeInformations = new TypeInformation[length];
            for (int i = 0; i < length; i++) {
                typeInformations[i] = getTypeInformationFromTypeInfo(fieldTypeInfos[i]);
            }
            return new org.apache.flink.api.java.typeutils.RowTypeInfo(typeInformations, rowTypeInfo.getFieldNames());
        }

        throw new IllegalArgumentException("Unsupported type info " + typeInfo);
    }
}
