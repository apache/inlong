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

package org.apache.inlong.sort.singletenant.flink.serialization;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.inlong.sort.formats.common.ArrayFormatInfo;
import org.apache.inlong.sort.formats.common.BooleanFormatInfo;
import org.apache.inlong.sort.formats.common.ByteFormatInfo;
import org.apache.inlong.sort.formats.common.DateFormatInfo;
import org.apache.inlong.sort.formats.common.DecimalTypeInfo;
import org.apache.inlong.sort.formats.common.DoubleFormatInfo;
import org.apache.inlong.sort.formats.common.FloatTypeInfo;
import org.apache.inlong.sort.formats.common.FormatInfo;
import org.apache.inlong.sort.formats.common.IntFormatInfo;
import org.apache.inlong.sort.formats.common.LongFormatInfo;
import org.apache.inlong.sort.formats.common.MapFormatInfo;
import org.apache.inlong.sort.formats.common.RowFormatInfo;
import org.apache.inlong.sort.formats.common.ShortFormatInfo;
import org.apache.inlong.sort.formats.common.StringFormatInfo;
import org.apache.inlong.sort.formats.common.TimeFormatInfo;
import org.apache.inlong.sort.formats.common.TimestampFormatInfo;
import org.apache.inlong.sort.protocol.FieldInfo;
import org.apache.inlong.sort.protocol.serialization.AvroSerializationInfo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AvroUtils {

    private static final String TYPE = "type";
    private static final String NAME = "name";
    private static final String RECORD_FIELDS = "fields";
    private static final String ARRAY_ITEMS = "items";
    private static final String MAP_VALUES = "values";

    private static final String BOOLEAN_TYPE = "boolean";
    private static final String INT_TYPE = "int";
    private static final String LONG_TYPE = "long";
    private static final String FLOAT_TYPE = "float";
    private static final String DOUBLE_TYPE = "double";
    private static final String STRING_TYPE = "string";
    private static final String ARRAY_TYPE = "array";
    private static final String MAP_TYPE = "map";
    private static final String RECORD_TYPE = "record";

    public static String buildAvroRecordSchemaInJson(
            FieldInfo[] fieldInfos,
            AvroSerializationInfo avroSerializationInfo
    ) throws JsonProcessingException {
        int fieldLength = fieldInfos.length;
        String[] fieldNames = new String[fieldLength];
        FormatInfo[] fieldFormatInfos = new FormatInfo[fieldLength];
        for (int i = 0; i < fieldLength; i++) {
            fieldNames[i] = fieldInfos[i].getName();
            fieldFormatInfos[i] = fieldInfos[i].getFormatInfo();
        }

        Map<String, Object> resultMap =
                convertFormatInfosToMap(avroSerializationInfo.getRecordName(), fieldNames, fieldFormatInfos);

        return new ObjectMapper().writeValueAsString(resultMap);
    }

    private static Map<String, Object> convertFormatInfosToMap(
            String recordName,
            String[] fieldNames,
            FormatInfo[] formatInfos
    ) {
        Map<String, Object> resultMap = new HashMap<>();
        resultMap.put(TYPE, RECORD_TYPE);
        resultMap.put(NAME, recordName);

        int fieldLength = fieldNames.length;
        List<Map<String, Object>> fields = new ArrayList<>(fieldLength);
        for (int i = 0; i < fieldLength; i++) {
            Map<String, Object> fieldMap = new HashMap<>();
            fieldMap.put(NAME, fieldNames[i]);
            fieldMap.put(TYPE, convertFormatInfo(fieldNames[i], formatInfos[i]));
            fields.add(fieldMap);
        }

        resultMap.put(RECORD_FIELDS, fields);

        return resultMap;
    }

    private static Object convertFormatInfo(String fieldName, FormatInfo formatInfo) {
        String typeStr = getTypeStrFromFormatInfo(formatInfo);
        switch (typeStr) {
            case ARRAY_TYPE:
                Map<String, Object> rstForArrayType = new HashMap<>();
                rstForArrayType.put(TYPE, ARRAY_TYPE);
                ArrayFormatInfo arrayFormatInfo = (ArrayFormatInfo) formatInfo;
                FormatInfo elementFormatInfo = arrayFormatInfo.getElementFormatInfo();
                rstForArrayType.put(ARRAY_ITEMS, convertFormatInfo(fieldName, elementFormatInfo));
                return rstForArrayType;

            case MAP_TYPE:
                Map<String, Object> rstForMapType = new HashMap<>();
                rstForMapType.put(TYPE, MAP_TYPE);
                MapFormatInfo mapFormatInfo = (MapFormatInfo) formatInfo;
                FormatInfo valueFormatInfo = mapFormatInfo.getValueFormatInfo();
                rstForMapType.put(MAP_VALUES, convertFormatInfo(fieldName, valueFormatInfo));
                return rstForMapType;

            case RECORD_TYPE:
                RowFormatInfo rowFormatInfo = (RowFormatInfo) formatInfo;
                return convertFormatInfosToMap(
                        fieldName, rowFormatInfo.getFieldNames(), rowFormatInfo.getFieldFormatInfos());

            default:
                return typeStr;
        }
    }

    private static String getTypeStrFromFormatInfo(FormatInfo formatInfo) {
        // TODO: "bytes" not currently supported
        if (formatInfo instanceof StringFormatInfo
                || formatInfo instanceof DecimalTypeInfo
                || formatInfo instanceof TimeFormatInfo
                || formatInfo instanceof DateFormatInfo
                || formatInfo instanceof TimestampFormatInfo) {
            return STRING_TYPE;
        } else if (formatInfo instanceof BooleanFormatInfo) {
            return BOOLEAN_TYPE;
        } else if (formatInfo instanceof ByteFormatInfo
                || formatInfo instanceof ShortFormatInfo
                || formatInfo instanceof IntFormatInfo) {
            return INT_TYPE;
        } else if (formatInfo instanceof LongFormatInfo) {
            return LONG_TYPE;
        } else if (formatInfo instanceof FloatTypeInfo) {
            return FLOAT_TYPE;
        } else if (formatInfo instanceof DoubleFormatInfo) {
            return DOUBLE_TYPE;
        } else if (formatInfo instanceof ArrayFormatInfo) {
            return ARRAY_TYPE;
        } else if (formatInfo instanceof MapFormatInfo) {
            return MAP_TYPE;
        } else if (formatInfo instanceof RowFormatInfo) {
            return RECORD_TYPE;
        }

        throw new IllegalArgumentException("Unsupported format info " + formatInfo);
    }

}
