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

package org.apache.inlong.sdk.transform.utils;

import org.apache.inlong.sdk.transform.decode.TransformException;

import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.MapType;

import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

public class FieldToRowDataUtils {

    public static final String DAY_FORMAT = "yyyy-MM-dd";
    public static final String SECOND_FORMAT = "yyyy-MM-dd HH:mm:ss";
    public static final String MILLI_FORMAT = "yyyy-MM-dd HH:mm:ss.S";

    /**
     * Base class of Field To RowData Converters.
     */
    public interface FieldToRowDataConverter extends Serializable {

        Object convert(Object obj);
    }

    private static final AtomicBoolean isIgnoreError = new AtomicBoolean(true);

    private static final Map<LogicalTypeRoot, FieldToRowDataConverter> converterMap = new ConcurrentHashMap<>();

    static {
        converterMap.put(LogicalTypeRoot.NULL, (obj) -> null);
        converterMap.put(LogicalTypeRoot.BOOLEAN, (obj) -> parseBoolean(obj));
        converterMap.put(LogicalTypeRoot.TINYINT, (obj) -> parseTinyint(obj));
        converterMap.put(LogicalTypeRoot.SMALLINT, (obj) -> parseSmallint(obj));
        converterMap.put(LogicalTypeRoot.INTERVAL_YEAR_MONTH, (obj) -> parseInteger(obj));
        converterMap.put(LogicalTypeRoot.INTEGER, (obj) -> parseInteger(obj));
        converterMap.put(LogicalTypeRoot.INTERVAL_DAY_TIME, (obj) -> parseLong(obj));
        converterMap.put(LogicalTypeRoot.BIGINT, (obj) -> parseLong(obj));
        converterMap.put(LogicalTypeRoot.FLOAT, (obj) -> parseFloat(obj));
        converterMap.put(LogicalTypeRoot.DOUBLE, (obj) -> parseDouble(obj));
        converterMap.put(LogicalTypeRoot.VARBINARY, (obj) -> parseBinary(obj));
        converterMap.put(LogicalTypeRoot.CHAR, (obj) -> parseVarchar(obj));
        converterMap.put(LogicalTypeRoot.VARCHAR, (obj) -> parseVarchar(obj));
        converterMap.put(LogicalTypeRoot.DATE, (obj) -> parseDate(obj));
        converterMap.put(LogicalTypeRoot.TIME_WITHOUT_TIME_ZONE, (obj) -> parseTimeWithoutTimeZone(obj));
        converterMap.put(LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE, (obj) -> parseTimestampWithLocalTimeZone(obj));
        converterMap.put(LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE, (obj) -> parseTimestampWithLocalTimeZone(obj));
        converterMap.put(LogicalTypeRoot.TIMESTAMP_WITH_TIME_ZONE, (obj) -> parseTimestampWithLocalTimeZone(obj));
        converterMap.put(LogicalTypeRoot.DECIMAL, (obj) -> parseDecimal(obj));
        converterMap.put(LogicalTypeRoot.BINARY, (obj) -> parseBinary(obj));
        converterMap.put(LogicalTypeRoot.ARRAY, (obj) -> parseArray(obj));
        converterMap.put(LogicalTypeRoot.MAP, (obj) -> parseMap(obj));
        converterMap.put(LogicalTypeRoot.ROW, (obj) -> parseRow(obj));
    }

    private static final ThreadLocal<Map<String, SimpleDateFormat>> formatLocal = new ThreadLocal<>();

    public static void setIgnoreError(boolean isIgnoreError) {
        FieldToRowDataUtils.isIgnoreError.set(isIgnoreError);
    }

    public static boolean isIgnoreError() {
        return isIgnoreError.get();
    }

    public static void replaceConverter(LogicalTypeRoot type, FieldToRowDataConverter converter) {
        converterMap.put(type, converter);
    }

    public static FieldToRowDataConverter createConverter(LogicalType logicalType) {
        return wrapIntoNullableConverter(createFieldRowConverter(logicalType));
    }

    private static FieldToRowDataConverter wrapIntoNullableConverter(
            FieldToRowDataConverter converter) {
        return obj -> {
            if (obj == null) {
                return null;
            }
            return converter.convert(obj);
        };
    }

    private static FieldToRowDataConverter createFieldRowConverter(LogicalType fieldType) {
        LogicalTypeRoot type = fieldType.getTypeRoot();
        FieldToRowDataConverter converter = converterMap.get(type);
        if (converter != null) {
            return converter;
        }
        switch (type) {
            case ARRAY:
                return obj -> {
                    final Object[] array = (Object[]) obj;
                    FieldToRowDataConverter elementConverter = createFieldRowConverter(
                            ((ArrayType) fieldType).getElementType());
                    Object[] converted = Arrays.stream(array)
                            .map(elementConverter::convert)
                            .toArray();
                    return new GenericArrayData(converted);
                };
            case MAP:
                return obj -> {
                    FieldToRowDataConverter keyConverter = createFieldRowConverter(((MapType) fieldType).getKeyType());
                    FieldToRowDataConverter valueConverter = createFieldRowConverter(
                            ((MapType) fieldType).getValueType());
                    Map<?, ?> map = (Map<?, ?>) obj;
                    Map<Object, Object> internalMap = new HashMap<>();
                    for (Object k : map.keySet()) {
                        internalMap.put(keyConverter.convert(k),
                                valueConverter.convert(map.get(k)));
                    }
                    return new GenericMapData(internalMap);
                };
            case ROW:
            case MULTISET:
            case RAW:
            default:
                throw new UnsupportedOperationException("Unsupported type:" + fieldType);
        }
    }

    private static Object parseBoolean(Object obj) {
        try {
            if (obj == null) {
                return null;
            }
            if (obj instanceof Boolean) {
                return obj;
            }
            return Boolean.parseBoolean(String.valueOf(obj));
        } catch (RuntimeException e) {
            if (isIgnoreError()) {
                return null;
            }
            throw e;
        }
    }

    private static Object parseTinyint(Object obj) {
        try {
            if (obj == null) {
                return null;
            }
            if (obj instanceof Byte) {
                return obj;
            }
            return Byte.parseByte(String.valueOf(obj));
        } catch (RuntimeException e) {
            if (isIgnoreError()) {
                return null;
            }
            throw e;
        }
    }

    private static Object parseSmallint(Object obj) {
        try {
            if (obj == null) {
                return null;
            }
            if (obj instanceof Short) {
                return obj;
            }
            return Short.parseShort(String.valueOf(obj));
        } catch (RuntimeException e) {
            if (isIgnoreError()) {
                return null;
            }
            throw e;
        }
    }

    private static Object parseInteger(Object obj) {
        try {
            if (obj == null) {
                return null;
            }
            if (obj instanceof Integer) {
                return obj;
            }
            return Integer.parseInt(String.valueOf(obj));
        } catch (RuntimeException e) {
            if (isIgnoreError()) {
                return null;
            }
            throw e;
        }
    }

    private static Object parseLong(Object obj) {
        try {
            if (obj == null) {
                return null;
            }
            if (obj instanceof Long) {
                return obj;
            }
            return Long.parseLong(String.valueOf(obj));
        } catch (RuntimeException e) {
            if (isIgnoreError()) {
                return null;
            }
            throw e;
        }
    }

    private static Object parseFloat(Object obj) {
        try {
            if (obj == null) {
                return null;
            }
            if (obj instanceof Float) {
                return obj;
            }
            return Float.parseFloat(String.valueOf(obj));
        } catch (RuntimeException e) {
            if (isIgnoreError()) {
                return null;
            }
            throw e;
        }
    }

    private static Object parseDouble(Object obj) {
        try {
            if (obj == null) {
                return null;
            }
            if (obj instanceof Double) {
                return obj;
            }
            return Double.parseDouble(String.valueOf(obj));
        } catch (RuntimeException e) {
            if (isIgnoreError()) {
                return null;
            }
            throw e;
        }
    }

    private static Object parseVarchar(Object obj) {
        try {
            if (obj == null) {
                return null;
            }
            if (obj instanceof BinaryStringData) {
                return obj;
            }
            if (obj instanceof String) {
                return new BinaryStringData((String) obj);
            }
            if (obj instanceof byte[]) {
                return new BinaryStringData(new String((byte[]) obj));
            }
            return new BinaryStringData(String.valueOf(obj));
        } catch (RuntimeException e) {
            if (isIgnoreError()) {
                return null;
            }
            throw e;
        }
    }

    private static Object parseDate(Object obj) {
        try {
            if (obj == null) {
                return null;
            }
            if (obj instanceof Date) {
                return ((Date) obj).toLocalDate().toEpochDay();
            }
            String strObj = String.valueOf(obj);
            Date date = parseDateTime(strObj);
            return date.toLocalDate().toEpochDay();
        } catch (RuntimeException e) {
            if (isIgnoreError()) {
                return null;
            }
            throw e;
        }
    }

    private static Date parseDateTime(String str) {
        try {
            Map<String, SimpleDateFormat> formatMap = formatLocal.get();
            if (formatMap == null) {
                formatLocal.set(new ConcurrentHashMap<>());
                formatMap = formatLocal.get();
            }
            int length = str.length();
            if (length == DAY_FORMAT.length()) {
                SimpleDateFormat format = formatMap.computeIfAbsent(DAY_FORMAT, k -> new SimpleDateFormat(DAY_FORMAT));
                java.util.Date dateTime = format.parse(str);
                return new Date(dateTime.getTime());
            } else if (length == SECOND_FORMAT.length()) {
                SimpleDateFormat format = formatMap.computeIfAbsent(SECOND_FORMAT,
                        k -> new SimpleDateFormat(SECOND_FORMAT));
                java.util.Date dateTime = format.parse(str);
                return new Date(dateTime.getTime());
            } else {
                SimpleDateFormat format = formatMap.computeIfAbsent(MILLI_FORMAT,
                        k -> new SimpleDateFormat(MILLI_FORMAT));
                java.util.Date dateTime = format.parse(str);
                return new Date(dateTime.getTime());
            }
        } catch (ParseException e) {
            throw new TransformException(e.getMessage(), e);
        }
    }

    private static Object parseTimeWithoutTimeZone(Object obj) {
        try {
            if (obj == null) {
                return null;
            }
            if (obj instanceof Time) {
                return ((Time) obj).toLocalTime().toSecondOfDay() * 1000;
            }
            String strObj = String.valueOf(obj);
            Date date = parseDateTime(strObj);
            return new Time(date.getTime()).toLocalTime().toSecondOfDay() * 1000;
        } catch (RuntimeException e) {
            if (isIgnoreError()) {
                return null;
            }
            throw e;
        }
    }

    private static Object parseTimestampWithLocalTimeZone(Object obj) {
        try {
            if (obj == null) {
                return null;
            }
            if (obj instanceof Timestamp) {
                return TimestampData.fromTimestamp((Timestamp) obj);
            }
            String strObj = String.valueOf(obj);
            Date date = parseDateTime(strObj);
            return TimestampData.fromTimestamp(new Timestamp(date.getTime()));
        } catch (RuntimeException e) {
            if (isIgnoreError()) {
                return null;
            }
            throw e;
        }
    }

    private static Object parseDecimal(Object obj) {
        try {
            if (obj == null) {
                return null;
            }
            if (obj instanceof BigDecimal) {
                return DecimalData.fromBigDecimal(
                        (BigDecimal) obj,
                        DecimalType.DEFAULT_PRECISION,
                        DecimalType.DEFAULT_SCALE);
            }
            String strObj = String.valueOf(obj);
            return DecimalData.fromBigDecimal(
                    new BigDecimal(strObj),
                    DecimalType.DEFAULT_PRECISION,
                    DecimalType.DEFAULT_SCALE);
        } catch (RuntimeException e) {
            if (isIgnoreError()) {
                return null;
            }
            throw e;
        }
    }

    private static Object parseBinary(Object obj) {
        try {
            if (obj == null) {
                return null;
            }
            if (obj instanceof byte[]) {
                return obj;
            }
            return String.valueOf(obj).getBytes();
        } catch (RuntimeException e) {
            if (isIgnoreError()) {
                return null;
            }
            throw e;
        }
    }

    private static Object parseArray(Object obj) {
        try {
            if (obj == null) {
                return null;
            }
            if (obj instanceof GenericArrayData) {
                return obj;
            }
            if (obj instanceof List<?>) {
                return new GenericArrayData(((List<?>) obj).toArray());
            }
            return null;
        } catch (RuntimeException e) {
            if (isIgnoreError()) {
                return null;
            }
            throw e;
        }
    }

    private static Object parseMap(Object obj) {
        try {
            if (obj == null) {
                return null;
            }
            if (obj instanceof GenericMapData) {
                return obj;
            }
            if (obj instanceof Map<?, ?>) {
                return new GenericMapData((Map<?, ?>) obj);
            }
            return null;
        } catch (RuntimeException e) {
            if (isIgnoreError()) {
                return null;
            }
            throw e;
        }
    }

    private static Object parseRow(Object obj) {
        try {
            if (obj == null) {
                return null;
            }
            if (obj instanceof GenericRowData) {
                return obj;
            }
            return null;
        } catch (RuntimeException e) {
            if (isIgnoreError()) {
                return null;
            }
            throw e;
        }
    }
}
