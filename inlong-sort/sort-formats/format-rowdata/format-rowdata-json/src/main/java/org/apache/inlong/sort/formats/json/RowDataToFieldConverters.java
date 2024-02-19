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

package org.apache.inlong.sort.formats.json;

import org.apache.inlong.sort.formats.base.TextFormatOptions.MapNullKeyMode;
import org.apache.inlong.sort.formats.base.TextFormatOptions.TimestampFormat;

import org.apache.flink.table.api.TableException;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.MultisetType;
import org.apache.flink.table.types.logical.RowType;

import java.io.Serializable;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.ISO8601_TIMESTAMP_FORMAT;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.ISO8601_TIMESTAMP_WITH_LOCAL_TIMEZONE_FORMAT;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.SQL_TIMESTAMP_FORMAT;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.SQL_TIMESTAMP_WITH_LOCAL_TIMEZONE_FORMAT;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.SQL_TIME_FORMAT;
import static org.apache.inlong.sort.formats.base.TextFormatOptions.MAP_NULL_KEY_MODE;

/** Tool class used to convert from Flink SQL/Table API internal types to normal java class. */
public class RowDataToFieldConverters implements Serializable {

    private static final long serialVersionUID = -2427899310525067758L;

    /** Timestamp format specification which is used to parse timestamp. */
    private final TimestampFormat timestampFormat;

    /** The handling mode when serializing null keys for map data. */
    private final MapNullKeyMode mapNullKeyMode;

    /** The string literal when handling mode for map null key LITERAL. is */
    private final String mapNullKeyLiteral;

    public RowDataToFieldConverters(
            TimestampFormat timestampFormat,
            MapNullKeyMode mapNullKeyMode,
            String mapNullKeyLiteral) {
        this.timestampFormat = timestampFormat;
        this.mapNullKeyMode = mapNullKeyMode;
        this.mapNullKeyLiteral = mapNullKeyLiteral;
    }

    /**
     * Runtime converter that converts objects of Flink Table & SQL internal data structures to
     * corresponding normal java class.
     */
    public interface RowDataToFieldConverter extends Serializable {

        Object convert(Object inputObj);
    }

    /** Creates a runtime converter which is null safe. */
    public RowDataToFieldConverter createConverter(LogicalType type) {
        return wrapIntoNullableConverter(createNotNullConverter(type));
    }

    /** Creates a runtime converter which assuming input object is not null. */
    private RowDataToFieldConverter createNotNullConverter(LogicalType type) {
        switch (type.getTypeRoot()) {
            case NULL:
                return inputObj -> null;
            case BOOLEAN:
            case BINARY:
            case VARBINARY:
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case INTERVAL_YEAR_MONTH:
            case FLOAT:
            case DOUBLE:
            case BIGINT:
            case INTERVAL_DAY_TIME:
                return inputObj -> inputObj;
            case CHAR:
            case VARCHAR:
                return Object::toString;
            case DATE:
                return createDateConverter();
            case TIME_WITHOUT_TIME_ZONE:
                return createTimeConverter();
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return createTimestampConverter();
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return createTimestampWithLocalZone();
            case DECIMAL:
                return inputObj -> ((DecimalData) inputObj).toBigDecimal();
            case ARRAY:
                return createArrayConverter((ArrayType) type);
            case MAP:
                MapType mapType = (MapType) type;
                return createMapConverter(mapType.asSummaryString(), mapType.getKeyType(), mapType.getValueType());
            case MULTISET:
                MultisetType multisetType = (MultisetType) type;
                return createMapConverter(multisetType.asSummaryString(), multisetType.getElementType(),
                        new IntType());
            case ROW:
                return createRowConverter((RowType) type);
            case RAW:
            default:
                throw new UnsupportedOperationException("Not support to parse type: " + type);
        }
    }

    private RowDataToFieldConverter createDateConverter() {
        return inputObj -> {
            int days = (int) inputObj;
            LocalDate localDate = LocalDate.ofEpochDay(days);
            return ISO_LOCAL_DATE.format(localDate);
        };
    }

    private RowDataToFieldConverter createTimeConverter() {
        return inputObj -> {
            int millisecond = (int) inputObj;
            LocalTime localTime = LocalTime.ofSecondOfDay(millisecond / 1000L);
            return SQL_TIME_FORMAT.format(localTime);
        };
    }

    private RowDataToFieldConverter createTimestampConverter() {
        switch (timestampFormat) {
            case ISO_8601:
                return inputObj -> {
                    TimestampData timestamp = (TimestampData) inputObj;
                    return ISO8601_TIMESTAMP_FORMAT.format(timestamp.toLocalDateTime());
                };
            case SQL:
                return inputObj -> {
                    TimestampData timestamp = (TimestampData) inputObj;
                    return SQL_TIMESTAMP_FORMAT.format(timestamp.toLocalDateTime());
                };
            default:
                throw new TableException("Unsupported timestamp format. Validator should have checked that.");
        }
    }

    private RowDataToFieldConverter createTimestampWithLocalZone() {
        switch (timestampFormat) {
            case ISO_8601:
                return inputObj -> {
                    TimestampData timestampWithLocalZone = (TimestampData) inputObj;
                    return ISO8601_TIMESTAMP_WITH_LOCAL_TIMEZONE_FORMAT.format(
                            timestampWithLocalZone
                                    .toInstant()
                                    .atOffset(ZoneOffset.UTC));
                };
            case SQL:
                return inputObj -> {
                    TimestampData timestampWithLocalZone = (TimestampData) inputObj;
                    return SQL_TIMESTAMP_WITH_LOCAL_TIMEZONE_FORMAT.format(
                            timestampWithLocalZone
                                    .toInstant()
                                    .atOffset(ZoneOffset.UTC));
                };
            default:
                throw new TableException(
                        "Unsupported timestamp format. Validator should have checked that.");
        }
    }

    private RowDataToFieldConverter createArrayConverter(ArrayType type) {
        final LogicalType elementType = type.getElementType();
        Class<?> elementClass = toJavaClass(elementType);
        final RowDataToFieldConverter elementConverter = createConverter(elementType);
        final ArrayData.ElementGetter elementGetter = ArrayData.createElementGetter(elementType);
        return inputObj -> {
            ArrayData array = (ArrayData) inputObj;
            int numElements = array.size();
            Object[] output = (Object[]) Array.newInstance(elementClass, numElements);
            for (int i = 0; i < numElements; i++) {
                Object element = elementGetter.getElementOrNull(array, i);
                output[i] = elementConverter.convert(element);
            }

            return output;
        };
    }

    private static Class<?> toJavaClass(LogicalType type) {
        switch (type.getTypeRoot()) {
            case NULL:
                return Object.class;
            case BOOLEAN:
                return Boolean.class;
            case BINARY:
            case VARBINARY:
                return byte[].class;
            case TINYINT:
                return Byte.class;
            case SMALLINT:
                return Short.class;
            case INTEGER:
            case INTERVAL_YEAR_MONTH:
                return Integer.class;
            case FLOAT:
                return Float.class;
            case DOUBLE:
                return Double.class;
            case BIGINT:
            case INTERVAL_DAY_TIME:
                return Long.class;
            case CHAR:
            case VARCHAR:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return String.class;
            case DECIMAL:
                return BigDecimal.class;
            case ARRAY:
                ArrayType arrayType = (ArrayType) type;
                return Array.newInstance(toJavaClass(arrayType.getElementType()), 1).getClass();
            case MAP:
            case MULTISET:
            case ROW:
                return Map.class;
            case RAW:
            default:
                throw new UnsupportedOperationException("Not support to parse type: " + type);
        }
    }

    private RowDataToFieldConverter createMapConverter(
            String typeSummary, LogicalType keyType,
            LogicalType valueType) {
        if (!keyType.is(LogicalTypeFamily.CHARACTER_STRING)) {
            throw new UnsupportedOperationException(
                    "JSON format doesn't support non-string as key type of map. The type is: " + typeSummary);
        }
        final RowDataToFieldConverter valueConverter = createConverter(valueType);
        final ArrayData.ElementGetter valueGetter = ArrayData.createElementGetter(valueType);
        return inputObj -> {
            MapData map = (MapData) inputObj;
            ArrayData keyArray = map.keyArray();
            ArrayData valueArray = map.valueArray();
            int numElements = map.size();
            Map<String, Object> output = new HashMap<>();
            for (int i = 0; i < numElements; i++) {
                String fieldName;
                if (keyArray.isNullAt(i)) { // when map key is null
                    switch (mapNullKeyMode) {
                        case LITERAL:
                            fieldName = mapNullKeyLiteral;
                            break;
                        case DROP:
                            continue;
                        case FAIL:
                            throw new RuntimeException(
                                    String.format("JSON format doesn't support to serialize map data with null keys."
                                            + " You can drop null key entries or encode null in"
                                            + " literals by specifying %s option.",
                                            MAP_NULL_KEY_MODE.key()));
                        default:
                            throw new RuntimeException(
                                    "Unsupported map null key mode. Validator should have checked that.");
                    }
                } else {
                    fieldName = keyArray.getString(i).toString();
                }

                Object value = valueGetter.getElementOrNull(valueArray, i);
                output.put(fieldName, valueConverter.convert(value));
            }

            return output;
        };
    }

    private RowDataToFieldConverter createRowConverter(RowType type) {
        final String[] fieldNames = type.getFieldNames().toArray(new String[0]);
        final LogicalType[] fieldTypes =
                type.getFields().stream()
                        .map(RowType.RowField::getType)
                        .toArray(LogicalType[]::new);
        final RowDataToFieldConverter[] fieldConverters =
                Arrays.stream(fieldTypes)
                        .map(this::createConverter)
                        .toArray(RowDataToFieldConverter[]::new);
        final int fieldCount = type.getFieldCount();
        final RowData.FieldGetter[] fieldGetters = new RowData.FieldGetter[fieldTypes.length];
        for (int i = 0; i < fieldCount; i++) {
            fieldGetters[i] = RowData.createFieldGetter(fieldTypes[i], i);
        }

        return inputObj -> {
            RowData rowData = (RowData) inputObj;
            Map<String, Object> output = new HashMap<>();
            for (int i = 0; i < fieldCount; i++) {
                String fieldName = fieldNames[i];
                try {
                    Object field = fieldGetters[i].getFieldOrNull(rowData);
                    output.put(fieldName, fieldConverters[i].convert(field));
                } catch (Throwable t) {
                    throw new RuntimeException(String.format("Fail to serialize at field: %s.", fieldName), t);
                }
            }
            return output;
        };
    }

    private RowDataToFieldConverter wrapIntoNullableConverter(RowDataToFieldConverter converter) {
        return inputObj -> {
            if (inputObj == null) {
                return null;
            }

            return converter.convert(inputObj);
        };
    }
}
