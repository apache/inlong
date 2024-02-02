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

import org.apache.inlong.sort.formats.base.TextFormatOptions.TimestampFormat;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.MultisetType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.utils.LogicalTypeUtils;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalQueries;
import java.util.Base64;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.ISO8601_TIMESTAMP_FORMAT;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.ISO8601_TIMESTAMP_WITH_LOCAL_TIMEZONE_FORMAT;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.SQL_TIMESTAMP_FORMAT;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.SQL_TIMESTAMP_WITH_LOCAL_TIMEZONE_FORMAT;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.SQL_TIME_FORMAT;

/** Tool class used to convert from {@link String} to Flink SQL/Table API internal types. */
public class FieldToRowDataConverters implements Serializable {

    private static final long serialVersionUID = 2768836678230179136L;

    /** Flag indicating whether to fail if a field is missing. */
    private final boolean failOnMissingField;

    /** Flag indicating whether to ignore invalid fields/rows (default: throw an exception). */
    private final boolean ignoreParseErrors;

    /** Timestamp format specification which is used to parse timestamp. */
    private final TimestampFormat timestampFormat;

    private final ObjectMapper objectMapper;

    public FieldToRowDataConverters(
            boolean failOnMissingField,
            boolean ignoreParseErrors,
            TimestampFormat timestampFormat) {
        this(failOnMissingField, ignoreParseErrors, timestampFormat, new ObjectMapper());
    }

    public FieldToRowDataConverters(
            boolean failOnMissingField,
            boolean ignoreParseErrors,
            TimestampFormat timestampFormat,
            ObjectMapper objectMapper) {
        this.failOnMissingField = failOnMissingField;
        this.ignoreParseErrors = ignoreParseErrors;
        this.timestampFormat = timestampFormat;
        this.objectMapper = objectMapper;
    }

    /**
     * Runtime converter that converts {@link String} into objects of Flink Table & SQL internal
     * data structures.
     */
    @FunctionalInterface
    public interface FieldToRowDataConverter extends Serializable {

        Object convert(String origStr) throws IOException;
    }

    /** Creates a runtime converter which is null safe. */
    public FieldToRowDataConverter createConverter(LogicalType type) {
        return wrapIntoNullableConverter(createNotNullConverter(type));
    }

    /** Creates a runtime converter which assuming input object is not null. */
    private FieldToRowDataConverter createNotNullConverter(LogicalType type) {
        switch (type.getTypeRoot()) {
            case NULL:
                return origStr -> null;
            case BOOLEAN:
                return origStr -> Boolean.parseBoolean(origStr.trim());
            case TINYINT:
                return origStr -> Byte.parseByte(origStr.trim());
            case SMALLINT:
                return origStr -> Short.parseShort(origStr.trim());
            case INTEGER:
            case INTERVAL_YEAR_MONTH:
                return orig -> Integer.parseInt(orig.trim());
            case BIGINT:
            case INTERVAL_DAY_TIME:
                return orig -> Long.parseLong(orig.trim());
            case DATE:
                return this::convertToDate;
            case TIME_WITHOUT_TIME_ZONE:
                return this::convertToTime;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return this::convertToTimestamp;
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return this::convertToTimestampWithLocalZone;
            case FLOAT:
                return origStr -> Float.parseFloat(origStr.trim());
            case DOUBLE:
                return origStr -> Double.parseDouble(origStr.trim());
            case BINARY:
            case VARBINARY:
                return this::convertToBytes;
            case CHAR:
            case VARCHAR:
                return StringData::fromString;
            case DECIMAL:
                return createDecimalConverter((DecimalType) type);
            case ARRAY:
                return createArrayConverterByObjectMapper((ArrayType) type);
            case MAP:
                MapType mapType = (MapType) type;
                return createMapConverterByObjectMapper(
                        mapType.asSummaryString(),
                        mapType.getKeyType(),
                        mapType.getValueType());
            case MULTISET:
                MultisetType multisetType = (MultisetType) type;
                return createMapConverterByObjectMapper(
                        multisetType.asSummaryString(),
                        multisetType.getElementType(),
                        new IntType());
            case ROW:
                return createRowConverterByObjectMapper((RowType) type);
            case RAW:
            default:
                throw new UnsupportedOperationException("Unsupported type: " + type);
        }
    }

    private int convertToDate(String origStr) {
        LocalDate date = ISO_LOCAL_DATE.parse(origStr).query(TemporalQueries.localDate());
        return (int) date.toEpochDay();
    }

    private int convertToTime(String origStr) {
        TemporalAccessor parsedTime = SQL_TIME_FORMAT.parse(origStr);
        LocalTime localTime = parsedTime.query(TemporalQueries.localTime());

        // get number of milliseconds of the day
        return localTime.toSecondOfDay() * 1000;
    }

    private TimestampData convertToTimestamp(String origStr) {
        TemporalAccessor parsedTimestamp;
        switch (timestampFormat) {
            case SQL:
                parsedTimestamp = SQL_TIMESTAMP_FORMAT.parse(origStr);
                break;
            case ISO_8601:
                parsedTimestamp = ISO8601_TIMESTAMP_FORMAT.parse(origStr);
                break;
            default:
                throw new TableException(
                        String.format("Unsupported timestamp format '%s'. Validator should have checked that.",
                                timestampFormat));
        }
        LocalTime localTime = parsedTimestamp.query(TemporalQueries.localTime());
        LocalDate localDate = parsedTimestamp.query(TemporalQueries.localDate());

        return TimestampData.fromLocalDateTime(LocalDateTime.of(localDate, localTime));
    }

    private TimestampData convertToTimestampWithLocalZone(String origStr) {
        TemporalAccessor parsedTimestampWithLocalZone;
        switch (timestampFormat) {
            case SQL:
                parsedTimestampWithLocalZone = SQL_TIMESTAMP_WITH_LOCAL_TIMEZONE_FORMAT.parse(origStr);
                break;
            case ISO_8601:
                parsedTimestampWithLocalZone = ISO8601_TIMESTAMP_WITH_LOCAL_TIMEZONE_FORMAT.parse(origStr);
                break;
            default:
                throw new TableException(
                        String.format("Unsupported timestamp format '%s'. Validator should have checked that.",
                                timestampFormat));
        }
        LocalTime localTime = parsedTimestampWithLocalZone.query(TemporalQueries.localTime());
        LocalDate localDate = parsedTimestampWithLocalZone.query(TemporalQueries.localDate());

        return TimestampData.fromInstant(LocalDateTime.of(localDate, localTime).toInstant(ZoneOffset.UTC));
    }

    private byte[] convertToBytes(String origStr) {
        return Base64.getDecoder().decode(origStr);
    }

    private FieldToRowDataConverter createDecimalConverter(DecimalType decimalType) {
        final int precision = decimalType.getPrecision();
        final int scale = decimalType.getScale();
        return origStr -> DecimalData.fromBigDecimal(new BigDecimal(origStr), precision, scale);
    }

    private FieldToRowDataConverter createArrayConverterByObjectMapper(ArrayType arrayType) {
        FieldToRowDataConverter elementConverter = createConverter(arrayType.getElementType());
        final Class<?> elementClass = LogicalTypeUtils.toInternalConversionClass(arrayType.getElementType());
        return origStr -> {
            JsonNode jsonNode = objectMapper.readTree(origStr);
            final ArrayNode node = (ArrayNode) jsonNode;
            final Object[] array = (Object[]) Array.newInstance(elementClass, node.size());
            for (int i = 0; i < node.size(); i++) {
                final JsonNode innerNode = node.get(i);
                array[i] = elementConverter.convert(getStringFromJsonNode(innerNode));
            }
            return new GenericArrayData(array);
        };
    }

    private FieldToRowDataConverter createMapConverterByObjectMapper(
            String typeSummary,
            LogicalType keyType,
            LogicalType valueType) {
        if (!keyType.is(LogicalTypeFamily.CHARACTER_STRING)) {
            throw new UnsupportedOperationException(
                    "Currently we don't support non-string as key type of map. The type is: " + typeSummary);
        }
        final FieldToRowDataConverter keyConverter = createConverter(keyType);
        final FieldToRowDataConverter valueConverter = createConverter(valueType);

        return origStr -> {
            JsonNode jsonNode = objectMapper.readTree(origStr);
            Iterator<Entry<String, JsonNode>> fields = jsonNode.fields();
            Map<Object, Object> result = new HashMap<>();
            while (fields.hasNext()) {
                Map.Entry<String, JsonNode> entry = fields.next();
                Object key = keyConverter.convert(entry.getKey());
                Object value = valueConverter.convert(getStringFromJsonNode(entry.getValue()));
                result.put(key, value);
            }
            return new GenericMapData(result);
        };
    }

    public FieldToRowDataConverter createRowConverterByObjectMapper(RowType rowType) {
        final FieldToRowDataConverter[] fieldConverters =
                rowType.getFields().stream()
                        .map(RowType.RowField::getType)
                        .map(this::createConverter)
                        .toArray(FieldToRowDataConverter[]::new);
        final String[] fieldNames = rowType.getFieldNames().toArray(new String[0]);

        return origStr -> {
            JsonNode jsonNode = objectMapper.readTree(origStr);
            ObjectNode node = (ObjectNode) jsonNode;
            int arity = fieldNames.length;
            GenericRowData rowData = new GenericRowData(arity);
            for (int i = 0; i < arity; i++) {
                String fieldName = fieldNames[i];
                JsonNode field = node.get(fieldName);
                try {
                    Object convertedField = convertField(fieldConverters[i], fieldName, field);
                    rowData.setField(i, convertedField);
                } catch (Throwable t) {
                    throw new RuntimeException(String.format("Fail to deserialize at field: %s.", fieldName), t);
                }
            }
            return rowData;
        };
    }

    private Object convertField(
            FieldToRowDataConverter fieldConverter,
            String fieldName,
            JsonNode field) throws IOException {
        if (field == null) {
            if (failOnMissingField) {
                throw new RuntimeException("Could not find field with name '" + fieldName + "'.");
            } else {
                return null;
            }
        } else {
            return fieldConverter.convert(getStringFromJsonNode(field));
        }
    }

    private static String getStringFromJsonNode(JsonNode jsonNode) {
        if (jsonNode.isContainerNode()) {
            return jsonNode.toString();
        }
        return jsonNode.asText();
    }

    private FieldToRowDataConverter wrapIntoNullableConverter(FieldToRowDataConverter converter) {
        return origStr -> {
            if (StringUtils.isBlank(origStr)) {
                return null;
            }

            return converter.convert(origStr);
        };
    }
}
