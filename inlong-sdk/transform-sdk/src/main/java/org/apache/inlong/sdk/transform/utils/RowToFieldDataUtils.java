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

import org.apache.inlong.sort.formats.base.RowDataToFieldConverters;

import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.types.Row;

import java.io.Serializable;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Arrays;

public class RowToFieldDataUtils {

    private static final long serialVersionUID = 1L;

    /**
     * Runtime converter that converts objects of Flink Table & SQL internal data structures to
     * corresponding {@link Object}s.
     */
    public interface RowFieldConverter extends Serializable {

        Object convert(RowData row, int pos);
    }

    private interface ArrayElementConverter extends Serializable {

        Object convert(ArrayData array, int pos);
    }

    public static RowFieldConverter createNullableRowFieldConverter(LogicalType fieldType) {
        final RowFieldConverter fieldConverter = createRowFieldConverter(fieldType);
        return (row, pos) -> {
            if (row.isNullAt(pos)) {
                return null;
            }
            return fieldConverter.convert(row, pos);
        };
    }

    private static RowFieldConverter createRowFieldConverter(LogicalType fieldType) {
        switch (fieldType.getTypeRoot()) {
            case NULL:
                return (row, pos) -> null;
            case BOOLEAN:
                return RowData::getBoolean;
            case TINYINT:
                return RowData::getByte;
            case SMALLINT:
                return RowData::getShort;
            case INTEGER:
            case INTERVAL_YEAR_MONTH:
                return RowData::getInt;
            case BIGINT:
            case INTERVAL_DAY_TIME:
                return RowData::getLong;
            case FLOAT:
                return RowData::getFloat;
            case DOUBLE:
                return RowData::getDouble;
            case CHAR:
            case VARCHAR:
                return (row, pos) -> row.getString(pos).toString();
            case BINARY:
            case VARBINARY:
                return RowData::getBinary;
            case DATE:
                return (row, pos) -> convertDate(row.getLong(pos));
            case TIME_WITHOUT_TIME_ZONE:
                return (row, pos) -> convertTime(row.getInt(pos));
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                final int timestampPrecision = ((TimestampType) fieldType).getPrecision();
                return (row, pos) -> convertTimestamp(
                        row.getTimestamp(pos, timestampPrecision));
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                final int zonedTimestampPrecision =
                        ((LocalZonedTimestampType) fieldType).getPrecision();
                return (row, pos) -> convertTimestamp(
                        row.getTimestamp(pos, zonedTimestampPrecision));
            case DECIMAL:
                return createDecimalRowFieldConverter((DecimalType) fieldType);
            case ARRAY:
                return createArrayRowFieldConverter((ArrayType) fieldType);
            case ROW:
                return createRowRowFieldConverter((RowType) fieldType);
            case MAP:
            case MULTISET:
            case RAW:
            default:
                throw new UnsupportedOperationException("Unsupported type: " + fieldType);
        }
    }

    private static ArrayElementConverter createNullableArrayElementConverter(
            LogicalType fieldType) {
        final ArrayElementConverter elementConverter = createArrayElementConverter(fieldType);
        return (array, pos) -> {
            if (array.isNullAt(pos)) {
                return null;
            }
            return elementConverter.convert(array, pos);
        };
    }

    private static ArrayElementConverter createArrayElementConverter(LogicalType fieldType) {
        switch (fieldType.getTypeRoot()) {
            case NULL:
                return (array, pos) -> null;
            case BOOLEAN:
                return ArrayData::getBoolean;
            case TINYINT:
                return ArrayData::getByte;
            case SMALLINT:
                return ArrayData::getShort;
            case INTEGER:
            case INTERVAL_YEAR_MONTH:
                return ArrayData::getInt;
            case BIGINT:
            case INTERVAL_DAY_TIME:
                return ArrayData::getLong;
            case FLOAT:
                return ArrayData::getFloat;
            case DOUBLE:
                return ArrayData::getDouble;
            case CHAR:
            case VARCHAR:
                return (array, pos) -> array.getString(pos).toString();
            case BINARY:
            case VARBINARY:
                return ArrayData::getBinary;
            case DATE:
                return (array, pos) -> convertDate(array.getLong(pos));
            case TIME_WITHOUT_TIME_ZONE:
                return (array, pos) -> convertTime(array.getInt(pos));
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                final int timestampPrecision = ((TimestampType) fieldType).getPrecision();
                return (array, pos) -> convertTimestamp(
                        array.getTimestamp(pos, timestampPrecision));
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                final int localZonedTimestampPrecision =
                        ((LocalZonedTimestampType) fieldType).getPrecision();
                return (array, pos) -> convertTimestamp(
                        array.getTimestamp(pos, localZonedTimestampPrecision));
            case DECIMAL:
                return createDecimalArrayElementConverter((DecimalType) fieldType);
            // we don't support ARRAY and ROW in an ARRAY, see
            // CsvRowSchemaConverter#validateNestedField
            case ARRAY:
            case ROW:
            case MAP:
            case MULTISET:
            case RAW:
            default:
                throw new UnsupportedOperationException("Unsupported type: " + fieldType);
        }
    }

    // ------------------------------------------------------------------------------------------
    // Field/Element Converters
    // ------------------------------------------------------------------------------------------

    private static RowFieldConverter createDecimalRowFieldConverter(DecimalType decimalType) {
        final int precision = decimalType.getPrecision();
        final int scale = decimalType.getScale();
        return (row, pos) -> {
            DecimalData decimal = row.getDecimal(pos, precision, scale);
            return decimal.toBigDecimal();
        };
    }

    private static ArrayElementConverter createDecimalArrayElementConverter(
            DecimalType decimalType) {
        final int precision = decimalType.getPrecision();
        final int scale = decimalType.getScale();
        return (array, pos) -> {
            DecimalData decimal = array.getDecimal(pos, precision, scale);
            return decimal.toBigDecimal();
        };
    }

    private static Date convertDate(long days) {
        LocalDate localDate = LocalDate.ofEpochDay(days);
        return Date.valueOf(localDate);
    }

    private static Time convertTime(int millisecond) {
        LocalTime time = LocalTime.ofNanoOfDay(millisecond * 1000_000L);
        return Time.valueOf(time);
    }

    private static Timestamp convertTimestamp(TimestampData timestamp) {
        return timestamp.toTimestamp();
    }

    private static RowFieldConverter createArrayRowFieldConverter(ArrayType type) {
        LogicalType elementType = type.getElementType();
        final ArrayElementConverter elementConverter =
                createNullableArrayElementConverter(elementType);
        return (row, pos) -> {
            ArrayData arrayData = row.getArray(pos);
            int numElements = arrayData.size();
            Object[] result = new Object[numElements];
            for (int i = 0; i < numElements; i++) {
                result[i] = elementConverter.convert(arrayData, i);
            }
            return result;
        };
    }

    private static RowFieldConverter createRowRowFieldConverter(RowType type) {
        LogicalType[] fieldTypes =
                type.getFields().stream()
                        .map(RowType.RowField::getType)
                        .toArray(LogicalType[]::new);
        final RowFieldConverter[] fieldConverters =
                Arrays.stream(fieldTypes)
                        .map(RowDataToFieldConverters::createNullableRowFieldConverter)
                        .toArray(RowFieldConverter[]::new);
        final int rowArity = type.getFieldCount();
        return (row, pos) -> {
            final RowData value = row.getRow(pos, rowArity);
            Row result = new Row(rowArity);
            for (int i = 0; i < rowArity; i++) {
                result.setField(i, fieldConverters[i].convert(value, i));
            }
            return result;
        };
    }
}
