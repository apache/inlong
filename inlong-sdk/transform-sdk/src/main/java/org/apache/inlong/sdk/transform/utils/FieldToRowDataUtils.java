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

import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;

import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class FieldToRowDataUtils {

    private static final long serialVersionUID = 1L;

    /**
     * Base class of Field To RowData Converters.
     */
    public interface FieldToRowDataConverter extends Serializable {

        Object convert(Object obj);
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
        switch (fieldType.getTypeRoot()) {
            case NULL:
                return (obj) -> null;
            case BOOLEAN:
                return obj -> Boolean.parseBoolean(obj.toString());
            case TINYINT:
                return obj -> Byte.parseByte(obj.toString());
            case SMALLINT:
                return obj -> Short.parseShort(obj.toString());
            case INTERVAL_YEAR_MONTH:
            case INTEGER:
                return obj -> Integer.parseInt(obj.toString());
            case INTERVAL_DAY_TIME:
            case BIGINT:
                return obj -> Long.parseLong(obj.toString());
            case FLOAT:
                return obj -> Float.parseFloat(obj.toString());
            case DOUBLE:
                return obj -> Double.parseDouble(obj.toString());
            case BINARY:
            case VARBINARY:
                return obj -> obj.toString().getBytes();
            case CHAR:
            case VARCHAR:
                return (obj -> StringData.fromString((String) obj));
            case DATE:
                return (obj -> ((Date) obj).toLocalDate().toEpochDay());
            case TIME_WITHOUT_TIME_ZONE:
                return (obj -> ((Time) obj).toLocalTime().toSecondOfDay() * 1000);
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_TIME_ZONE:
                return obj -> TimestampData.fromTimestamp((Timestamp) obj);
            case DECIMAL:
                return obj -> DecimalData.fromBigDecimal(
                        (BigDecimal) obj,
                        DecimalType.DEFAULT_PRECISION,
                        DecimalType.DEFAULT_SCALE);
            case ARRAY:
                return obj -> {
                    final Object[] array = (Object[]) obj;
                    FieldToRowDataConverter elementConverter =
                            createFieldRowConverter(((ArrayType) fieldType).getElementType());
                    Object[] converted = Arrays.stream(array)
                            .map(elementConverter::convert)
                            .toArray();
                    return new GenericArrayData(converted);
                };
            case MAP:
                return obj -> {
                    FieldToRowDataConverter keyConverter =
                            createFieldRowConverter(((MapType) fieldType).getKeyType());
                    FieldToRowDataConverter valueConverter =
                            createFieldRowConverter(((MapType) fieldType).getValueType());
                    Map map = (Map) obj;
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
}
