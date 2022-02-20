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

package org.apache.inlong.sort.flink.clickhouse;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.flink.shaded.guava18.com.google.common.annotations.VisibleForTesting;
import org.apache.inlong.sort.formats.common.ArrayTypeInfo;
import org.apache.inlong.sort.formats.common.BooleanTypeInfo;
import org.apache.inlong.sort.formats.common.ByteTypeInfo;
import org.apache.inlong.sort.formats.common.DateTypeInfo;
import org.apache.inlong.sort.formats.common.DecimalTypeInfo;
import org.apache.inlong.sort.formats.common.DoubleTypeInfo;
import org.apache.inlong.sort.formats.common.FloatTypeInfo;
import org.apache.inlong.sort.formats.common.FormatInfo;
import org.apache.inlong.sort.formats.common.IntTypeInfo;
import org.apache.inlong.sort.formats.common.LongTypeInfo;
import org.apache.inlong.sort.formats.common.MapTypeInfo;
import org.apache.inlong.sort.formats.common.ShortTypeInfo;
import org.apache.inlong.sort.formats.common.StringTypeInfo;
import org.apache.inlong.sort.formats.common.TimeTypeInfo;
import org.apache.inlong.sort.formats.common.TimestampTypeInfo;
import org.apache.inlong.sort.formats.common.TypeInfo;
import org.apache.flink.types.Row;
import ru.yandex.clickhouse.ClickHouseArray;
import ru.yandex.clickhouse.domain.ClickHouseDataType;
import ru.yandex.clickhouse.util.Utils;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Map;

public class ClickHouseRowConverter {

    public static void setRow(
            PreparedStatement statement,
            FormatInfo[] formatInfos,
            Row row) throws SQLException {
        for (int i = 0; i < row.getArity(); ++i) {
            setField(statement, i, formatInfos[i], row.getField(i));
        }
    }

    private static void setField(
            PreparedStatement statement,
            int index,
            FormatInfo formatInfo,
            Object value) throws SQLException {
        TypeInfo typeInfo = formatInfo.getTypeInfo();

        if (typeInfo instanceof StringTypeInfo) {
            statement.setString(index + 1, (String) value);
        } else if (typeInfo instanceof BooleanTypeInfo) {
            statement.setBoolean(index + 1, (Boolean) value);
        } else if (typeInfo instanceof ByteTypeInfo) {
            statement.setByte(index + 1, (Byte) value);
        } else if (typeInfo instanceof ShortTypeInfo) {
            statement.setShort(index + 1, (Short) value);
        } else if (typeInfo instanceof IntTypeInfo) {
            statement.setInt(index + 1, (Integer) value);
        } else if (typeInfo instanceof LongTypeInfo) {
            statement.setLong(index + 1, (Long) value);
        } else if (typeInfo instanceof FloatTypeInfo) {
            statement.setFloat(index + 1, (Float) value);
        } else if (typeInfo instanceof DoubleTypeInfo) {
            statement.setDouble(index + 1, (Double) value);
        } else if (typeInfo instanceof DecimalTypeInfo) {
            statement.setBigDecimal(index + 1, (BigDecimal) value);
        } else if (typeInfo instanceof TimeTypeInfo) {
            statement.setTime(index + 1, (Time) value);
        } else if (typeInfo instanceof DateTypeInfo) {
            statement.setDate(index + 1, (Date) value);
        } else if (typeInfo instanceof TimestampTypeInfo) {
            statement.setTimestamp(index + 1, (Timestamp) value);
        } else if (typeInfo instanceof ArrayTypeInfo) {
            TypeInfo elementTypeInfo = ((ArrayTypeInfo) typeInfo).getElementTypeInfo();
            statement.setArray(index + 1, new ClickHouseArray(
                    getClickHouseDataTypeFromTypeInfo(elementTypeInfo), toObjectArray(elementTypeInfo, value)));
        } else if (typeInfo instanceof MapTypeInfo) {
            Map<?, ?> mapValue = (Map<?, ?>) value;
            int size = mapValue.size();
            Object[] kvps = new Object[size * 2];
            int i = 0;
            for (Map.Entry<?, ?> entry : mapValue.entrySet()) {
                kvps[i] = entry.getKey();
                kvps[i + 1] = entry.getValue();
                i += 2;
            }
            statement.setObject(index + 1, Utils.mapOf(kvps));
        } else {
            throw new IllegalArgumentException("Unsupported TypeInfo " + typeInfo.getClass().getName());
        }
    }

    private static ClickHouseDataType getClickHouseDataTypeFromTypeInfo(TypeInfo typeInfo) {
        if (typeInfo instanceof StringTypeInfo) {
            return ClickHouseDataType.String;
        } else if (typeInfo instanceof BooleanTypeInfo || typeInfo instanceof ByteTypeInfo) {
            return ClickHouseDataType.Int8;
        } else if (typeInfo instanceof ShortTypeInfo) {
            return ClickHouseDataType.Int16;
        } else if (typeInfo instanceof IntTypeInfo) {
            return ClickHouseDataType.Int32;
        } else if (typeInfo instanceof LongTypeInfo) {
            return ClickHouseDataType.Int64;
        } else if (typeInfo instanceof FloatTypeInfo) {
            return ClickHouseDataType.Float32;
        } else if (typeInfo instanceof DoubleTypeInfo) {
            return ClickHouseDataType.Float64;
        } else {
            throw new IllegalArgumentException("Unsupported TypeInfo " + typeInfo.getClass().getName());
        }
    }

    @VisibleForTesting
    static Object toObjectArray(TypeInfo typeInfo, Object object) {
        if (typeInfo instanceof BooleanTypeInfo && object instanceof boolean[]) {
            return ArrayUtils.toObject((boolean[]) object);
        } else if (typeInfo instanceof ByteTypeInfo && object instanceof byte[]) {
            return ArrayUtils.toObject((byte[]) object);
        } else if (typeInfo instanceof ShortTypeInfo && object instanceof short[]) {
            return ArrayUtils.toObject((short[]) object);
        } else if (typeInfo instanceof IntTypeInfo && object instanceof int[]) {
            return ArrayUtils.toObject((int[]) object);
        } else if (typeInfo instanceof LongTypeInfo && object instanceof long[]) {
            return ArrayUtils.toObject((long[]) object);
        } else if (typeInfo instanceof FloatTypeInfo && object instanceof float[]) {
            return ArrayUtils.toObject((float[]) object);
        } else if (typeInfo instanceof DoubleTypeInfo && object instanceof double[]) {
            return ArrayUtils.toObject((double[]) object);
        } else {
            return object;
        }
    }
}
