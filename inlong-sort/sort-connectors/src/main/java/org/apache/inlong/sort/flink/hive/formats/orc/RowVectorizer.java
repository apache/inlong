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

package org.apache.inlong.sort.flink.hive.formats.orc;

import java.io.Serializable;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Date;
import java.util.Map;

import org.apache.flink.shaded.guava18.com.google.common.annotations.VisibleForTesting;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.types.Row;
import org.apache.orc.TypeDescription;
import org.apache.orc.storage.common.type.HiveDecimal;
import org.apache.orc.storage.ql.exec.vector.BytesColumnVector;
import org.apache.orc.storage.ql.exec.vector.ColumnVector;
import org.apache.orc.storage.ql.exec.vector.DecimalColumnVector;
import org.apache.orc.storage.ql.exec.vector.DoubleColumnVector;
import org.apache.orc.storage.ql.exec.vector.ListColumnVector;
import org.apache.orc.storage.ql.exec.vector.LongColumnVector;
import org.apache.orc.storage.ql.exec.vector.MapColumnVector;
import org.apache.orc.storage.ql.exec.vector.TimestampColumnVector;
import org.apache.orc.storage.ql.exec.vector.VectorizedRowBatch;

/**
 * Copy from org.apache.flink:flink-orc_2.11:1.13.1
 *
 * TODO: UT
 */
public class RowVectorizer implements Serializable {

    private static final long serialVersionUID = 49467892954190649L;

    private final TypeDescription schema;
    private final LogicalType[] fieldTypes;

    public RowVectorizer(TypeDescription schema, LogicalType[] fieldTypes) {
        this.schema = schema;
        this.fieldTypes = fieldTypes;
    }

    /**
     * Provides the ORC schema.
     *
     * @return the ORC schema
     */
    public TypeDescription getSchema() {
        return this.schema;
    }

    public void vectorize(Row row, VectorizedRowBatch batch) {
        int rowId = batch.size++;
        for (int i = 0; i < row.getArity(); ++i) {
            setColumn(rowId, batch.cols[i], fieldTypes[i], row, i);
        }
    }

    /**
     * Referenced from RowDataVector in flink-orc and ParquetRowWriter
     */
    @VisibleForTesting
    static void setColumn(int rowId, ColumnVector column, LogicalType type, Row row, int columnId) {
        final Object field = row.getField(columnId);
        if (field == null) {
            setNull(column, rowId);
            return;
        }
        switch (type.getTypeRoot()) {
            case CHAR:
            case VARCHAR: {
                BytesColumnVector vector = (BytesColumnVector) column;
                final byte[] bytes = ((String) field).getBytes(StandardCharsets.UTF_8);
                vector.setVal(rowId, bytes, 0, bytes.length);
                break;
            }
            case BOOLEAN: {
                LongColumnVector vector = (LongColumnVector) column;
                vector.vector[rowId] = (Boolean) field ? 1 : 0;
                break;
            }
            case BINARY:
            case VARBINARY: {
                BytesColumnVector vector = (BytesColumnVector) column;
                byte[] bytes = (byte[]) field;
                vector.setVal(rowId, bytes, 0, bytes.length);
                break;
            }
            case DECIMAL: {
                DecimalColumnVector vector = (DecimalColumnVector) column;
                vector.set(rowId, HiveDecimal.create((BigDecimal) field));
                break;
            }
            case TINYINT: {
                LongColumnVector vector = (LongColumnVector) column;
                vector.vector[rowId] = (Byte) field;
                break;
            }
            case SMALLINT: {
                LongColumnVector vector = (LongColumnVector) column;
                vector.vector[rowId] = (Short) field;
                break;
            }
            case DATE: {
                LongColumnVector vector = (LongColumnVector) column;
                vector.vector[rowId] = ((Date) field).getTime();
                break;
            }
            case INTEGER: {
                LongColumnVector vector = (LongColumnVector) column;
                vector.vector[rowId] = (Integer) field;
                break;
            }
            case TIME_WITHOUT_TIME_ZONE: {
                LongColumnVector vector = (LongColumnVector) column;
                vector.vector[rowId] = ((Time) field).getTime();
                break;
            }
            case BIGINT: {
                LongColumnVector vector = (LongColumnVector) column;
                vector.vector[rowId] = (Long) field;
                break;
            }
            case FLOAT: {
                DoubleColumnVector vector = (DoubleColumnVector) column;
                vector.vector[rowId] = (Float) field;
                break;
            }
            case DOUBLE: {
                DoubleColumnVector vector = (DoubleColumnVector) column;
                vector.vector[rowId] = (Double) field;
                break;
            }
            case TIMESTAMP_WITHOUT_TIME_ZONE: {
                Timestamp timestamp = (Timestamp) field;
                TimestampColumnVector vector = (TimestampColumnVector) column;
                vector.set(rowId, timestamp);
                break;
            }
            case ARRAY: {
                ColumnVector innerVector =
                        constructColumnVectorFromArray(field, ((ArrayType) type).getElementType());
                if (innerVector == null) {
                    setNull(column, rowId);
                    break;
                }

                ListColumnVector tempListVector = new ListColumnVector(1, innerVector);
                tempListVector.lengths[0] = innerVector.isNull.length;
                ListColumnVector vector = (ListColumnVector) column;
                vector.setElement(rowId, 0, tempListVector);
                break;
            }
            case MAP: {
                Map<?, ?> mapField = (Map<?, ?>) field;
                int mapEleSize = mapField.size();
                if (mapEleSize == 0) {
                    setNull(column, rowId);
                    break;
                }

                Object[] keys = new Object[mapEleSize];
                Object[] values = new Object[mapEleSize];
                int i = 0;
                for (Map.Entry<?, ?> entry : mapField.entrySet()) {
                    keys[i] = entry.getKey();
                    values[i] = entry.getValue();
                    ++i;
                }

                MapType mapType = (MapType) type;
                ColumnVector keysVector = constructColumnVectorFromArray(keys, mapType.getKeyType());
                ColumnVector valuesVector = constructColumnVectorFromArray(values, mapType.getValueType());
                MapColumnVector tempMapVector = new MapColumnVector(1, keysVector, valuesVector);
                tempMapVector.lengths[0] = mapEleSize;

                MapColumnVector vector = (MapColumnVector) column;
                vector.setElement(rowId, 0, tempMapVector);
                break;
            }
            default:
                throw new UnsupportedOperationException("Unsupported type: " + type);
        }
    }

    @VisibleForTesting
    static ColumnVector constructColumnVectorFromArray(Object input, LogicalType elementType) {
        switch (elementType.getTypeRoot()) {
            case CHAR:
            case VARCHAR: {
                return createVectorFromStringArray(input);
            }
            case BOOLEAN: {
                return createVectorFromBooleanArray(input);
            }
            case DECIMAL: {
                DecimalType decimalType = (DecimalType) elementType;
                return createVectorFromDecimalArray(input, decimalType.getPrecision(), decimalType.getScale());
            }
            case TINYINT: {
                return createVectorFromTinyIntArray(input);
            }
            case SMALLINT: {
                return createVectorFromShortArray(input);
            }
            case DATE: {
                return createVectorFromDateArray(input);
            }
            case INTEGER: {
                return createVectorFromIntArray(input);
            }
            case TIME_WITHOUT_TIME_ZONE: {
                return createVectorFromTimeArray(input);
            }
            case BIGINT: {
                return createVectorFromLongArray(input);
            }
            case FLOAT: {
                return createVectorFromFloatArray(input);
            }
            case DOUBLE: {
                return createVectorFromDoubleArray(input);
            }
            case TIMESTAMP_WITHOUT_TIME_ZONE: {
                return createVectorFromTimestampArray(input);
            }
            default:
                throw new UnsupportedOperationException("Unsupported type: " + elementType);
        }
    }

    private static BytesColumnVector createVectorFromStringArray(Object input) {
        Object[] inputArray = (Object[]) input;
        int length = inputArray.length;
        if (inputArray.length == 0) {
            return null;
        }

        BytesColumnVector vector = new BytesColumnVector(length);
        vector.initBuffer();
        for (int i = 0; i < length; i++) {
            vector.setVal(i, ((String) inputArray[i]).getBytes(StandardCharsets.UTF_8));
        }

        return vector;
    }

    private static LongColumnVector createVectorFromBooleanArray(Object input) {
        if (input instanceof boolean[]) {
            boolean[] inputArray = (boolean[]) input;
            int length = inputArray.length;
            if (length == 0) {
                return null;
            }

            LongColumnVector vector = new LongColumnVector(length);
            for (int i = 0; i < length; i++) {
                vector.vector[i] = inputArray[i] ? 1 : 0;
            }
            return vector;
        } else {
            Object[] inputArray = (Object[]) input;
            int length = inputArray.length;
            if (length == 0) {
                return null;
            }

            LongColumnVector vector = new LongColumnVector(length);
            for (int i = 0; i < length; i++) {
                vector.vector[i] = ((Boolean) inputArray[i]) ? 1 : 0;
            }
            return vector;
        }
    }

    private static DecimalColumnVector createVectorFromDecimalArray(Object input, int precision, int scale) {
        Object[] inputArray = (Object[]) input;
        int length = inputArray.length;
        if (length == 0) {
            return null;
        }

        DecimalColumnVector vector = new DecimalColumnVector(length, precision, scale);
        for (int i = 0; i < length; i++) {
            vector.set(i, HiveDecimal.create((BigDecimal) inputArray[i]));
        }
        return vector;
    }

    private static LongColumnVector createVectorFromTinyIntArray(Object input) {
        if (input instanceof byte[]) {
            byte[] inputArray = (byte[]) input;
            int length = inputArray.length;
            if (length == 0) {
                return null;
            }

            LongColumnVector vector = new LongColumnVector(length);
            for (int i = 0; i < length; i++) {
                vector.vector[i] = inputArray[i];
            }
            return vector;
        } else {
            Object[] inputArray = (Object[]) input;
            int length = inputArray.length;
            if (length == 0) {
                return null;
            }

            LongColumnVector vector = new LongColumnVector(length);
            for (int i = 0; i < length; i++) {
                vector.vector[i] = (Byte) inputArray[i];
            }
            return vector;
        }
    }

    private static LongColumnVector createVectorFromShortArray(Object input) {
        if (input instanceof short[]) {
            short[] inputArray = (short[]) input;
            int length = inputArray.length;
            if (length == 0) {
                return null;
            }

            LongColumnVector vector = new LongColumnVector(length);
            for (int i = 0; i < length; i++) {
                vector.vector[i] = inputArray[i];
            }
            return vector;
        } else {
            Object[] inputArray = (Object[]) input;
            int length = inputArray.length;
            if (length == 0) {
                return null;
            }

            LongColumnVector vector = new LongColumnVector(length);
            for (int i = 0; i < length; i++) {
                vector.vector[i] = (Short) inputArray[i];
            }
            return vector;
        }
    }

    private static LongColumnVector createVectorFromIntArray(Object input) {
        if (input instanceof int[]) {
            int[] inputArray = (int[]) input;
            int length = inputArray.length;
            if (length == 0) {
                return null;
            }

            LongColumnVector vector = new LongColumnVector(length);
            for (int i = 0; i < length; i++) {
                vector.vector[i] = inputArray[i];
            }
            return vector;
        } else {
            Object[] inputArray = (Object[]) input;
            int length = inputArray.length;
            if (length == 0) {
                return null;
            }

            LongColumnVector vector = new LongColumnVector(length);
            for (int i = 0; i < length; i++) {
                vector.vector[i] = (Integer) inputArray[i];
            }
            return vector;
        }
    }

    private static LongColumnVector createVectorFromLongArray(Object input) {
        if (input instanceof long[]) {
            long[] inputArray = (long[]) input;
            int length = inputArray.length;
            if (length == 0) {
                return null;
            }

            LongColumnVector vector = new LongColumnVector(length);
            System.arraycopy(inputArray, 0, vector.vector, 0, length);
            return vector;
        } else {
            Object[] inputArray = (Object[]) input;
            int length = inputArray.length;
            if (length == 0) {
                return null;
            }

            LongColumnVector vector = new LongColumnVector(length);
            for (int i = 0; i < length; i++) {
                vector.vector[i] = (Long) inputArray[i];
            }
            return vector;
        }
    }

    private static DoubleColumnVector createVectorFromFloatArray(Object input) {
        if (input instanceof float[]) {
            float[] inputArray = (float[]) input;
            int length = inputArray.length;
            if (length == 0) {
                return null;
            }

            DoubleColumnVector vector = new DoubleColumnVector(length);
            for (int i = 0; i < length; i++) {
                vector.vector[i] = inputArray[i];
            }
            return vector;
        } else {
            Object[] inputArray = (Object[]) input;
            int length = inputArray.length;
            if (length == 0) {
                return null;
            }

            DoubleColumnVector vector = new DoubleColumnVector(length);
            for (int i = 0; i < length; i++) {
                vector.vector[i] = (Float) inputArray[i];
            }
            return vector;
        }
    }

    private static DoubleColumnVector createVectorFromDoubleArray(Object input) {
        if (input instanceof double[]) {
            double[] inputArray = (double[]) input;
            int length = inputArray.length;
            if (length == 0) {
                return null;
            }

            DoubleColumnVector vector = new DoubleColumnVector(length);
            System.arraycopy(inputArray, 0, vector.vector, 0, length);
            return vector;
        } else {
            Object[] inputArray = (Object[]) input;
            int length = inputArray.length;
            if (length == 0) {
                return null;
            }

            DoubleColumnVector vector = new DoubleColumnVector(length);
            for (int i = 0; i < length; i++) {
                vector.vector[i] = (Double) inputArray[i];
            }
            return vector;
        }
    }

    private static LongColumnVector createVectorFromDateArray(Object input) {
        Object[] inputArray = (Object[]) input;
        int length = inputArray.length;
        if (length == 0) {
            return null;
        }

        LongColumnVector vector = new LongColumnVector(length);
        for (int i = 0; i < length; i++) {
            vector.vector[i] = ((Date) inputArray[i]).getTime();
        }
        return vector;
    }

    private static LongColumnVector createVectorFromTimeArray(Object input) {
        Object[] inputArray = (Object[]) input;
        int length = inputArray.length;
        if (length == 0) {
            return null;
        }

        LongColumnVector vector = new LongColumnVector(length);
        for (int i = 0; i < length; i++) {
            vector.vector[i] = ((Time) inputArray[i]).getTime();
        }
        return vector;
    }

    private static TimestampColumnVector createVectorFromTimestampArray(Object input) {
        Object[] inputArray = (Object[]) input;
        int length = inputArray.length;
        if (length == 0) {
            return null;
        }

        TimestampColumnVector vector = new TimestampColumnVector(length);
        for (int i = 0; i < length; i++) {
            vector.set(i, (Timestamp) inputArray[i]);
        }
        return vector;
    }

    private static void setNull(ColumnVector column, int rowId) {
        column.noNulls = false;
        column.isNull[rowId] = true;
    }

}
