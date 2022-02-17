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
import org.apache.flink.table.types.logical.LogicalType;
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

import static org.apache.flink.shaded.guava18.com.google.common.base.Preconditions.checkState;

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
            column.noNulls = false;
            column.isNull[rowId] = true;
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
                Object[] inputArray = (Object[]) field;
                ColumnVector innerVector = constructColumnVectorFromArray(inputArray);
                ListColumnVector tempListVector = new ListColumnVector(1, innerVector);
                tempListVector.lengths[0] = inputArray.length;
                ListColumnVector vector = (ListColumnVector) column;
                vector.setElement(rowId, 0, tempListVector);
                break;
            }
            case MAP: {
                Map<?, ?> mapField = (Map<?, ?>) field;
                int mapEleSize = mapField.size();
                Object[] keys = new Object[mapEleSize];
                Object[] values = new Object[mapEleSize];
                int i = 0;
                for (Map.Entry<?, ?> entry : mapField.entrySet()) {
                    keys[i] = entry.getKey();
                    values[i] = entry.getValue();
                    ++i;
                }

                ColumnVector keysVector = constructColumnVectorFromArray(keys);
                ColumnVector valuesVector = constructColumnVectorFromArray(values);
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
    static ColumnVector constructColumnVectorFromArray(Object[] inputArray) {
        int length = inputArray.length;
        checkState(length > 0);
        Object ele0 = inputArray[0];
        if (ele0 instanceof String) {
            BytesColumnVector vector = new BytesColumnVector(length);
            for (int i = 0; i < length; i++) {
                vector.setVal(i, ((String) inputArray[i]).getBytes(StandardCharsets.UTF_8));
            }
            return vector;

        } else if (ele0 instanceof Boolean) {
            LongColumnVector vector = new LongColumnVector(length);
            for (int i = 0; i < length; i++) {
                vector.vector[i] = (Boolean) inputArray[i] ? 1 : 0;
            }
            return vector;

        } else if (ele0 instanceof byte[]) {
            BytesColumnVector vector = new BytesColumnVector(length);
            for (int i = 0; i < length; i++) {
                vector.setVal(i, (byte[]) inputArray[i]);
            }
            return vector;

        } else if (ele0 instanceof BigDecimal) {
            BigDecimal decimal = (BigDecimal) ele0;
            DecimalColumnVector vector = new DecimalColumnVector(length, decimal.precision(), decimal.scale());
            for (int i = 0; i < length; i++) {
                vector.set(i, HiveDecimal.create((BigDecimal) inputArray[i]));
            }
            return vector;

        } else if (ele0 instanceof Byte) {
            LongColumnVector vector = new LongColumnVector(length);
            for (int i = 0; i < length; i++) {
                vector.vector[i] = (Byte) inputArray[i];
            }
            return vector;

        } else if (ele0 instanceof Short) {
            LongColumnVector vector = new LongColumnVector(length);
            for (int i = 0; i < length; i++) {
                vector.vector[i] = (Short) inputArray[i];
            }
            return vector;

        } else if (ele0 instanceof Integer) {
            LongColumnVector vector = new LongColumnVector(length);
            for (int i = 0; i < length; i++) {
                vector.vector[i] = (Integer) inputArray[i];
            }
            return vector;

        } else if (ele0 instanceof Long) {
            LongColumnVector vector = new LongColumnVector(length);
            for (int i = 0; i < length; i++) {
                vector.vector[i] = (Long) inputArray[i];
            }
            return vector;

        } else if (ele0 instanceof Float) {
            DoubleColumnVector vector = new DoubleColumnVector(length);
            for (int i = 0; i < length; i++) {
                vector.vector[i] = (Float) inputArray[i];
            }
            return vector;

        } else if (ele0 instanceof Double) {
            DoubleColumnVector vector = new DoubleColumnVector(length);
            for (int i = 0; i < length; i++) {
                vector.vector[i] = (Double) inputArray[i];
            }
            return vector;

        } else if (ele0 instanceof Time) {
            LongColumnVector vector = new LongColumnVector(length);
            for (int i = 0; i < length; i++) {
                vector.vector[i] = ((Time) inputArray[i]).getTime();
            }
            return vector;

        } else if (ele0 instanceof Timestamp) {
            TimestampColumnVector vector = new TimestampColumnVector(length);
            for (int i = 0; i < length; i++) {
                vector.set(i, (Timestamp) inputArray[i]);
            }
            return vector;

        } else if (ele0 instanceof Date) {
            LongColumnVector vector = new LongColumnVector(length);
            for (int i = 0; i < length; i++) {
                vector.vector[i] = ((Date) inputArray[i]).getTime();
            }
            return vector;

        } else {
            throw new UnsupportedOperationException("Unsupported class: " + ele0.getClass());
        }
    }

}
