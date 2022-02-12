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
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.types.Row;
import org.apache.orc.TypeDescription;
import org.apache.orc.storage.common.type.HiveDecimal;
import org.apache.orc.storage.ql.exec.vector.BytesColumnVector;
import org.apache.orc.storage.ql.exec.vector.ColumnVector;
import org.apache.orc.storage.ql.exec.vector.DecimalColumnVector;
import org.apache.orc.storage.ql.exec.vector.DoubleColumnVector;
import org.apache.orc.storage.ql.exec.vector.LongColumnVector;
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
    private static void setColumn(int rowId, ColumnVector column, LogicalType type, Row row, int columnId) {
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
            // TODO: support MAP type
            default:
                throw new UnsupportedOperationException("Unsupported type: " + type);
        }
    }
}
