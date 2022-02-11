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

package org.apache.inlong.sort.flink.hive.formats.parquet;

import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.Type;

/** Writes a record to the Parquet API with the expected schema in order to be written to a file. */
public class ParquetRowWriter {

    private final RecordConsumer recordConsumer;

    private final FieldWriter[] filedWriters;

    private final String[] fieldNames;

    public ParquetRowWriter(
            RecordConsumer recordConsumer,
            RowType rowType,
            GroupType schema) {
        this.recordConsumer = recordConsumer;
        this.filedWriters = new FieldWriter[rowType.getFieldCount()];
        this.fieldNames = rowType.getFieldNames().toArray(new String[0]);
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            this.filedWriters[i] = createWriter(rowType.getTypeAt(i), schema.getType(i));
        }
    }

    /**
     * It writes a record to Parquet.
     *
     * @param record Contains the record that is going to be written.
     */
    public void write(final Row record) {
        recordConsumer.startMessage();
        for (int i = 0; i < filedWriters.length; i++) {
            if (record.getField(i) != null) {
                String fieldName = fieldNames[i];
                FieldWriter writer = filedWriters[i];

                recordConsumer.startField(fieldName, i);
                writer.write(record, i);
                recordConsumer.endField(fieldName, i);
            }
        }
        recordConsumer.endMessage();
    }

    private FieldWriter createWriter(LogicalType t, Type type) {
        if (type.isPrimitive()) {
            switch (t.getTypeRoot()) {
                case CHAR:
                case VARCHAR:
                case DECIMAL:
                    return new StringWriter();
                case BOOLEAN:
                    return new BooleanWriter();
                case BINARY:
                case VARBINARY:
                    return new BinaryWriter();
                case TINYINT:
                    return new ByteWriter();
                case SMALLINT:
                    return new ShortWriter();
                case DATE:
                case TIME_WITHOUT_TIME_ZONE:
                case INTEGER:
                    return new IntWriter();
                case TIMESTAMP_WITHOUT_TIME_ZONE:
                case BIGINT:
                    return new LongWriter();
                case FLOAT:
                    return new FloatWriter();
                case DOUBLE:
                    return new DoubleWriter();
                default:
                    throw new UnsupportedOperationException("Unsupported type: " + type);
            }
        } else {
            throw new IllegalArgumentException("Unsupported  data type: " + t);
        }
    }

    private interface FieldWriter {

        void write(Row row, int ordinal);
    }

    private class BooleanWriter implements FieldWriter {

        @Override
        public void write(Row row, int ordinal) {
            recordConsumer.addBoolean((Boolean) row.getField(ordinal));
        }
    }

    private class ByteWriter implements FieldWriter {

        @Override
        public void write(Row row, int ordinal) {
            recordConsumer.addInteger((Integer) row.getField(ordinal));
        }
    }

    private class ShortWriter implements FieldWriter {

        @Override
        public void write(Row row, int ordinal) {
            recordConsumer.addInteger((Integer) row.getField(ordinal));
        }
    }

    private class IntWriter implements FieldWriter {

        @Override
        public void write(Row row, int ordinal) {
            recordConsumer.addInteger((Integer) row.getField(ordinal));
        }
    }

    private class LongWriter implements FieldWriter {

        @Override
        public void write(Row row, int ordinal) {
            recordConsumer.addLong((Long) row.getField(ordinal));
        }
    }

    private class FloatWriter implements FieldWriter {

        @Override
        public void write(Row row, int ordinal) {
            recordConsumer.addFloat((Float) row.getField(ordinal));
        }
    }

    private class DoubleWriter implements FieldWriter {

        @Override
        public void write(Row row, int ordinal) {
            recordConsumer.addDouble((Double) row.getField(ordinal));
        }
    }

    private class StringWriter implements FieldWriter {

        @Override
        public void write(Row row, int ordinal) {
            recordConsumer.addBinary(Binary.fromReusedByteArray(((String) row.getField(ordinal)).getBytes()));
        }
    }

    private class BinaryWriter implements FieldWriter {

        @Override
        public void write(Row row, int ordinal) {
            recordConsumer.addBinary(Binary.fromReusedByteArray((byte[]) row.getField(ordinal)));
        }
    }
}
