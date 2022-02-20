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

import java.sql.Timestamp;
import java.util.Date;
import java.util.Map;

import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.Type;

/** Writes a record to the Parquet API with the expected schema in order to be written to a file. */
public class ParquetRowWriter {

    static final String ARRAY_FIELD_NAME = "list";

    static final String MAP_ENTITY_FIELD_NAME = "key_value";
    static final String MAP_KEY_FIELD_NAME = "key";
    static final String MAP_VALUE_FIELD_NAME = "value";

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
                    return new StringWriter();
                case BOOLEAN:
                    return new BooleanWriter();
                case BINARY:
                case VARBINARY:
                    return new BinaryWriter();
                case DECIMAL:
                    return (row, ordinal) -> recordConsumer.addBinary(Binary.fromReusedByteArray(
                            row.getField(ordinal).toString().getBytes()));
                case TINYINT:
                    return new ByteWriter();
                case SMALLINT:
                    return new ShortWriter();
                case INTEGER:
                    return new IntWriter();
                case BIGINT:
                    return new LongWriter();
                case FLOAT:
                    return new FloatWriter();
                case DOUBLE:
                    return new DoubleWriter();
                case DATE:
                case TIME_WITHOUT_TIME_ZONE:
                    return (row, ordinal) -> recordConsumer.addInteger((int) ((Date) row.getField(ordinal)).getTime());
                case TIMESTAMP_WITHOUT_TIME_ZONE:
                    return (row, ordinal) -> recordConsumer.addLong(((Timestamp) row.getField(ordinal)).getTime());
                default:
                    throw new UnsupportedOperationException("Unsupported type: " + type);
            }
        } else {
            switch (t.getTypeRoot()) {
                case ARRAY:
                    return new ArrayWriter(((ArrayType) t).getElementType(),
                            type.asGroupType().getType(0).asGroupType().getType(0));
                case MAP:
                    return new MapWriter((MapType) t, type);
                default:
                    throw new UnsupportedOperationException("Unsupported type: " + type);
            }
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
            recordConsumer.addInteger((Byte) row.getField(ordinal));
        }
    }

    private class ShortWriter implements FieldWriter {

        @Override
        public void write(Row row, int ordinal) {
            recordConsumer.addInteger((Short) row.getField(ordinal));
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

    private class ArrayWriter implements FieldWriter {

        private final LogicalType elementTypeFlink;

        private final Type elementTypeParquet;

        public ArrayWriter(LogicalType elementTypeFlink, Type elementTypeParquet) {
            this.elementTypeFlink = elementTypeFlink;
            this.elementTypeParquet = elementTypeParquet;
        }

        @Override
        public void write(Row row, int ordinal) {
            if (elementTypeParquet.isPrimitive()) {
                switch (elementTypeFlink.getTypeRoot()) {
                    case CHAR:
                    case VARCHAR:
                    case DECIMAL:
                    case DATE:
                    case TIME_WITHOUT_TIME_ZONE:
                    case TIMESTAMP_WITHOUT_TIME_ZONE:
                        writeObjectArray(row.getField(ordinal));
                        break;
                    case BOOLEAN:
                        writeBooleanArray(row.getField(ordinal));
                        break;
                    case TINYINT:
                        writeTinyIntArray(row.getField(ordinal));
                        break;
                    case SMALLINT:
                        writeShortArray(row.getField(ordinal));
                        break;
                    case INTEGER:
                        writeIntArray(row.getField(ordinal));
                        break;
                    case BIGINT:
                        writeLongArray(row.getField(ordinal));
                        break;
                    case FLOAT:
                        writeFloatArray(row.getField(ordinal));
                        break;
                    case DOUBLE:
                        writeDoubleArray(row.getField(ordinal));
                        break;
                    default:
                        throw new UnsupportedOperationException(
                                "Unsupported element type in array: " + elementTypeParquet);
                }
            } else {
                throw new UnsupportedOperationException("Unsupported element type in array: " + elementTypeParquet);
            }
        }

        private void writeObjectArray(Object input) {
            recordConsumer.startGroup();
            if (input != null) {
                Object[] inputArray = (Object[]) input;
                if (inputArray.length > 0) {
                    recordConsumer.startField(ARRAY_FIELD_NAME, 0);
                    for (Object ele : inputArray) {
                        startGroupAndField(elementTypeParquet.getName(), 0);
                        FieldWriter writer = createWriter(elementTypeFlink, elementTypeParquet);
                        writer.write(Row.of(ele), 0);
                        endGroupAndField(elementTypeParquet.getName(), 0);
                    }
                    recordConsumer.endField(ARRAY_FIELD_NAME, 0);
                }
            }
            recordConsumer.endGroup();
        }

        private void writeBooleanArray(Object input) {
            if (input instanceof boolean[]) {
                boolean[] inputArray = (boolean[]) input;
                recordConsumer.startGroup();
                if (inputArray.length > 0) {
                    recordConsumer.startField(ARRAY_FIELD_NAME, 0);
                    for (boolean ele : inputArray) {
                        startGroupAndField(elementTypeParquet.getName(), 0);
                        FieldWriter writer = createWriter(elementTypeFlink, elementTypeParquet);
                        writer.write(Row.of(ele), 0);
                        endGroupAndField(elementTypeParquet.getName(), 0);
                    }
                    recordConsumer.endField(ARRAY_FIELD_NAME, 0);
                }
                recordConsumer.endGroup();
            } else {
                writeObjectArray(input);
            }
        }

        private void writeTinyIntArray(Object input) {
            if (input instanceof byte[]) {
                byte[] inputArray = (byte[]) input;
                recordConsumer.startGroup();
                if (inputArray.length > 0) {
                    recordConsumer.startField(ARRAY_FIELD_NAME, 0);
                    for (byte ele : inputArray) {
                        startGroupAndField(elementTypeParquet.getName(), 0);
                        FieldWriter writer = createWriter(elementTypeFlink, elementTypeParquet);
                        writer.write(Row.of(ele), 0);
                        endGroupAndField(elementTypeParquet.getName(), 0);
                    }
                    recordConsumer.endField(ARRAY_FIELD_NAME, 0);
                }
                recordConsumer.endGroup();
            } else {
                writeObjectArray(input);
            }
        }

        private void writeShortArray(Object input) {
            if (input instanceof short[]) {
                short[] inputArray = (short[]) input;
                recordConsumer.startGroup();
                if (inputArray.length > 0) {
                    recordConsumer.startField(ARRAY_FIELD_NAME, 0);
                    for (short ele : inputArray) {
                        startGroupAndField(elementTypeParquet.getName(), 0);
                        FieldWriter writer = createWriter(elementTypeFlink, elementTypeParquet);
                        writer.write(Row.of(ele), 0);
                        endGroupAndField(elementTypeParquet.getName(), 0);
                    }
                    recordConsumer.endField(ARRAY_FIELD_NAME, 0);
                }
                recordConsumer.endGroup();
            } else {
                writeObjectArray(input);
            }
        }

        private void writeIntArray(Object input) {
            if (input instanceof int[]) {
                int[] inputArray = (int[]) input;
                recordConsumer.startGroup();
                if (inputArray.length > 0) {
                    recordConsumer.startField(ARRAY_FIELD_NAME, 0);
                    for (int ele : inputArray) {
                        startGroupAndField(elementTypeParquet.getName(), 0);
                        FieldWriter writer = createWriter(elementTypeFlink, elementTypeParquet);
                        writer.write(Row.of(ele), 0);
                        endGroupAndField(elementTypeParquet.getName(), 0);
                    }
                    recordConsumer.endField(ARRAY_FIELD_NAME, 0);
                }
                recordConsumer.endGroup();
            } else {
                writeObjectArray(input);
            }
        }

        private void writeLongArray(Object input) {
            if (input instanceof long[]) {
                long[] inputArray = (long[]) input;
                recordConsumer.startGroup();
                if (inputArray.length > 0) {
                    recordConsumer.startField(ARRAY_FIELD_NAME, 0);
                    for (long ele : inputArray) {
                        startGroupAndField(elementTypeParquet.getName(), 0);
                        FieldWriter writer = createWriter(elementTypeFlink, elementTypeParquet);
                        writer.write(Row.of(ele), 0);
                        endGroupAndField(elementTypeParquet.getName(), 0);
                    }
                    recordConsumer.endField(ARRAY_FIELD_NAME, 0);
                }
                recordConsumer.endGroup();
            } else {
                writeObjectArray(input);
            }
        }

        private void writeFloatArray(Object input) {
            if (input instanceof float[]) {
                float[] inputArray = (float[]) input;
                recordConsumer.startGroup();
                if (inputArray.length > 0) {
                    recordConsumer.startField(ARRAY_FIELD_NAME, 0);
                    for (float ele : inputArray) {
                        startGroupAndField(elementTypeParquet.getName(), 0);
                        FieldWriter writer = createWriter(elementTypeFlink, elementTypeParquet);
                        writer.write(Row.of(ele), 0);
                        endGroupAndField(elementTypeParquet.getName(), 0);
                    }
                    recordConsumer.endField(ARRAY_FIELD_NAME, 0);
                }
                recordConsumer.endGroup();
            } else {
                writeObjectArray(input);
            }
        }

        private void writeDoubleArray(Object input) {
            if (input instanceof double[]) {
                double[] inputArray = (double[]) input;
                recordConsumer.startGroup();
                if (inputArray.length > 0) {
                    recordConsumer.startField(ARRAY_FIELD_NAME, 0);
                    for (double ele : inputArray) {
                        startGroupAndField(elementTypeParquet.getName(), 0);
                        FieldWriter writer = createWriter(elementTypeFlink, elementTypeParquet);
                        writer.write(Row.of(ele), 0);
                        endGroupAndField(elementTypeParquet.getName(), 0);
                    }
                    recordConsumer.endField(ARRAY_FIELD_NAME, 0);
                }
                recordConsumer.endGroup();
            } else {
                writeObjectArray(input);
            }
        }

    }

    private class MapWriter implements FieldWriter {

        private final LogicalType keyTypeFlink;

        private final LogicalType valueTypeFlink;

        private final Type keyTypeParquet;

        private final Type valueTypeParquet;

        public MapWriter(MapType mapTypeFlink, Type mapTypeParquet) {
            this.keyTypeFlink = mapTypeFlink.getKeyType();
            this.valueTypeFlink = mapTypeFlink.getValueType();
            GroupType groupType = mapTypeParquet.asGroupType().getType(0).asGroupType();
            this.keyTypeParquet = groupType.getType(0);
            this.valueTypeParquet = groupType.getType(1);
        }

        @Override
        public void write(Row row, int ordinal) {
            recordConsumer.startGroup();
            Object inputField = row.getField(ordinal);
            if (inputField == null) {
                return;
            }

            Map<?, ?> inputMap = (Map<?, ?>) row.getField(ordinal);
            if (inputMap.size() > 0) {
                FieldWriter keyWriter = createWriter(keyTypeFlink, keyTypeParquet);
                FieldWriter valueWriter = createWriter(valueTypeFlink, valueTypeParquet);
                recordConsumer.startField(MAP_ENTITY_FIELD_NAME, 0);
                for (Map.Entry<?, ?> entry : inputMap.entrySet()) {
                    recordConsumer.startGroup();

                    recordConsumer.startField(MAP_KEY_FIELD_NAME, 0);
                    keyWriter.write(Row.of(entry.getKey()), 0);
                    recordConsumer.endField(MAP_KEY_FIELD_NAME,0);

                    Object value = entry.getValue();
                    if (value != null) {
                        recordConsumer.startField(MAP_VALUE_FIELD_NAME, 1);
                        valueWriter.write(Row.of(value), 0);
                        recordConsumer.endField(MAP_VALUE_FIELD_NAME, 1);
                    }

                    recordConsumer.endGroup();
                }
                recordConsumer.endField(MAP_ENTITY_FIELD_NAME, 0);
            }
            recordConsumer.endGroup();
        }

    }

    private void startGroupAndField(String fieldName, int index) {
        recordConsumer.startGroup();
        recordConsumer.startField(fieldName, index);
    }

    private void endGroupAndField(String fieldName, int index) {
        recordConsumer.endField(fieldName, index);
        recordConsumer.endGroup();
    }
}
