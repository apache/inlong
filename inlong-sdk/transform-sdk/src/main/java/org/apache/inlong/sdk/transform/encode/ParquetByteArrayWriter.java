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

package org.apache.inlong.sdk.transform.encode;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;

public final class ParquetByteArrayWriter<T> implements Closeable {

    private final org.apache.parquet.hadoop.ParquetWriter<T> writer;
    private final ParquetOutputByteArray outputByteArray;

    public static <T> ParquetByteArrayWriter<T> buildWriter(MessageType schema, ParquetWriteRunner<T> writeRunner)
            throws IOException {
        return new ParquetByteArrayWriter<>(new ParquetOutputByteArray(), schema, writeRunner);
    }

    private ParquetByteArrayWriter(ParquetOutputByteArray outputFile, MessageType schema,
            ParquetWriteRunner<T> writeRunner)
            throws IOException {
        this.writer = new Builder<T>(outputFile)
                .withType(schema)
                .withWriteRunner(writeRunner)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .withWriterVersion(ParquetProperties.WriterVersion.PARQUET_2_0)
                .build();
        outputByteArray = outputFile;
    }

    @Override
    public void close() throws IOException {
        this.writer.close();
    }

    public void write(T record) throws IOException {
        this.writer.write(record);
    }

    public ByteArrayOutputStream getByteArrayOutputStream() {
        return outputByteArray.getByteArrayOutputStream();
    }

    private static final class Builder<T>
            extends
                org.apache.parquet.hadoop.ParquetWriter.Builder<T, ParquetByteArrayWriter.Builder<T>> {

        private MessageType schema;
        private ParquetWriteRunner<T> writeRunner;

        private Builder(OutputFile file) {
            super(file);
        }

        public Builder<T> withType(MessageType schema) {
            this.schema = schema;
            return this;
        }

        public Builder<T> withWriteRunner(ParquetWriteRunner<T> writeRunner) {
            this.writeRunner = writeRunner;
            return this;
        }

        @Override
        protected Builder<T> self() {
            return this;
        }

        @Override
        protected WriteSupport<T> getWriteSupport(Configuration conf) {
            return new ParquetByteArrayWriter.SimpleWriteSupport<>(schema, writeRunner);
        }
    }

    private static class SimpleWriteSupport<T> extends WriteSupport<T> {

        private final MessageType schema;
        private final ParquetWriteRunner<T> writeRunner;
        private final ParquetValueWriter valueWriter;

        private RecordConsumer recordConsumer;

        SimpleWriteSupport(MessageType schema, ParquetWriteRunner<T> writeRunner) {
            this.schema = schema;
            this.writeRunner = writeRunner;
            this.valueWriter = this::write;
        }

        public void write(String name, Object value) {
            int fieldIndex = schema.getFieldIndex(name);
            PrimitiveType type = schema.getType(fieldIndex).asPrimitiveType();
            recordConsumer.startField(name, fieldIndex);

            switch (type.getPrimitiveTypeName()) {
                case INT32:
                    recordConsumer.addInteger((int) value);
                    break;
                case INT64:
                    recordConsumer.addLong((long) value);
                    break;
                case DOUBLE:
                    recordConsumer.addDouble((double) value);
                    break;
                case BOOLEAN:
                    recordConsumer.addBoolean((boolean) value);
                    break;
                case FLOAT:
                    recordConsumer.addFloat((float) value);
                    break;
                case BINARY:
                    if (type.getLogicalTypeAnnotation() == LogicalTypeAnnotation.stringType()) {
                        recordConsumer.addBinary(Binary.fromString((String) value));
                    } else {
                        throw new UnsupportedOperationException(
                                "Don't support writing " + type.getLogicalTypeAnnotation());
                    }
                    break;
                default:
                    throw new UnsupportedOperationException("Don't support writing " + type.getPrimitiveTypeName());
            }
            recordConsumer.endField(name, fieldIndex);
        }

        @Override
        public WriteContext init(Configuration configuration) {
            return new WriteContext(schema, Collections.emptyMap());
        }

        @Override
        public void prepareForWrite(RecordConsumer recordConsumer) {
            this.recordConsumer = recordConsumer;
        }

        @Override
        public void write(T record) {
            recordConsumer.startMessage();
            writeRunner.doWrite(record, valueWriter);
            recordConsumer.endMessage();
        }

        @Override
        public String getName() {
            return null;
        }
    }
}
