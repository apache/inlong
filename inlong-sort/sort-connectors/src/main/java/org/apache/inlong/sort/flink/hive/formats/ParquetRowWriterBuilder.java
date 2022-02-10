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

package org.apache.inlong.sort.flink.hive.formats;

import static org.apache.inlong.sort.flink.hive.formats.ParquetSchemaConverter.convertToParquetMessageType;

import java.io.IOException;
import java.util.HashMap;
import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.formats.parquet.ParquetBuilder;
import org.apache.flink.formats.parquet.ParquetWriterFactory;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.apache.hadoop.conf.Configuration;
import org.apache.inlong.sort.protocol.sink.HiveSinkInfo.ParquetFileFormat;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;

/** {@link Row} of {@link ParquetWriter.Builder}. */
public class ParquetRowWriterBuilder extends ParquetWriter.Builder<Row, ParquetRowWriterBuilder> {

    private final RowType rowType;

    public ParquetRowWriterBuilder(OutputFile path, RowType rowType) {
        super(path);
        this.rowType = rowType;
    }

    @Override
    protected ParquetRowWriterBuilder self() {
        return this;
    }

    @Override
    protected WriteSupport<Row> getWriteSupport(Configuration conf) {
        return new ParquetWriteSupport();
    }

    private class ParquetWriteSupport extends WriteSupport<Row> {

        private final MessageType schema = convertToParquetMessageType("flink_schema", rowType);

        private ParquetRowWriter writer;

        @Override
        public WriteContext init(Configuration configuration) {
            return new WriteContext(schema, new HashMap<>());
        }

        @Override
        public void prepareForWrite(RecordConsumer recordConsumer) {
            this.writer = new ParquetRowWriter(recordConsumer, rowType, schema);
        }

        @Override
        public void write(Row record) {
            try {
                this.writer.write(record);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Create a parquet {@link BulkWriter.Factory}.
     *
     * @param rowType row type of parquet table.
     */
    public static ParquetWriterFactory<Row> createWriterFactory(
            RowType rowType,
            ParquetFileFormat parquetFileFormat) {
        return new ParquetWriterFactory<>(new FlinkParquetBuilder(rowType, parquetFileFormat));
    }

    /** Flink Row {@link ParquetBuilder}. */
    public static class FlinkParquetBuilder implements ParquetBuilder<Row> {

        private final RowType rowType;
        private final ParquetFileFormat parquetFileFormat;

        public FlinkParquetBuilder(
                RowType rowType,
                ParquetFileFormat parquetFileFormat) {
            this.rowType = rowType;
            this.parquetFileFormat = parquetFileFormat;
        }

        @Override
        public ParquetWriter<Row> createWriter(OutputFile out) throws IOException {
            return new ParquetRowWriterBuilder(out, rowType).build();
        }
    }
}
