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

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.zip.GZIPOutputStream;
import org.anarres.lzo.LzoAlgorithm;
import org.anarres.lzo.LzoCompressor;
import org.anarres.lzo.LzoLibrary;
import org.anarres.lzo.LzopOutputStream;
import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.types.Row;
import org.apache.inlong.sort.configuration.Configuration;
import org.apache.inlong.sort.configuration.Constants;
import org.apache.inlong.sort.protocol.sink.HiveSinkInfo.TextFileFormat;

public class TextRowWriter implements BulkWriter<Row> {

    private final OutputStream outputStream;

    private final TextFileFormat textFileFormat;

    private final int bufferSize;

    public TextRowWriter(
            FSDataOutputStream fsDataOutputStream,
            TextFileFormat textFileFormat,
            Configuration config) throws IOException {
        this.bufferSize = checkNotNull(config).getInteger(Constants.SINK_HIVE_TEXT_BUFFER_SIZE);
        this.outputStream = getCompressionOutputStream(checkNotNull(fsDataOutputStream), textFileFormat);
        this.textFileFormat = checkNotNull(textFileFormat);
    }

    @Override
    public void addElement(Row row) throws IOException {
        for (int i = 0; i < row.getArity(); i++) {
            outputStream.write(String.valueOf(row.getField(i)).getBytes(StandardCharsets.UTF_8));
            if (i != row.getArity() - 1) {
                outputStream.write(textFileFormat.getSplitter());
            }
        }
        outputStream.write(10); // start a new line
    }

    @Override
    public void flush() throws IOException {
        outputStream.flush();
    }

    @Override
    public void finish() throws IOException {
        outputStream.close();
    }

    private OutputStream getCompressionOutputStream(
            FSDataOutputStream outputStream,
            TextFileFormat textFileFormat) throws IOException {
        switch (textFileFormat.getCompressionType()) {
            case GZIP:
                return new GZIPOutputStream(outputStream, bufferSize, false);
            case LZO:
                LzoAlgorithm algorithm = LzoAlgorithm.LZO1X;
                LzoCompressor compressor = LzoLibrary.getInstance().newCompressor(algorithm, null);
                return new LzopOutputStream(outputStream, compressor, bufferSize);
            default:
                // TODO, should be wrapped with a buffered stream? we need a performance testing
                return outputStream;
        }
    }

    public static class Factory implements BulkWriter.Factory<Row> {

        private static final long serialVersionUID = 431993007405042674L;

        private final TextFileFormat textFileFormat;

        private final Configuration config;

        public Factory(
                TextFileFormat textFileFormat,
                Configuration config) {
            this.textFileFormat = checkNotNull(textFileFormat);
            this.config = checkNotNull(config);
        }

        @Override
        public BulkWriter<Row> create(FSDataOutputStream fsDataOutputStream) throws IOException {
            return new TextRowWriter(
                    fsDataOutputStream,
                    textFileFormat,
                    config);
        }
    }
}
