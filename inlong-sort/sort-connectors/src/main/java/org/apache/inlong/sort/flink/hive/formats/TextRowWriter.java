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
import java.nio.charset.StandardCharsets;
import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.types.Row;
import org.apache.inlong.sort.protocol.sink.HiveSinkInfo.TextFileFormat;

public class TextRowWriter implements BulkWriter<Row> {

    private final FSDataOutputStream fsDataOutputStream;

    private final TextFileFormat textFileFormat;

    public TextRowWriter(FSDataOutputStream fsDataOutputStream, TextFileFormat textFileFormat) {
        this.fsDataOutputStream = checkNotNull(fsDataOutputStream);
        this.textFileFormat = checkNotNull(textFileFormat);
    }

    @Override
    public void addElement(Row row) throws IOException {
        for (int i = 0; i < row.getArity(); i++) {
            fsDataOutputStream.write(String.valueOf(row.getField(i)).getBytes(StandardCharsets.UTF_8));
            if (i != row.getArity() - 1) {
                fsDataOutputStream.write(textFileFormat.getSplitter());
            }
        }
        fsDataOutputStream.write(10); // start a new line
    }

    @Override
    public void flush() throws IOException {
        fsDataOutputStream.flush();
    }

    @Override
    public void finish() throws IOException {
        fsDataOutputStream.close();
    }

    public static class Factory implements BulkWriter.Factory<Row> {

        private static final long serialVersionUID = 431993007405042674L;

        private final TextFileFormat textFileFormat;

        public Factory(TextFileFormat textFileFormat) {
            this.textFileFormat = checkNotNull(textFileFormat);
        }

        @Override
        public BulkWriter<Row> create(FSDataOutputStream fsDataOutputStream) throws IOException {
            return new TextRowWriter(fsDataOutputStream, textFileFormat);
        }
    }
}
