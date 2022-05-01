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
import java.util.Map;
import java.util.zip.GZIPOutputStream;

import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.shaded.guava18.com.google.common.annotations.VisibleForTesting;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.types.Row;
import org.apache.inlong.sort.configuration.Configuration;
import org.apache.inlong.sort.configuration.Constants;
import org.apache.inlong.sort.protocol.sink.HiveSinkInfo.TextFileFormat;

public class TextRowWriter implements BulkWriter<Row> {

    private static final String NULL_STRING = "null";

    private static final String ARRAY_SPLITTER = ",";
    private static final String ARRAY_START_SYMBOL = "[";
    private static final String ARRAY_END_SYMBOL = "]";

    private static final String MAP_START_SYMBOL = "{";
    private static final String MAP_END_SYMBOL = "}";
    private static final String MAP_ENTRY_SPLITTER = ",";
    private static final String MAP_KEY_VALUE_SPLITTER = "=";

    private final OutputStream outputStream;

    private final TextFileFormat textFileFormat;

    private final int bufferSize;

    private final LogicalType[] fieldTypes;

    public TextRowWriter(
            FSDataOutputStream fsDataOutputStream,
            TextFileFormat textFileFormat,
            Configuration config,
            LogicalType[] fieldTypes) throws IOException {
        this.bufferSize = checkNotNull(config).getInteger(Constants.SINK_HIVE_TEXT_BUFFER_SIZE);
        this.outputStream = getCompressionOutputStream(checkNotNull(fsDataOutputStream), textFileFormat);
        this.textFileFormat = checkNotNull(textFileFormat);
        this.fieldTypes = checkNotNull(fieldTypes);
    }

    @Override
    public void addElement(Row row) throws IOException {
        for (int i = 0; i < row.getArity(); i++) {
            String fieldStr = convertField(row.getField(i), fieldTypes[i]);
            outputStream.write(fieldStr.getBytes(StandardCharsets.UTF_8));
            if (i != row.getArity() - 1) {
                outputStream.write(textFileFormat.getSplitter());
            }
        }
        outputStream.write(10); // start a new line
    }

    @VisibleForTesting
    static String convertField(Object field, LogicalType fieldType) {
        if (field == null) {
            return NULL_STRING;
        }

        switch (fieldType.getTypeRoot()) {
            case ARRAY:
                return convertArray(field, ((ArrayType) fieldType).getElementType());
            case MAP:
                return convertMap((Map<?, ?>) field);
            default:
                return String.valueOf(field);
        }
    }

    private static String convertArray(Object input, LogicalType elementType) {
        switch (elementType.getTypeRoot()) {
            case BOOLEAN:
                return convertBooleanArray(input);
            case TINYINT:
                return convertByteArray(input);
            case SMALLINT:
                return convertShortArray(input);
            case INTEGER:
                return convertIntArray(input);
            case BIGINT:
                return convertLongArray(input);
            case FLOAT:
                return convertFloatArray(input);
            case DOUBLE:
                return convertDoubleArray(input);
            default:
                return convertObjectArray((Object[]) input);
        }
    }

    private static String convertObjectArray(Object[] objArray) {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(ARRAY_START_SYMBOL);
        for (int i = 0; i < objArray.length; i++) {
            stringBuilder.append(objArray[i]);
            if (i != objArray.length - 1) {
                stringBuilder.append(ARRAY_SPLITTER);
            }
        }
        stringBuilder.append(ARRAY_END_SYMBOL);
        return stringBuilder.toString();
    }

    private static String convertBooleanArray(Object input) {
        if (input instanceof boolean[]) {
            boolean[] inputArray = (boolean[]) input;
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append(ARRAY_START_SYMBOL);
            for (int i = 0; i < inputArray.length; i++) {
                stringBuilder.append(inputArray[i]);
                if (i != inputArray.length - 1) {
                    stringBuilder.append(ARRAY_SPLITTER);
                }
            }
            stringBuilder.append(ARRAY_END_SYMBOL);
            return stringBuilder.toString();
        } else {
            return convertObjectArray((Object[]) input);
        }
    }

    private static String convertByteArray(Object input) {
        if (input instanceof byte[]) {
            byte[] inputArray = (byte[]) input;
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append(ARRAY_START_SYMBOL);
            for (int i = 0; i < inputArray.length; i++) {
                stringBuilder.append(inputArray[i]);
                if (i != inputArray.length - 1) {
                    stringBuilder.append(ARRAY_SPLITTER);
                }
            }
            stringBuilder.append(ARRAY_END_SYMBOL);
            return stringBuilder.toString();
        } else {
            return convertObjectArray((Object[]) input);
        }
    }

    private static String convertShortArray(Object input) {
        if (input instanceof short[]) {
            short[] inputArray = (short[]) input;
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append(ARRAY_START_SYMBOL);
            for (int i = 0; i < inputArray.length; i++) {
                stringBuilder.append(inputArray[i]);
                if (i != inputArray.length - 1) {
                    stringBuilder.append(ARRAY_SPLITTER);
                }
            }
            stringBuilder.append(ARRAY_END_SYMBOL);
            return stringBuilder.toString();
        } else {
            return convertObjectArray((Object[]) input);
        }
    }

    private static String convertIntArray(Object input) {
        if (input instanceof int[]) {
            int[] inputArray = (int[]) input;
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append(ARRAY_START_SYMBOL);
            for (int i = 0; i < inputArray.length; i++) {
                stringBuilder.append(inputArray[i]);
                if (i != inputArray.length - 1) {
                    stringBuilder.append(ARRAY_SPLITTER);
                }
            }
            stringBuilder.append(ARRAY_END_SYMBOL);
            return stringBuilder.toString();
        } else {
            return convertObjectArray((Object[]) input);
        }
    }

    private static String convertLongArray(Object input) {
        if (input instanceof long[]) {
            long[] inputArray = (long[]) input;
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append(ARRAY_START_SYMBOL);
            for (int i = 0; i < inputArray.length; i++) {
                stringBuilder.append(inputArray[i]);
                if (i != inputArray.length - 1) {
                    stringBuilder.append(ARRAY_SPLITTER);
                }
            }
            stringBuilder.append(ARRAY_END_SYMBOL);
            return stringBuilder.toString();
        } else {
            return convertObjectArray((Object[]) input);
        }
    }

    private static String convertFloatArray(Object input) {
        if (input instanceof float[]) {
            float[] inputArray = (float[]) input;
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append(ARRAY_START_SYMBOL);
            for (int i = 0; i < inputArray.length; i++) {
                stringBuilder.append(inputArray[i]);
                if (i != inputArray.length - 1) {
                    stringBuilder.append(ARRAY_SPLITTER);
                }
            }
            stringBuilder.append(ARRAY_END_SYMBOL);
            return stringBuilder.toString();
        } else {
            return convertObjectArray((Object[]) input);
        }
    }

    private static String convertDoubleArray(Object input) {
        if (input instanceof double[]) {
            double[] inputArray = (double[]) input;
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append(ARRAY_START_SYMBOL);
            for (int i = 0; i < inputArray.length; i++) {
                stringBuilder.append(inputArray[i]);
                if (i != inputArray.length - 1) {
                    stringBuilder.append(ARRAY_SPLITTER);
                }
            }
            stringBuilder.append(ARRAY_END_SYMBOL);
            return stringBuilder.toString();
        } else {
            return convertObjectArray((Object[]) input);
        }
    }

    private static String convertMap(Map<?, ?> inputMap) {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(MAP_START_SYMBOL);
        int i = 0;
        int mapSize = inputMap.size();
        for (Map.Entry<?, ?> entry : inputMap.entrySet()) {
            ++i;
            stringBuilder.append(entry.getKey());
            stringBuilder.append(MAP_KEY_VALUE_SPLITTER);
            stringBuilder.append(entry.getValue());
            if (i != mapSize) {
                stringBuilder.append(MAP_ENTRY_SPLITTER);
            }
        }
        stringBuilder.append(MAP_END_SYMBOL);
        return stringBuilder.toString();
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
                throw new IllegalArgumentException("LZO compression is not supported yet!");
            default:
                // TODO, should be wrapped with a buffered stream? we need a performance testing
                return outputStream;
        }
    }

    public static class Factory implements BulkWriter.Factory<Row> {

        private static final long serialVersionUID = 431993007405042674L;

        private final TextFileFormat textFileFormat;

        private final Configuration config;

        private final LogicalType[] fieldTypes;

        public Factory(TextFileFormat textFileFormat, LogicalType[] fieldTypes, Configuration config) {
            this.textFileFormat = checkNotNull(textFileFormat);
            this.fieldTypes = checkNotNull(fieldTypes);
            this.config = checkNotNull(config);
        }

        @Override
        public BulkWriter<Row> create(FSDataOutputStream fsDataOutputStream) throws IOException {
            return new TextRowWriter(fsDataOutputStream, textFileFormat, config, fieldTypes);
        }
    }
}
