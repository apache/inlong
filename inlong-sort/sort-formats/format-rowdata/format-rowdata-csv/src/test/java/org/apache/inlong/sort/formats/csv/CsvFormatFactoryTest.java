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

package org.apache.inlong.sort.formats.csv;

import org.apache.inlong.sort.formats.common.DateFormatInfo;
import org.apache.inlong.sort.formats.common.FloatFormatInfo;
import org.apache.inlong.sort.formats.common.FormatInfo;
import org.apache.inlong.sort.formats.common.FormatUtils;
import org.apache.inlong.sort.formats.common.RowFormatInfo;
import org.apache.inlong.sort.formats.common.StringFormatInfo;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.TestDynamicTableFactory;
import org.apache.flink.table.runtime.connector.source.ScanRuntimeProviderContext;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.TestLogger;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.table.factories.utils.FactoryMocks.createTableSink;
import static org.apache.flink.table.factories.utils.FactoryMocks.createTableSource;
import static org.junit.Assert.assertEquals;

/**
 * The unit test for csv format factory.
 */
public class CsvFormatFactoryTest extends TestLogger {

    public RowFormatInfo testFormatInfo;

    public TypeInformation<RowData> testTypeInformation;

    public ResolvedSchema resolvedSchema;

    public DataType dataType;

    @Before
    public void setup() {
        resolvedSchema =
                ResolvedSchema.of(
                        Column.physical("student_name", DataTypes.STRING()),
                        Column.physical("score", DataTypes.FLOAT()),
                        Column.physical("date", DataTypes.DATE()));
        dataType = resolvedSchema.toPhysicalRowDataType();
        RowType rowType = (RowType) dataType.getLogicalType();
        testTypeInformation = InternalTypeInfo.of(rowType);
        testFormatInfo = new RowFormatInfo(
                new String[]{"student_name", "score", "date"},
                new FormatInfo[]{
                        StringFormatInfo.INSTANCE,
                        FloatFormatInfo.INSTANCE,
                        new DateFormatInfo("yyyy-MM-dd")
                });
    }

    @Test
    public void testDeSeSchema() {
        final CsvRowDataDeserializationSchema expectedDeSer =
                new CsvRowDataDeserializationSchema.Builder(
                        testFormatInfo, testTypeInformation)
                                .setCharset("UTF-8")
                                .setFieldDelimiter(';')
                                .setQuoteCharacter('\'')
                                .setEscapeCharacter('\\')
                                .setNullLiteral("n/a")
                                .build();
        final Map<String, String> options = getAllOptions();
        DeserializationSchema<RowData> actualDeser = createDeserializationSchema(options);
        assertEquals(expectedDeSer, actualDeser);

        final CsvRowDataSerializationSchema expectedSer =
                new CsvRowDataSerializationSchema.Builder(testFormatInfo)
                        .setCharset("UTF-8")
                        .setFieldDelimiter(';')
                        .setQuoteCharacter('\'')
                        .setEscapeCharacter('\\')
                        .setNullLiteral("n/a")
                        .build();
        SerializationSchema<RowData> actualSer = createSerializationSchema(options);
        assertEquals(expectedSer, actualSer);
    }

    private Map<String, String> getAllOptions() {
        final Map<String, String> options = new HashMap<>();
        options.put("connector", TestDynamicTableFactory.IDENTIFIER);
        options.put("target", "MyTarget");
        options.put("buffer-size", "1000");

        options.put("format", CsvFormatFactory.IDENTIFIER);
        options.put("inlong-csv.row.format.info", FormatUtils.marshall(testFormatInfo));
        options.put("inlong-csv.format.field-delimiter", ";");
        options.put("inlong-csv.format.quote-character", "'");
        options.put("inlong-csv.format.escape-character", "\\");
        options.put("inlong-csv.format.null-literal", "n/a");
        return options;
    }

    private DeserializationSchema<RowData> createDeserializationSchema(
            Map<String, String> options) {
        final DynamicTableSource actualSource = createTableSource(resolvedSchema, options);
        assert actualSource instanceof TestDynamicTableFactory.DynamicTableSourceMock;
        TestDynamicTableFactory.DynamicTableSourceMock sourceMock =
                (TestDynamicTableFactory.DynamicTableSourceMock) actualSource;

        return sourceMock.valueFormat.createRuntimeDecoder(
                ScanRuntimeProviderContext.INSTANCE, dataType);
    }

    private SerializationSchema<RowData> createSerializationSchema(
            Map<String, String> options) {
        final DynamicTableSink actualSink = createTableSink(resolvedSchema, options);
        assert actualSink instanceof TestDynamicTableFactory.DynamicTableSinkMock;
        TestDynamicTableFactory.DynamicTableSinkMock sinkMock =
                (TestDynamicTableFactory.DynamicTableSinkMock) actualSink;

        return sinkMock.valueFormat.createRuntimeEncoder(null, dataType);
    }

}
