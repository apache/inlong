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

package org.apache.inlong.sort.formats.kv;

import org.apache.inlong.common.pojo.sort.dataflow.field.format.BooleanFormatInfo;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.FormatInfo;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.FormatUtils;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.IntFormatInfo;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.RowFormatInfo;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.StringFormatInfo;
import org.apache.inlong.sort.formats.base.TableFormatConstants;
import org.apache.inlong.sort.formats.base.TableFormatOptions;
import org.apache.inlong.sort.formats.base.TableFormatUtils;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.TestDynamicTableFactory;
import org.apache.flink.table.factories.utils.FactoryMocks;
import org.apache.flink.table.runtime.connector.source.ScanRuntimeProviderContext;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.LogicalType;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import static org.apache.flink.table.factories.utils.FactoryMocks.PHYSICAL_DATA_TYPE;
import static org.apache.flink.table.factories.utils.FactoryMocks.SCHEMA;
import static org.apache.flink.table.factories.utils.FactoryMocks.createTableSink;
import static org.apache.flink.table.factories.utils.FactoryMocks.createTableSource;
import static org.junit.Assert.assertEquals;

/** Tests for the {@link KvFormatFactory}. */
public class KvFormatFactoryTest {

    private static final RowFormatInfo TEST_FORMAT_SCHEMA =
            new RowFormatInfo(
                    new String[]{"a", "b", "c"},
                    new FormatInfo[]{
                            StringFormatInfo.INSTANCE,
                            IntFormatInfo.INSTANCE,
                            BooleanFormatInfo.INSTANCE
                    });

    @Test
    public void testSerDeSchema() {
        final Map<String, String> tableOptions = getAllOptions();
        testSerializationSchema(tableOptions);
        testDeserializationSchema(tableOptions);
    }

    private Map<String, String> getModifiedOptions(
            Consumer<Map<String, String>> optionModifier) {
        Map<String, String> options = getAllOptions();
        optionModifier.accept(options);
        return options;
    }

    private void testDeserializationSchema(Map<String, String> tableOptions) {
        LogicalType rowType = TableFormatUtils.deriveLogicalType(TEST_FORMAT_SCHEMA);
        TypeInformation<RowData> producedType = InternalTypeInfo.of(rowType);
        final KvRowDataDeserializationSchema expectedDeser =
                new KvRowDataDeserializationSchema.Builder(TEST_FORMAT_SCHEMA, producedType)
                        .setEntryDelimiter('&')
                        .setKvDelimiter('=')
                        .setCharset(StandardCharsets.ISO_8859_1.name())
                        .setEscapeCharacter('\\')
                        .setQuoteCharacter('\"')
                        .setNullLiteral("n/a")
                        .setIgnoreErrors(true)
                        .build();

        DeserializationSchema<RowData> actualDeser = createDeserializationSchema(tableOptions);
        assertEquals(expectedDeser, actualDeser);
    }

    private void testSerializationSchema(Map<String, String> tableOptions) {
        final KvRowDataSerializationSchema expectedSer =
                new KvRowDataSerializationSchema.Builder(TEST_FORMAT_SCHEMA)
                        .setEntryDelimiter('&')
                        .setKvDelimiter('=')
                        .setCharset(StandardCharsets.ISO_8859_1.name())
                        .setEscapeCharacter('\\')
                        .setQuoteCharacter('\"')
                        .setNullLiteral("n/a")
                        .setIgnoreErrors(true)
                        .build();
        SerializationSchema<RowData> actualSer = createSerializationSchema(tableOptions);
        assertEquals(expectedSer, actualSer);
    }

    private DeserializationSchema<RowData> createDeserializationSchema(
            Map<String, String> options) {
        final DynamicTableSource actualSource = createTableSource(FactoryMocks.SCHEMA, options);
        assert actualSource instanceof TestDynamicTableFactory.DynamicTableSourceMock;
        TestDynamicTableFactory.DynamicTableSourceMock sourceMock =
                (TestDynamicTableFactory.DynamicTableSourceMock) actualSource;
        return sourceMock.valueFormat.createRuntimeDecoder(
                ScanRuntimeProviderContext.INSTANCE, PHYSICAL_DATA_TYPE);
    }

    private SerializationSchema<RowData> createSerializationSchema(Map<String, String> options) {
        final DynamicTableSink actualSink = createTableSink(SCHEMA, options);
        assert actualSink instanceof TestDynamicTableFactory.DynamicTableSinkMock;
        TestDynamicTableFactory.DynamicTableSinkMock sinkMock =
                (TestDynamicTableFactory.DynamicTableSinkMock) actualSink;
        return sinkMock.valueFormat.createRuntimeEncoder(null, PHYSICAL_DATA_TYPE);
    }

    private Map<String, String> getAllOptions() {
        final Map<String, String> options = new HashMap<>();
        options.put("connector", TestDynamicTableFactory.IDENTIFIER);
        options.put("target", "MyTarget");
        options.put("buffer-size", "1000");

        options.put("format", KvFormatFactory.IDENTIFIER);
        options.put(KvFormatFactory.KV_PREFIX + TableFormatOptions.ROW_FORMAT_INFO.key(),
                FormatUtils.marshall(TEST_FORMAT_SCHEMA));
        options.put(KvFormatFactory.KV_PREFIX + TableFormatConstants.FORMAT_KV_ENTRY_DELIMITER, "&");
        options.put(KvFormatFactory.KV_PREFIX + TableFormatConstants.FORMAT_KV_DELIMITER, "=");
        options.put(KvFormatFactory.KV_PREFIX + TableFormatConstants.FORMAT_CHARSET, "ISO-8859-1");
        options.put(KvFormatFactory.KV_PREFIX + TableFormatConstants.FORMAT_IGNORE_ERRORS, "true");
        options.put(KvFormatFactory.KV_PREFIX + TableFormatConstants.FORMAT_ESCAPE_CHARACTER, "\\");
        options.put(KvFormatFactory.KV_PREFIX + TableFormatConstants.FORMAT_QUOTE_CHARACTER, "\"");
        options.put(KvFormatFactory.KV_PREFIX + TableFormatConstants.FORMAT_NULL_LITERAL, "n/a");

        return options;
    }
}
