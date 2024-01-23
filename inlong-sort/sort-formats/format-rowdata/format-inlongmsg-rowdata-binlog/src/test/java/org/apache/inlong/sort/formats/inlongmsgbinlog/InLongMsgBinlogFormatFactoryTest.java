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

package org.apache.inlong.sort.formats.inlongmsgbinlog;

import org.apache.inlong.sort.formats.common.DateFormatInfo;
import org.apache.inlong.sort.formats.common.FormatInfo;
import org.apache.inlong.sort.formats.common.FormatUtils;
import org.apache.inlong.sort.formats.common.IntFormatInfo;
import org.apache.inlong.sort.formats.common.RowFormatInfo;
import org.apache.inlong.sort.formats.common.StringFormatInfo;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.TestDynamicTableFactory;
import org.apache.flink.table.runtime.connector.source.ScanRuntimeProviderContext;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.table.factories.utils.FactoryMocks.createTableSource;
import static org.junit.Assert.assertEquals;

/**
 * The unit test for InLongMsgBinlogFormatFactory.
 */
public class InLongMsgBinlogFormatFactoryTest {

    public RowFormatInfo testFormatInfo;

    public TypeInformation<RowData> testTypeInformation;

    public ResolvedSchema resolvedSchema;

    public DataType dataType;

    @Before
    public void setup() {
        resolvedSchema =
                ResolvedSchema.of(
                        Column.physical("student_name", DataTypes.STRING()),
                        Column.physical("score", DataTypes.INT()),
                        Column.physical("date", DataTypes.DATE()));
        dataType = resolvedSchema.toPhysicalRowDataType();
        RowType rowType = (RowType) dataType.getLogicalType();
        testTypeInformation = InternalTypeInfo.of(rowType);
        testFormatInfo = new RowFormatInfo(
                new String[]{"student_name", "score", "date"},
                new FormatInfo[]{
                        StringFormatInfo.INSTANCE,
                        IntFormatInfo.INSTANCE,
                        new DateFormatInfo("yyyy-MM-dd")
                });
    }

    @Test
    public void testDeSchema() {
        final InLongMsgBinlogRowDataDeserializationSchema expectedDeSer =
                new InLongMsgBinlogRowDataDeserializationSchema.Builder(
                        testFormatInfo)
                                .setTimeFieldName("time")
                                .setAttributesFieldName("attributes")
                                .setIgnoreErrors(true).setIncludeUpdateBefore(false)
                                .build();
        final Map<String, String> options = getAllOptions();
        DeserializationSchema<RowData> actualDeser = createDeserializationSchema(options);
        assertEquals(expectedDeSer, actualDeser);
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

    private Map<String, String> getAllOptions() {
        final Map<String, String> options = new HashMap<>();
        options.put("connector", TestDynamicTableFactory.IDENTIFIER);
        options.put("target", "MyTarget");
        options.put("buffer-size", "1000");

        options.put("format", InLongMsgBinlogFormatFactory.IDENTIFIER);
        options.put("InLongMsg-Binlog.row.format.info", FormatUtils.marshall(testFormatInfo));
        options.put("InLongMsg-Binlog.format.time-field-name", "time");
        options.put("InLongMsg-Binlog.format.attribute-field-name", "attributes");
        options.put("InLongMsg-Binlog.format.ignore-errors", "true");
        options.put("InLongMsg-Binlog.format.include-update-before", "false");
        return options;
    }
}
