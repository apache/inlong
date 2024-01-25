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

import org.apache.inlong.sort.formats.base.TableFormatUtils;
import org.apache.inlong.sort.formats.common.BasicFormatInfo;
import org.apache.inlong.sort.formats.common.BooleanFormatInfo;
import org.apache.inlong.sort.formats.common.ByteFormatInfo;
import org.apache.inlong.sort.formats.common.DateFormatInfo;
import org.apache.inlong.sort.formats.common.DecimalFormatInfo;
import org.apache.inlong.sort.formats.common.DoubleFormatInfo;
import org.apache.inlong.sort.formats.common.FloatFormatInfo;
import org.apache.inlong.sort.formats.common.FormatInfo;
import org.apache.inlong.sort.formats.common.IntFormatInfo;
import org.apache.inlong.sort.formats.common.LongFormatInfo;
import org.apache.inlong.sort.formats.common.RowFormatInfo;
import org.apache.inlong.sort.formats.common.ShortFormatInfo;
import org.apache.inlong.sort.formats.common.StringFormatInfo;
import org.apache.inlong.sort.formats.common.TimeFormatInfo;
import org.apache.inlong.sort.formats.common.TimestampFormatInfo;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.TestLogger;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.function.Consumer;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link CsvRowDataDeserializationSchema}.
 */
public class CsvRowDataDeserializationSchemaTest extends TestLogger {

    public RowFormatInfo testFormatInfo;

    public TypeInformation<RowData> testTypeInformation;

    public ResolvedSchema resolvedSchema;

    public DataType dataType;

    @Before
    public void setup() {
        resolvedSchema =
                ResolvedSchema.of(
                        Column.physical("f1", DataTypes.INT()),
                        Column.physical("f2", DataTypes.STRING()),
                        Column.physical("f3", DataTypes.STRING()),
                        Column.physical("f4", DataTypes.STRING()));
        dataType = resolvedSchema.toPhysicalRowDataType();
        RowType rowType = (RowType) dataType.getLogicalType();
        testTypeInformation = InternalTypeInfo.of(rowType);
        testFormatInfo = new RowFormatInfo(
                new String[]{"f1", "f2", "f3", "f4"},
                new FormatInfo[]{
                        IntFormatInfo.INSTANCE,
                        StringFormatInfo.INSTANCE,
                        StringFormatInfo.INSTANCE,
                        StringFormatInfo.INSTANCE
                });
    }

    @Test
    public void testNormal() throws Exception {

        Consumer<CsvRowDataDeserializationSchema.Builder> config = builder -> {
        };

        testBasicDeserialization(config, StringFormatInfo.INSTANCE, StringData.fromString("hello"), "hello");
        testBasicDeserialization(config, BooleanFormatInfo.INSTANCE, true, "true");
        testBasicDeserialization(config, ByteFormatInfo.INSTANCE, (byte) 124, "124");
        testBasicDeserialization(config, ShortFormatInfo.INSTANCE, (short) 10000, "10000");
        testBasicDeserialization(config, IntFormatInfo.INSTANCE, 1234567, "1234567");
        testBasicDeserialization(config, LongFormatInfo.INSTANCE, 12345678910L, "12345678910");
        testBasicDeserialization(config, FloatFormatInfo.INSTANCE, 0.33333334f, "0.33333334");
        testBasicDeserialization(config, DoubleFormatInfo.INSTANCE, 0.33333333332, "0.33333333332");
        testBasicDeserialization(config, DecimalFormatInfo.INSTANCE,
                DecimalData.fromBigDecimal(new BigDecimal("1234.0000000000000000000000001"), 10, 0),
                "1234.0000000000000000000000001");
        testBasicDeserialization(config, new DateFormatInfo("dd/MM/yyyy"),
                Date.valueOf("2020-03-22").toLocalDate().toEpochDay(), "22/03/2020");
        testBasicDeserialization(config, new TimeFormatInfo("ss/mm/hh"),
                Time.valueOf("11:12:13").toLocalTime().toSecondOfDay() * 1000, "13/12/11");
        testBasicDeserialization(config, new TimestampFormatInfo("dd/MM/yyyy hh:mm:ss"),
                TimestampData.fromTimestamp(Timestamp.valueOf("2020-03-22 11:12:13")), "22/03/2020 11:12:13");
    }

    @Test
    public void testNullLiteral() throws Exception {
        String nullLiteral = "n/a";

        Consumer<CsvRowDataDeserializationSchema.Builder> config =
                builder -> builder.setNullLiteral(nullLiteral);

        testBasicDeserialization(config, StringFormatInfo.INSTANCE, null, nullLiteral);
        testBasicDeserialization(config, BooleanFormatInfo.INSTANCE, null, nullLiteral);
        testBasicDeserialization(config, ByteFormatInfo.INSTANCE, null, nullLiteral);
        testBasicDeserialization(config, ShortFormatInfo.INSTANCE, null, nullLiteral);
        testBasicDeserialization(config, IntFormatInfo.INSTANCE, null, nullLiteral);
        testBasicDeserialization(config, LongFormatInfo.INSTANCE, null, nullLiteral);
        testBasicDeserialization(config, FloatFormatInfo.INSTANCE, null, nullLiteral);
        testBasicDeserialization(config, DoubleFormatInfo.INSTANCE, null, nullLiteral);
        testBasicDeserialization(config, DecimalFormatInfo.INSTANCE, null, nullLiteral);
        testBasicDeserialization(config, new DateFormatInfo("dd/MM/yyyy"), null, nullLiteral);
        testBasicDeserialization(config, new TimeFormatInfo("ss/mm/hh"), null, nullLiteral);
        testBasicDeserialization(config, new TimestampFormatInfo("dd/MM/yyyy hh:mm:ss"), null, nullLiteral);
    }

    @Test
    public void testDelimiter() throws Exception {
        Consumer<CsvRowDataDeserializationSchema.Builder> config =
                builder -> builder.setFieldDelimiter('|');
        GenericRowData rowData = new GenericRowData(4);
        rowData.setField(0, 10);
        rowData.setField(1, StringData.fromString("field1"));
        rowData.setField(2, StringData.fromString("field2"));
        rowData.setField(3, StringData.fromString("field3"));

        testRowDataDeserialization(
                config,
                rowData,
                "10|field1|field2|field3".getBytes(),
                false);
    }

    @Test
    public void testEscape() throws Exception {
        Consumer<CsvRowDataDeserializationSchema.Builder> config =
                builder -> builder.setEscapeCharacter('\\').setQuoteCharacter('\"');

        GenericRowData rowData1 = new GenericRowData(4);
        rowData1.setField(0, 10);
        rowData1.setField(1, StringData.fromString("field1,field2"));
        rowData1.setField(2, StringData.fromString("field3"));
        rowData1.setField(3, StringData.fromString("field4"));
        testRowDataDeserialization(
                config,
                rowData1,
                "10,field1\\,field2,field3,field4".getBytes(),
                false);

        GenericRowData rowData2 = new GenericRowData(4);
        rowData2.setField(0, 10);
        rowData2.setField(1, StringData.fromString("field1\\"));
        rowData2.setField(2, StringData.fromString("field2"));
        rowData2.setField(3, StringData.fromString("field3"));
        testRowDataDeserialization(
                config,
                rowData2,
                "10,field1\\\\,field2,field3".getBytes(),
                false);

        GenericRowData rowData3 = new GenericRowData(4);
        rowData3.setField(0, 10);
        rowData3.setField(1, StringData.fromString("field1\""));
        rowData3.setField(2, StringData.fromString("field2"));
        rowData3.setField(3, StringData.fromString("field3"));
        testRowDataDeserialization(
                config,
                rowData3,
                "10,field1\\\",field2,field3".getBytes(),
                false);
    }

    @Test
    public void testQuote() throws Exception {
        Consumer<CsvRowDataDeserializationSchema.Builder> config =
                builder -> builder.setEscapeCharacter('\\').setQuoteCharacter('\"');

        GenericRowData rowData1 = new GenericRowData(4);
        rowData1.setField(0, 10);
        rowData1.setField(1, StringData.fromString("field1,field2"));
        rowData1.setField(2, StringData.fromString("field3"));
        rowData1.setField(3, StringData.fromString("field4"));
        testRowDataDeserialization(
                config,
                rowData1,
                "10,\"field1,field2\",field3,field4".getBytes(),
                false);

        GenericRowData rowData2 = new GenericRowData(4);
        rowData2.setField(0, 10);
        rowData2.setField(1, StringData.fromString("field1\\"));
        rowData2.setField(2, StringData.fromString("field2"));
        rowData2.setField(3, StringData.fromString("field3"));
        testRowDataDeserialization(
                config,
                rowData2,
                "10,\"field1\\\",field2,field3".getBytes(),
                false);
    }

    @Test
    public void testCharset() throws Exception {
        Consumer<CsvRowDataDeserializationSchema.Builder> config =
                builder -> builder.setCharset(StandardCharsets.UTF_16.name());

        GenericRowData rowData = new GenericRowData(4);
        rowData.setField(0, 10);
        rowData.setField(1, StringData.fromString("field1"));
        rowData.setField(2, StringData.fromString("field2"));
        rowData.setField(3, StringData.fromString("field3"));
        testRowDataDeserialization(
                config,
                rowData,
                "10,field1,field2,field3".getBytes(StandardCharsets.UTF_16),
                false);
    }

    @Test
    public void testMoreFields() throws Exception {
        Consumer<CsvRowDataDeserializationSchema.Builder> config = builder -> {
        };

        GenericRowData rowData = new GenericRowData(4);
        rowData.setField(0, 1);
        rowData.setField(1, StringData.fromString("field1"));
        rowData.setField(2, StringData.fromString("field2"));
        rowData.setField(3, StringData.fromString("field3"));
        testRowDataDeserialization(
                config,
                rowData,
                "1,field1,field2,field3,field4".getBytes(),
                false);
    }

    @Test
    public void testLessFields() throws Exception {
        Consumer<CsvRowDataDeserializationSchema.Builder> config = builder -> {
        };
        GenericRowData rowData = new GenericRowData(4);
        rowData.setField(0, 1);
        rowData.setField(1, StringData.fromString("field1"));
        rowData.setField(2, StringData.fromString("field2"));
        rowData.setField(3, null);
        testRowDataDeserialization(
                config,
                rowData,
                "1,field1,field2".getBytes(),
                false);
    }

    @Test(expected = Exception.class)
    public void testErrors() throws Exception {
        Consumer<CsvRowDataDeserializationSchema.Builder> config = builder -> {
        };
        GenericRowData rowData = new GenericRowData(4);
        rowData.setField(0, null);
        rowData.setField(1, StringData.fromString("field1"));
        rowData.setField(2, StringData.fromString("field2"));
        rowData.setField(3, StringData.fromString("field3"));
        testRowDataDeserialization(
                config,
                rowData,
                "na,field1,field2,field3".getBytes(),
                false);
    }

    @Test
    public void testIgnoreErrors() throws Exception {
        Consumer<CsvRowDataDeserializationSchema.Builder> config = builder -> {
        };
        testRowDataDeserialization(
                config,
                null,
                "na,field1,field2,field3".getBytes(),
                true);
    }

    private <T> void testBasicDeserialization(
            Consumer<CsvRowDataDeserializationSchema.Builder> config,
            BasicFormatInfo<T> basicFormatInfo,
            Object expectedRecord,
            String text) throws IOException {
        LogicalType logicalType = TableFormatUtils.deriveLogicalType(basicFormatInfo);
        TypeInformation<RowData> typeInformation = InternalTypeInfo.of(logicalType);
        RowFormatInfo rowFormatInfo =
                new RowFormatInfo(
                        new String[]{"f"},
                        new FormatInfo[]{basicFormatInfo});

        CsvRowDataDeserializationSchema.Builder builder =
                new CsvRowDataDeserializationSchema.Builder(rowFormatInfo, typeInformation);
        config.accept(builder);

        CsvRowDataDeserializationSchema deserializer = builder.build();

        GenericRowData row = (GenericRowData) deserializer.deserialize(text.getBytes());
        assertEquals(1, row.getArity());
        assertEquals(expectedRecord, row.getField(0));
    }

    private void testRowDataDeserialization(
            Consumer<CsvRowDataDeserializationSchema.Builder> config,
            RowData expectedRow,
            byte[] bytes,
            boolean ignoreErrors) throws Exception {
        CsvRowDataDeserializationSchema.Builder builder =
                new CsvRowDataDeserializationSchema.Builder(testFormatInfo, testTypeInformation);
        builder.setIgnoreErrors(ignoreErrors);
        config.accept(builder);

        CsvRowDataDeserializationSchema deserializer = builder.build();
        GenericRowData rowData = (GenericRowData) deserializer.deserialize(bytes);
        assertEquals(expectedRow, rowData);
    }
}
