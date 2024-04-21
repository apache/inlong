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

import org.apache.inlong.common.pojo.sort.dataflow.field.format.BasicFormatInfo;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.BooleanFormatInfo;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.ByteFormatInfo;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.DateFormatInfo;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.DecimalFormatInfo;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.DoubleFormatInfo;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.FloatFormatInfo;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.FormatInfo;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.IntFormatInfo;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.LongFormatInfo;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.RowFormatInfo;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.ShortFormatInfo;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.StringFormatInfo;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.TimeFormatInfo;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.TimestampFormatInfo;
import org.apache.inlong.sort.formats.base.TableFormatUtils;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.LogicalType;
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
 * Tests for {@link KvRowDataDeserializationSchema}.
 */
public class KvRowDataDeserializationSchemaTest {

    private static final RowFormatInfo TEST_ROW_INFO =
            new RowFormatInfo(
                    new String[]{"f1", "f2", "f3", "f4"},
                    new FormatInfo[]{
                            IntFormatInfo.INSTANCE,
                            StringFormatInfo.INSTANCE,
                            StringFormatInfo.INSTANCE,
                            StringFormatInfo.INSTANCE
                    });

    @Test
    public void testNormal() throws Exception {
        Consumer<KvRowDataDeserializationSchema.Builder> config = builder -> {
        };

        testBasicDeserialization(config, StringFormatInfo.INSTANCE, StringData.fromString("hello"), "f=hello");
        testBasicDeserialization(config, BooleanFormatInfo.INSTANCE, true, "f=true");
        testBasicDeserialization(config, ByteFormatInfo.INSTANCE, (byte) 124, "f=124");
        testBasicDeserialization(config, ShortFormatInfo.INSTANCE, (short) 10000, "f=10000");
        testBasicDeserialization(config, IntFormatInfo.INSTANCE, 1234567, "f=1234567");
        testBasicDeserialization(config, LongFormatInfo.INSTANCE, 12345678910L, "f=12345678910");
        testBasicDeserialization(config, FloatFormatInfo.INSTANCE, 0.33333334f, "f=0.33333334");
        testBasicDeserialization(config, DoubleFormatInfo.INSTANCE, 0.33333333332, "f=0.33333333332");
        testBasicDeserialization(config, DecimalFormatInfo.INSTANCE,
                DecimalData.fromBigDecimal(new BigDecimal("1234.0000000000000000000000001"), 10, 0),
                "f=1234.0000000000000000000000001");
        testBasicDeserialization(config, new DateFormatInfo("dd/MM/yyyy"),
                Date.valueOf("2020-03-22").toLocalDate().toEpochDay(), "f=22/03/2020");
        testBasicDeserialization(config, new TimeFormatInfo("ss/mm/hh"),
                Time.valueOf("11:12:13").toLocalTime().toSecondOfDay() * 1000, "f=13/12/11");
        testBasicDeserialization(config, new TimestampFormatInfo("dd/MM/yyyy hh:mm:ss"),
                TimestampData.fromTimestamp(Timestamp.valueOf("2020-03-22 11:12:13")), "f=22/03/2020 11:12:13");
    }

    @Test
    public void testNullIteral() throws Exception {
        String nullLiteral = "n/a";
        String nullField = "f=n/a";
        Consumer<KvRowDataDeserializationSchema.Builder> config = builder -> builder.setNullLiteral(nullLiteral);

        testBasicDeserialization(config, StringFormatInfo.INSTANCE, null, nullField);
        testBasicDeserialization(config, BooleanFormatInfo.INSTANCE, null, nullField);
        testBasicDeserialization(config, ByteFormatInfo.INSTANCE, null, nullField);
        testBasicDeserialization(config, ShortFormatInfo.INSTANCE, null, nullField);
        testBasicDeserialization(config, IntFormatInfo.INSTANCE, null, nullField);
        testBasicDeserialization(config, LongFormatInfo.INSTANCE, null, nullField);
        testBasicDeserialization(config, FloatFormatInfo.INSTANCE, null, nullField);
        testBasicDeserialization(config, DoubleFormatInfo.INSTANCE, null, nullField);
        testBasicDeserialization(config, DecimalFormatInfo.INSTANCE, null, nullField);
        testBasicDeserialization(config, new DateFormatInfo("dd/MM/yyyy"), null, nullField);
        testBasicDeserialization(config, new TimeFormatInfo("ss/mm/hh"), null, nullField);
        testBasicDeserialization(config, new TimestampFormatInfo("dd/MM/yyyy hh:mm:ss"), null, nullField);
    }

    @Test
    public void testDelimiter() throws Exception {
        Consumer<KvRowDataDeserializationSchema.Builder> config =
                builder -> builder.setEntryDelimiter('|').setKvDelimiter(',');

        GenericRowData rowData = new GenericRowData(4);
        rowData.setField(0, 10);
        rowData.setField(1, StringData.fromString("aa"));
        rowData.setField(2, StringData.fromString("bb"));
        rowData.setField(3, StringData.fromString("cc"));

        testRowDeserialization(
                config,
                rowData,
                "f1,10|f2,aa|f3,bb|f4,cc".getBytes());
    }

    @Test
    public void testEscape() throws Exception {
        Consumer<KvRowDataDeserializationSchema.Builder> config =
                builder -> builder.setEscapeCharacter('\\').setQuoteCharacter('\"');

        GenericRowData rowData1 = new GenericRowData(4);
        rowData1.setField(0, 10);
        rowData1.setField(1, StringData.fromString("field1&field2"));
        rowData1.setField(2, StringData.fromString("field3"));
        rowData1.setField(3, StringData.fromString("field4"));

        testRowDeserialization(
                config,
                rowData1,
                "f1=10&f2=field1\\&field2&f3=field3&f4=field4".getBytes());

        GenericRowData rowData2 = new GenericRowData(4);
        rowData2.setField(0, 10);
        rowData2.setField(1, StringData.fromString("field1\\"));
        rowData2.setField(2, StringData.fromString("field2"));
        rowData2.setField(3, StringData.fromString("field3"));

        testRowDeserialization(
                config,
                rowData2,
                "f1=10&f2=field1\\\\&f3=field2&f4=field3".getBytes());

        GenericRowData rowData3 = new GenericRowData(4);
        rowData3.setField(0, 10);
        rowData3.setField(1, StringData.fromString("field1\""));
        rowData3.setField(2, StringData.fromString("field2"));
        rowData3.setField(3, StringData.fromString("field3"));

        testRowDeserialization(
                config,
                rowData3,
                "f1=10&f2=field1\\\"&f3=field2&f4=field3".getBytes());
    }

    @Test
    public void testQuote() throws Exception {
        Consumer<KvRowDataDeserializationSchema.Builder> config =
                builder -> builder.setEscapeCharacter('\\').setQuoteCharacter('\"');

        GenericRowData rowData1 = new GenericRowData(4);
        rowData1.setField(0, 10);
        rowData1.setField(1, StringData.fromString("field1&field2"));
        rowData1.setField(2, StringData.fromString("field3"));
        rowData1.setField(3, StringData.fromString("field4"));

        testRowDeserialization(
                config,
                rowData1,
                "f1=10&f2=\"field1&field2\"&f3=field3&f4=field4".getBytes());

        GenericRowData rowData2 = new GenericRowData(4);
        rowData2.setField(0, 10);
        rowData2.setField(1, StringData.fromString("field1\\"));
        rowData2.setField(2, StringData.fromString("field2"));
        rowData2.setField(3, StringData.fromString("field3"));

        testRowDeserialization(
                config,
                rowData2,
                "f1=10&f2=\"field1\\\"&f3=field2&f4=field3".getBytes());
    }

    @Test
    public void testExtractSpecificKeys() throws Exception {
        Consumer<KvRowDataDeserializationSchema.Builder> config =
                builder -> builder.setEscapeCharacter('\\').setQuoteCharacter('\"');

        GenericRowData rowData = new GenericRowData(4);
        rowData.setField(0, 10);
        rowData.setField(1, StringData.fromString("field1"));
        rowData.setField(2, StringData.fromString("field2"));
        rowData.setField(3, StringData.fromString("field3"));

        testRowDeserialization(
                config,
                rowData,
                "f1=10&f2=field1&f3=field2&f4=field3&f5=field4".getBytes());
    }

    @Test
    public void testCharset() throws Exception {
        Consumer<KvRowDataDeserializationSchema.Builder> config =
                builder -> builder.setCharset(StandardCharsets.UTF_16.name());

        GenericRowData rowData = new GenericRowData(4);
        rowData.setField(0, 10);
        rowData.setField(1, StringData.fromString("aa"));
        rowData.setField(2, StringData.fromString("bb"));
        rowData.setField(3, StringData.fromString("cc"));

        testRowDeserialization(
                config,
                rowData,
                "f1=10&f2=aa&f3=bb&f4=cc".getBytes(StandardCharsets.UTF_16));
    }

    @Test
    public void testErrors() throws Exception {
        Consumer<KvRowDataDeserializationSchema.Builder> config = builder -> {
        };

        GenericRowData rowData = new GenericRowData(4);
        rowData.setField(0, null);
        rowData.setField(1, StringData.fromString("field1"));
        rowData.setField(2, StringData.fromString("field2"));
        rowData.setField(3, StringData.fromString("field3"));

        testRowDeserialization(
                config,
                rowData,
                "f1=na&f2=field1&f3=field2&f4=field3".getBytes());
    }

    @Test
    public void testMissingField() throws Exception {
        Consumer<KvRowDataDeserializationSchema.Builder> config = builder -> {
        };

        GenericRowData rowData = new GenericRowData(4);
        rowData.setField(0, null);
        rowData.setField(1, StringData.fromString("aa"));
        rowData.setField(2, StringData.fromString("bb"));
        rowData.setField(3, StringData.fromString("cc"));

        testRowDeserialization(
                config,
                rowData,
                "f2=aa&f3=bb&f4=cc".getBytes());
    }

    @Test
    public void testExtraField() throws Exception {
        Consumer<KvRowDataDeserializationSchema.Builder> config = builder -> {
        };

        GenericRowData rowData = new GenericRowData(4);
        rowData.setField(0, 10);
        rowData.setField(1, StringData.fromString("aa"));
        rowData.setField(2, StringData.fromString("bb"));
        rowData.setField(3, StringData.fromString("cc"));

        testRowDeserialization(
                config,
                rowData,
                "f1=10&f2=aa&f3=bb&f4=cc&f5=dd".getBytes());
    }

    private static <T> void testBasicDeserialization(
            Consumer<KvRowDataDeserializationSchema.Builder> config,
            BasicFormatInfo<T> basicFormatInfo,
            Object expectedRecord,
            String text) throws IOException {
        RowFormatInfo rowFormatInfo =
                new RowFormatInfo(
                        new String[]{"f"},
                        new FormatInfo[]{basicFormatInfo});
        LogicalType logicalType = TableFormatUtils.deriveLogicalType(basicFormatInfo);
        TypeInformation<RowData> typeInformation = InternalTypeInfo.of(logicalType);
        KvRowDataDeserializationSchema.Builder builder =
                new KvRowDataDeserializationSchema.Builder(rowFormatInfo, typeInformation);
        config.accept(builder);

        KvRowDataDeserializationSchema deserializer = builder.build();

        GenericRowData row = (GenericRowData) deserializer.deserialize(text.getBytes());
        assertEquals(1, row.getArity());
        assertEquals(expectedRecord, row.getField(0));
    }

    private void testRowDeserialization(
            Consumer<KvRowDataDeserializationSchema.Builder> config,
            RowData expectedRow,
            byte[] bytes) throws Exception {
        LogicalType rowType = TableFormatUtils.deriveLogicalType(TEST_ROW_INFO);
        TypeInformation<RowData> producedTypeInfo = InternalTypeInfo.of(rowType);
        KvRowDataDeserializationSchema.Builder builder =
                new KvRowDataDeserializationSchema.Builder(TEST_ROW_INFO, producedTypeInfo);
        config.accept(builder);

        KvRowDataDeserializationSchema deserializer = builder.build();

        RowData row = deserializer.deserialize(bytes);
        assertEquals(expectedRow, row);
    }
}
