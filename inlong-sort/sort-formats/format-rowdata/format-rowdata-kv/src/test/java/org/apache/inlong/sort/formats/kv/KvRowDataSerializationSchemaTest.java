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

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.junit.Test;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.function.Consumer;

import static org.apache.flink.table.data.DecimalData.fromBigDecimal;
import static org.apache.flink.table.data.StringData.fromString;
import static org.apache.flink.table.data.TimestampData.fromTimestamp;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link KvRowDataSerializationSchema}.
 */
public class KvRowDataSerializationSchemaTest {

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
    public void testNormal() {
        Consumer<KvRowDataSerializationSchema.Builder> config = builder -> {
        };

        testBasicSerialization(config, StringFormatInfo.INSTANCE, "hello", "f=hello");
        testBasicSerialization(config, BooleanFormatInfo.INSTANCE, true, "f=true");
        testBasicSerialization(config, ByteFormatInfo.INSTANCE, (byte) 124, "f=124");
        testBasicSerialization(config, ShortFormatInfo.INSTANCE, (short) 10000, "f=10000");
        testBasicSerialization(config, IntFormatInfo.INSTANCE, 1234567, "f=1234567");
        testBasicSerialization(config, LongFormatInfo.INSTANCE, 12345678910L, "f=12345678910");
        testBasicSerialization(config, FloatFormatInfo.INSTANCE, 0.33333334f, "f=0.33333334");
        testBasicSerialization(config, DoubleFormatInfo.INSTANCE, 0.33333333332, "f=0.33333333332");
        testBasicSerialization(config, DecimalFormatInfo.INSTANCE, new BigDecimal("1234.0000000000000000000000001"),
                "f=1234.0000000000000000000000001");
        testBasicSerialization(config, new DateFormatInfo("dd/MM/yyyy"), Date.valueOf("2020-03-22"), "f=22/03/2020");
        testBasicSerialization(config, new TimeFormatInfo("ss/mm/hh"), Time.valueOf("11:12:13"), "f=13/12/11");
        testBasicSerialization(config, new TimestampFormatInfo("dd/MM/yyyy hh:mm:ss"),
                Timestamp.valueOf("2020-03-22 11:12:13"), "f=22/03/2020 11:12:13");
    }

    @Test
    public void testNullIteral() {
        String nullLiteral = "n/a";
        String nullField = "f=n/a";
        Consumer<KvRowDataSerializationSchema.Builder> config = builder -> builder.setNullLiteral(nullLiteral);

        testBasicSerialization(config, StringFormatInfo.INSTANCE, null, nullField);
        testBasicSerialization(config, BooleanFormatInfo.INSTANCE, null, nullField);
        testBasicSerialization(config, ByteFormatInfo.INSTANCE, null, nullField);
        testBasicSerialization(config, ShortFormatInfo.INSTANCE, null, nullField);
        testBasicSerialization(config, IntFormatInfo.INSTANCE, null, nullField);
        testBasicSerialization(config, LongFormatInfo.INSTANCE, null, nullField);
        testBasicSerialization(config, FloatFormatInfo.INSTANCE, null, nullField);
        testBasicSerialization(config, DoubleFormatInfo.INSTANCE, null, nullField);
        testBasicSerialization(config, DecimalFormatInfo.INSTANCE, null, nullField);
        testBasicSerialization(config, new DateFormatInfo("dd/MM/yyyy"), null, nullField);
        testBasicSerialization(config, new TimeFormatInfo("ss/mm/hh"), null, nullField);
        testBasicSerialization(config, new TimestampFormatInfo("dd/MM/yyyy hh:mm:ss"), null, nullField);
    }

    @Test
    public void testDelimiter() {
        Consumer<KvRowDataSerializationSchema.Builder> config =
                builder -> builder.setEntryDelimiter('|').setKvDelimiter(',');

        testRowSerialization(
                config,
                GenericRowData.of(
                        10,
                        fromString("field1"),
                        fromString("field2"),
                        fromString("field3")),
                "f1,10|f2,field1|f3,field2|f4,field3".getBytes());

    }

    @Test
    public void testEscape() {
        Consumer<KvRowDataSerializationSchema.Builder> config =
                builder -> builder.setEscapeCharacter('\\').setQuoteCharacter('\"');

        testRowSerialization(
                config,
                GenericRowData.of(
                        10,
                        fromString("field1&field2"),
                        fromString("field3"),
                        fromString("field4")),
                "f1=10&f2=field1\\&field2&f3=field3&f4=field4".getBytes());
        testRowSerialization(
                config,
                GenericRowData.of(
                        10,
                        fromString("field1\\"),
                        fromString("field2"),
                        fromString("field3")),
                "f1=10&f2=field1\\\\&f3=field2&f4=field3".getBytes());
        testRowSerialization(
                config,
                GenericRowData.of(
                        10,
                        fromString("field1\""),
                        fromString("field2"),
                        fromString("field3")),
                "f1=10&f2=field1\\\"&f3=field2&f4=field3".getBytes());
    }

    @Test
    public void testQuote() {
        Consumer<KvRowDataSerializationSchema.Builder> config = builder -> builder.setQuoteCharacter('\"');

        testRowSerialization(
                config,
                GenericRowData.of(
                        10,
                        fromString("field1&field2"),
                        fromString("field3"),
                        fromString("field4")),
                "f1=10&f2=field1\"&\"field2&f3=field3&f4=field4".getBytes());
    }

    @Test
    public void testCharset() {
        Consumer<KvRowDataSerializationSchema.Builder> config =
                builder -> builder.setCharset(StandardCharsets.UTF_16.name());

        testRowSerialization(
                config,
                GenericRowData.of(
                        10,
                        fromString("field1"),
                        fromString("field2"),
                        fromString("field3")),
                "f1=10&f2=field1&f3=field2&f4=field3".getBytes(StandardCharsets.UTF_16));
    }

    @Test
    public void testNullFields() {
        Consumer<KvRowDataSerializationSchema.Builder> config = builder -> {
        };

        testRowSerialization(
                config,
                GenericRowData.of(
                        10,
                        fromString("field1"),
                        null,
                        fromString("field3")),
                "f1=10&f2=field1&f3=&f4=field3".getBytes());
    }

    @Test
    public void testMoreFields() {
        Consumer<KvRowDataSerializationSchema.Builder> config = builder -> {
        };

        testRowSerialization(
                config,
                GenericRowData.of(10,
                        fromString("field1"),
                        fromString("field2"),
                        fromString("field3"),
                        fromString("field4")),
                "f1=10&f2=field1&f3=field2&f4=field3".getBytes());
    }

    @Test
    public void testLessFields() {
        Consumer<KvRowDataSerializationSchema.Builder> config = builder -> {
        };

        testRowSerialization(
                config,
                GenericRowData.of(
                        10,
                        fromString("field1"),
                        fromString("field2")),
                "f1=10&f2=field1&f3=field2&f4=".getBytes());
    }

    @Test(expected = RuntimeException.class)
    public void testErrors() {
        Consumer<KvRowDataSerializationSchema.Builder> config = builder -> {
        };
        testRowSerialization(
                config,
                GenericRowData.of(
                        fromString("na"),
                        fromString("field1"),
                        fromString("field2"),
                        fromString("field3")),
                "f1=&f2=field1&f3=field2&f4=field3".getBytes());
    }

    private static <T> void testBasicSerialization(
            Consumer<KvRowDataSerializationSchema.Builder> config,
            BasicFormatInfo<T> basicFormatInfo,
            T record,
            String expectedText) {
        RowFormatInfo rowFormatInfo =
                new RowFormatInfo(
                        new String[]{"f"},
                        new FormatInfo[]{basicFormatInfo});

        KvRowDataSerializationSchema.Builder builder =
                new KvRowDataSerializationSchema.Builder(rowFormatInfo);
        config.accept(builder);

        KvRowDataSerializationSchema serializer = builder.build();

        GenericRowData rowData = new GenericRowData(1);
        if (record instanceof String) {
            rowData.setField(0, fromString((String) record));
        } else if (record instanceof BigDecimal) {
            rowData.setField(0, fromBigDecimal((BigDecimal) record, 30, 25));
        } else if (record instanceof Timestamp) {
            rowData.setField(0, fromTimestamp((Timestamp) record));
        } else if (record instanceof Date) {
            rowData.setField(0, ((Date) record).toLocalDate().toEpochDay());
        } else if (record instanceof Time) {
            rowData.setField(0, ((Time) record).toLocalTime().toSecondOfDay() * 1000);
        } else {
            rowData.setField(0, record);
        }

        String text = new String(serializer.serialize(rowData));
        assertEquals(expectedText, text);
    }

    private static void testRowSerialization(
            Consumer<KvRowDataSerializationSchema.Builder> config,
            RowData row,
            byte[] expectedBytes) {
        KvRowDataSerializationSchema.Builder builder =
                new KvRowDataSerializationSchema.Builder(TEST_ROW_INFO);
        config.accept(builder);

        KvRowDataSerializationSchema serializer = builder.build();
        byte[] bytes = serializer.serialize(row);
        assertArrayEquals(expectedBytes, bytes);
    }
}
