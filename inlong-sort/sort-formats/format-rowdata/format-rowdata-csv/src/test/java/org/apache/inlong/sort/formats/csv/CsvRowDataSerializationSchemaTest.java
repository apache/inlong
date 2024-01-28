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
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.Before;
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
 * Tests for {@link CsvRowDataSerializationSchema}.
 */
public class CsvRowDataSerializationSchemaTest {

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
    public void testNormal() {
        Consumer<CsvRowDataSerializationSchema.Builder> config = builder -> {
        };

        testBasicSerialization(config, StringFormatInfo.INSTANCE, "hello", "hello");
        testBasicSerialization(config, BooleanFormatInfo.INSTANCE, true, "true");
        testBasicSerialization(config, ByteFormatInfo.INSTANCE, (byte) 124, "124");
        testBasicSerialization(config, ShortFormatInfo.INSTANCE, (short) 10000, "10000");
        testBasicSerialization(config, IntFormatInfo.INSTANCE, 1234567, "1234567");
        testBasicSerialization(config, LongFormatInfo.INSTANCE, 12345678910L, "12345678910");
        testBasicSerialization(config, FloatFormatInfo.INSTANCE, 0.33333334f, "0.33333334");
        testBasicSerialization(config, DoubleFormatInfo.INSTANCE, 0.33333333332, "0.33333333332");
        testBasicSerialization(config, DecimalFormatInfo.INSTANCE, new BigDecimal("1234.0000000000000000000000001"),
                "1234.0000000000000000000000001");
        testBasicSerialization(config, new DateFormatInfo("dd/MM/yyyy"), Date.valueOf("2020-03-22"), "22/03/2020");
        testBasicSerialization(config, new TimeFormatInfo("ss/mm/hh"), Time.valueOf("11:12:13"), "13/12/11");
        testBasicSerialization(config, new TimestampFormatInfo("dd/MM/yyyy hh:mm:ss"),
                Timestamp.valueOf("2020-03-22 11:12:13"), "22/03/2020 11:12:13");
    }

    @Test
    public void testNullLiteral() {
        String nullLiteral = "n/a";

        Consumer<CsvRowDataSerializationSchema.Builder> config =
                builder -> builder.setNullLiteral(nullLiteral);

        testBasicSerialization(config, StringFormatInfo.INSTANCE, null, nullLiteral);
        testBasicSerialization(config, BooleanFormatInfo.INSTANCE, null, nullLiteral);
        testBasicSerialization(config, ByteFormatInfo.INSTANCE, null, nullLiteral);
        testBasicSerialization(config, ShortFormatInfo.INSTANCE, null, nullLiteral);
        testBasicSerialization(config, IntFormatInfo.INSTANCE, null, nullLiteral);
        testBasicSerialization(config, LongFormatInfo.INSTANCE, null, nullLiteral);
        testBasicSerialization(config, FloatFormatInfo.INSTANCE, null, nullLiteral);
        testBasicSerialization(config, DoubleFormatInfo.INSTANCE, null, nullLiteral);
        testBasicSerialization(config, DecimalFormatInfo.INSTANCE, null, nullLiteral);
        testBasicSerialization(config, new DateFormatInfo("dd/MM/yyyy"), null, nullLiteral);
        testBasicSerialization(config, new TimeFormatInfo("ss/mm/hh"), null, nullLiteral);
        testBasicSerialization(config, new TimestampFormatInfo("dd/MM/yyyy hh:mm:ss"), null, nullLiteral);
    }

    @Test
    public void testDelimiter() {
        Consumer<CsvRowDataSerializationSchema.Builder> config =
                builder -> builder.setFieldDelimiter('|');
        GenericRowData rowData = GenericRowData.of(
                10,
                fromString("field1"),
                fromString("field2"),
                fromString("field3"));

        testRowDataSerialization(
                config,
                rowData,
                "10|field1|field2|field3".getBytes(),
                false);
    }

    @Test
    public void testEscape() {
        Consumer<CsvRowDataSerializationSchema.Builder> config =
                builder -> builder.setEscapeCharacter('\\').setQuoteCharacter('\"');

        GenericRowData rowData1 = GenericRowData.of(
                10,
                fromString("field1,field2"),
                fromString("field3"),
                fromString("field4"));
        testRowDataSerialization(
                config,
                rowData1,
                "10,field1\\,field2,field3,field4".getBytes(),
                false);

        GenericRowData rowData2 = GenericRowData.of(
                10,
                fromString("field1\\"),
                fromString("field2"),
                fromString("field3"));
        testRowDataSerialization(
                config,
                rowData2,
                "10,field1\\\\,field2,field3".getBytes(),
                false);

        GenericRowData rowData3 = GenericRowData.of(
                10,
                fromString("field1\""),
                fromString("field2"),
                fromString("field3"));
        testRowDataSerialization(
                config,
                rowData3,
                "10,field1\\\",field2,field3".getBytes(),
                false);
    }

    @Test
    public void testQuote() {
        Consumer<CsvRowDataSerializationSchema.Builder> config =
                builder -> builder.setQuoteCharacter('\"');

        GenericRowData rowData = GenericRowData.of(
                10,
                fromString("field1,field2"),
                fromString("field3"),
                fromString("field4"));
        testRowDataSerialization(
                config,
                rowData,
                "10,field1\",\"field2,field3,field4".getBytes(),
                false);
    }

    @Test
    public void testCharset() {
        Consumer<CsvRowDataSerializationSchema.Builder> config =
                builder -> builder.setCharset(StandardCharsets.UTF_16.name());

        GenericRowData rowData = GenericRowData.of(
                10,
                fromString("field1"),
                fromString("field2"),
                fromString("field3"));
        testRowDataSerialization(
                config,
                rowData,
                "10,field1,field2,field3".getBytes(StandardCharsets.UTF_16),
                false);
    }

    @Test
    public void testMoreFields() {
        Consumer<CsvRowDataSerializationSchema.Builder> config = builder -> {
        };

        GenericRowData rowData = GenericRowData.of(
                10,
                fromString("field1"),
                fromString("field2"),
                fromString("field3"),
                fromString("field4")

        );
        testRowDataSerialization(
                config,
                rowData,
                "10,field1,field2,field3".getBytes(),
                false);
    }

    @Test
    public void testLessFields() {
        Consumer<CsvRowDataSerializationSchema.Builder> config = builder -> {
        };

        GenericRowData rowData = GenericRowData.of(
                10,
                fromString("field1"),
                fromString("field2")

        );
        testRowDataSerialization(
                config,
                rowData,
                "10,field1,field2,".getBytes(),
                false);
    }

    @Test(expected = Exception.class)
    public void testErrors() {
        Consumer<CsvRowDataSerializationSchema.Builder> config = builder -> {
        };
        GenericRowData rowData = GenericRowData.of(
                fromString("na"),
                fromString("field1"),
                fromString("field2"),
                fromString("field3")

        );
        testRowDataSerialization(
                config,
                rowData,
                ",field1,field2,field3".getBytes(),
                false);
    }

    @Test
    public void testIngoreErrors() {
        Consumer<CsvRowDataSerializationSchema.Builder> config = builder -> {
        };
        GenericRowData rowData = GenericRowData.of(
                fromString("na"),
                fromString("field1"),
                fromString("field2"),
                fromString("field3")

        );
        testRowDataSerialization(
                config,
                rowData,
                null,
                true);
    }

    private <T> void testBasicSerialization(
            Consumer<CsvRowDataSerializationSchema.Builder> config,
            BasicFormatInfo<T> basicFormatInfo,
            T record,
            String expectedText) {
        RowFormatInfo rowFormatInfo =
                new RowFormatInfo(
                        new String[]{"f"},
                        new FormatInfo[]{basicFormatInfo});

        CsvRowDataSerializationSchema.Builder builder =
                new CsvRowDataSerializationSchema.Builder(rowFormatInfo);
        config.accept(builder);

        CsvRowDataSerializationSchema serializer = builder.build();

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

    private void testRowDataSerialization(
            Consumer<CsvRowDataSerializationSchema.Builder> config,
            RowData rowData,
            byte[] expectedBytes,
            boolean ignoreErrors) {
        CsvRowDataSerializationSchema.Builder builder =
                new CsvRowDataSerializationSchema.Builder(testFormatInfo);
        builder.setIgnoreErrors(ignoreErrors);
        config.accept(builder);

        CsvRowDataSerializationSchema serializer = builder.build();

        byte[] bytes = serializer.serialize(rowData);
        assertArrayEquals(expectedBytes, bytes);
    }
}
