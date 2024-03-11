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

package org.apache.inlong.sort.formats.json;

import org.apache.inlong.sort.formats.base.TextFormatOptions;
import org.apache.inlong.sort.formats.json.FieldToRowDataConverters.FieldToRowDataConverter;

import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.NullType;
import org.junit.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.flink.table.api.DataTypes.ARRAY;
import static org.apache.flink.table.api.DataTypes.BIGINT;
import static org.apache.flink.table.api.DataTypes.BINARY;
import static org.apache.flink.table.api.DataTypes.BOOLEAN;
import static org.apache.flink.table.api.DataTypes.DATE;
import static org.apache.flink.table.api.DataTypes.DECIMAL;
import static org.apache.flink.table.api.DataTypes.DOUBLE;
import static org.apache.flink.table.api.DataTypes.FIELD;
import static org.apache.flink.table.api.DataTypes.FLOAT;
import static org.apache.flink.table.api.DataTypes.INT;
import static org.apache.flink.table.api.DataTypes.MAP;
import static org.apache.flink.table.api.DataTypes.ROW;
import static org.apache.flink.table.api.DataTypes.SMALLINT;
import static org.apache.flink.table.api.DataTypes.STRING;
import static org.apache.flink.table.api.DataTypes.TIME;
import static org.apache.flink.table.api.DataTypes.TIMESTAMP;
import static org.apache.flink.table.api.DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE;
import static org.apache.flink.table.api.DataTypes.TINYINT;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/** tests for {@link FieldToRowDataConverters}. */
public class FieldToRowDataConvertersTest {

    private final FieldToRowDataConverters converters = new FieldToRowDataConverters(true,
            false, TextFormatOptions.TimestampFormat.SQL);

    @Test
    public void testConvertToNull() throws IOException {
        FieldToRowDataConverter converter = converters.createConverter(new NullType());
        assertNull(converter.convert("testStr"));
    }

    @Test
    public void testConvertToBoolean() throws IOException {
        FieldToRowDataConverter converter = converters.createConverter(BOOLEAN().getLogicalType());
        assertTrue((Boolean) converter.convert("true"));
        assertFalse((Boolean) converter.convert("false"));
    }

    @Test
    public void testConvertToTinyInt() throws IOException {
        FieldToRowDataConverter converter = converters.createConverter(TINYINT().getLogicalType());
        assertEquals((byte) 127, converter.convert("127"));
    }

    @Test
    public void testConvertToSmallInt() throws IOException {
        FieldToRowDataConverter converter = converters.createConverter(SMALLINT().getLogicalType());
        assertEquals((short) 32767, converter.convert("32767"));
    }

    @Test
    public void testConvertToInt() throws IOException {
        FieldToRowDataConverter converter = converters.createConverter(INT().getLogicalType());
        assertEquals(32768, converter.convert("32768"));
    }

    @Test
    public void testConvertToLong() throws IOException {
        FieldToRowDataConverter converter = converters.createConverter(BIGINT().getLogicalType());
        assertEquals(123123123123123123L, converter.convert("123123123123123123"));
    }

    @Test
    public void testConvertToDate() throws IOException {
        FieldToRowDataConverter converter = converters.createConverter(DATE().getLogicalType());
        assertEquals(1, converter.convert("1970-01-02"));
    }

    @Test
    public void testConvertToTime() throws IOException {
        FieldToRowDataConverter converter = converters.createConverter(TIME().getLogicalType());
        assertEquals(1000, converter.convert("00:00:01"));
    }

    @Test
    public void testConvertToTimestamp() throws IOException {
        FieldToRowDataConverter converter = converters.createConverter(TIMESTAMP().getLogicalType());
        TimestampData expected = TimestampData.fromEpochMillis(86401000);
        assertEquals(expected, converter.convert("1970-01-02 00:00:01"));
    }

    @Test
    public void testConvertToTimestampWithLocalZone() throws IOException {
        TimeZone.setDefault(TimeZone.getDefault().getTimeZone("GMT+0"));
        FieldToRowDataConverter converter =
                converters.createConverter(TIMESTAMP_WITH_LOCAL_TIME_ZONE().getLogicalType());
        TimestampData expected = TimestampData.fromTimestamp(new Timestamp(0));
        assertEquals(expected, converter.convert("1970-01-01 00:00:00Z"));
    }

    @Test
    public void testConvertToFloat() throws IOException {
        FieldToRowDataConverter converter = converters.createConverter(FLOAT().getLogicalType());
        assertEquals(1.0F, converter.convert("1.0"));
    }

    @Test
    public void testConvertToDouble() throws IOException {
        FieldToRowDataConverter converter = converters.createConverter(DOUBLE().getLogicalType());
        assertEquals(101010101011.11213213123333, converter.convert("101010101011.11213213123333"));
    }

    @Test
    public void testConvertToBytes() throws IOException {
        byte[] expectedBytes = new byte[1024];
        ThreadLocalRandom.current().nextBytes(expectedBytes);
        String input = new String(Base64.getEncoder().encode(expectedBytes));
        FieldToRowDataConverter converter = converters.createConverter(BINARY(100).getLogicalType());
        assertArrayEquals(expectedBytes, (byte[]) converter.convert(input));
    }

    @Test
    public void testConvertToStringData() throws IOException {
        String testStr = "test";
        FieldToRowDataConverter converter = converters.createConverter(STRING().getLogicalType());
        StringData expected = StringData.fromString(testStr);
        assertEquals(expected, converter.convert(testStr));
    }

    @Test
    public void testConvertToDecimal() throws IOException {
        String test = "123123123123231234566789009876";
        FieldToRowDataConverter converter = converters.createConverter(DECIMAL(30, 0).getLogicalType());
        DecimalData expected = DecimalData.fromBigDecimal(new BigDecimal(test), 30, 0);
        assertEquals(expected, converter.convert(test));
    }

    @Test
    public void testConvertToArray() throws IOException {
        // int element
        String testIntArray = "[1,2,3,4,5,6]";
        FieldToRowDataConverter intArrayConverter = converters.createConverter(ARRAY(INT()).getLogicalType());
        GenericArrayData expectedIntArray = new GenericArrayData(new Integer[]{1, 2, 3, 4, 5, 6});
        assertEquals(expectedIntArray, intArrayConverter.convert(testIntArray));

        // string element
        String testStrArray = "[\"f1\",\"f2\"]";
        FieldToRowDataConverter strArrayConverter = converters.createConverter(ARRAY(STRING()).getLogicalType());
        GenericArrayData expectedStrArray = new GenericArrayData(
                new StringData[]{StringData.fromString("f1"), StringData.fromString("f2")});
        assertEquals(expectedStrArray, strArrayConverter.convert(testStrArray));

        // array element
        String testArrayArray = "[[1,2,3],[4,5,6]]";
        FieldToRowDataConverter arrayArrayConverter = converters.createConverter(ARRAY(ARRAY(INT())).getLogicalType());
        GenericArrayData innerArray1 = new GenericArrayData(new Integer[]{1, 2, 3});
        GenericArrayData innerArray2 = new GenericArrayData(new Integer[]{4, 5, 6});
        GenericArrayData expectedArrayArray = new GenericArrayData(new GenericArrayData[]{innerArray1, innerArray2});
        assertEquals(expectedArrayArray, arrayArrayConverter.convert(testArrayArray));
    }

    @Test
    public void testConvertToMap() throws IOException {
        // normal map
        String testStrIntMap = "{\"f1\":1,\"f2\":2}";
        FieldToRowDataConverter strIntConverter = converters.createConverter(MAP(STRING(), INT()).getLogicalType());
        Map<StringData, Integer> expectedStrIntMap = new HashMap<>();
        expectedStrIntMap.put(StringData.fromString("f1"), 1);
        expectedStrIntMap.put(StringData.fromString("f2"), 2);
        assertEquals(new GenericMapData(expectedStrIntMap), strIntConverter.convert(testStrIntMap));

        // nested map
        String testNestedMap = "{\"f1\":{\"f11\":11},\"f2\":{\"f21\":21}}";
        FieldToRowDataConverter nestedConverter =
                converters.createConverter(MAP(STRING(), MAP(STRING(), INT())).getLogicalType());

        Map<StringData, GenericMapData> expectedNestedMap = new HashMap<>();

        Map<StringData, Integer> innerMapF1 = new HashMap<>();
        innerMapF1.put(StringData.fromString("f11"), 11);
        expectedNestedMap.put(StringData.fromString("f1"), new GenericMapData(innerMapF1));

        Map<StringData, Integer> innerMapF2 = new HashMap<>();
        innerMapF2.put(StringData.fromString("f21"), 21);
        expectedNestedMap.put(StringData.fromString("f2"), new GenericMapData(innerMapF2));

        assertEquals(new GenericMapData(expectedNestedMap), nestedConverter.convert(testNestedMap));
    }

    @Test
    public void testConvertToRowData() throws IOException {
        String testStr = "{\"f1\":1,\"f2\":\"testStr\",\"f3\":{\"f31\":31,\"f32\":\"innerStr\"}}";
        DataType dataType = ROW(
                FIELD("f1", INT()),
                FIELD("f2", STRING()),
                FIELD("f3", ROW(
                        FIELD("f31", INT()),
                        FIELD("f32", STRING()))));
        FieldToRowDataConverter converter = converters.createConverter(dataType.getLogicalType());

        GenericRowData expectedRowData = new GenericRowData(3);
        expectedRowData.setField(0, 1);
        expectedRowData.setField(1, StringData.fromString("testStr"));
        GenericRowData innerRowData = new GenericRowData(2);
        innerRowData.setField(0, 31);
        innerRowData.setField(1, StringData.fromString("innerStr"));
        expectedRowData.setField(2, innerRowData);

        assertEquals(expectedRowData, converter.convert(testStr));
    }
}
