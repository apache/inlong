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

import org.apache.inlong.sort.formats.base.TextFormatOptions.MapNullKeyMode;
import org.apache.inlong.sort.formats.base.TextFormatOptions.TimestampFormat;
import org.apache.inlong.sort.formats.json.RowDataToFieldConverters.RowDataToFieldConverter;

import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.NullType;
import org.junit.Test;

import java.math.BigDecimal;
import java.sql.Timestamp;
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

/** test for {@link RowDataToFieldConverters}. */
public class RowDataToFieldConvertersTest {

    private final RowDataToFieldConverters converters =
            new RowDataToFieldConverters(TimestampFormat.SQL, MapNullKeyMode.FAIL, "null");

    @Test
    public void testConvertNull() {
        RowDataToFieldConverters.RowDataToFieldConverter converter = converters.createConverter(new NullType());
        assertNull(converter.convert(123));
    }

    @Test
    public void testConvertBoolean() {
        RowDataToFieldConverter converter = converters.createConverter(BOOLEAN().getLogicalType());
        assertTrue((Boolean) converter.convert(true));
        assertFalse((Boolean) converter.convert(false));
    }

    @Test
    public void testConvertTinyInt() {
        RowDataToFieldConverter converter = converters.createConverter(TINYINT().getLogicalType());
        assertEquals((byte) 127, converter.convert((byte) 127));
    }

    @Test
    public void testConvertToSmallInt() {
        RowDataToFieldConverter converter = converters.createConverter(SMALLINT().getLogicalType());
        assertEquals((short) 32767, converter.convert((short) 32767));
    }

    @Test
    public void testConvertInt() {
        RowDataToFieldConverter converter = converters.createConverter(INT().getLogicalType());
        assertEquals(32768, converter.convert(32768));
    }

    @Test
    public void testConvertLong() {
        RowDataToFieldConverter converter = converters.createConverter(BIGINT().getLogicalType());
        assertEquals(123123123123123123L, converter.convert(123123123123123123L));
    }

    @Test
    public void testConvertDate() {
        RowDataToFieldConverter converter = converters.createConverter(DATE().getLogicalType());
        assertEquals("1970-01-02", converter.convert(1));
    }

    @Test
    public void testConvertTime() {
        RowDataToFieldConverter converter = converters.createConverter(TIME().getLogicalType());
        assertEquals("00:00:01", converter.convert(1000));
    }

    @Test
    public void testConvertTimestamp() {
        RowDataToFieldConverter converter = converters.createConverter(TIMESTAMP().getLogicalType());
        TimestampData testTimestampData = TimestampData.fromEpochMillis(86401000);
        assertEquals("1970-01-02 00:00:01", converter.convert(testTimestampData));
    }

    @Test
    public void testConvertTimestampWithLocalZone() {
        TimeZone.setDefault(TimeZone.getDefault().getTimeZone("GMT+0"));
        RowDataToFieldConverter converter =
                converters.createConverter(TIMESTAMP_WITH_LOCAL_TIME_ZONE().getLogicalType());
        TimestampData testTimestampData = TimestampData.fromTimestamp(new Timestamp(0));
        assertEquals("1970-01-01 00:00:00Z", converter.convert(testTimestampData));
    }

    @Test
    public void testConvertFloat() {
        RowDataToFieldConverter converter = converters.createConverter(FLOAT().getLogicalType());
        assertEquals(1.0F, converter.convert(1.0F));
    }

    @Test
    public void testConvertDouble() {
        RowDataToFieldConverter converter = converters.createConverter(DOUBLE().getLogicalType());
        assertEquals(101010101011.11213213123333, converter.convert(101010101011.11213213123333));
    }

    @Test
    public void testConvertBytes() {
        byte[] testBytes = new byte[1024];
        ThreadLocalRandom.current().nextBytes(testBytes);
        RowDataToFieldConverter converter = converters.createConverter(BINARY(100).getLogicalType());
        assertArrayEquals(testBytes, (byte[]) converter.convert(testBytes));
    }

    @Test
    public void testConvertStringData() {
        String testStr = "test";
        RowDataToFieldConverter converter = converters.createConverter(STRING().getLogicalType());
        StringData stringData = StringData.fromString(testStr);
        assertEquals(testStr, converter.convert(stringData));
    }

    @Test
    public void testConvertDecimal() {
        String testStr = "123123123123231234566789009876";
        DecimalData decimalData = DecimalData.fromBigDecimal(new BigDecimal(testStr), 30, 0);
        RowDataToFieldConverter converter = converters.createConverter(DECIMAL(30, 0).getLogicalType());
        assertEquals(new BigDecimal(testStr), converter.convert(decimalData));
    }

    @Test
    public void testConvertToArray() {
        // int element
        RowDataToFieldConverter intArrayConverter = converters.createConverter(ARRAY(INT()).getLogicalType());
        GenericArrayData testIntArray = new GenericArrayData(new Integer[]{1, 2, 3, 4, 5, 6});
        assertArrayEquals(new Integer[]{1, 2, 3, 4, 5, 6}, (Object[]) intArrayConverter.convert(testIntArray));

        // string element
        RowDataToFieldConverter strArrayConverter = converters.createConverter(ARRAY(STRING()).getLogicalType());
        GenericArrayData testStrArray = new GenericArrayData(
                new StringData[]{StringData.fromString("f1"), StringData.fromString("f2")});
        assertArrayEquals(new String[]{"f1", "f2"}, (Object[]) strArrayConverter.convert(testStrArray));

        // array element
        RowDataToFieldConverter arrayArrayConverter = converters.createConverter(ARRAY(ARRAY(INT())).getLogicalType());
        GenericArrayData innerArray1 = new GenericArrayData(new Integer[]{1, 2, 3});
        GenericArrayData innerArray2 = new GenericArrayData(new Integer[]{4, 5, 6});
        GenericArrayData testArrayData = new GenericArrayData(new GenericArrayData[]{innerArray1, innerArray2});
        Integer[][] expected = new Integer[2][];
        expected[0] = new Integer[]{1, 2, 3};
        expected[1] = new Integer[]{4, 5, 6};
        assertArrayEquals(expected, (Object[]) arrayArrayConverter.convert(testArrayData));
    }

    @Test
    public void testConvertToMap() {
        // normal map
        RowDataToFieldConverter strIntMapConverter = converters.createConverter(MAP(STRING(), INT()).getLogicalType());
        Map<StringData, Integer> innerMap = new HashMap<>();
        innerMap.put(StringData.fromString("f1"), 1);
        innerMap.put(StringData.fromString("f2"), 2);
        GenericMapData genericStrIntMapData = new GenericMapData(innerMap);

        Map<String, Integer> expectedStrIntMap = new HashMap<>();
        expectedStrIntMap.put("f1", 1);
        expectedStrIntMap.put("f2", 2);

        assertEquals(expectedStrIntMap, strIntMapConverter.convert(genericStrIntMapData));

        // nested map
        RowDataToFieldConverter nestedConverter =
                converters.createConverter(MAP(STRING(), MAP(STRING(), INT())).getLogicalType());

        Map<StringData, GenericMapData> nestedInnerMap = new HashMap<>();
        Map<StringData, Integer> innerMapF1 = new HashMap<>();
        innerMapF1.put(StringData.fromString("f11"), 11);
        nestedInnerMap.put(StringData.fromString("f1"), new GenericMapData(innerMapF1));
        Map<StringData, Integer> innerMapF2 = new HashMap<>();
        innerMapF2.put(StringData.fromString("f21"), 21);
        nestedInnerMap.put(StringData.fromString("f2"), new GenericMapData(innerMapF2));
        GenericMapData genericNestedMapData = new GenericMapData(nestedInnerMap);

        Map<String, Map<String, Integer>> expectedNestedMap = new HashMap<>();
        Map<String, Integer> innerMap1 = new HashMap<>();
        innerMap1.put("f11", 11);
        expectedNestedMap.put("f1", innerMap1);
        Map<String, Integer> innerMap2 = new HashMap<>();
        innerMap2.put("f21", 21);
        expectedNestedMap.put("f2", innerMap2);

        assertEquals(expectedNestedMap, nestedConverter.convert(genericNestedMapData));
    }

    @Test
    public void testConvertToRowData() {
        DataType dataType = ROW(
                FIELD("f1", INT()),
                FIELD("f2", STRING()),
                FIELD("f3", ROW(
                        FIELD("f31", INT()),
                        FIELD("f32", STRING()))));
        RowDataToFieldConverter converter = converters.createConverter(dataType.getLogicalType());

        GenericRowData testRowData = new GenericRowData(3);
        testRowData.setField(0, 1);
        testRowData.setField(1, StringData.fromString("testStr"));
        GenericRowData innerRowData = new GenericRowData(2);
        innerRowData.setField(0, 31);
        innerRowData.setField(1, StringData.fromString("innerStr"));
        testRowData.setField(2, innerRowData);

        Map<String, Object> expectedMap = new HashMap<>();
        expectedMap.put("f1", 1);
        expectedMap.put("f2", "testStr");
        Map<String, Object> innerMap = new HashMap<>();
        innerMap.put("f31", 31);
        innerMap.put("f32", "innerStr");
        expectedMap.put("f3", innerMap);

        assertEquals(expectedMap, converter.convert(testRowData));
    }
}
