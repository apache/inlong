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

import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.Before;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.flink.table.api.DataTypes.ARRAY;
import static org.apache.flink.table.api.DataTypes.BIGINT;
import static org.apache.flink.table.api.DataTypes.BOOLEAN;
import static org.apache.flink.table.api.DataTypes.BYTES;
import static org.apache.flink.table.api.DataTypes.DATE;
import static org.apache.flink.table.api.DataTypes.DECIMAL;
import static org.apache.flink.table.api.DataTypes.DOUBLE;
import static org.apache.flink.table.api.DataTypes.FIELD;
import static org.apache.flink.table.api.DataTypes.FLOAT;
import static org.apache.flink.table.api.DataTypes.INT;
import static org.apache.flink.table.api.DataTypes.MAP;
import static org.apache.flink.table.api.DataTypes.MULTISET;
import static org.apache.flink.table.api.DataTypes.ROW;
import static org.apache.flink.table.api.DataTypes.SMALLINT;
import static org.apache.flink.table.api.DataTypes.STRING;
import static org.apache.flink.table.api.DataTypes.TIME;
import static org.apache.flink.table.api.DataTypes.TIMESTAMP;
import static org.apache.flink.table.api.DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE;
import static org.apache.flink.table.api.DataTypes.TINYINT;

/** Base test class for {@link JsonRowDataDeserializationSchema} and {@link JsonRowDataSerializationSchema}.*/
public abstract class JsonRowDataSerDeTestBase {

    public String testJson;

    public GenericRowData testRowData;

    public RowType rowType;

    @Before
    public void init() {
        TimeZone.setDefault(TimeZone.getDefault().getTimeZone("GMT+0"));
        byte[] bytes = new byte[10];
        ThreadLocalRandom.current().nextBytes(bytes);
        String base64Str = Base64.getEncoder().encodeToString(bytes);

        testJson = "{\n"
                + "    \"bool\":true,\n"
                + "    \"tinyint\":99,\n"
                + "    \"smallint\":128,\n"
                + "    \"int\":45536,\n"
                + "    \"bigint\":1238123899121,\n"
                + "    \"float\":33.333,\n"
                + "    \"name\":\"asdlkjasjkdla998y1122\",\n"
                + "    \"bytes\":\"" + base64Str + "\",\n"
                + "    \"decimal\":123.456789,\n"
                + "    \"doubles\":[\n"
                + "        1.1,\n"
                + "        2.2,\n"
                + "        3.3\n"
                + "    ],\n"
                + "    \"arrayArray\":[\n"
                + "        [\n"
                + "            1,\n"
                + "            2,\n"
                + "            3\n"
                + "        ],\n"
                + "        [\n"
                + "            4,\n"
                + "            5,\n"
                + "            6\n"
                + "        ]\n"
                + "    ],\n"
                + "    \"mapArray\":[\n"
                + "        {\n"
                + "            \"mapArrayF1\":1,\n"
                + "            \"mapArrayF2\":2\n"
                + "        }\n"
                + "    ],\n"
                + "    \"rowArray\":[\n"
                + "        {\n"
                + "            \"rowArrayF1\":1,\n"
                + "            \"rowArrayF2\":\"test1\"\n"
                + "        },\n"
                + "        {\n"
                + "            \"rowArrayF1\":2,\n"
                + "            \"rowArrayF2\":\"test2\"\n"
                + "        }\n"
                + "    ],\n"
                + "    \"date\":\"1970-01-02\",\n"
                + "    \"time\":\"00:00:01\",\n"
                + "    \"timestamp3\":\"1970-01-01T00:00:01.123\",\n"
                + "    \"timestamp9\":\"1970-01-01T00:00:01.123456789\",\n"
                + "    \"timestampWithLocalZone\":\"1970-01-01T00:00:01.123Z\",\n"
                + "    \"map\":{\n"
                + "        \"element\":123\n"
                + "    },\n"
                + "    \"multiSet\":{\n"
                + "        \"element\":2\n"
                + "    },\n"
                + "    \"map2map\":{\n"
                + "        \"inner_map\":{\n"
                + "            \"key\":234\n"
                + "        }\n"
                + "    },\n"
                + "    \"arrayInMap\":{\n"
                + "        \"arrayInMapField\":[\n"
                + "            1.1,\n"
                + "            2.2,\n"
                + "            3.3\n"
                + "        ]\n"
                + "    },\n"
                + "    \"innerRow\":{\n"
                + "        \"innerRowF1\":\"testStr\",\n"
                + "        \"innerRowF2\":1.1\n"
                + "    }\n"
                + "}";

        // expected row
        testRowData = new GenericRowData(23);
        testRowData.setField(0, true);
        testRowData.setField(1, (byte) 99);
        testRowData.setField(2, (short) 128);
        testRowData.setField(3, 45536);
        testRowData.setField(4, 1238123899121L);
        testRowData.setField(5, 33.333F);
        testRowData.setField(6, StringData.fromString("asdlkjasjkdla998y1122"));
        testRowData.setField(7, bytes);
        testRowData.setField(8, DecimalData.fromBigDecimal(new BigDecimal("123.456789"), 9, 6));
        testRowData.setField(9, new GenericArrayData(new Double[]{1.1, 2.2, 3.3}));
        testRowData.setField(
                10,
                new GenericArrayData(new ArrayData[]{
                        new GenericArrayData(new Integer[]{1, 2, 3}),
                        new GenericArrayData(new Integer[]{4, 5, 6})
                }));

        Map<Object, Object> innerMapArray = new HashMap<>();
        innerMapArray.put(StringData.fromString("mapArrayF1"), 1);
        innerMapArray.put(StringData.fromString("mapArrayF2"), 2);
        MapData mapData = new GenericMapData(innerMapArray);
        testRowData.setField(11, new GenericArrayData(new MapData[]{mapData}));

        GenericRowData innerRowArray1 = new GenericRowData(2);
        innerRowArray1.setField(0, 1);
        innerRowArray1.setField(1, StringData.fromString("test1"));
        GenericRowData innerRowArray2 = new GenericRowData(2);
        innerRowArray2.setField(0, 2);
        innerRowArray2.setField(1, StringData.fromString("test2"));
        testRowData.setField(12, new GenericArrayData(new RowData[]{innerRowArray1, innerRowArray2}));

        testRowData.setField(13, 1);
        testRowData.setField(14, 1000);
        testRowData.setField(15, TimestampData.fromEpochMillis(1123));
        testRowData.setField(16, TimestampData.fromEpochMillis(1123, 456789));
        testRowData.setField(17, TimestampData.fromTimestamp(new Timestamp(1123)));

        Map<Object, Object> innerMap = new HashMap<>();
        innerMap.put(StringData.fromString("element"), 123);
        GenericMapData mapField = new GenericMapData(innerMap);
        testRowData.setField(18, mapField);

        Map<Object, Object> innerMapset = new HashMap<>();
        innerMapset.put(StringData.fromString("element"), 2);
        GenericMapData mapsetField = new GenericMapData(innerMapset);
        testRowData.setField(19, mapsetField);

        Map<Object, Object> innerMap2Map = new HashMap<>();
        innerMap2Map.put(StringData.fromString("key"), 234);
        GenericMapData innerGenericMap2Map = new GenericMapData(innerMap2Map);
        Map<Object, Object> outMap = new HashMap<>();
        outMap.put(StringData.fromString("inner_map"), innerGenericMap2Map);
        GenericMapData map2mapField = new GenericMapData(outMap);
        testRowData.setField(20, map2mapField);

        Map<Object, Object> actualArrayInMap = new HashMap<>();
        GenericArrayData innerArrayForArrayInMap = new GenericArrayData(new Double[]{1.1, 2.2, 3.3});
        actualArrayInMap.put(StringData.fromString("arrayInMapField"), innerArrayForArrayInMap);
        GenericMapData genericMapData = new GenericMapData(actualArrayInMap);
        testRowData.setField(21, genericMapData);

        GenericRowData innerRowField = new GenericRowData(2);
        innerRowField.setField(0, StringData.fromString("testStr"));
        innerRowField.setField(1, 1.1);
        testRowData.setField(22, innerRowField);

        // data type
        DataType dataType =
                ROW(
                        FIELD("bool", BOOLEAN()),
                        FIELD("tinyint", TINYINT()),
                        FIELD("smallint", SMALLINT()),
                        FIELD("int", INT()),
                        FIELD("bigint", BIGINT()),
                        FIELD("float", FLOAT()),
                        FIELD("name", STRING()),
                        FIELD("bytes", BYTES()),
                        FIELD("decimal", DECIMAL(9, 6)),
                        FIELD("doubles", ARRAY(DOUBLE())),
                        FIELD("arrayArray", ARRAY(ARRAY(INT()))),
                        FIELD("mapArray", ARRAY(MAP(STRING(), INT()))),
                        FIELD("rowArray", ARRAY(ROW(
                                FIELD("rowArrayF1", INT()),
                                FIELD("rowArrayF2", STRING())))),
                        FIELD("date", DATE()),
                        FIELD("time", TIME(0)),
                        FIELD("timestamp3", TIMESTAMP(3)),
                        FIELD("timestamp9", TIMESTAMP(9)),
                        FIELD("timestampWithLocalZone", TIMESTAMP_WITH_LOCAL_TIME_ZONE(3)),
                        FIELD("map", MAP(STRING(), INT())),
                        FIELD("multiSet", MULTISET(STRING())),
                        FIELD("map2map", MAP(STRING(), MAP(STRING(), INT()))),
                        FIELD("arrayInMap", MAP(STRING(), ARRAY(DOUBLE()))),
                        FIELD("innerRow", ROW(
                                FIELD("innerRowF1", STRING()),
                                FIELD("innerRowF2", DOUBLE()))));

        rowType = (RowType) dataType.getLogicalType();
    }
}
