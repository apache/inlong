/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.inlong.sort.singletenant.flink.deserialization;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.types.Row;
import org.apache.inlong.sort.formats.common.BinaryFormatInfo;
import org.apache.inlong.sort.formats.common.DateFormatInfo;
import org.apache.inlong.sort.formats.common.FloatFormatInfo;
import org.apache.inlong.sort.formats.common.IntFormatInfo;
import org.apache.inlong.sort.formats.common.LongFormatInfo;
import org.apache.inlong.sort.formats.common.MapFormatInfo;
import org.apache.inlong.sort.formats.common.StringFormatInfo;
import org.apache.inlong.sort.formats.common.TimeFormatInfo;
import org.apache.inlong.sort.formats.common.TimestampFormatInfo;
import org.apache.inlong.sort.protocol.FieldInfo;
import org.apache.inlong.sort.protocol.deserialization.CanalDeserializationInfo;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class CanalDeserializationTest {

    private final FieldInfo[] fieldInfos = new FieldInfo[]{
            new FieldInfo("id", LongFormatInfo.INSTANCE),
            new FieldInfo("name", StringFormatInfo.INSTANCE),
            new FieldInfo("bytes", BinaryFormatInfo.INSTANCE),
            new FieldInfo("date", new DateFormatInfo("yyyy-MM-dd")),
            new FieldInfo("time", new TimeFormatInfo("HH:mm:ss")),
            new FieldInfo("timestamp", new TimestampFormatInfo("yyyy-MM-dd HH:mm:ss")),
            new FieldInfo("map",
                    new MapFormatInfo(StringFormatInfo.INSTANCE, LongFormatInfo.INSTANCE)),
            new FieldInfo("map2map", new MapFormatInfo(StringFormatInfo.INSTANCE,
                    new MapFormatInfo(StringFormatInfo.INSTANCE, IntFormatInfo.INSTANCE)))
    };

    private Row generateTestRow() {
        Row testRow = new Row(8);
        testRow.setField(0, 1238123899121L);
        testRow.setField(1, "testName");

        byte[] bytes = new byte[]{1, 2, 3, 4, 5, 6};
        testRow.setField(2, bytes);

        testRow.setField(3, Date.valueOf("1990-10-14"));
        testRow.setField(4, Time.valueOf("12:12:43"));
        testRow.setField(5, Timestamp.valueOf("1990-10-14 12:12:43"));

        Map<String, Long> map = new HashMap<>();
        map.put("flink", 123L);
        testRow.setField(6, map);

        Map<String, Map<String, Integer>> nestedMap = new HashMap<>();
        Map<String, Integer> innerMap = new HashMap<>();
        innerMap.put("key", 234);
        nestedMap.put("inner_map", innerMap);
        testRow.setField(7, nestedMap);

        return testRow;
    }

    @Test
    public void testDeserializeStringWithoutMetadata() throws IOException, ClassNotFoundException {
        String testString = "{\n"
                + "    \"data\":[\n"
                + "        {\n"
                + "            \"id\":1238123899121,\n"
                + "            \"name\":\"testName\",\n"
                + "            \"bytes\":\"AQIDBAUG\",\n"
                + "            \"date\":\"1990-10-14\",\n"
                + "            \"time\":\"12:12:43\",\n"
                + "            \"timestamp\":\"1990-10-14 12:12:43\",\n"
                + "            \"map\":{\n"
                + "                \"flink\":123\n"
                + "            },\n"
                + "            \"map2map\":{\n"
                + "                \"inner_map\":{\n"
                + "                    \"key\":234\n"
                + "                }\n"
                + "            }\n"
                + "        }\n"
                + "    ],\n"
                + "    \"type\":\"INSERT\"\n"
                + "}";
        byte[] testBytes = testString.getBytes(StandardCharsets.UTF_8);
        DeserializationSchema<Row> schema = DeserializationSchemaFactory.build(
                fieldInfos,
                new CanalDeserializationInfo(null, null, false, "ISO_8601", false)
        );
        ListCollector<Row> collector = new ListCollector<>();
        schema.deserialize(testBytes, collector);
        assertEquals(generateTestRow(), collector.getInnerList().get(0));
    }

    @Test
    public void testCanalDeserializationSchema() throws IOException, ClassNotFoundException {
        String testCanalData = "{\n"
                + "    \"data\":[\n"
                + "        {\n"
                + "            \"id\":\"101\",\n"
                + "            \"name\":\"scooter\",\n"
                + "            \"description\":\"Small 2-wheel scooter\",\n"
                + "            \"weight\":\"3.14\"\n"
                + "        },\n"
                + "        {\n"
                + "            \"id\":\"102\",\n"
                + "            \"name\":\"car battery\",\n"
                + "            \"description\":\"12V car battery\",\n"
                + "            \"weight\":\"8.1\"\n"
                + "        },\n"
                + "        {\n"
                + "            \"id\":\"103\",\n"
                + "            \"name\":\"12-pack drill bits\",\n"
                + "            \"description\":\"12-pack of drill bits with sizes ranging from #40 to #3\",\n"
                + "            \"weight\":\"0.8\"\n"
                + "        }\n"
                + "    ],\n"
                + "    \"database\":\"inventory\",\n"
                + "    \"es\":1589373515000,\n"
                + "    \"id\":3,\n"
                + "    \"isDdl\":false,\n"
                + "    \"mysqlType\":{\n"
                + "        \"id\":\"INTEGER\",\n"
                + "        \"name\":\"VARCHAR(255)\",\n"
                + "        \"description\":\"VARCHAR(512)\",\n"
                + "        \"weight\":\"FLOAT\"\n"
                + "    },\n"
                + "    \"old\":null,\n"
                + "    \"pkNames\":[\n"
                + "        \"id\"\n"
                + "    ],\n"
                + "    \"sql\":\"\",\n"
                + "    \"sqlType\":{\n"
                + "        \"id\":4,\n"
                + "        \"name\":12,\n"
                + "        \"description\":12,\n"
                + "        \"weight\":7\n"
                + "    },\n"
                + "    \"table\":\"products2\",\n"
                + "    \"ts\":1589373515477,\n"
                + "    \"type\":\"INSERT\"\n"
                + "}";

        byte[] testBytes = testCanalData.getBytes(StandardCharsets.UTF_8);
        FieldInfo[] fieldInfos = new FieldInfo[]{
                new FieldInfo("id", IntFormatInfo.INSTANCE),
                new FieldInfo("name", StringFormatInfo.INSTANCE),
                new FieldInfo("description", StringFormatInfo.INSTANCE),
                new FieldInfo("weight", FloatFormatInfo.INSTANCE)
        };

        DeserializationSchema<Row> schemaWithoutFilter = DeserializationSchemaFactory.build(
                fieldInfos,
                new CanalDeserializationInfo(null, null, false, "ISO_8601", true)
        );
        ListCollector<Row> collector1 = new ListCollector<>();
        schemaWithoutFilter.deserialize(testBytes, collector1);
        List<Row> innerList = collector1.getInnerList();
        assertEquals(3, innerList.size());
        Row row = innerList.get(0);
        assertEquals(101, row.getField(0));
        assertEquals("scooter", row.getField(1).toString());
        assertEquals("Small 2-wheel scooter", row.getField(2).toString());
        assertEquals(3.14f, (Float) row.getField(3), 0);
        assertEquals("inventory", row.getField(4).toString());
        assertEquals("products2", row.getField(5).toString());
        assertEquals(4, ((Map<?, ?>) row.getField(6)).size());
        assertEquals("id", ((String[]) row.getField(7))[0]);
        // "es" and "ts" are treated as local in flink-canal
        assertEquals(1589373515477L, TimestampData.fromLocalDateTime((LocalDateTime) row.getField(8)).getMillisecond());
        assertEquals(1589373515000L, TimestampData.fromLocalDateTime((LocalDateTime) row.getField(9)).getMillisecond());

        DeserializationSchema<Row> schemaWithFilter = DeserializationSchemaFactory.build(
                fieldInfos,
                new CanalDeserializationInfo("NoExistDB", null, false, "ISO_8601", true)
        );
        ListCollector<Row> collector2 = new ListCollector<>();
        schemaWithFilter.deserialize(testBytes, collector2);
        assertTrue(collector2.getInnerList().isEmpty());
    }
}
