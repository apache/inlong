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
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;
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
import org.apache.inlong.sort.protocol.deserialization.AvroDeserializationInfo;
import org.apache.inlong.sort.protocol.deserialization.CanalDeserializationInfo;
import org.apache.inlong.sort.protocol.deserialization.DebeziumDeserializationInfo;
import org.apache.inlong.sort.protocol.deserialization.JsonDeserializationInfo;
import org.apache.inlong.sort.protocol.serialization.AvroSerializationInfo;
import org.apache.inlong.sort.singletenant.flink.serialization.RowSerializationSchemaFactory;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DeserializationSchemaTest {

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

    @Test
    public void testJsonDeserializationSchema() throws IOException, ClassNotFoundException {
        Row expectedRow = generateTestRow();
        ObjectMapper objectMapper = new ObjectMapper();
        ObjectNode root = objectMapper.createObjectNode();
        //noinspection ConstantConditions
        root.put("id", (long) expectedRow.getField(0));
        root.put("name", (String) expectedRow.getField(1));
        root.put("bytes", (byte[]) expectedRow.getField(2));
        root.put("date", "1990-10-14");
        root.put("time", "12:12:43Z");
        root.put("timestamp", "1990-10-14T12:12:43Z");
        root.putObject("map").put("flink", 123);
        root.putObject("map2map").putObject("inner_map").put("key", 234);

        byte[] serializedJson = objectMapper.writeValueAsBytes(root);

        DeserializationSchema<Row> schema =
                DeserializationSchemaFactory.build(fieldInfos, new JsonDeserializationInfo());

        Row actualRow = schema.deserialize(serializedJson);

        assertEquals(expectedRow, actualRow);
    }

    private Row generateTestRow() {
        Row testRow = new Row(8);
        testRow.setField(0, 1238123899121L);
        testRow.setField(1, "testName");

        byte[] bytes = new byte[1024];
        ThreadLocalRandom.current().nextBytes(bytes);
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
    public void testAvroDeserializationSchema() throws IOException, ClassNotFoundException {
        SerializationSchema<Row> serializationSchema =
                RowSerializationSchemaFactory.build(fieldInfos, new AvroSerializationInfo());

        Row expectedRow = generateTestRow();
        byte[] bytes = serializationSchema.serialize(expectedRow);

        DeserializationSchema<Row> deserializationSchema =
                DeserializationSchemaFactory.build(fieldInfos, new AvroDeserializationInfo());
        Row actualRow = deserializationSchema.deserialize(bytes);

        assertEquals(expectedRow, actualRow);
    }

    @Test
    public void testCanalDeserializationSchema() throws IOException {
        List<String> lines = readLines("canal-data.txt");
        byte[] testBytes = lines.get(0).getBytes(StandardCharsets.UTF_8);
        FieldInfo[] fieldInfos = new FieldInfo[]{
                new FieldInfo("id", IntFormatInfo.INSTANCE),
                new FieldInfo("name", StringFormatInfo.INSTANCE),
                new FieldInfo("description", StringFormatInfo.INSTANCE),
                new FieldInfo("weight", FloatFormatInfo.INSTANCE)
        };

        DeserializationSchema<RowData> schemaWithoutFilter = RowDataDeserializationSchemaFactory.build(
                fieldInfos,
                new CanalDeserializationInfo(null, null, false, "ISO_8601", true)
        );
        SimpleCollector collector1 = new SimpleCollector();
        schemaWithoutFilter.deserialize(testBytes, collector1);
        assertEquals(9, collector1.list.size());
        RowData rowData = collector1.list.get(0);
        assertEquals(101, rowData.getInt(0));
        assertEquals("scooter", rowData.getString(1).toString());
        assertEquals("Small 2-wheel scooter", rowData.getString(2).toString());
        assertEquals(3.14f, rowData.getFloat(3), 0);
        assertEquals("inventory", rowData.getString(4).toString());
        assertEquals("products2", rowData.getString(5).toString());
        assertEquals(4, rowData.getMap(6).size());
        assertEquals("id", rowData.getArray(7).getString(0).toString());
        assertEquals(1589373515477L, rowData.getTimestamp(8, 3).getMillisecond());
        assertEquals(1589373515000L, rowData.getTimestamp(9, 3).getMillisecond());

        DeserializationSchema<RowData> schemaWithFilter = RowDataDeserializationSchemaFactory.build(
                fieldInfos,
                new CanalDeserializationInfo("NoExistDB", null, false, "ISO_8601", true)
        );
        SimpleCollector collector2 = new SimpleCollector();
        schemaWithFilter.deserialize(testBytes, collector2);
        assertTrue(collector2.list.isEmpty());
    }

    @Test
    public void testDebeziumDeserializationScheme() throws IOException {
        String insertMsg = "{\n"
                + "    \"before\":null,\n"
                + "    \"after\":{\n"
                + "        \"name\":\"asd\",\n"
                + "        \"age\":12\n"
                + "    },\n"
                + "    \"source\":{\n"
                + "        \"version\":\"1.4.2.Final\",\n"
                + "        \"connector\":\"mysql\",\n"
                + "        \"name\":\"my_server_01\",\n"
                + "        \"ts_ms\":1644896917000,\n"
                + "        \"snapshot\":\"false\",\n"
                + "        \"db\":\"test\",\n"
                + "        \"table\":\"test\",\n"
                + "        \"server_id\":1,\n"
                + "        \"gtid\":null,\n"
                + "        \"file\":\"mysql-bin.000067\",\n"
                + "        \"pos\":944,\n"
                + "        \"row\":0,\n"
                + "        \"thread\":13,\n"
                + "        \"query\":null\n"
                + "    },\n"
                + "    \"op\":\"c\",\n"
                + "    \"ts_ms\":1644896917208,\n"
                + "    \"transaction\":null\n"
                + "}";
        byte[] testBytes1 = insertMsg.getBytes(StandardCharsets.UTF_8);
        FieldInfo[] fieldInfos = new FieldInfo[]{
                new FieldInfo("name", StringFormatInfo.INSTANCE),
                new FieldInfo("age", IntFormatInfo.INSTANCE),
        };

        DeserializationSchema<RowData> schemaWithoutFilter = RowDataDeserializationSchemaFactory.build(
                fieldInfos,
                new DebeziumDeserializationInfo(true, "ISO_8601")
        );
        SimpleCollector collector1 = new SimpleCollector();
        schemaWithoutFilter.deserialize(testBytes1, collector1);
        RowData insertData = collector1.list.get(0);
        assertEquals("asd", insertData.getString(0));
        assertEquals(12, insertData.getInt(1));
        assertEquals(RowKind.INSERT, insertData.getRowKind());

    }

    private static List<String> readLines(String resource) throws IOException {
        final URL url = DeserializationSchemaTest.class.getClassLoader().getResource(resource);
        //noinspection ConstantConditions
        Path path = new File(url.getFile()).toPath();
        return Files.readAllLines(path);
    }

    private static class SimpleCollector implements Collector<RowData> {

        private final List<RowData> list = new ArrayList<>();

        @Override
        public void collect(RowData record) {
            list.add(record);
        }

        @Override
        public void close() {
            // do nothing
        }

    }

}
