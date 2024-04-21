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

import org.apache.inlong.common.pojo.sort.dataflow.field.format.ArrayFormatInfo;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.BooleanFormatInfo;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.ByteFormatInfo;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.DateFormatInfo;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.DecimalFormatInfo;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.DoubleFormatInfo;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.FloatFormatInfo;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.FormatInfo;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.IntFormatInfo;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.LongFormatInfo;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.MapFormatInfo;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.RowFormatInfo;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.ShortFormatInfo;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.StringFormatInfo;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.TimeFormatInfo;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.TimestampFormatInfo;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.TextNode;
import org.apache.flink.types.Row;
import org.junit.Test;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Collections;
import java.util.function.Consumer;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link JsonRowDeserializationSchema}.
 */
public class JsonRowDeserializationSchemaTest {

    @Test
    public void testNormal() throws Exception {
        Consumer<JsonRowDeserializationSchema.Builder> config = builder -> {
        };

        testFieldDeserialization(config, StringFormatInfo.INSTANCE, "hello", "hello");
        testFieldDeserialization(config, BooleanFormatInfo.INSTANCE, true, "true");
        testFieldDeserialization(config, ByteFormatInfo.INSTANCE, (byte) 124, "124");
        testFieldDeserialization(config, ShortFormatInfo.INSTANCE, (short) 10000, "10000");
        testFieldDeserialization(config, IntFormatInfo.INSTANCE, 1234567, "1234567");
        testFieldDeserialization(config, LongFormatInfo.INSTANCE, 12345678910L, "12345678910");
        testFieldDeserialization(config, FloatFormatInfo.INSTANCE, 0.33333334f, "0.33333334");
        testFieldDeserialization(config, DoubleFormatInfo.INSTANCE, 0.33333333332, "0.33333333332");
        testFieldDeserialization(config, DecimalFormatInfo.INSTANCE, new BigDecimal("1234.0000000000000000000000001"),
                "1234.0000000000000000000000001");
        testFieldDeserialization(config, new DateFormatInfo("dd/MM/yyyy"), Date.valueOf("2020-03-22"), "22/03/2020");
        testFieldDeserialization(config, new TimeFormatInfo("ss/mm/hh"), Time.valueOf("11:12:13"), "13/12/11");
        testFieldDeserialization(config, new TimestampFormatInfo("dd/MM/yyyy hh:mm:ss"),
                Timestamp.valueOf("2020-03-22 11:12:13"), "22/03/2020 11:12:13");
    }

    @Test
    public void testStringArrayDeserialization() throws Exception {
        Consumer<JsonRowDeserializationSchema.Builder> config = builder -> {
        };

        ArrayFormatInfo arrayFormatInfo = new ArrayFormatInfo(StringFormatInfo.INSTANCE);

        ObjectMapper objectMapper = new ObjectMapper();
        ArrayNode root = objectMapper.createArrayNode();
        root.add("aaa").add("bbb").add("ccc");

        testFieldDeserialization(
                config,
                arrayFormatInfo,
                new String[]{"aaa", "bbb", "ccc"},
                root);
    }

    @Test
    public void testObjectArrayDeserialization() throws Exception {
        Consumer<JsonRowDeserializationSchema.Builder> config = builder -> {
        };

        ArrayFormatInfo arrayFormatInfo = new ArrayFormatInfo(LongFormatInfo.INSTANCE);

        ObjectMapper objectMapper = new ObjectMapper();
        ArrayNode root = objectMapper.createArrayNode();
        root.add(1L).add(2L).add(3L);

        testFieldDeserialization(
                config,
                arrayFormatInfo,
                new Long[]{1L, 2L, 3L},
                root);
    }

    @Test
    public void testMapDeserialization() throws Exception {
        Consumer<JsonRowDeserializationSchema.Builder> config = builder -> {
        };

        MapFormatInfo mapFormatInfo =
                new MapFormatInfo(
                        IntFormatInfo.INSTANCE,
                        StringFormatInfo.INSTANCE);

        ObjectMapper objectMapper = new ObjectMapper();
        ObjectNode root = objectMapper.createObjectNode();
        root.put("123", "aaa");

        testFieldDeserialization(
                config,
                mapFormatInfo,
                Collections.singletonMap(123, "aaa"),
                root);
    }

    @Test
    public void testRowDeserialization() throws Exception {
        Consumer<JsonRowDeserializationSchema.Builder> config = builder -> {
        };

        RowFormatInfo rowFormatInfo =
                new RowFormatInfo(
                        new String[]{"in"},
                        new FormatInfo[]{StringFormatInfo.INSTANCE});

        ObjectMapper objectMapper = new ObjectMapper();
        ObjectNode root = objectMapper.createObjectNode();
        root.put("in", "aaa");

        testFieldDeserialization(
                config,
                rowFormatInfo,
                Row.of("aaa"),
                root);
    }

    @Test
    public void testIgnoreMissingField() throws Exception {
        Consumer<JsonRowDeserializationSchema.Builder> config = builder -> {
        };

        RowFormatInfo rowFormatInfo =
                new RowFormatInfo(
                        new String[]{"f1", "f2"},
                        new FormatInfo[]{StringFormatInfo.INSTANCE, IntFormatInfo.INSTANCE});

        ObjectMapper objectMapper = new ObjectMapper();
        ObjectNode root = objectMapper.createObjectNode();
        root.put("f1", "aaa");
        String serializedJson = objectMapper.writeValueAsString(root);

        testRowDeserialization(
                config,
                rowFormatInfo,
                Row.of("aaa", null),
                serializedJson);
    }

    private static <T> void testFieldDeserialization(
            Consumer<JsonRowDeserializationSchema.Builder> config,
            FormatInfo formatInfo,
            T expectedRecord,
            String text) throws Exception {
        testFieldDeserialization(
                config,
                formatInfo,
                expectedRecord,
                new TextNode(text));
    }

    private static <T> void testFieldDeserialization(
            Consumer<JsonRowDeserializationSchema.Builder> config,
            FormatInfo formatInfo,
            T expectedRecord,
            JsonNode node) throws Exception {
        RowFormatInfo rowFormatInfo =
                new RowFormatInfo(
                        new String[]{"f"},
                        new FormatInfo[]{formatInfo});

        ObjectMapper objectMapper = new ObjectMapper();
        ObjectNode root = objectMapper.createObjectNode();
        root.put("f", node);
        String serializedJson = objectMapper.writeValueAsString(root);

        JsonRowDeserializationSchema.Builder builder =
                new JsonRowDeserializationSchema.Builder(rowFormatInfo);
        config.accept(builder);

        JsonRowDeserializationSchema deserializer = builder.build();

        Row row = deserializer.deserialize(serializedJson.getBytes());
        assertEquals(1, row.getArity());

        Object record = row.getField(0);
        if (expectedRecord.getClass().isArray()) {
            ArrayFormatInfo arrayFormatInfo = (ArrayFormatInfo) formatInfo;
            if (arrayFormatInfo.getElementFormatInfo() instanceof StringFormatInfo) {
                assertArrayEquals((String[]) expectedRecord, (String[]) record);
            } else {
                assertArrayEquals((Object[]) expectedRecord, (Object[]) record);
            }
        } else {
            assertEquals(expectedRecord, record);
        }
    }

    private static void testRowDeserialization(
            Consumer<JsonRowDeserializationSchema.Builder> config,
            RowFormatInfo rowFormatInfo,
            Row expectedRow,
            String text) {
        JsonRowDeserializationSchema.Builder builder =
                new JsonRowDeserializationSchema.Builder(rowFormatInfo);
        config.accept(builder);

        JsonRowDeserializationSchema deserializer = builder.build();

        Row row = deserializer.deserialize(text.getBytes());
        assertEquals(expectedRow, row);
    }

    @Test
    public void testJsonDeserializationWithObjectMapperParameter() throws Exception {
        String serializedJson = "{\"idcname\":\"N/A\",\"dept\":\"\"," +
                "\"alarm_msg\":\"[SKB]10.0.0.2UDP_FLOOD" +
                "[:42510pps\t:164Mbps]\"}";

        Consumer<JsonRowDeserializationSchema.Builder> config = builder -> {
        };

        RowFormatInfo rowFormatInfo =
                new RowFormatInfo(
                        new String[]{"idcname", "dept", "alarm_msg"},
                        new FormatInfo[]{
                                StringFormatInfo.INSTANCE,
                                StringFormatInfo.INSTANCE,
                                StringFormatInfo.INSTANCE
                        });

        ObjectMapper objectMapper = new ObjectMapper();
        ObjectNode root = objectMapper.createObjectNode();
        root.put("f1", "aaa");

        Configuration configuration = new Configuration();
        configuration.setBoolean("allow-unquoted-control-chars", true);
        testRowDeserializationWithObjectMapper(
                config,
                rowFormatInfo,
                Row.of("N/A", "", "[SKB]10.0.0.2UDP_FLOOD" +
                        "[:42510pps\t:164Mbps]"),
                configuration,
                serializedJson);
    }

    private static void testRowDeserializationWithObjectMapper(
            Consumer<JsonRowDeserializationSchema.Builder> config,
            RowFormatInfo rowFormatInfo,
            Row expectedRow,
            Configuration configuration,
            String text) {
        JsonRowDeserializationSchema.Builder builder =
                new JsonRowDeserializationSchema.Builder(rowFormatInfo);
        config.accept(builder);
        builder.setConfiguration(configuration);
        JsonRowDeserializationSchema deserializer = builder.build();

        Row row = deserializer.deserialize(text.getBytes());
        assertEquals(expectedRow, row);
    }
}
