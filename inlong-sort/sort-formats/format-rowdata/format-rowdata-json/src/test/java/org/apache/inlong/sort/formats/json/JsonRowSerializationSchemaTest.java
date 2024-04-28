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

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.types.Row;
import org.junit.Test;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Collections;
import java.util.function.Consumer;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link JsonRowSerializationSchema}.
 */
public class JsonRowSerializationSchemaTest {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

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
        Consumer<JsonRowSerializationSchema.Builder> config = builder -> {
        };

        JsonNodeFactory nodeFactory = OBJECT_MAPPER.getNodeFactory();

        testFieldSerialization(config, StringFormatInfo.INSTANCE, "hello", nodeFactory.textNode("hello"));
        testFieldSerialization(config, BooleanFormatInfo.INSTANCE, true, nodeFactory.booleanNode(true));
        testFieldSerialization(config, ByteFormatInfo.INSTANCE, (byte) 124, nodeFactory.numberNode((byte) 124));
        testFieldSerialization(config, ShortFormatInfo.INSTANCE, (short) 10000, nodeFactory.numberNode((short) 10000));
        testFieldSerialization(config, IntFormatInfo.INSTANCE, 1234567, nodeFactory.numberNode(1234567));
        testFieldSerialization(config, LongFormatInfo.INSTANCE, 12345678910L, nodeFactory.numberNode(12345678910L));
        testFieldSerialization(config, FloatFormatInfo.INSTANCE, 0.33333334f, nodeFactory.numberNode(0.33333334f));
        testFieldSerialization(config, DoubleFormatInfo.INSTANCE, 0.33333333332,
                nodeFactory.numberNode(0.33333333332));
        testFieldSerialization(
                config,
                DecimalFormatInfo.INSTANCE,
                new BigDecimal("1234.0000000000000000000000001"),
                nodeFactory.numberNode(new BigDecimal("1234.0000000000000000000000001")));
        testFieldSerialization(
                config,
                new DateFormatInfo("dd/MM/yyyy"),
                Date.valueOf("2020-03-22"),
                nodeFactory.textNode("22/03/2020"));
        testFieldSerialization(
                config,
                new TimeFormatInfo("ss/mm/hh"),
                Time.valueOf("11:12:13"),
                nodeFactory.textNode("13/12/11"));
        testFieldSerialization(
                config,
                new TimestampFormatInfo("dd/MM/yyyy hh:mm:ss"),
                Timestamp.valueOf("2020-03-22 11:12:13"),
                nodeFactory.textNode("22/03/2020 11:12:13"));
    }

    @Test
    public void testArraySerialization() throws Exception {
        Consumer<JsonRowSerializationSchema.Builder> config = builder -> {
        };

        ArrayFormatInfo arrayFormatInfo = new ArrayFormatInfo(StringFormatInfo.INSTANCE);

        ObjectMapper objectMapper = new ObjectMapper();
        ArrayNode root = objectMapper.createArrayNode();
        root.add("aaa").add("bbb").add("ccc");

        testFieldSerialization(config, arrayFormatInfo, new String[]{"aaa", "bbb", "ccc"}, root);
    }

    @Test
    public void testMapSerialization() throws Exception {
        Consumer<JsonRowSerializationSchema.Builder> config = builder -> {
        };

        MapFormatInfo mapFormatInfo = new MapFormatInfo(IntFormatInfo.INSTANCE, StringFormatInfo.INSTANCE);

        ObjectMapper objectMapper = new ObjectMapper();
        ObjectNode root = objectMapper.createObjectNode();
        root.put("123", "aaa");

        testFieldSerialization(config, mapFormatInfo, Collections.singletonMap(123, "aaa"), root);
    }

    @Test
    public void testRowSerialization() throws Exception {
        Consumer<JsonRowSerializationSchema.Builder> config = builder -> {
        };

        RowFormatInfo rowFormatInfo = new RowFormatInfo(
                new String[]{"in"},
                new FormatInfo[]{StringFormatInfo.INSTANCE});

        ObjectMapper objectMapper = new ObjectMapper();
        ObjectNode root = objectMapper.createObjectNode();
        root.put("in", "aaa");

        testFieldSerialization(config, rowFormatInfo, Row.of("aaa"), root);
    }

    @Test
    public void testMoreFields() throws JsonProcessingException {
        Consumer<JsonRowSerializationSchema.Builder> config = builder -> {
        };

        ObjectMapper objectMapper = new ObjectMapper();
        ObjectNode root = objectMapper.createObjectNode();
        root.put("f1", 10);
        root.put("f2", "field1");
        root.put("f3", "field2");
        root.put("f4", "field3");

        testRowSerialization(
                config,
                TEST_ROW_INFO,
                Row.of(10, "field1", "field2", "field3", "field4"),
                root);
    }

    @Test
    public void testLessFields() throws JsonProcessingException {
        Consumer<JsonRowSerializationSchema.Builder> config = builder -> {
        };

        ObjectMapper objectMapper = new ObjectMapper();
        ObjectNode root = objectMapper.createObjectNode();
        root.put("f1", 10);
        root.put("f2", "field1");
        root.put("f3", "field2");

        testRowSerialization(config, TEST_ROW_INFO, Row.of(10, "field1", "field2"), root);
    }

    private static <T> void testFieldSerialization(
            Consumer<JsonRowSerializationSchema.Builder> config,
            FormatInfo fieldFormatInfo,
            T record,
            JsonNode expectedNode) throws Exception {
        RowFormatInfo rowFormatInfo =
                new RowFormatInfo(
                        new String[]{"f"},
                        new FormatInfo[]{fieldFormatInfo});

        ObjectNode rowNode = OBJECT_MAPPER.createObjectNode();
        rowNode.put("f", expectedNode);

        testRowSerialization(config, rowFormatInfo, Row.of(record), rowNode);
    }

    private static void testRowSerialization(
            Consumer<JsonRowSerializationSchema.Builder> config,
            RowFormatInfo rowFormatInfo,
            Row row,
            JsonNode expectedNode) throws JsonProcessingException {
        JsonRowSerializationSchema.Builder builder = new JsonRowSerializationSchema.Builder(rowFormatInfo);
        config.accept(builder);

        JsonRowSerializationSchema serializer = builder.build();
        String json = new String(serializer.serialize(row));

        String expectedJson = OBJECT_MAPPER.writeValueAsString(expectedNode);

        assertEquals(expectedJson, json);
    }
}
