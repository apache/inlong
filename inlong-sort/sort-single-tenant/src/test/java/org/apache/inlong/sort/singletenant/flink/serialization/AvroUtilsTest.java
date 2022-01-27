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

package org.apache.inlong.sort.singletenant.flink.serialization;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.inlong.sort.formats.common.ArrayFormatInfo;
import org.apache.inlong.sort.formats.common.ByteFormatInfo;
import org.apache.inlong.sort.formats.common.FormatInfo;
import org.apache.inlong.sort.formats.common.IntFormatInfo;
import org.apache.inlong.sort.formats.common.MapFormatInfo;
import org.apache.inlong.sort.formats.common.RowFormatInfo;
import org.apache.inlong.sort.formats.common.ShortFormatInfo;
import org.apache.inlong.sort.formats.common.StringFormatInfo;
import org.apache.inlong.sort.protocol.FieldInfo;
import org.junit.Test;

import java.io.IOException;

import static org.apache.inlong.sort.singletenant.flink.serialization.AvroUtils.buildAvroRecordSchemaInJson;
import static org.junit.Assert.assertEquals;

public class AvroUtilsTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    public void testNormalFields() throws IOException {
        FieldInfo[] testFieldInfos = new FieldInfo[]{
                new FieldInfo("f1", new StringFormatInfo()),
                new FieldInfo("f2", new ByteFormatInfo())
        };

        JsonNode expectedJsonNode = objectMapper.readTree("{\n"
                + "    \"type\":\"record\",\n"
                + "    \"name\":\"record\",\n"
                + "    \"fields\":[\n"
                + "        {\n"
                + "            \"name\":\"f1\",\n"
                + "            \"type\":[\n"
                + "                \"null\",\n"
                + "                \"string\"\n"
                + "            ],\n"
                + "            \"default\":null\n"
                + "        },\n"
                + "        {\n"
                + "            \"name\":\"f2\",\n"
                + "            \"type\":[\n"
                + "                \"null\",\n"
                + "                \"int\"\n"
                + "            ],\n"
                + "            \"default\":null\n"
                + "        }\n"
                + "    ]\n"
                + "}");

        String actualJson = buildAvroRecordSchemaInJson(testFieldInfos);
        JsonNode actualJsonNode = objectMapper.readTree(actualJson);

        assertEquals(expectedJsonNode, actualJsonNode);
    }

    @Test
    public void testRecursiveFields() throws IOException {
        FieldInfo[] testFieldInfos = new FieldInfo[]{
                new FieldInfo("f1", new ArrayFormatInfo(
                        new MapFormatInfo(
                                new StringFormatInfo(),
                                new ArrayFormatInfo(new ArrayFormatInfo(new ShortFormatInfo()))
                        )
                )),
                new FieldInfo("f2", new MapFormatInfo(
                        new StringFormatInfo(),
                        new MapFormatInfo(
                                new StringFormatInfo(),
                                new RowFormatInfo(
                                        new String[]{"f21", "f22"},
                                        new FormatInfo[]{new IntFormatInfo(), new ArrayFormatInfo(new ByteFormatInfo())}
                                )
                        )
                )),
                new FieldInfo("f3", new RowFormatInfo(
                        new String[]{"f31", "f32"},
                        new FormatInfo[]{
                                new ArrayFormatInfo(new StringFormatInfo()),
                                new RowFormatInfo(
                                        new String[]{"f321", "f322"},
                                        new FormatInfo[]{
                                                new ArrayFormatInfo(new IntFormatInfo()),
                                                new MapFormatInfo(
                                                        new StringFormatInfo(),
                                                        new ArrayFormatInfo(new ByteFormatInfo())
                                                )
                                        }
                                )
                        }
                ))
        };

        JsonNode expectedJsonNode = objectMapper.readTree("{\n"
                + "    \"type\":\"record\",\n"
                + "    \"name\":\"record\",\n"
                + "    \"fields\":[\n"
                + "        {\n"
                + "            \"name\":\"f1\",\n"
                + "            \"type\":[\n"
                + "                \"null\",\n"
                + "                {\n"
                + "                    \"type\":\"array\",\n"
                + "                    \"items\":[\n"
                + "                        \"null\",\n"
                + "                        {\n"
                + "                            \"type\":\"map\",\n"
                + "                            \"values\":[\n"
                + "                                \"null\",\n"
                + "                                {\n"
                + "                                    \"type\":\"array\",\n"
                + "                                    \"items\":[\n"
                + "                                        \"null\",\n"
                + "                                        {\n"
                + "                                            \"type\":\"array\",\n"
                + "                                            \"items\":[\n"
                + "                                                \"null\",\n"
                + "                                                \"int\"\n"
                + "                                            ]\n"
                + "                                        }\n"
                + "                                    ]\n"
                + "                                }\n"
                + "                            ]\n"
                + "                        }\n"
                + "                    ]\n"
                + "                }\n"
                + "            ],\n"
                + "            \"default\":null\n"
                + "        },\n"
                + "        {\n"
                + "            \"name\":\"f2\",\n"
                + "            \"type\":[\n"
                + "                \"null\",\n"
                + "                {\n"
                + "                    \"type\":\"map\",\n"
                + "                    \"values\":[\n"
                + "                        \"null\",\n"
                + "                        {\n"
                + "                            \"type\":\"map\",\n"
                + "                            \"values\":[\n"
                + "                                \"null\",\n"
                + "                                {\n"
                + "                                    \"type\":\"record\",\n"
                + "                                    \"name\":\"record_f2\",\n"
                + "                                    \"fields\":[\n"
                + "                                        {\n"
                + "                                            \"name\":\"f21\",\n"
                + "                                            \"type\":[\n"
                + "                                                \"null\",\n"
                + "                                                \"int\"\n"
                + "                                            ],\n"
                + "                                            \"default\":null\n"
                + "                                        },\n"
                + "                                        {\n"
                + "                                            \"name\":\"f22\",\n"
                + "                                            \"type\":[\n"
                + "                                                \"null\",\n"
                + "                                                {\n"
                + "                                                    \"type\":\"array\",\n"
                + "                                                    \"items\":[\n"
                + "                                                        \"null\",\n"
                + "                                                        \"int\"\n"
                + "                                                    ]\n"
                + "                                                }\n"
                + "                                            ],\n"
                + "                                            \"default\":null\n"
                + "                                        }\n"
                + "                                    ]\n"
                + "                                }\n"
                + "                            ]\n"
                + "                        }\n"
                + "                    ]\n"
                + "                }\n"
                + "            ],\n"
                + "            \"default\":null\n"
                + "        },\n"
                + "        {\n"
                + "            \"name\":\"f3\",\n"
                + "            \"type\":[\n"
                + "                \"null\",\n"
                + "                {\n"
                + "                    \"type\":\"record\",\n"
                + "                    \"name\":\"record_f3\",\n"
                + "                    \"fields\":[\n"
                + "                        {\n"
                + "                            \"name\":\"f31\",\n"
                + "                            \"type\":[\n"
                + "                                \"null\",\n"
                + "                                {\n"
                + "                                    \"type\":\"array\",\n"
                + "                                    \"items\":[\n"
                + "                                        \"null\",\n"
                + "                                        \"string\"\n"
                + "                                    ]\n"
                + "                                }\n"
                + "                            ],\n"
                + "                            \"default\":null\n"
                + "                        },\n"
                + "                        {\n"
                + "                            \"name\":\"f32\",\n"
                + "                            \"type\":[\n"
                + "                                \"null\",\n"
                + "                                {\n"
                + "                                    \"type\":\"record\",\n"
                + "                                    \"name\":\"record_f3_f32\",\n"
                + "                                    \"fields\":[\n"
                + "                                        {\n"
                + "                                            \"name\":\"f321\",\n"
                + "                                            \"type\":[\n"
                + "                                                \"null\",\n"
                + "                                                {\n"
                + "                                                    \"type\":\"array\",\n"
                + "                                                    \"items\":[\n"
                + "                                                        \"null\",\n"
                + "                                                        \"int\"\n"
                + "                                                    ]\n"
                + "                                                }\n"
                + "                                            ],\n"
                + "                                            \"default\":null\n"
                + "                                        },\n"
                + "                                        {\n"
                + "                                            \"name\":\"f322\",\n"
                + "                                            \"type\":[\n"
                + "                                                \"null\",\n"
                + "                                                {\n"
                + "                                                    \"type\":\"map\",\n"
                + "                                                    \"values\":[\n"
                + "                                                        \"null\",\n"
                + "                                                        {\n"
                + "                                                            \"type\":\"array\",\n"
                + "                                                            \"items\":[\n"
                + "                                                                \"null\",\n"
                + "                                                                \"int\"\n"
                + "                                                            ]\n"
                + "                                                        }\n"
                + "                                                    ]\n"
                + "                                                }\n"
                + "                                            ],\n"
                + "                                            \"default\":null\n"
                + "                                        }\n"
                + "                                    ]\n"
                + "                                }\n"
                + "                            ],\n"
                + "                            \"default\":null\n"
                + "                        }\n"
                + "                    ]\n"
                + "                }\n"
                + "            ],\n"
                + "            \"default\":null\n"
                + "        }\n"
                + "    ]\n"
                + "}");

        String actualJson = buildAvroRecordSchemaInJson(testFieldInfos);
        JsonNode actualJsonNode = objectMapper.readTree(actualJson);

        assertEquals(expectedJsonNode, actualJsonNode);
    }

}
