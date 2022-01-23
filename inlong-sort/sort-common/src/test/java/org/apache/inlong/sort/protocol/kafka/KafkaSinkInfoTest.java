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

package org.apache.inlong.sort.protocol.kafka;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.inlong.sort.formats.common.StringFormatInfo;
import org.apache.inlong.sort.protocol.FieldInfo;
import org.apache.inlong.sort.protocol.ProtocolBaseTest;
import org.apache.inlong.sort.protocol.serialization.JsonSerializationInfo;
import org.apache.inlong.sort.protocol.sink.KafkaSinkInfo;

public class KafkaSinkInfoTest extends ProtocolBaseTest {

    @Override
    public void init() {
        expectedObject = new KafkaSinkInfo(
                new FieldInfo[]{new FieldInfo("field1", new StringFormatInfo())},
                "testAddress",
                "testTopic",
                new JsonSerializationInfo()
        );
        try {
            System.out.println(new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(expectedObject));
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }

        expectedJson = "{\n"
                + "  \"type\" : \"kafka\",\n"
                + "  \"fields\" : [ {\n"
                + "    \"name\" : \"field1\",\n"
                + "    \"format_info\" : {\n"
                + "      \"type\" : \"string\"\n"
                + "    }\n"
                + "  } ],\n"
                + "  \"address\" : \"testAddress\",\n"
                + "  \"topic\" : \"testTopic\",\n"
                + "  \"serialization_info\" : {\n"
                + "    \"type\" : \"json\",\n"
                + "    \"fields\" : [ {\n"
                + "      \"name\" : \"field11\",\n"
                + "      \"format_info\" : {\n"
                + "        \"type\" : \"string\"\n"
                + "      }\n"
                + "    } ]\n"
                + "  }\n"
                + "}";

        equalObj1 = expectedObject;
        equalObj2 = new KafkaSinkInfo(
                new FieldInfo[]{new FieldInfo("field1", new StringFormatInfo())},
                "testAddress",
                "testTopic",
                new JsonSerializationInfo()
        );
        unequalObj = new KafkaSinkInfo(
                new FieldInfo[]{new FieldInfo("field1", new StringFormatInfo())},
                "testAddress",
                "testTopic",
                new JsonSerializationInfo()
        );
    }
}
