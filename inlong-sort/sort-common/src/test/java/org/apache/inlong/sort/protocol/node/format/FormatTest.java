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

package org.apache.inlong.sort.protocol.node.format;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;

import static org.junit.Assert.assertEquals;


public class FormatTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    public void testJsonFormat() throws JsonProcessingException {
        JsonFormat jsonFormat = new JsonFormat();
        String jsonStr = objectMapper.writeValueAsString(jsonFormat);
        Format format = objectMapper.readValue(jsonStr, Format.class);
        assertEquals(format, jsonFormat);
    }

    @Test
    public void testCanalJsonFormat() throws JsonProcessingException {
        CanalJsonFormat canalJsonFormat = new CanalJsonFormat();
        String jsonStr = objectMapper.writeValueAsString(canalJsonFormat);
        Format format = objectMapper.readValue(jsonStr, Format.class);
        assertEquals(format, canalJsonFormat);
    }

    @Test
    public void testDebeziumJsonFormat() throws JsonProcessingException {
        DebeziumJsonFormat debeziumJsonFormat = new DebeziumJsonFormat();
        String jsonStr = objectMapper.writeValueAsString(debeziumJsonFormat);
        Format format = objectMapper.readValue(jsonStr, Format.class);
        assertEquals(format, debeziumJsonFormat);
    }

    @Test
    public void testAvroFormat() throws JsonProcessingException {
        AvroFormat avroFormat = new AvroFormat();
        String jsonStr = objectMapper.writeValueAsString(avroFormat);
        Format format = objectMapper.readValue(jsonStr, Format.class);
        assertEquals(format, avroFormat);
    }

}
