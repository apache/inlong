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

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.table.data.GenericRowData;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;

import static org.apache.inlong.sort.formats.base.TextFormatOptions.CHARSET;
import static org.apache.inlong.sort.formats.base.TextFormatOptionsUtil.ISO_8601;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link JsonRowDataSerializationSchema}.
 */
public class JsonRowDataSerializationSchemaTest extends JsonRowDataSerDeTestBase {

    @Rule
    public ExpectedException exceptionRule = ExpectedException.none();

    @Test
    public void testSerialize() throws IOException {
        JsonRowDataSerializationSchema.Builder serBuilder = JsonRowDataSerializationSchema.builder(rowType);
        JsonRowDataSerializationSchema serializationSchema = serBuilder.setCharset(CHARSET.defaultValue())
                .setTimestampFormat(ISO_8601)
                .build();
        byte[] serialize = serializationSchema.serialize(testRowData);

        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode expectedNode = objectMapper.readTree(testJson);
        JsonNode actualNode = objectMapper.readTree(new String(serialize));
        assertEquals(expectedNode, actualNode);
        assertFalse(serializationSchema.skipCurrentRecord(testRowData));
    }

    @Test
    public void testSerializationErrors() throws Exception {
        exceptionRule.expect(RuntimeException.class);
        exceptionRule.expectMessage("Failed to serialize data");

        JsonRowDataSerializationSchema.Builder serBuilder = JsonRowDataSerializationSchema.builder(rowType);
        JsonRowDataSerializationSchema serializationSchema = serBuilder.setCharset(CHARSET.defaultValue())
                .setTimestampFormat(ISO_8601)
                .setIgnoreErrors(false)
                .build();
        GenericRowData rowData = new GenericRowData(1);
        rowData.setField(0, "123");
        byte[] data = serializationSchema.serialize(rowData);
    }

    @Test
    public void testSerializationIgnoreErrors() throws Exception {
        JsonRowDataSerializationSchema.Builder serBuilder = JsonRowDataSerializationSchema.builder(rowType);
        JsonRowDataSerializationSchema serializationSchema = serBuilder.setCharset(CHARSET.defaultValue())
                .setTimestampFormat(ISO_8601)
                .setIgnoreErrors(true)
                .build();
        GenericRowData rowData = new GenericRowData(1);
        rowData.setField(0, "123");
        byte[] data = serializationSchema.serialize(rowData);

        assertNull(data);
        assertTrue(serializationSchema.skipCurrentRecord(rowData));

        data = serializationSchema.serialize(testRowData);
        assertNotNull(data);
        assertFalse(serializationSchema.skipCurrentRecord(testRowData));
    }
}
