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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.EnvironmentSettings.Builder;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.util.Objects;

import static org.apache.inlong.sort.formats.base.TextFormatOptions.CHARSET;
import static org.apache.inlong.sort.formats.base.TextFormatOptionsUtil.ISO_8601;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link JsonRowDataDeserializationSchema}.
 */
public class JsonRowDataDeserializationSchemaTest extends JsonRowDataSerDeTestBase {

    @Rule
    public ExpectedException exceptionRule = ExpectedException.none();

    @Test
    public void test() throws IOException {
        TypeInformation<RowData> resultTypeInfo = InternalTypeInfo.of(rowType);

        JsonRowDataDeserializationSchema.Builder deBuilder =
                JsonRowDataDeserializationSchema.builder(rowType, resultTypeInfo);
        JsonRowDataDeserializationSchema deserializationSchema =
                deBuilder.setCharset(CHARSET.defaultValue())
                        .setTimestampFormat(ISO_8601)
                        .setIgnoreParseErrors(false)
                        .setFailOnMissingField(false)
                        .build();
        RowData rowData = deserializationSchema.deserialize(testJson.getBytes(CHARSET.defaultValue()));
        assertTrue(rowData instanceof GenericRowData);

        assertEquals(testRowData, rowData);
        assertNotNull(rowData);
    }

    // @Test
    public void testAfterDeserialization() {
        String sourcePath =
                Objects.requireNonNull(this.getClass().getClassLoader().getResource("json_source.txt")).getPath();
        String sourceTableName = "my_source_table";
        String sourceTableDdl = "CREATE TABLE " + sourceTableName + " (\n"
                + "    emp_id INT,\n"
                + "    name VARCHAR,\n"
                + "    dept_id INT\n"
                + ") WITH ( \n"
                + "    'connector' = 'filesystem',\n"
                + "    'path' = '" + sourcePath + "',\n"
                + "    'format' = 'inlong-json'\n"
                + ")";

        Builder envSettingsBuilder = new Builder();
        EnvironmentSettings environmentSettings = envSettingsBuilder.inStreamingMode().build();
        TableEnvironment tEnv = TableEnvironment.create(environmentSettings);

        tEnv.executeSql(sourceTableDdl);

        Table table = tEnv.sqlQuery("SELECT * FROM " + sourceTableName + " WHERE name='Anna'");
        table.execute().print();
    }

    @Test
    public void testDeserializationErrors() throws Exception {
        exceptionRule.expect(IOException.class);
        exceptionRule.expectMessage("Failed to deserialize data");
        byte[] data = "{\"bool\"}".getBytes(CHARSET.defaultValue());

        TypeInformation<RowData> resultTypeInfo = InternalTypeInfo.of(rowType);

        JsonRowDataDeserializationSchema.Builder deBuilder =
                JsonRowDataDeserializationSchema.builder(rowType, resultTypeInfo);
        JsonRowDataDeserializationSchema deserializationSchema =
                deBuilder.setCharset(CHARSET.defaultValue())
                        .setTimestampFormat(ISO_8601)
                        .setIgnoreParseErrors(false)
                        .setFailOnMissingField(false)
                        .build();
        RowData rowData = deserializationSchema.deserialize(data);
    }

    @Test
    public void testDeserializationIgnoreErrors() throws Exception {
        byte[] data = "{\"bool\"}".getBytes(CHARSET.defaultValue());

        TypeInformation<RowData> resultTypeInfo = InternalTypeInfo.of(rowType);

        JsonRowDataDeserializationSchema.Builder deBuilder =
                JsonRowDataDeserializationSchema.builder(rowType, resultTypeInfo);
        JsonRowDataDeserializationSchema deserializationSchema =
                deBuilder.setCharset(CHARSET.defaultValue())
                        .setTimestampFormat(ISO_8601)
                        .setIgnoreParseErrors(true)
                        .setFailOnMissingField(false)
                        .build();
        RowData rowData = deserializationSchema.deserialize(data);
        assertNull(rowData);

        rowData = deserializationSchema.deserialize((testJson.getBytes(CHARSET.defaultValue())));
        assertNotNull(rowData);
    }
}
