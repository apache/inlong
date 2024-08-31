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

package org.apache.inlong.sort.formats.inlongmsgkv;

import org.apache.inlong.common.pojo.sort.dataflow.field.format.DateFormatInfo;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.FormatInfo;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.IntFormatInfo;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.RowFormatInfo;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.StringFormatInfo;
import org.apache.inlong.sort.formats.base.TableFormatDeserializer;
import org.apache.inlong.sort.formats.base.TableFormatForRowUtils;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.apache.inlong.sort.formats.inlongmsg.row.InLongMsgUtils.DEFAULT_ATTRIBUTES_FIELD_NAME;
import static org.apache.inlong.sort.formats.inlongmsg.row.InLongMsgUtils.DEFAULT_TIME_FIELD_NAME;
import static org.apache.inlong.sort.formats.inlongmsgkv.InLongMsgKvUtils.DEFAULT_INLONGMSGKV_CHARSET;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Tests for {@link InLongMsgKvFormatFactory}.
 */
public class InLongMsgKvFormatFactoryTest {

    private static final TypeInformation<Row> SCHEMA =
            Types.ROW(
                    new String[]{"time", "attributes", "student_name", "score", "date"},
                    new TypeInformation[]{
                            Types.SQL_TIMESTAMP(),
                            Types.MAP(Types.STRING(), Types.STRING()),
                            Types.STRING(),
                            Types.INT(),
                            Types.SQL_DATE()
                    });

    private static final RowFormatInfo TEST_FORMAT_SCHEMA =
            new RowFormatInfo(
                    new String[]{"student_name", "score", "date"},
                    new FormatInfo[]{
                            StringFormatInfo.INSTANCE,
                            IntFormatInfo.INSTANCE,
                            new DateFormatInfo("yyyy-MM-dd")
                    });

    @Test
    public void testCreateTableFormatDeserializer() throws Exception {
        final Map<String, String> properties =
                new InLongMsgKv()
                        .schema(TEST_FORMAT_SCHEMA)
                        .entryDelimiter('&')
                        .kvDelimiter('=')
                        .escapeCharacter('\\')
                        .quoteCharacter('\"')
                        .nullLiteral("null")
                        .toProperties();
        assertNotNull(properties);

        final InLongMsgKvFormatDeserializer expectedDeser =
                new InLongMsgKvFormatDeserializer(
                        TEST_FORMAT_SCHEMA,
                        DEFAULT_TIME_FIELD_NAME,
                        DEFAULT_ATTRIBUTES_FIELD_NAME,
                        DEFAULT_INLONGMSGKV_CHARSET,
                        '&',
                        '=',
                        null,
                        '\\',
                        '\"',
                        "null",
                        false);

        final TableFormatDeserializer actualDeser =
                TableFormatForRowUtils.getTableFormatDeserializer(
                        properties,
                        getClass().getClassLoader());

        assertEquals(expectedDeser, actualDeser);
    }

    @Test(expected = Exception.class)
    public void testCreateTableFormatDeserializerWithDerivation() {
        final Map<String, String> properties = new HashMap<>();
        properties.putAll(
                new Schema()
                        .schema(TableSchema.fromTypeInfo(SCHEMA))
                        .toProperties());
        properties.putAll(new InLongMsgKv().deriveSchema().toProperties());

        final InLongMsgKvFormatDeserializer expectedDeser =
                new InLongMsgKvFormatDeserializer.Builder(TEST_FORMAT_SCHEMA).build();

        final TableFormatDeserializer actualDeser =
                TableFormatForRowUtils.getTableFormatDeserializer(
                        properties,
                        getClass().getClassLoader());

        assertEquals(expectedDeser, actualDeser);
    }
}
