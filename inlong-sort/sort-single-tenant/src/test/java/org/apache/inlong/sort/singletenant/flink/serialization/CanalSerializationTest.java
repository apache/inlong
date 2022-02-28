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

package org.apache.inlong.sort.singletenant.flink.serialization;

import static org.junit.Assert.assertEquals;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema.InitializationContext;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.types.Row;
import org.apache.flink.util.UserCodeClassLoader;
import org.apache.inlong.sort.formats.common.BooleanFormatInfo;
import org.apache.inlong.sort.formats.common.IntFormatInfo;
import org.apache.inlong.sort.formats.common.LongFormatInfo;
import org.apache.inlong.sort.formats.common.StringFormatInfo;
import org.apache.inlong.sort.protocol.BuiltInFieldInfo;
import org.apache.inlong.sort.protocol.BuiltInFieldInfo.BuiltInField;
import org.apache.inlong.sort.protocol.FieldInfo;
import org.apache.inlong.sort.protocol.serialization.CanalSerializationInfo;
import org.junit.Test;

public class CanalSerializationTest {

    private final FieldInfo[] fieldInfos = new FieldInfo[]{
            new FieldInfo("name", StringFormatInfo.INSTANCE),
            new FieldInfo("age", IntFormatInfo.INSTANCE),
            new BuiltInFieldInfo("db", StringFormatInfo.INSTANCE, BuiltInField.MYSQL_METADATA_DATABASE),
            new BuiltInFieldInfo("table", StringFormatInfo.INSTANCE, BuiltInField.MYSQL_METADATA_TABLE),
            new BuiltInFieldInfo("es", LongFormatInfo.INSTANCE, BuiltInField.MYSQL_METADATA_EVENT_TIME),
            new BuiltInFieldInfo("isDdl", BooleanFormatInfo.INSTANCE, BuiltInField.MYSQL_METADATA_IS_DDL),
            new BuiltInFieldInfo("type", StringFormatInfo.INSTANCE, BuiltInField.MYSQL_METADATA_EVENT_TYPE)
    };

    @Test
    public void test() throws Exception {
        SerializationSchema<Row> canalJsonSerializationSchema = SerializationSchemaFactory.build(
                fieldInfos,
                new CanalSerializationInfo("Sql", "Literal", null, false)
        );
        canalJsonSerializationSchema.open(new InitializationContext() {
            @Override
            public MetricGroup getMetricGroup() {
                return null;
            }

            @Override
            public UserCodeClassLoader getUserCodeClassLoader() {
                return null;
            }
        });

        Row row = Row.of(
                "name",
                29,
                "database",
                "table",
                123L,
                false,
                "INSERT");
        String result = new String(canalJsonSerializationSchema.serialize(row));

        String expectedResult =
                "{\"data\":[{\"name\":\"name\",\"age\":29}],"
                        + "\"type\":\"INSERT\",\"database\":\"database\","
                        + "\"table\":\"table\",\"es\":123,\"isDdl\":false}";
        assertEquals(expectedResult, result);
    }
}
