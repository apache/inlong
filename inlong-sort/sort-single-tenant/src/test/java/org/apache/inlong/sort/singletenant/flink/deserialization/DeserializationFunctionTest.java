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

package org.apache.inlong.sort.singletenant.flink.deserialization;

import org.apache.flink.types.Row;
import org.apache.inlong.common.msg.InLongMsg;
import org.apache.inlong.sort.configuration.Configuration;
import org.apache.inlong.sort.formats.common.BooleanFormatInfo;
import org.apache.inlong.sort.formats.common.FloatFormatInfo;
import org.apache.inlong.sort.formats.common.IntFormatInfo;
import org.apache.inlong.sort.formats.common.LongFormatInfo;
import org.apache.inlong.sort.formats.common.StringFormatInfo;
import org.apache.inlong.sort.protocol.BuiltInFieldInfo;
import org.apache.inlong.sort.protocol.BuiltInFieldInfo.BuiltInField;
import org.apache.inlong.sort.protocol.FieldInfo;
import org.apache.inlong.sort.protocol.deserialization.CanalDeserializationInfo;
import org.apache.inlong.sort.singletenant.flink.SerializedRecord;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertEquals;

public class DeserializationFunctionTest {

    @Test
    public void testProcessElement() throws Exception {
        InLongMsg inLongMsg = InLongMsg.newInLongMsg();
        String testData = "testData";
        inLongMsg.addMsg("m=12&iname=tid", testData.getBytes(StandardCharsets.UTF_8));
        SerializedRecord serializedRecord = new SerializedRecord(1, inLongMsg.buildArray());

        FieldInfo[] fieldInfos = {new FieldInfo("content", StringFormatInfo.INSTANCE)};
        DeserializationFunction function = new DeserializationFunction(
                DeserializationSchemaFactory.build(fieldInfos, null),
                new FieldMappingTransformer(new Configuration(), fieldInfos),
                true,
                new Configuration(),
                "",
                "");

        ListCollector<Row> collector = new ListCollector<>();
        function.processElement(serializedRecord,null, collector);
        Row row = collector.getInnerList().get(0);
        assertEquals(1, row.getArity());
        assertEquals(testData, row.getField(0));
    }

    @Test
    public void testDeserializeCanalJson() throws Exception {
        String testCanalData = "{\n"
                + "    \"data\":[\n"
                + "        {\n"
                + "            \"id\":\"101\",\n"
                + "            \"name\":\"scooter\",\n"
                + "            \"description\":\"Small 2-wheel scooter\",\n"
                + "            \"weight\":\"3.14\"\n"
                + "        },\n"
                + "        {\n"
                + "            \"id\":\"102\",\n"
                + "            \"name\":\"car battery\",\n"
                + "            \"description\":\"12V car battery\",\n"
                + "            \"weight\":\"8.1\"\n"
                + "        },\n"
                + "        {\n"
                + "            \"id\":\"103\",\n"
                + "            \"name\":\"12-pack drill bits\",\n"
                + "            \"description\":\"12-pack of drill bits with sizes ranging from #40 to #3\",\n"
                + "            \"weight\":\"0.8\"\n"
                + "        }\n"
                + "    ],\n"
                + "    \"database\":\"database\",\n"
                + "    \"es\":1589373515000,\n"
                + "    \"id\":3,\n"
                + "    \"isDdl\":false,\n"
                + "    \"mysqlType\":{\n"
                + "        \"id\":\"INTEGER\",\n"
                + "        \"name\":\"VARCHAR(255)\",\n"
                + "        \"description\":\"VARCHAR(512)\",\n"
                + "        \"weight\":\"FLOAT\"\n"
                + "    },\n"
                + "    \"old\":null,\n"
                + "    \"pkNames\":[\n"
                + "        \"id\"\n"
                + "    ],\n"
                + "    \"sql\":\"\",\n"
                + "    \"sqlType\":{\n"
                + "        \"id\":4,\n"
                + "        \"name\":12,\n"
                + "        \"description\":12,\n"
                + "        \"weight\":7\n"
                + "    },\n"
                + "    \"table\":\"table\",\n"
                + "    \"ts\":1589373515477,\n"
                + "    \"type\":\"INSERT\"\n"
                + "}";

        InLongMsg inLongMsg = InLongMsg.newInLongMsg(true);
        String attrs = "m=0"
                + "&dt=" + System.currentTimeMillis()
                + "&iname=" + "tid";
        inLongMsg.addMsg(attrs, testCanalData.getBytes());
        SerializedRecord serializedRecord = new SerializedRecord(1, inLongMsg.buildArray());

        FieldInfo[] fieldInfos = new FieldInfo[]{
                new FieldInfo("id", IntFormatInfo.INSTANCE),
                new FieldInfo("name", StringFormatInfo.INSTANCE),
                new FieldInfo("description", StringFormatInfo.INSTANCE),
                new FieldInfo("weight", FloatFormatInfo.INSTANCE),
                new BuiltInFieldInfo("database", StringFormatInfo.INSTANCE, BuiltInField.MYSQL_METADATA_DATABASE),
                new BuiltInFieldInfo("table", StringFormatInfo.INSTANCE, BuiltInField.MYSQL_METADATA_TABLE),
                new BuiltInFieldInfo(
                        "event-timestamp", LongFormatInfo.INSTANCE, BuiltInField.MYSQL_METADATA_EVENT_TIME),
                new BuiltInFieldInfo("is-ddl", BooleanFormatInfo.INSTANCE, BuiltInField.MYSQL_METADATA_IS_DDL),
                new BuiltInFieldInfo("is-ddl", BooleanFormatInfo.INSTANCE, BuiltInField.MYSQL_METADATA_EVENT_TYPE)
        };

        DeserializationFunction function = new DeserializationFunction(
                DeserializationSchemaFactory.build(
                        fieldInfos,
                        new CanalDeserializationInfo(null, null, false, "ISO_8601", false)),
                new FieldMappingTransformer(new Configuration(), fieldInfos),
                false,
                new Configuration(),
                "",
                "");

        ListCollector<Row> collector = new ListCollector<>();
        function.processElement(serializedRecord,null, collector);
        Row row = collector.getInnerList().get(0);

        Row expected = Row.of(
                101, "scooter", "Small 2-wheel scooter", 3.14f, "database", "table", 1589373515000L, false, "INSERT");
        assertEquals(expected, row);
    }

}
