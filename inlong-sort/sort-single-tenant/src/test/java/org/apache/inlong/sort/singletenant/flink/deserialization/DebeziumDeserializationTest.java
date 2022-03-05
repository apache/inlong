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

package org.apache.inlong.sort.singletenant.flink.deserialization;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.types.Row;
import org.apache.inlong.sort.formats.common.BinaryFormatInfo;
import org.apache.inlong.sort.formats.common.DateFormatInfo;
import org.apache.inlong.sort.formats.common.IntFormatInfo;
import org.apache.inlong.sort.formats.common.LongFormatInfo;
import org.apache.inlong.sort.formats.common.MapFormatInfo;
import org.apache.inlong.sort.formats.common.StringFormatInfo;
import org.apache.inlong.sort.formats.common.TimeFormatInfo;
import org.apache.inlong.sort.formats.common.TimestampFormatInfo;
import org.apache.inlong.sort.formats.json.MysqlBinLogData;
import org.apache.inlong.sort.protocol.BuiltInFieldInfo;
import org.apache.inlong.sort.protocol.BuiltInFieldInfo.BuiltInField;
import org.apache.inlong.sort.protocol.FieldInfo;
import org.apache.inlong.sort.protocol.deserialization.DebeziumDeserializationInfo;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class DebeziumDeserializationTest {

    private final FieldInfo[] fieldInfos = new FieldInfo[]{
            new FieldInfo("id", LongFormatInfo.INSTANCE),
            new FieldInfo("name", StringFormatInfo.INSTANCE),
            new FieldInfo("bytes", BinaryFormatInfo.INSTANCE),
            new FieldInfo("date", new DateFormatInfo("yyyy-MM-dd")),
            new FieldInfo("time", new TimeFormatInfo("HH:mm:ss")),
            new FieldInfo("timestamp", new TimestampFormatInfo("yyyy-MM-dd HH:mm:ss")),
            new FieldInfo("map",
                    new MapFormatInfo(StringFormatInfo.INSTANCE, LongFormatInfo.INSTANCE)),
            new FieldInfo("map2map", new MapFormatInfo(StringFormatInfo.INSTANCE,
                    new MapFormatInfo(StringFormatInfo.INSTANCE, IntFormatInfo.INSTANCE))),
            new BuiltInFieldInfo("db", StringFormatInfo.INSTANCE, BuiltInField.MYSQL_METADATA_DATABASE),
            new BuiltInFieldInfo("table", StringFormatInfo.INSTANCE, BuiltInField.MYSQL_METADATA_TABLE),
            new BuiltInFieldInfo("es", LongFormatInfo.INSTANCE, BuiltInField.MYSQL_METADATA_EVENT_TIME)
    };

    private Row generateTestRow() {
        Row testRow = new Row(9);
        testRow.setField(0, new HashMap<String, String>() {
            {
                put(MysqlBinLogData.MYSQL_METADATA_IS_DDL, "false");
                put(MysqlBinLogData.MYSQL_METADATA_DATABASE, "test");
                put(MysqlBinLogData.MYSQL_METADATA_TABLE, "test");
                put(MysqlBinLogData.MYSQL_METADATA_EVENT_TIME, "1644896917208");
            }
        });
        testRow.setField(1, 1238123899121L);
        testRow.setField(2, "testName");

        byte[] bytes = new byte[]{1, 2, 3, 4, 5, 6};
        testRow.setField(3, bytes);

        testRow.setField(4, Date.valueOf("1990-10-14"));
        testRow.setField(5, Time.valueOf("12:12:43"));
        testRow.setField(6, Timestamp.valueOf("1990-10-14 12:12:43"));

        Map<String, Long> map = new HashMap<>();
        map.put("flink", 123L);
        testRow.setField(7, map);

        Map<String, Map<String, Integer>> nestedMap = new HashMap<>();
        Map<String, Integer> innerMap = new HashMap<>();
        innerMap.put("key", 234);
        nestedMap.put("inner_map", innerMap);
        testRow.setField(8, nestedMap);

        return testRow;
    }

    @Test
    public void testDebeziumDeserializationSchema() throws IOException, ClassNotFoundException {
        String testString = "{\n"
                                    + "    \"before\":null,\n"
                                    + "    \"after\":{\n"
                                    + "        \"id\":1238123899121,\n"
                                    + "        \"name\":\"testName\",\n"
                                    + "        \"bytes\":\"AQIDBAUG\",\n"
                                    + "        \"date\":\"1990-10-14\",\n"
                                    + "        \"time\":\"12:12:43\",\n"
                                    + "        \"timestamp\":\"1990-10-14 12:12:43\",\n"
                                    + "        \"map\":{\n"
                                    + "            \"flink\":123\n"
                                    + "        },\n"
                                    + "        \"map2map\":{\n"
                                    + "            \"inner_map\":{\n"
                                    + "                \"key\":234\n"
                                    + "            }\n"
                                    + "        }\n"
                                    + "    },\n"
                                    + "    \"source\":{\n"
                                    + "        \"version\":\"1.4.2.Final\",\n"
                                    + "        \"connector\":\"mysql\",\n"
                                    + "        \"name\":\"my_server_01\",\n"
                                    + "        \"ts_ms\":1644896917000,\n"
                                    + "        \"snapshot\":\"false\",\n"
                                    + "        \"db\":\"test\",\n"
                                    + "        \"table\":\"test\",\n"
                                    + "        \"server_id\":1,\n"
                                    + "        \"gtid\":null,\n"
                                    + "        \"file\":\"mysql-bin.000067\",\n"
                                    + "        \"pos\":944,\n"
                                    + "        \"row\":0,\n"
                                    + "        \"thread\":13,\n"
                                    + "        \"query\":null\n"
                                    + "    },\n"
                                    + "    \"op\":\"c\",\n"
                                    + "    \"ts_ms\":1644896917208,\n"
                                    + "    \"transaction\":null\n"
                                    + "}";

        byte[] testBytes = testString.getBytes(StandardCharsets.UTF_8);
        DeserializationSchema<Row> schemaWithoutFilter = DeserializationSchemaFactory.build(
                fieldInfos,
                new DebeziumDeserializationInfo(false, "ISO_8601")
        );
        ListCollector<Row> collector = new ListCollector<>();
        schemaWithoutFilter.deserialize(testBytes, collector);
        assertEquals(generateTestRow(), collector.getInnerList().get(0));
    }

}
