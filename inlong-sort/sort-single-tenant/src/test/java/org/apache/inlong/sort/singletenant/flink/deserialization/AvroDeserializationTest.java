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
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.types.Row;
import org.apache.inlong.sort.formats.common.BinaryFormatInfo;
import org.apache.inlong.sort.formats.common.DateFormatInfo;
import org.apache.inlong.sort.formats.common.IntFormatInfo;
import org.apache.inlong.sort.formats.common.LongFormatInfo;
import org.apache.inlong.sort.formats.common.MapFormatInfo;
import org.apache.inlong.sort.formats.common.StringFormatInfo;
import org.apache.inlong.sort.formats.common.TimeFormatInfo;
import org.apache.inlong.sort.formats.common.TimestampFormatInfo;
import org.apache.inlong.sort.protocol.FieldInfo;
import org.apache.inlong.sort.protocol.deserialization.AvroDeserializationInfo;
import org.apache.inlong.sort.protocol.serialization.AvroSerializationInfo;
import org.apache.inlong.sort.singletenant.flink.serialization.SerializationSchemaFactory;
import org.junit.Test;

import java.io.IOException;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class AvroDeserializationTest {

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
                    new MapFormatInfo(StringFormatInfo.INSTANCE, IntFormatInfo.INSTANCE)))
    };

    private Row generateTestRow() {
        Row testRow = new Row(8);
        testRow.setField(0, 1238123899121L);
        testRow.setField(1, "testName");

        byte[] bytes = new byte[]{1, 2, 3, 4, 5, 6};
        testRow.setField(2, bytes);

        testRow.setField(3, Date.valueOf("1990-10-14"));
        testRow.setField(4, Time.valueOf("12:12:43"));
        testRow.setField(5, Timestamp.valueOf("1990-10-14 12:12:43"));

        Map<String, Long> map = new HashMap<>();
        map.put("flink", 123L);
        testRow.setField(6, map);

        Map<String, Map<String, Integer>> nestedMap = new HashMap<>();
        Map<String, Integer> innerMap = new HashMap<>();
        innerMap.put("key", 234);
        nestedMap.put("inner_map", innerMap);
        testRow.setField(7, nestedMap);

        return testRow;
    }

    @Test
    public void testAvroDeserializationSchema() throws IOException, ClassNotFoundException {
        SerializationSchema<Row> serializationSchema =
                SerializationSchemaFactory.build(fieldInfos, new AvroSerializationInfo());

        Row expectedRow = generateTestRow();
        byte[] bytes = serializationSchema.serialize(expectedRow);

        DeserializationSchema<Row> deserializationSchema =
                DeserializationSchemaFactory.build(fieldInfos, new AvroDeserializationInfo());
        Row actualRow = deserializationSchema.deserialize(bytes);

        assertEquals(expectedRow, actualRow);
    }

}
