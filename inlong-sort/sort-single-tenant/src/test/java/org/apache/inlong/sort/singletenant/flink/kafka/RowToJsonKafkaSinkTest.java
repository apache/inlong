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

package org.apache.inlong.sort.singletenant.flink.kafka;

import org.apache.flink.types.Row;
import org.apache.inlong.sort.formats.common.ArrayFormatInfo;
import org.apache.inlong.sort.formats.common.DoubleFormatInfo;
import org.apache.inlong.sort.formats.common.IntFormatInfo;
import org.apache.inlong.sort.formats.common.MapFormatInfo;
import org.apache.inlong.sort.formats.common.StringFormatInfo;
import org.apache.inlong.sort.protocol.FieldInfo;
import org.apache.inlong.sort.protocol.serialization.JsonSerializationInfo;
import org.apache.inlong.sort.singletenant.flink.serialization.SerializationSchemaFactory;
import org.apache.kafka.common.utils.Bytes;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class RowToJsonKafkaSinkTest extends KafkaSinkTestBase {
    @Override
    protected void prepareData() throws IOException, ClassNotFoundException {
        topic = "test_kafka_row_to_json";
        serializationSchema = SerializationSchemaFactory.build(
                new FieldInfo[]{
                        new FieldInfo("f1", new StringFormatInfo()),
                        new FieldInfo("f2", new MapFormatInfo(new StringFormatInfo(), new DoubleFormatInfo())),
                        new FieldInfo("f3", new ArrayFormatInfo(new IntFormatInfo()))
                },
                new JsonSerializationInfo()
        );

        prepareTestRows();
    }

    private void prepareTestRows() {
        testRows = new ArrayList<>();

        Map<String, Double> map1 = new HashMap<>();
        map1.put("high", 170.5);
        testRows.add(Row.of("zhangsan", map1, new Integer[]{123}));

        Map<String, Double> map2 = new HashMap<>();
        map2.put("high", 180.5);
        testRows.add(Row.of("lisi", map2, new Integer[]{1234}));

        Map<String, Double> map3 = new HashMap<>();
        map3.put("high", 190.5);
        testRows.add(Row.of("wangwu", map3, new Integer[]{12345}));
    }

    @Override
    protected void verifyData(List<Bytes> results) {
        List<String> actualData = new ArrayList<>();
        results.forEach(value -> actualData.add(new String(value.get(), StandardCharsets.UTF_8)));
        actualData.sort(String::compareTo);

        List<String> expectedData = new ArrayList<>();
        expectedData.add("{\"f1\":\"zhangsan\",\"f2\":{\"high\":170.5},\"f3\":[123]}");
        expectedData.add("{\"f1\":\"lisi\",\"f2\":{\"high\":180.5},\"f3\":[1234]}");
        expectedData.add("{\"f1\":\"wangwu\",\"f2\":{\"high\":190.5},\"f3\":[12345]}");
        expectedData.sort(String::compareTo);

        assertEquals(expectedData, actualData);
    }
}
