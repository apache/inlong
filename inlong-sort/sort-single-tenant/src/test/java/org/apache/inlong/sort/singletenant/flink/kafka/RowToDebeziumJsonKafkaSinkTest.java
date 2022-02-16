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
import org.apache.flink.types.RowKind;
import org.apache.inlong.sort.formats.common.IntFormatInfo;
import org.apache.inlong.sort.formats.common.StringFormatInfo;
import org.apache.inlong.sort.protocol.FieldInfo;
import org.apache.inlong.sort.protocol.serialization.DebeziumSerializationInfo;
import org.apache.inlong.sort.singletenant.flink.serialization.SerializationSchemaFactory;
import org.apache.kafka.common.utils.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class RowToDebeziumJsonKafkaSinkTest extends KafkaSinkTestBase {

    @Override
    protected void prepareData() throws IOException, ClassNotFoundException {
        topic = "test_kafka_row_to_debezium";
        fieldInfos = new FieldInfo[]{
                new FieldInfo("f1", new StringFormatInfo()),
                new FieldInfo("f2", new IntFormatInfo())
        };

        serializationSchema = SerializationSchemaFactory.build(
                fieldInfos, new DebeziumSerializationInfo("sql", "literal", "null", true)
        );

        prepareTestData();
    }

    private void prepareTestData() {
        testRows = new ArrayList<>();
        Row row1 = Row.of("Anna", 100);
        row1.setKind(RowKind.INSERT);
        testRows.add(row1);

        Row row2 = Row.of("Lisa", 90);
        row2.setKind(RowKind.DELETE);
        testRows.add(row2);

        Row row3 = Row.of("Bob", 80);
        row3.setKind(RowKind.UPDATE_BEFORE);
        testRows.add(row3);

        Row row4 = Row.of("Tom", 70);
        row4.setKind(RowKind.UPDATE_AFTER);
        testRows.add(row4);
    }

    @Override
    protected void verifyData(List<Bytes> results) {
        List<String> actualData = new ArrayList<>();
        results.forEach(value -> actualData.add(new String(value.get())));
        actualData.sort(String::compareTo);

        List<String> expectedData = new ArrayList<>();
        expectedData.add("{\"before\":null,\"after\":{\"f1\":\"Anna\",\"f2\":100},\"op\":\"c\"}");
        expectedData.add("{\"before\":null,\"after\":{\"f1\":\"Tom\",\"f2\":70},\"op\":\"c\"}");
        expectedData.add("{\"before\":{\"f1\":\"Bob\",\"f2\":80},\"after\":null,\"op\":\"d\"}");
        expectedData.add("{\"before\":{\"f1\":\"Lisa\",\"f2\":90},\"after\":null,\"op\":\"d\"}");
        expectedData.sort(String::compareTo);

        assertEquals(expectedData, actualData);
    }

}
