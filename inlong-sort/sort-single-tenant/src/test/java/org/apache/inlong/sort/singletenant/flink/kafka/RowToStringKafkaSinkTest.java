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

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.types.Row;
import org.apache.inlong.sort.formats.common.DoubleFormatInfo;
import org.apache.inlong.sort.formats.common.StringFormatInfo;
import org.apache.inlong.sort.protocol.FieldInfo;
import org.apache.inlong.sort.singletenant.flink.serialization.RowSerializationSchemaFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.utils.Bytes;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class RowToStringKafkaSinkTest extends KafkaSinkTestBaseForRow {

    @Override
    protected void prepareData() throws JsonProcessingException {
        topic = "test_kafka_row_to_string";
        serializationSchema = RowSerializationSchemaFactory.build(
                new FieldInfo[]{
                        new FieldInfo("f1", new StringFormatInfo()),
                        new FieldInfo("f2", new DoubleFormatInfo())
                },
                null
        );

        testRows = new ArrayList<>();
        testRows.add(Row.of("f1", 12.0));
        testRows.add(Row.of("f2", 12.1));
        testRows.add(Row.of("f3", 12.3));
    }

    @Override
    protected void verifyData(ConsumerRecords<String, Bytes> records) {
        List<String> results = new ArrayList<>();
        for (ConsumerRecord<String, Bytes> record : records) {
            assertNull(record.key());
            results.add(new String(record.value().get()));
        }

        List<String> expectedData = new ArrayList<>();
        testRows.forEach(row -> expectedData.add(row.toString()));

        results.sort(String::compareTo);
        expectedData.sort(String::compareTo);
        assertEquals(expectedData, results);

    }
}
