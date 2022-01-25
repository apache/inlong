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

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.formats.avro.AvroDeserializationSchema;
import org.apache.flink.types.Row;
import org.apache.inlong.sort.formats.common.IntFormatInfo;
import org.apache.inlong.sort.formats.common.StringFormatInfo;
import org.apache.inlong.sort.protocol.FieldInfo;
import org.apache.inlong.sort.protocol.serialization.AvroSerializationInfo;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.utils.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.inlong.sort.singletenant.flink.serialization.AvroUtils.buildAvroRecordSchemaInJson;
import static org.junit.Assert.assertEquals;

public class RowToAvroKafkaSinkTest extends KafkaSinkTestBase {

    @Override
    protected void prepareData() {
        topic = "test_kafka_row_to_avro";

        fieldInfos = new FieldInfo[]{
                new FieldInfo("f1", new StringFormatInfo()),
                new FieldInfo("f2", new IntFormatInfo())
        };

        testRows = new ArrayList<>();
        testRows.add(Row.of("Anna", 100));
        testRows.add(Row.of("Lisa", 50));
        testRows.add(Row.of("Bob", 10));

        serializationInfo = new AvroSerializationInfo("KafkaSinkInAvro");
    }

    @Override
    protected void verifyData(ConsumerRecords<String, Bytes> records) throws IOException {
        AvroDeserializationSchema<GenericRecord> deserializationSchema =
                AvroDeserializationSchema.forGeneric(new Schema.Parser().parse(
                                buildAvroRecordSchemaInJson(fieldInfos, (AvroSerializationInfo) serializationInfo)));

        List<String> actualResult = new ArrayList<>(testRows.size());
        for (ConsumerRecord<String, Bytes> record : records) {
            GenericRecord genericRecord = deserializationSchema.deserialize(record.value().get());
            actualResult.add(Row.of(genericRecord.get("f1"), genericRecord.get("f2")).toString());
        }

        List<String> expectedResult = new ArrayList<>(testRows.size());
        testRows.forEach(row -> expectedResult.add(row.toString()));
        expectedResult.sort(String::compareTo);
        actualResult.sort(String::compareTo);
        assertEquals(expectedResult, actualResult);
    }

}
