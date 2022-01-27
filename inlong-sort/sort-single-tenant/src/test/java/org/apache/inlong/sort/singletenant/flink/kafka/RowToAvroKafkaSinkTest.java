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
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.flink.formats.avro.AvroDeserializationSchema;
import org.apache.flink.types.Row;
import org.apache.inlong.sort.formats.common.ArrayFormatInfo;
import org.apache.inlong.sort.formats.common.BinaryFormatInfo;
import org.apache.inlong.sort.formats.common.DoubleFormatInfo;
import org.apache.inlong.sort.formats.common.FormatInfo;
import org.apache.inlong.sort.formats.common.IntFormatInfo;
import org.apache.inlong.sort.formats.common.MapFormatInfo;
import org.apache.inlong.sort.formats.common.NullFormatInfo;
import org.apache.inlong.sort.formats.common.RowFormatInfo;
import org.apache.inlong.sort.formats.common.StringFormatInfo;
import org.apache.inlong.sort.protocol.FieldInfo;
import org.apache.inlong.sort.protocol.serialization.AvroSerializationInfo;
import org.apache.inlong.sort.singletenant.flink.serialization.RowSerializationSchemaFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.utils.Bytes;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.inlong.sort.singletenant.flink.serialization.AvroUtils.buildAvroRecordSchemaInJson;
import static org.junit.Assert.assertEquals;

public class RowToAvroKafkaSinkTest extends KafkaSinkTestBaseForRow {

    @Override
    protected void prepareData() throws JsonProcessingException {
        fieldInfos = new FieldInfo[]{
                new FieldInfo("f1", new StringFormatInfo()),
                new FieldInfo("f2", new IntFormatInfo()),
                new FieldInfo("f3", new NullFormatInfo()),
                new FieldInfo("f4", new BinaryFormatInfo()),
                new FieldInfo("f5", new MapFormatInfo(
                        new StringFormatInfo(),
                        new RowFormatInfo(
                                new String[]{"f51", "f52"},
                                new FormatInfo[]{
                                        new IntFormatInfo(),
                                        new ArrayFormatInfo(new DoubleFormatInfo())
                                }
                        )
                ))
        };
        topic = "test_kafka_row_to_avro";
        serializationSchema = RowSerializationSchemaFactory.build(fieldInfos, new AvroSerializationInfo());

        prepareTestRows();
    }

    private void prepareTestRows() {
        testRows = new ArrayList<>();
        Map<String, Row> map1 = new HashMap<>();
        Double[] doubles1 = new Double[]{1.0, 2.0, 3.0};
        map1.put("AnnaMap", Row.of(1, doubles1));
        testRows.add(Row.of("Anna", 100, null, new byte[]{1, 2, 3}, map1));

        Map<String, Row> map2 = new HashMap<>();
        Double[] doubles2 = new Double[]{4.0, 5.0, 6.0};
        map2.put("LisaMap", Row.of(2, doubles2));
        testRows.add(Row.of("Lisa", 50, null, new byte[]{4, 5, 6}, map2));

        Map<String, Row> map3 = new HashMap<>();
        Double[] doubles3 = new Double[]{7.0, 8.0, 9.0};
        map3.put("BobMap", Row.of(3, doubles3));
        testRows.add(Row.of("Bob", 10, null, new byte[]{7, 8, 9}, map3));
    }

    @Override
    protected void verifyData(ConsumerRecords<String, Bytes> records) throws IOException {
        AvroDeserializationSchema<GenericRecord> deserializationSchema = AvroDeserializationSchema.forGeneric(
                new Schema.Parser().parse(buildAvroRecordSchemaInJson(fieldInfos)));

        List<String> actualResult = new ArrayList<>(testRows.size());
        for (ConsumerRecord<String, Bytes> record : records) {
            GenericRecord genericRecord = deserializationSchema.deserialize(record.value().get());
            Row tempRow = new Row(5);
            tempRow.setField(0, genericRecord.get("f1"));
            tempRow.setField(1, genericRecord.get("f2"));
            tempRow.setField(2, genericRecord.get("f3"));
            ByteBuffer f3 = (ByteBuffer) genericRecord.get("f4");
            tempRow.setField(3, f3.array());
            Map<Utf8, GenericRecord> f5 = (Map<Utf8, GenericRecord>) genericRecord.get("f5");
            Map<String, Row> tempMap = new HashMap<>();
            for (Map.Entry<Utf8, GenericRecord> utf8GenericRecordEntry : f5.entrySet()) {
                String key = new String(utf8GenericRecordEntry.getKey().getBytes());
                GenericRecord value = utf8GenericRecordEntry.getValue();
                tempMap.put(key, Row.of(value.get("f51"), value.get("f52")));
            }
            tempRow.setField(4, tempMap);
            actualResult.add(tempRow.toString());
        }

        List<String> expectedResult = new ArrayList<>(testRows.size());
        testRows.forEach(row -> expectedResult.add(row.toString()));
        expectedResult.sort(String::compareTo);
        actualResult.sort(String::compareTo);
        assertEquals(expectedResult, actualResult);
    }

}
