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

package org.apache.inlong.sort.singletenant.flink;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.types.Row;
import org.apache.inlong.common.msg.InLongMsg;
import org.apache.inlong.sort.configuration.Configuration;
import org.apache.inlong.sort.formats.common.BooleanFormatInfo;
import org.apache.inlong.sort.formats.common.LongFormatInfo;
import org.apache.inlong.sort.formats.common.StringFormatInfo;
import org.apache.inlong.sort.protocol.BuiltInFieldInfo;
import org.apache.inlong.sort.protocol.BuiltInFieldInfo.BuiltInField;
import org.apache.inlong.sort.protocol.FieldInfo;
import org.apache.inlong.sort.protocol.deserialization.DebeziumDeserializationInfo;
import org.apache.inlong.sort.protocol.serialization.CanalSerializationInfo;
import org.apache.inlong.sort.singletenant.flink.deserialization.DeserializationFunction;
import org.apache.inlong.sort.singletenant.flink.deserialization.DeserializationSchemaFactory;
import org.apache.inlong.sort.singletenant.flink.deserialization.FieldMappingTransformer;
import org.apache.inlong.sort.singletenant.flink.serialization.SerializationSchemaFactory;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertEquals;

public class WholeDBMigrationITCase {

    private static final Logger logger = LoggerFactory.getLogger(WholeDBMigrationITCase.class);

    private static final CountDownLatch verificationFinishedLatch = new CountDownLatch(1);

    private static final CountDownLatch jobFinishedLatch = new CountDownLatch(1);

    private final FieldInfo[] fieldInfos = new FieldInfo[] {
            new BuiltInFieldInfo("mydata", StringFormatInfo.INSTANCE, BuiltInField.MYSQL_METADATA_DATA),
            new BuiltInFieldInfo("db", StringFormatInfo.INSTANCE, BuiltInField.MYSQL_METADATA_DATABASE),
            new BuiltInFieldInfo("table", StringFormatInfo.INSTANCE, BuiltInField.MYSQL_METADATA_TABLE),
            new BuiltInFieldInfo("es", LongFormatInfo.INSTANCE, BuiltInField.MYSQL_METADATA_EVENT_TIME),
            new BuiltInFieldInfo("isDdl", BooleanFormatInfo.INSTANCE, BuiltInField.MYSQL_METADATA_IS_DDL),
            new BuiltInFieldInfo("type", StringFormatInfo.INSTANCE, BuiltInField.MYSQL_METADATA_EVENT_TYPE)
    };

    private static final List<String> expectedResults = new ArrayList<>();

    @Before
    public void prepare() {
        expectedResults.add("{\"data\":[{\"name\":\"testName\",\"age\":29}],"
                + "\"type\":\"INSERT\",\"database\":\"test\",\"table\":\"test\","
                + "\"es\":1644896917208,\"isDdl\":false}");
        expectedResults.add("{\"data\":[{\"id\":106,\"name\":\"hammer\",\"description\":\"16oz carpenter's hammer\","
                + "\"weight\":1}],\"type\":\"DELETE\",\"database\":\"inventory\",\"table\":\"products\","
                + "\"es\":1589361987936,\"isDdl\":false}");
        expectedResults.add("{\"data\":[{\"id\":106,\"name\":\"hammer\",\"description\":\"18oz carpenter hammer\","
                + "\"weight\":1}],\"type\":\"INSERT\",\"database\":\"inventory\",\"table\":\"products\","
                + "\"es\":1589361987936,\"isDdl\":false}");
        expectedResults.add("{\"data\":[{\"id\":111,\"name\":\"scooter\",\"description\":\"Big 2-wheel scooter \","
                + "\"weight\":5.170000076293945}],\"type\":\"DELETE\",\"database\":\"inventory\","
                + "\"table\":\"products\",\"es\":1589362344455,\"isDdl\":false}");

        expectedResults.sort(String::compareTo);
    }

    @Test(timeout = 60 * 1000)
    public void test() throws Exception {

        final ExecutorService executorService = Executors.newSingleThreadExecutor();

        executorService.execute(() -> {
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

            try {
                DataStream<SerializedRecord> sourceStream = env.addSource(new TestSource());

                // Deserialize
                DeserializationSchema<Row> deserializationSchema = DeserializationSchemaFactory.build(
                        fieldInfos,
                        new DebeziumDeserializationInfo(false, "ISO_8601", true));
                FieldMappingTransformer fieldMappingTransformer = new FieldMappingTransformer(
                        new Configuration(), fieldInfos);
                DeserializationFunction function = new DeserializationFunction(
                        deserializationSchema, fieldMappingTransformer, false);
                DataStream<Row> deserializedStream = sourceStream.process(function);

                // Serialize and output
                SerializationSchema<Row> serializationSchema = SerializationSchemaFactory.build(
                        fieldInfos, new CanalSerializationInfo()
                );
                deserializedStream.addSink(new TestSink(serializationSchema));

                env.execute();

            } catch (Exception e) {
                logger.error("Error occurred when executing flink test job: ", e);
            } finally {
                jobFinishedLatch.countDown();
            }
        });

        try {
            while (!verify()) {
                Thread.sleep(500);
            }
            verificationFinishedLatch.countDown();
            jobFinishedLatch.await();
        } finally {
            executorService.shutdown();
        }

        Thread.sleep(10000);
    }

    private boolean verify() {
        if (TestSink.results.size() != 4) {
            return false;
        } else {
            TestSink.results.sort(String::compareTo);
            assertEquals(expectedResults, TestSink.results);
            return true;
        }
    }

    private static class TestSource extends RichSourceFunction<SerializedRecord> {

        private static final long serialVersionUID = 330038533445098042L;

        String testStringInsert = "{\n"
                + "    \"before\":null,\n"
                + "    \"after\":{\n"
                + "        \"name\":\"testName\",\n"
                + "        \"age\":29\n"
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

        String testStringUpdate = "{\n"
                + "    \"before\":{\n"
                + "        \"id\":106,\n"
                + "        \"name\":\"hammer\",\n"
                + "        \"description\":\"16oz carpenter's hammer\",\n"
                + "        \"weight\":1\n"
                + "    },\n"
                + "    \"after\":{\n"
                + "        \"id\":106,\n"
                + "        \"name\":\"hammer\",\n"
                + "        \"description\":\"18oz carpenter hammer\",\n"
                + "        \"weight\":1\n"
                + "    },\n"
                + "    \"source\":{\n"
                + "        \"version\":\"1.1.1.Final\",\n"
                + "        \"connector\":\"mysql\",\n"
                + "        \"name\":\"dbserver1\",\n"
                + "        \"ts_ms\":1589361987000,\n"
                + "        \"snapshot\":\"false\",\n"
                + "        \"db\":\"inventory\",\n"
                + "        \"table\":\"products\",\n"
                + "        \"server_id\":223344,\n"
                + "        \"gtid\":null,\n"
                + "        \"file\":\"mysql-bin.000003\",\n"
                + "        \"pos\":362,\n"
                + "        \"row\":0,\n"
                + "        \"thread\":2,\n"
                + "        \"query\":null\n"
                + "    },\n"
                + "    \"op\":\"u\",\n"
                + "    \"ts_ms\":1589361987936,\n"
                + "    \"transaction\":null\n"
                + "}";

        String testStringDelete = "{\n"
                + "    \"before\":{\n"
                + "        \"id\":111,\n"
                + "        \"name\":\"scooter\",\n"
                + "        \"description\":\"Big 2-wheel scooter \",\n"
                + "        \"weight\":5.170000076293945\n"
                + "    },\n"
                + "    \"after\":null,\n"
                + "    \"source\":{\n"
                + "        \"version\":\"1.1.1.Final\",\n"
                + "        \"connector\":\"mysql\",\n"
                + "        \"name\":\"dbserver1\",\n"
                + "        \"ts_ms\":1589362344000,\n"
                + "        \"snapshot\":\"false\",\n"
                + "        \"db\":\"inventory\",\n"
                + "        \"table\":\"products\",\n"
                + "        \"server_id\":223344,\n"
                + "        \"gtid\":null,\n"
                + "        \"file\":\"mysql-bin.000003\",\n"
                + "        \"pos\":2443,\n"
                + "        \"row\":0,\n"
                + "        \"thread\":2,\n"
                + "        \"query\":null\n"
                + "    },\n"
                + "    \"op\":\"d\",\n"
                + "    \"ts_ms\":1589362344455,\n"
                + "    \"transaction\":null\n"
                + "}";

        @Override
        public void open(org.apache.flink.configuration.Configuration configuration) {
        }

        @Override
        public void run(SourceContext<SerializedRecord> sourceContext) throws Exception {
            InLongMsg inLongMsg = InLongMsg.newInLongMsg(true);
            String attrs = "m=0"
                   + "&dt=" + System.currentTimeMillis()
                   + "&iname=" + "tid";
            inLongMsg.addMsg(attrs, testStringInsert.getBytes(StandardCharsets.UTF_8));
            inLongMsg.addMsg(attrs, testStringUpdate.getBytes(StandardCharsets.UTF_8));
            inLongMsg.addMsg(attrs, testStringDelete.getBytes(StandardCharsets.UTF_8));
            byte[] bytes = inLongMsg.buildArray();

            sourceContext.collect(new SerializedRecord(System.currentTimeMillis(), bytes));

            verificationFinishedLatch.await();
        }

        @Override
        public void cancel() {

        }
    }

    private static class TestSink extends RichSinkFunction<Row> {

        private static final long serialVersionUID = -3058986575780103102L;

        private static final List<String> results = new ArrayList<>();

        private final SerializationSchema<Row> serializationSchema;

        public TestSink(SerializationSchema<Row> serializationSchema) {
            this.serializationSchema = serializationSchema;
        }

        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
            serializationSchema.open(null);
        }

        @Override
        public void invoke(Row record, Context context) {
            results.add(new String(serializationSchema.serialize(record)));
        }
    }
}
