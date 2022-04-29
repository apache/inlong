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

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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
import org.apache.inlong.sort.formats.common.IntFormatInfo;
import org.apache.inlong.sort.formats.common.LongFormatInfo;
import org.apache.inlong.sort.formats.common.StringFormatInfo;
import org.apache.inlong.sort.protocol.BuiltInFieldInfo;
import org.apache.inlong.sort.protocol.BuiltInFieldInfo.BuiltInField;
import org.apache.inlong.sort.protocol.FieldInfo;
import org.apache.inlong.sort.protocol.deserialization.DebeziumDeserializationInfo;
import org.apache.inlong.sort.protocol.serialization.CanalSerializationInfo;
import org.apache.inlong.sort.protocol.transformation.FieldMappingRule;
import org.apache.inlong.sort.protocol.transformation.FieldMappingRule.FieldMappingUnit;
import org.apache.inlong.sort.protocol.transformation.TransformationInfo;
import org.apache.inlong.sort.singletenant.flink.deserialization.DeserializationFunction;
import org.apache.inlong.sort.singletenant.flink.deserialization.DeserializationSchemaFactory;
import org.apache.inlong.sort.singletenant.flink.deserialization.FieldMappingTransformer;
import org.apache.inlong.sort.singletenant.flink.serialization.SerializationSchemaFactory;
import org.apache.inlong.sort.singletenant.flink.transformation.Transformer;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DebeziumToCanalITCase {

    private static final Logger logger = LoggerFactory.getLogger(DebeziumToCanalITCase.class);

    private static final CountDownLatch verificationFinishedLatch = new CountDownLatch(1);

    private static final CountDownLatch jobFinishedLatch = new CountDownLatch(1);

    private final FieldInfo[] sourceFieldInfos = new FieldInfo[]{
            new FieldInfo("name", StringFormatInfo.INSTANCE),
            new FieldInfo("age", IntFormatInfo.INSTANCE),
            new BuiltInFieldInfo("db", StringFormatInfo.INSTANCE, BuiltInField.MYSQL_METADATA_DATABASE),
            new BuiltInFieldInfo("table", StringFormatInfo.INSTANCE, BuiltInField.MYSQL_METADATA_TABLE),
            new BuiltInFieldInfo("es", LongFormatInfo.INSTANCE, BuiltInField.MYSQL_METADATA_EVENT_TIME),
            new BuiltInFieldInfo("isDdl", BooleanFormatInfo.INSTANCE, BuiltInField.MYSQL_METADATA_IS_DDL),
            new BuiltInFieldInfo("type", StringFormatInfo.INSTANCE, BuiltInField.MYSQL_METADATA_EVENT_TYPE)
    };

    private final FieldInfo[] sinkFieldInfos = new FieldInfo[]{
            new FieldInfo("name", StringFormatInfo.INSTANCE),
            new FieldInfo("age", IntFormatInfo.INSTANCE),
            new FieldInfo("inner_type", StringFormatInfo.INSTANCE),
            new BuiltInFieldInfo("db", StringFormatInfo.INSTANCE, BuiltInField.MYSQL_METADATA_DATABASE),
            new BuiltInFieldInfo("table", StringFormatInfo.INSTANCE, BuiltInField.MYSQL_METADATA_TABLE),
            new BuiltInFieldInfo("es", LongFormatInfo.INSTANCE, BuiltInField.MYSQL_METADATA_EVENT_TIME),
            new BuiltInFieldInfo("isDdl", BooleanFormatInfo.INSTANCE, BuiltInField.MYSQL_METADATA_IS_DDL),
            new BuiltInFieldInfo("type", StringFormatInfo.INSTANCE, BuiltInField.MYSQL_METADATA_EVENT_TYPE)
    };

    private static final String expectedResult =
            "{\"data\":[{\"name\":\"testName\",\"age\":29,\"inner_type\":\"-D\"}],"
                    + "\"type\":\"DELETE\",\"database\":\"test\",\"table\":\"test\","
                    + "\"es\":1644896917208,\"isDdl\":false}";

    @Test(timeout = 60 * 1000)
    public void test() throws Exception {

        final ExecutorService executorService = Executors.newSingleThreadExecutor();

        executorService.execute(() -> {
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

            try {
                DataStream<SerializedRecord> sourceStream = env.addSource(new TestSource());

                // Deserialize
                DeserializationSchema<Row> deserializationSchema = DeserializationSchemaFactory.build(
                        sourceFieldInfos,
                        new DebeziumDeserializationInfo(false, "ISO_8601", false));
                FieldMappingTransformer fieldMappingTransformer = new FieldMappingTransformer(
                        new Configuration(), sourceFieldInfos);
                DeserializationFunction function = new DeserializationFunction(
                        deserializationSchema, fieldMappingTransformer, false, new Configuration(), "", "");
                DataStream<Row> deserializedStream = sourceStream.process(function);

                // Transform
                TransformationInfo transformationInfo = new TransformationInfo(
                        new FieldMappingRule(new FieldMappingUnit[] {
                                new FieldMappingUnit(
                                        new FieldInfo("name", StringFormatInfo.INSTANCE),
                                        new FieldInfo("name", IntFormatInfo.INSTANCE)),
                                new FieldMappingUnit(
                                        new FieldInfo("age", StringFormatInfo.INSTANCE),
                                        new FieldInfo("age", IntFormatInfo.INSTANCE)),
                                new FieldMappingUnit(
                                        new FieldInfo("type", StringFormatInfo.INSTANCE),
                                        new FieldInfo("inner_type", IntFormatInfo.INSTANCE)),
                                new FieldMappingUnit(
                                        new FieldInfo("db", StringFormatInfo.INSTANCE),
                                        new FieldInfo("db", IntFormatInfo.INSTANCE)),
                                new FieldMappingUnit(
                                        new FieldInfo("table", StringFormatInfo.INSTANCE),
                                        new FieldInfo("table", IntFormatInfo.INSTANCE)),
                                new FieldMappingUnit(
                                        new FieldInfo("es", StringFormatInfo.INSTANCE),
                                        new FieldInfo("es", IntFormatInfo.INSTANCE)),
                                new FieldMappingUnit(
                                        new FieldInfo("isDdl", StringFormatInfo.INSTANCE),
                                        new FieldInfo("isDdl", IntFormatInfo.INSTANCE)),
                                new FieldMappingUnit(
                                        new FieldInfo("type", StringFormatInfo.INSTANCE),
                                        new FieldInfo("type", IntFormatInfo.INSTANCE)),
                        }));
                DataStream<Row> transformedStream = deserializedStream.process(new Transformer(
                        transformationInfo,
                        sourceFieldInfos,
                        sinkFieldInfos));

                // Serialize and output
                SerializationSchema<Row> serializationSchema = SerializationSchemaFactory.build(
                        sinkFieldInfos, new CanalSerializationInfo()
                );
                transformedStream.addSink(new TestSink(serializationSchema));

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
        if (TestSink.results.size() == 0) {
            return false;
        } else {
            assertEquals(expectedResult, TestSink.results.get(0));
            return true;
        }
    }

    private static class TestSource extends RichSourceFunction<SerializedRecord> {

        String testString = "{\n"
                + "    \"after\":null,\n"
                + "    \"before\":{\n"
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
                + "    \"op\":\"d\",\n"
                + "    \"ts_ms\":1644896917208,\n"
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
            inLongMsg.addMsg(attrs, testString.getBytes());
            byte[] bytes = inLongMsg.buildArray();

            sourceContext.collect(new SerializedRecord(System.currentTimeMillis(), bytes));

            verificationFinishedLatch.await();
        }

        @Override
        public void cancel() {

        }
    }

    private static class TestSink extends RichSinkFunction<Row> {

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
