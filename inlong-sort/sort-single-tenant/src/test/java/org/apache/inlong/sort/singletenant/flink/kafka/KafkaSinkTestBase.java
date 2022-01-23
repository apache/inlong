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

import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import org.apache.curator.test.TestingServer;
import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.types.Row;
import org.apache.inlong.sort.configuration.Configuration;
import org.apache.inlong.sort.protocol.FieldInfo;
import org.apache.inlong.sort.protocol.serialization.SerializationInfo;
import org.apache.inlong.sort.protocol.sink.KafkaSinkInfo;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.Time;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import scala.Option;
import scala.collection.mutable.ArraySeq;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;

import static org.apache.flink.util.NetUtils.hostAndPortToUrlString;
import static org.apache.inlong.sort.singletenant.flink.kafka.KafkaSinkBuilder.buildKafkaSink;

public abstract class KafkaSinkTestBase {

    @ClassRule
    public static TemporaryFolder tempFolder = new TemporaryFolder();

    private final int zkTimeout = 30000;
    private final String topic = "test_kafka_sink";

    private TestingServer zkServer;

    private KafkaServer kafkaServer;
    private String brokerConnStr;
    private AdminClient kafkaAdmin;
    private KafkaConsumer<String, String> kafkaConsumer;
    private Properties kafkaClientProperties;

    // prepare data below in subclass
    protected List<Row> testRows;
    protected FieldInfo[] fieldInfos;
    protected SerializationInfo serializationInfo;

    @Before
    public void setup() throws Exception {
        startZK();
        startKafkaServer();
        prepareKafkaClientProps();
        kafkaAdmin = AdminClient.create(kafkaClientProperties);
        addTopic();
        kafkaConsumer = new KafkaConsumer<>(kafkaClientProperties);

        prepareData();
    }

    private void startZK() throws Exception {
        zkServer = new TestingServer(-1, tempFolder.newFolder());
        zkServer.start();
    }

    private void startKafkaServer() throws IOException {
        Properties kafkaProperties = new Properties();
        final String KAFKA_HOST = "localhost";
        kafkaProperties.put("advertised.host.name", KAFKA_HOST);
        kafkaProperties.put("broker.id", "1");
        kafkaProperties.put("log.dir", tempFolder.newFolder().getAbsolutePath());
        kafkaProperties.put("zookeeper.connect", zkServer.getConnectString());
        kafkaProperties.put("transaction.max.timeout.ms", Integer.toString(1000 * 60 * 60 * 2));
        kafkaProperties.put("log.retention.ms", "-1");
        kafkaProperties.put("zookeeper.session.timeout.ms", zkTimeout);
        kafkaProperties.put("zookeeper.connection.timeout.ms", zkTimeout);
        kafkaProperties.put("offsets.topic.replication.factor", (short) 1);

        KafkaConfig kafkaConfig = new KafkaConfig(kafkaProperties);
        kafkaServer = new KafkaServer(kafkaConfig, Time.SYSTEM, Option.apply(null), new ArraySeq<>(0));
        kafkaServer.startup();
        brokerConnStr = hostAndPortToUrlString(
                KAFKA_HOST,
                kafkaServer.socketServer().boundPort(
                        ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT)
                )
        );
    }

    private void prepareKafkaClientProps() {
        kafkaClientProperties = new Properties();
        kafkaClientProperties.setProperty("bootstrap.servers", brokerConnStr);
        kafkaClientProperties.setProperty("group.id", "flink-tests");
        kafkaClientProperties.setProperty("enable.auto.commit", "false");
        kafkaClientProperties.setProperty("zookeeper.session.timeout.ms", String.valueOf(zkTimeout));
        kafkaClientProperties.setProperty("zookeeper.connection.timeout.ms", String.valueOf(zkTimeout));
        kafkaClientProperties.setProperty("offsets.topic.replication.factor", "1");
        kafkaClientProperties.setProperty("auto.offset.reset", "earliest");
        kafkaClientProperties.setProperty("max.poll.records", "1000");
        kafkaClientProperties.setProperty("key.deserializer", StringDeserializer.class.getName());
        kafkaClientProperties.setProperty("value.deserializer", StringDeserializer.class.getName());
    }

    private void addTopic() throws InterruptedException, TimeoutException, ExecutionException {
        NewTopic topicObj = new NewTopic(topic, 1, (short) 1);
        kafkaAdmin.createTopics(Collections.singleton(topicObj)).all().get();
        CommonTestUtils.waitUtil(
                () -> kafkaServer.metadataCache().contains(topic),
                Duration.ofSeconds(10),
                "The topic metadata failed to propagate to Kafka broker.");
    }

    abstract protected void prepareData();

    @After
    public void clean() throws IOException {
        kafkaConsumer.close();
        kafkaAdmin.close();
        kafkaServer.shutdown();
        zkServer.close();
    }

    @Test(timeout = 3 * 60 * 1000)
    public void testKafkaSink() throws InterruptedException {
        TestingSource testingSource = createTestingSource();
        final ExecutorService executorService = Executors.newSingleThreadExecutor();
        CountDownLatch testFinishedCountDownLatch = new CountDownLatch(1);
        executorService.execute(() -> {
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.addSource(testingSource).addSink(
                    buildKafkaSink(
                            new KafkaSinkInfo(fieldInfos, brokerConnStr, topic, serializationInfo),
                            new HashMap<>(),
                            new Configuration()
                    )
            );

            try {
                env.execute();
                testFinishedCountDownLatch.await();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        verify();

        testFinishedCountDownLatch.countDown();
    }

    private void verify() throws InterruptedException {
        kafkaConsumer.subscribe(Collections.singleton(topic));
        while (true) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofSeconds(1));
            if (records.isEmpty() || records.count() != testRows.size()) {
                //noinspection BusyWait
                Thread.sleep(1000);
                continue;
            }

            verifyData(records);

            break;
        }
    }

    abstract protected void verifyData(ConsumerRecords<String, String> records);

    private TestingSource createTestingSource() {
        TestingSource testingSource = new TestingSource();
        testingSource.testRows = testRows;

        return testingSource;
    }

    public static class TestingSource implements SourceFunction<Row> {

        private static final long serialVersionUID = 7192240041886654672L;

        public List<Row> testRows;

        @Override
        public void run(SourceContext<Row> sourceContext) throws Exception {
            testRows.forEach(sourceContext::collect);

            Thread.sleep(20 * 60 * 1000);
        }

        @Override
        public void cancel() {

        }
    }

}
