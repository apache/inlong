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
import org.apache.kafka.clients.consumer.ConsumerRecord;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.collection.mutable.ArraySeq;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
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
import static org.apache.inlong.sort.singletenant.flink.utils.NetUtils.getUnusedLocalPort;
import static org.junit.Assert.assertNull;

public abstract class KafkaSinkTestBase {

    private static final Logger logger = LoggerFactory.getLogger(KafkaSinkTestBase.class);

    @ClassRule
    public static TemporaryFolder tempFolder = new TemporaryFolder();

    private final int zkTimeout = 30000;

    private TestingServer zkServer;

    private KafkaServer kafkaServer;
    private String brokerConnStr;
    private AdminClient kafkaAdmin;
    private KafkaConsumer<String, String> kafkaConsumer;
    private Properties kafkaClientProperties;

    // prepare data below in subclass
    protected String topic;
    protected List<Row> testRows;
    protected FieldInfo[] fieldInfos;
    protected SerializationInfo serializationInfo;

    @Before
    public void setup() throws Exception {
        prepareData();
        logger.info("Prepare data passed.");

        startZK();
        logger.info("ZK started.");
        startKafkaServer();
        logger.info("Kafka server started.");
        prepareKafkaClientProps();
        logger.info("Kafka client properties prepared.");
        kafkaAdmin = AdminClient.create(kafkaClientProperties);
        logger.info("Kafka admin started.");
        addTopic();
        logger.info("Topic added to kafka server.");
        kafkaConsumer = new KafkaConsumer<>(kafkaClientProperties);
        logger.info("Kafka consumer started.");
    }

    private void startZK() throws Exception {
        zkServer = new TestingServer(-1, tempFolder.newFolder());
        zkServer.start();
    }

    private void startKafkaServer() throws IOException {
        Properties kafkaProperties = new Properties();
        final String KAFKA_HOST = "localhost";
        kafkaProperties.put("advertised.host.name", KAFKA_HOST);
        kafkaProperties.put("port", Integer.toString(getUnusedLocalPort(1024)));
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
        logger.info("Kafka broker conn str = " + brokerConnStr);
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

    protected abstract void prepareData();

    @After
    public void clean() throws IOException {
        if (kafkaConsumer != null) {
            kafkaConsumer.close();
            kafkaConsumer = null;
            logger.info("Kafka consumer closed.");
        }

        if (kafkaAdmin != null) {
            kafkaAdmin.close();
            kafkaAdmin = null;
            logger.info("Kafka admin closed.");
        }

        if (kafkaServer != null) {
            kafkaServer.shutdown();
            kafkaServer = null;
            logger.info("Kafka server closed.");
        }

        if (zkServer != null) {
            zkServer.close();
            zkServer = null;
            logger.info("ZK closed.");
        }
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
        List<String> results = new ArrayList<>();
        while (true) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofSeconds(1));
            if (!records.isEmpty()) {
                for (ConsumerRecord<String, String> record : records) {
                    assertNull(record.key());
                    results.add(record.value());
                }
            }

            if (results.size() != testRows.size()) {
                //noinspection BusyWait
                Thread.sleep(1000);
                logger.info("for topic " + topic + ", record size = " + results.size());
                continue;
            }

            verifyData(results);

            break;
        }
    }

    protected abstract void verifyData(List<String> results);

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
