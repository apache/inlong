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

package org.apache.inlong.sort.tests;

import org.apache.inlong.sort.tests.utils.FlinkContainerTestEnvJRE8;
import org.apache.inlong.sort.tests.utils.JdbcProxy;
import org.apache.inlong.sort.tests.utils.OpenTelemetryContainer;
import org.apache.inlong.sort.tests.utils.StarRocksContainer;
import org.apache.inlong.sort.tests.utils.TestUtils;

import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.ClassRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.PulsarContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static org.apache.inlong.sort.tests.utils.StarRocksManager.INTER_CONTAINER_STAR_ROCKS_ALIAS;
import static org.apache.inlong.sort.tests.utils.StarRocksManager.STAR_ROCKS_LOG;
import static org.apache.inlong.sort.tests.utils.StarRocksManager.getNewStarRocksImageName;
import static org.apache.inlong.sort.tests.utils.StarRocksManager.initializeStarRocksTable;

public class Pulsar2StarRocksTest extends FlinkContainerTestEnvJRE8 {

    private static final String PULSAR_TEST_FIRST_MESSAGE =
            "{\"id\": 1, \"name\": \"Alice\", \"description\": \"Hello, Pulsar\"}";
    private static final String PULSAR_TEST_SECOND_MESSAGE =
            "{\"id\": 2, \"name\": \"Bob\", \"description\": \"Goodbye, Pulsar\"}";

    private static final Logger LOG = LoggerFactory.getLogger(Pulsar2StarRocksTest.class);

    public static final Logger PULSAR_LOG = LoggerFactory.getLogger(PulsarContainer.class);
    private static final Path jdbcJar = TestUtils.getResource("sort-connector-starrocks.jar");
    private static final Path mysqlJdbcJar = TestUtils.getResource("mysql-driver.jar");

    private static final Path pulsarJar = TestUtils.getResource("sort-connector-pulsar.jar");

    private static final String sqlFile;

    static {
        try {
            URI pulsarSqlFile = Objects
                    .requireNonNull(Pulsar2StarRocksTest.class.getResource("/flinkSql/pulsar_test.sql")).toURI();
            sqlFile = Paths.get(pulsarSqlFile).toString();
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    @ClassRule
    public static final PulsarContainer PULSAR = new PulsarContainer(DockerImageName.parse("apachepulsar/pulsar:2.8.2"))
            .withNetwork(NETWORK)
            .withNetworkAliases("pulsar")
            .withLogConsumer(new Slf4jLogConsumer(PULSAR_LOG));

    @ClassRule
    public static final StarRocksContainer STAR_ROCKS = (StarRocksContainer) new StarRocksContainer(
            getNewStarRocksImageName())
                    .withExposedPorts(9030, 8030, 8040)
                    .withNetwork(NETWORK)
                    .withNetworkAliases(INTER_CONTAINER_STAR_ROCKS_ALIAS)
                    .withLogConsumer(new Slf4jLogConsumer(STAR_ROCKS_LOG));

    @ClassRule
    public static final OpenTelemetryContainer OPEN_TELEMETRY_CONTAINER =
            (OpenTelemetryContainer) new OpenTelemetryContainer()
                    .withCopyFileToContainer(MountableFile.forClasspathResource("/env/otel-config.yaml"),
                            "/otel-config.yaml")
                    .withCommand("--config=/otel-config.yaml")
                    .withNetwork(NETWORK)
                    .withNetworkAliases("logcollector");

    @Before
    public void setup() {
        waitUntilJobRunning(Duration.ofSeconds(30));
        initializePulsarTopic();
        initializeStarRocksTable(STAR_ROCKS);
    }

    private void initializePulsarTopic() {
        try {
            Container.ExecResult result = PULSAR.execInContainer("bin/pulsar-admin", "topics", "create",
                    "persistent://public/default/test-topic");
            LOG.info("Create Pulsar topic: test-topic, std: {}", result.getStdout());
            if (result.getExitCode() != 0) {
                throw new RuntimeException("Init Pulsar topic failed. Exit code:" + result.getExitCode());
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @AfterClass
    public static void teardown() {
        if (PULSAR != null) {
            PULSAR.stop();
        }
        if (STAR_ROCKS != null) {
            STAR_ROCKS.stop();
        }
        if (OPEN_TELEMETRY_CONTAINER != null) {
            OPEN_TELEMETRY_CONTAINER.stop();
        }
    }

    public void testPulsarToStarRocks() throws Exception {
        submitSQLJob(sqlFile, pulsarJar, jdbcJar, mysqlJdbcJar);
        waitUntilJobRunning(Duration.ofSeconds(10));

        try (PulsarClient pulsarClient = PulsarClient.builder().serviceUrl(PULSAR.getPulsarBrokerUrl()).build()) {
            Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                    .topic("persistent://public/default/test-topic")
                    .create();

            producer.send(PULSAR_TEST_FIRST_MESSAGE);
            producer.send(PULSAR_TEST_SECOND_MESSAGE);

            producer.close();
        }

        JdbcProxy proxy = new JdbcProxy(STAR_ROCKS.getJdbcUrl(), STAR_ROCKS.getUsername(),
                STAR_ROCKS.getPassword(), STAR_ROCKS.getDriverClassName());

        List<String> expectedResult = Arrays.asList(
                "1,Alice,Hello, Pulsar",
                "2,Bob,Goodbye, Pulsar");
        proxy.checkResultWithTimeout(expectedResult, "test_output1", 3, 60000L);
        // check log appender
        String logs = OPEN_TELEMETRY_CONTAINER.getLogs();
        if (!logs.contains("OpenTelemetryLogger installed")) {
            throw new Exception("Failure to append logs to OpenTelemetry");
        }
    }

}
