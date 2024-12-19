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
import org.apache.inlong.sort.tests.utils.MySqlContainer;
import org.apache.inlong.sort.tests.utils.OpenTelemetryContainer;
import org.apache.inlong.sort.tests.utils.PlaceholderResolver;
import org.apache.inlong.sort.tests.utils.StarRocksContainer;
import org.apache.inlong.sort.tests.utils.TestUtils;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container.ExecResult;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.apache.inlong.sort.tests.utils.StarRocksManager.INTER_CONTAINER_STAR_ROCKS_ALIAS;
import static org.apache.inlong.sort.tests.utils.StarRocksManager.STAR_ROCKS_LOG;
import static org.apache.inlong.sort.tests.utils.StarRocksManager.getNewStarRocksImageName;
import static org.apache.inlong.sort.tests.utils.StarRocksManager.initializeStarRocksTable;

/**
 * End-to-end tests for sort-connector-kafka uber jar.
 */
public class Kafka2StarRocksTest extends FlinkContainerTestEnvJRE8 {

    private static final Logger LOG = LoggerFactory.getLogger(Kafka2StarRocksTest.class);

    public static final Logger MYSQL_LOG = LoggerFactory.getLogger(MySqlContainer.class);

    public static final Logger KAFKA_LOG = LoggerFactory.getLogger(KafkaContainer.class);

    private static final Path kafkaJar = TestUtils.getResource("sort-connector-kafka.jar");
    private static final Path mysqlJar = TestUtils.getResource("sort-connector-mysql-cdc.jar");
    private static final Path starrocksJar = TestUtils.getResource("sort-connector-starrocks.jar");
    private static final Path mysqlJdbcJar = TestUtils.getResource("mysql-driver.jar");

    private static final String sqlFile;

    static {
        try {
            URI kafkaSqlFile =
                    Objects.requireNonNull(Kafka2StarRocksTest.class.getResource("/flinkSql/kafka_test.sql")).toURI();
            sqlFile = Paths.get(kafkaSqlFile).toString();
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    @ClassRule
    public static final KafkaContainer KAFKA =
            new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:6.2.1"))
                    .withNetwork(NETWORK)
                    .withNetworkAliases("kafka")
                    .withEmbeddedZookeeper()
                    .withLogConsumer(new Slf4jLogConsumer(KAFKA_LOG));

    @ClassRule
    public static StarRocksContainer STAR_ROCKS =
            (StarRocksContainer) new StarRocksContainer(getNewStarRocksImageName())
                    .withExposedPorts(9030, 8030, 8040)
                    .withNetwork(NETWORK)
                    .withNetworkAliases(INTER_CONTAINER_STAR_ROCKS_ALIAS)
                    .withLogConsumer(new Slf4jLogConsumer(STAR_ROCKS_LOG));

    @ClassRule
    public static final MySqlContainer MYSQL_CONTAINER =
            (MySqlContainer) new MySqlContainer(MySqlContainer.MySqlVersion.V8_0)
                    .withDatabaseName("test")
                    .withNetwork(NETWORK)
                    .withNetworkAliases("mysql")
                    .withLogConsumer(new Slf4jLogConsumer(MYSQL_LOG));

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
        initializeMysqlTable();
        initializeStarRocksTable(STAR_ROCKS);
    }

    private void initializeMysqlTable() {
        try {
            Class.forName(MYSQL_CONTAINER.getDriverClassName());
            Connection conn = DriverManager
                    .getConnection(MYSQL_CONTAINER.getJdbcUrl(), MYSQL_CONTAINER.getUsername(),
                            MYSQL_CONTAINER.getPassword());
            Statement stat = conn.createStatement();
            stat.execute(
                    "CREATE TABLE test_input (\n"
                            + "  id SERIAL,\n"
                            + "  name VARCHAR(255) NOT NULL DEFAULT 'flink',\n"
                            + "  description VARCHAR(512),\n"
                            + "  PRIMARY  KEY(id)\n"
                            + ");");
            stat.close();
            conn.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @AfterClass
    public static void teardown() {
        if (KAFKA != null) {
            KAFKA.stop();
        }

        if (MYSQL_CONTAINER != null) {
            MYSQL_CONTAINER.stop();
        }

        if (STAR_ROCKS != null) {
            STAR_ROCKS.stop();
        }

        if (OPEN_TELEMETRY_CONTAINER != null) {
            OPEN_TELEMETRY_CONTAINER.stop();
        }
    }

    private void initializeKafkaTable(String topic) {
        String fileName = "kafka_test_kafka_init.txt";
        int port = KafkaContainer.ZOOKEEPER_PORT;

        Map<String, Object> properties = new HashMap<>();
        properties.put("TOPIC", topic);
        properties.put("ZOOKEEPER_PORT", port);

        try {
            String createKafkaStatement = getCreateStatement(fileName, properties);
            ExecResult result = KAFKA.execInContainer("bash", "-c", createKafkaStatement);
            LOG.info("Create kafka topic: {}, std: {}", createKafkaStatement, result.getStdout());
            if (result.getExitCode() != 0) {
                throw new RuntimeException("Init kafka topic failed. Exit code:" + result.getExitCode());
            }
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private String getCreateStatement(String fileName, Map<String, Object> properties) {
        URL url = Objects.requireNonNull(Kafka2StarRocksTest.class.getResource("/env/" + fileName));

        try {
            Path file = Paths.get(url.toURI());
            return PlaceholderResolver.getDefaultResolver().resolveByMap(
                    new String(Files.readAllBytes(file), StandardCharsets.UTF_8),
                    properties);
        } catch (IOException | URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Test flink sql mysql cdc to starrocks.
     *
     * @throws Exception The exception may throw when execute the case
     */
    @Test
    public void testKafkaWithSqlFile() throws Exception {
        final String topic = "test-topic";
        initializeKafkaTable(topic);

        submitSQLJob(sqlFile, kafkaJar, starrocksJar, mysqlJar, mysqlJdbcJar);
        waitUntilJobRunning(Duration.ofSeconds(10));

        // generate input
        try (Connection conn = DriverManager.getConnection(MYSQL_CONTAINER.getJdbcUrl(),
                MYSQL_CONTAINER.getUsername(), MYSQL_CONTAINER.getPassword());
                Statement stat = conn.createStatement()) {
            stat.execute("INSERT INTO test_input VALUES (1,'jacket','water resistant white wind breaker');");
            stat.execute("INSERT INTO test_input VALUES (2,'scooter','Big 2-wheel scooter ');");
        } catch (SQLException e) {
            LOG.error("Update table for CDC failed.", e);
            throw e;
        }

        JdbcProxy proxy = new JdbcProxy(STAR_ROCKS.getJdbcUrl(), STAR_ROCKS.getUsername(),
                STAR_ROCKS.getPassword(),
                STAR_ROCKS.getDriverClassName());

        List<String> expectResult = Arrays.asList(
                "1,jacket,water resistant white wind breaker",
                "2,scooter,Big 2-wheel scooter ");
        proxy.checkResultWithTimeout(
                expectResult,
                "test_output1",
                3,
                60000L);
        // check log appender
        String logs = OPEN_TELEMETRY_CONTAINER.getLogs();
        if (!logs.contains("OpenTelemetryLogger installed")) {
            throw new Exception("Failure to append logs to OpenTelemetry");
        }
    }
}