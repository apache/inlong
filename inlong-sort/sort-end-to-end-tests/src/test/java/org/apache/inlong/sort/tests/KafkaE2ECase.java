/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.inlong.sort.tests;

import org.apache.inlong.sort.tests.utils.FlinkContainerTestEnv;
import org.apache.inlong.sort.tests.utils.JdbcProxy;
import org.apache.inlong.sort.tests.utils.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container.ExecResult;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * End-to-end tests for sort-connector-kafka uber jar.
 */
public class KafkaE2ECase extends FlinkContainerTestEnv {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaE2ECase.class);

    private static final Path kafkaJar = TestUtils.getResource("sort-connector-kafka.jar");
    private static final Path jdbcJar = TestUtils.getResource("sort-connector-jdbc.jar");
    private static final Path mysqlJar = TestUtils.getResource("sort-connector-mysql-cdc.jar");
    private static final Path mysqlJdbcJar = TestUtils.getResource("mysql-driver.jar");
    // Can't use getResource("xxx").getPath(), windows will don't know that path
    private static final String sqlFile;

    static {
        try {
            sqlFile = Paths.get(KafkaE2ECase.class.getResource("/flinkSql/kafka_test.sql").toURI()).toString();
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    private static final String TOPIC = "test-topic";

    @ClassRule
    public static final KafkaContainer KAFKA =
            new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:6.2.1"))
                    .withNetwork(NETWORK)
                    .withNetworkAliases("kafka")
                    .withEmbeddedZookeeper()
                    .withLogConsumer(new Slf4jLogConsumer(LOG));

    @Before
    public void setup() {
        initializeMysqlTable();
        initializeKafkaTable();
    }

    @After
    public void teardown() {
        if (KAFKA != null) {
            KAFKA.stop();
        }
    }

    private void initializeKafkaTable() {
        List<String> commands = new ArrayList<>();
        commands.add("kafka-topics");
        commands.add("--create");
        commands.add("--topic");
        commands.add(TOPIC);
        commands.add("--replication-factor 1");
        commands.add("--partitions 1");
        commands.add("--zookeeper");
        commands.add("localhost:" + KafkaContainer.ZOOKEEPER_PORT);
        try {
            LOG.info(String.join(" ", commands));
            ExecResult result = KAFKA.execInContainer("bash", "-c", String.join(" ", commands));
            LOG.info(result.getStdout());
            if (result.getExitCode() != 0) {
                throw new RuntimeException("Init kafka topic failed. Exit code:" + result.getExitCode());
            }
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private void initializeMysqlTable() {
        try (Connection conn =
                DriverManager.getConnection(MYSQL.getJdbcUrl(), MYSQL.getUsername(), MYSQL.getPassword());
                Statement stat = conn.createStatement()) {
            stat.execute(
                    "CREATE TABLE test_input (\n"
                            + "  id INTEGER NOT NULL,\n"
                            + "  name VARCHAR(255) NOT NULL DEFAULT 'flink',\n"
                            + "  description VARCHAR(512),\n"
                            + "  weight FLOAT,\n"
                            + "  enum_c enum('red', 'white') default 'red',  -- test some complex types as well,\n"
                            + "  json_c JSON, -- because we use additional dependencies to deserialize complex types.\n"
                            + "  point_c POINT\n"
                            + ");");
            stat.execute(
                    "CREATE TABLE test_output (\n"
                            + "  id INTEGER NOT NULL AUTO_INCREMENT PRIMARY KEY,\n"
                            + "  name VARCHAR(255) NOT NULL DEFAULT 'flink',\n"
                            + "  description VARCHAR(512),\n"
                            + "  weight FLOAT,\n"
                            + "  enum_c VARCHAR(255),\n"
                            + "  json_c VARCHAR(255),\n"
                            + "  point_c VARCHAR(255)\n"
                            + ");");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }


    /**
     * Test flink sql mysql cdc to hive
     *
     * @throws Exception The exception may throws when execute the case
     */
    @Test
    public void testKafka() throws Exception {
        submitSQLJob(sqlFile, kafkaJar, jdbcJar, mysqlJar, mysqlJdbcJar);
        waitUntilJobRunning(Duration.ofSeconds(30));

        // generate input
        try (Connection conn =
                DriverManager.getConnection(MYSQL.getJdbcUrl(), MYSQL.getUsername(), MYSQL.getPassword());
                Statement stat = conn.createStatement()) {
            stat.execute(
                    "INSERT INTO test_input "
                            + "VALUES (1,'jacket','water resistent white wind breaker',0.2, null, null, null);");
            stat.execute(
                    "INSERT INTO test_input VALUES (2,'scooter','Big 2-wheel scooter ',5.18, null, null, null);");
        } catch (SQLException e) {
            LOG.error("Update table for CDC failed.", e);
            throw e;
        }

        // validate output
        JdbcProxy proxy =
                new JdbcProxy(MYSQL.getJdbcUrl(), MYSQL.getUsername(), MYSQL.getPassword(), MYSQL_DRIVER_CLASS);
        List<String> expectResult =
                Arrays.asList(
                        "1,jacket,water resistent white wind breaker,0.2,,,",
                        "2,scooter,Big 2-wheel scooter ,5.18,,,");
        proxy.checkResultWithTimeout(
                expectResult,
                "test_output",
                7,
                60000L);
    }

}
