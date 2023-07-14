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

import org.apache.inlong.sort.tests.utils.FlinkContainerTestEnv;
import org.apache.inlong.sort.tests.utils.JdbcProxy;
import org.apache.inlong.sort.tests.utils.TestUtils;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.utility.DockerImageName;

import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;

/**
 * End-to-end tests for sort-connector-postgres-cdc-v1.15 uber jar.
 */
public class PostgresTest extends FlinkContainerTestEnv {

    private static final Logger LOG = LoggerFactory.getLogger(PostgresTest.class);

    private static final Path postgresJar = TestUtils.getResource("sort-connector-postgres-cdc.jar");
    private static final Path jdbcJar = TestUtils.getResource("sort-connector-jdbc.jar");
    private static final Path mysqlJdbcJar = TestUtils.getResource("mysql-driver.jar");

    private static final String sqlFile;

    static {
        try {
            sqlFile = Paths.get(PostgresTest.class.getResource("/flinkSql/postgres_test.sql").toURI()).toString();
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    @ClassRule
    public static final PostgreSQLContainer POSTGRES_CONTAINER = (PostgreSQLContainer) new PostgreSQLContainer(
            DockerImageName.parse("debezium/postgres:13").asCompatibleSubstituteFor("postgres"))
                    .withUsername("flinkuser")
                    .withPassword("flinkpw")
                    .withInitScript("docker/postgresql/setup.sql")
                    .withDatabaseName("test")
                    .withNetwork(NETWORK)
                    .withNetworkAliases("postgres")
                    .withLogConsumer(new Slf4jLogConsumer(LOG));

    @Before
    public void setup() {
        initializePostgresTable();
        initializeMysqlTable();
    }

    private void initializePostgresTable() {
        try {
            Class.forName(POSTGRES_CONTAINER.getDriverClassName());
            Connection conn = DriverManager
                    .getConnection(POSTGRES_CONTAINER.getJdbcUrl(), POSTGRES_CONTAINER.getUsername(),
                            POSTGRES_CONTAINER.getPassword());
            Statement stat = conn.createStatement();
            stat.execute(
                    "CREATE TABLE test_input1 (\n"
                            + "  id INTEGER PRIMARY KEY NOT NULL,\n"
                            + "  name VARCHAR(255) NOT NULL DEFAULT 'flink',\n"
                            + "  description VARCHAR(512)\n"
                            + ");");
            stat.close();
            conn.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void initializeMysqlTable() {
        try (Connection conn =
                DriverManager.getConnection(MYSQL.getJdbcUrl(), MYSQL.getUsername(), MYSQL.getPassword());
                Statement stat = conn.createStatement()) {
            stat.execute("CREATE TABLE IF NOT EXISTS test_output1 (\n"
                    + "       id INT PRIMARY KEY,\n"
                    + "       name VARCHAR(255) NOT NULL DEFAULT 'flink',\n"
                    + "       description VARCHAR(512)\n"
                    + ")\n"
                    + "ENGINE=InnoDB DEFAULT CHARSET=utf8;");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @AfterClass
    public static void teardown() {
        if (POSTGRES_CONTAINER != null) {
            POSTGRES_CONTAINER.stop();
        }
    }

    /**
     * Test flink sql mysql cdc to postgresql
     *
     * @throws Exception The exception may throws when execute the case
     */
    @Test
    public void testPostgresUpdateAndDelete() throws Exception {
        submitSQLJob(sqlFile, jdbcJar, postgresJar, mysqlJdbcJar);
        waitUntilJobRunning(Duration.ofSeconds(30));

        // generate input
        try (Connection conn =
                DriverManager.getConnection(POSTGRES_CONTAINER.getJdbcUrl(), POSTGRES_CONTAINER.getUsername(),
                        POSTGRES_CONTAINER.getPassword());
                Statement stat = conn.createStatement()) {
            stat.execute(
                    "INSERT INTO test_input1 "
                            + "VALUES (1,'jacket','water resistent white wind breaker');");
            stat.execute(
                    "INSERT INTO test_input1 VALUES (2,'scooter','Big 2-wheel scooter ');");
            stat.execute(
                    "update test_input1 set name = 'tom' where id = 2;");
            stat.execute(
                    "delete from test_input1 where id = 1;");
        } catch (SQLException e) {
            LOG.error("Update table for CDC failed.", e);
            throw e;
        }

        JdbcProxy proxy =
                new JdbcProxy(MYSQL.getJdbcUrl(), MYSQL.getUsername(),
                        MYSQL.getPassword(),
                        MYSQL.getDriverClassName());
        List<String> expectResult =
                Arrays.asList("2,tom,Big 2-wheel scooter ");
        proxy.checkResultWithTimeout(
                expectResult,
                "test_output1",
                3,
                60000L);
    }
}
