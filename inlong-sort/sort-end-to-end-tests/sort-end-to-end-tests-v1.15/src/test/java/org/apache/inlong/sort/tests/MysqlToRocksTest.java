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

import org.apache.inlong.sort.tests.utils.*;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

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
import java.util.stream.Stream;

/**
 * End-to-end tests for sort-connector-postgres-cdc-v1.15 uber jar.
 * Test flink sql Postgres cdc to StarRocks
 */
public class MysqlToRocksTest extends FlinkContainerTestEnv {

    private static final Logger LOG = LoggerFactory.getLogger(PostgresToStarRocksTest.class);

    private static final Path mysqlJar = TestUtils.getResource("sort-connector-mysql-cdc.jar");
    private static final Path jdbcJar = TestUtils.getResource("sort-connector-starrocks.jar");
    private static final Path mysqlJdbcJar = TestUtils.getResource("mysql-driver.jar");

    private static final Logger STAR_ROCKS_LOG = LoggerFactory.getLogger(StarRocksContainer.class);

    private static final String sqlFile;

    // ----------------------------------------------------------------------------------------
    // StarRocks Variables
    // ----------------------------------------------------------------------------------------
    private static final String INTER_CONTAINER_STAR_ROCKS_ALIAS = "starrocks";
    private static final String NEW_STARROCKS_REPOSITORY = "inlong-starrocks";
    private static final String NEW_STARROCKS_TAG = "latest";
    private static final String STAR_ROCKS_IMAGE_NAME = "starrocks/allin1-ubi:3.0.4";

    static {
        try {
            sqlFile = Paths.get(PostgresToStarRocksTest.class.getResource("/flinkSql/mysql_test.sql").toURI()).toString();
            buildStarRocksImage();
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    private static String getNewStarRocksImageName() {
        return NEW_STARROCKS_REPOSITORY + ":" + NEW_STARROCKS_TAG;
    }

    public static void buildStarRocksImage() {
        GenericContainer oldStarRocks = new GenericContainer(STAR_ROCKS_IMAGE_NAME);
        Startables.deepStart(Stream.of(oldStarRocks)).join();
        oldStarRocks.copyFileToContainer(MountableFile.forClasspathResource("/docker/starrocks/start_fe_be.sh"),
                "/data/deploy/");
        try {
            oldStarRocks.execInContainer("chmod", "+x", "/data/deploy/start_fe_be.sh");
        } catch (Exception e) {
            e.printStackTrace();
        }
        oldStarRocks.getDockerClient()
                .commitCmd(oldStarRocks.getContainerId())
                .withRepository(NEW_STARROCKS_REPOSITORY)
                .withTag(NEW_STARROCKS_TAG).exec();
        oldStarRocks.stop();
    }

    @ClassRule
    public static StarRocksContainer STAR_ROCKS = (StarRocksContainer) new StarRocksContainer(getNewStarRocksImageName())
            .withExposedPorts(9030, 8030, 8040)
            .withNetwork(NETWORK)
            .withAccessToHost(true)
            .withNetworkAliases(INTER_CONTAINER_STAR_ROCKS_ALIAS)
            .withLogConsumer(new Slf4jLogConsumer(STAR_ROCKS_LOG));

    @ClassRule
    public static final MySqlContainer MYSQL_CONTAINER = (MySqlContainer)new MySqlContainer(MySqlContainer.MySqlVersion.V8_0)
            .withDatabaseName("test")
            .withNetwork(NETWORK)
            .withNetworkAliases("mysql")
            .withLogConsumer(new Slf4jLogConsumer(LOG));

    @Before
    public void setup() {
        waitUntilJobRunning(Duration.ofSeconds(30));
        initializeMysqlTable();
        initializeStarRocksTable();
    }

    private void initializeMysqlTable() {
        try {
            Class.forName(MYSQL_CONTAINER.getDriverClassName());
            Connection conn = DriverManager
                    .getConnection(MYSQL_CONTAINER.getJdbcUrl(), MYSQL_CONTAINER.getUsername(),
                            MYSQL_CONTAINER.getPassword());
            Statement stat = conn.createStatement();
            stat.execute(
                    "CREATE TABLE test_input1 (\n"
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

    private void initializeStarRocksTable() {
        try (Connection conn =
                     DriverManager.getConnection(STAR_ROCKS.getJdbcUrl(), STAR_ROCKS.getUsername(),
                             STAR_ROCKS.getPassword());
             Statement stat = conn.createStatement()) {
            stat.execute("CREATE TABLE IF NOT EXISTS test_output1 (\n"
                    + "       id INT NOT NULL,\n"
                    + "       name VARCHAR(255) NOT NULL DEFAULT 'flink',\n"
                    + "       description VARCHAR(512)\n"
                    + ")\n"
                    + "PRIMARY KEY(id)\n"
                    + "DISTRIBUTED by HASH(id) PROPERTIES (\"replication_num\" = \"1\");");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @AfterClass
    public static void teardown() {
        if (MYSQL_CONTAINER != null) {
            MYSQL_CONTAINER.stop();
        }
        if (STAR_ROCKS != null) {
            STAR_ROCKS.stop();
        }
    }

    /**
     * Test flink sql postgresql cdc to StarRocks
     *
     * @throws Exception The exception may throws when execute the case
     */
    @Test
    public void testMysqlUpdateAndDelete() throws Exception {
        submitSQLJob(sqlFile, jdbcJar, mysqlJar, mysqlJdbcJar);
        waitUntilJobRunning(Duration.ofSeconds(10));

        // generate input
        try (Connection conn =
                     DriverManager.getConnection(MYSQL_CONTAINER.getJdbcUrl(), MYSQL_CONTAINER.getUsername(),
                             MYSQL_CONTAINER.getPassword());
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
                new JdbcProxy(STAR_ROCKS.getJdbcUrl(), STAR_ROCKS.getUsername(),
                        STAR_ROCKS.getPassword(),
                        STAR_ROCKS.getDriverClassName());
        List<String> expectResult =
                Arrays.asList("2,tom,Big 2-wheel scooter ");
        proxy.checkResultWithTimeout(
                expectResult,
                "test_output1",
                3,
                60000L);
    }
}
