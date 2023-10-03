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
import org.apache.inlong.sort.tests.utils.MSSQLServerContainer;
import org.apache.inlong.sort.tests.utils.StarRocksContainer;
import org.apache.inlong.sort.tests.utils.TestUtils;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
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

public class SqlserverToStarRocksTest extends FlinkContainerTestEnv {

    private static final Logger LOG = LoggerFactory.getLogger(PostgresToStarRocksTest.class);

    private static final Path sqlserverJar = TestUtils.getResource("sort-connector-sqlserver-cdc.jar");
    private static final Path jdbcJar = TestUtils.getResource("sort-connector-starrocks.jar");
    private static final Path mssqlJar = TestUtils.getResource("mssql-driver.jar");

    private static final Path mysqlJar = TestUtils.getResource("mysql-driver.jar");

    private static final Logger STAR_ROCKS_LOG = LoggerFactory.getLogger(StarRocksContainer.class);

    private static final String sqlFile;

    private static final String sqlserverSetupFile;

    // ----------------------------------------------------------------------------------------
    // StarRocks Variables
    // ----------------------------------------------------------------------------------------
    private static final String INTER_CONTAINER_STAR_ROCKS_ALIAS = "starrocks";
    private static final String NEW_STARROCKS_REPOSITORY = "inlong-starrocks";
    private static final String NEW_STARROCKS_TAG = "latest";
    private static final String STAR_ROCKS_IMAGE_NAME = "starrocks/allin1-ubi:3.0.4";

    static {
        try {
            sqlFile = Paths.get(SqlserverToStarRocksTest.class.getResource("/flinkSql/sqlserver_test.sql").toURI())
                    .toString();
            sqlserverSetupFile = Paths
                    .get(SqlserverToStarRocksTest.class.getResource("/docker/sqlserver/setup.sql").toURI()).toString();
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
    public static StarRocksContainer STAR_ROCKS =
            (StarRocksContainer) new StarRocksContainer(getNewStarRocksImageName())
                    .withExposedPorts(9030, 8030, 8040)
                    .withNetwork(NETWORK)
                    .withAccessToHost(true)
                    .withNetworkAliases(INTER_CONTAINER_STAR_ROCKS_ALIAS)
                    .withLogConsumer(new Slf4jLogConsumer(STAR_ROCKS_LOG));

    @ClassRule
    public static final MSSQLServerContainer SQLSERVER_CONTAINER = (MSSQLServerContainer) new MSSQLServerContainer(
            DockerImageName.parse("mcr.microsoft.com/mssql/server").withTag("2022-latest"))
                    .acceptLicense()
                    .withNetwork(NETWORK)
                    .withNetworkAliases("sqlserver")
                    .withLogConsumer(new Slf4jLogConsumer(LOG));

    @Before
    public void setup() {
        waitUntilJobRunning(Duration.ofSeconds(30));
        initializeSqlserverTable();
        initializeStarRocksTable();
    }

    private void initializeSqlserverTable() {
        try {
            Class.forName(SQLSERVER_CONTAINER.getDriverClassName());
            Connection conn = DriverManager
                    .getConnection(SQLSERVER_CONTAINER.getJdbcUrl(), SQLSERVER_CONTAINER.getUsername(),
                            SQLSERVER_CONTAINER.getPassword());
            Statement stat = conn.createStatement();
            stat.execute("CREATE DATABASE test;");
            stat.execute("USE test;");
            stat.execute(
                    "CREATE TABLE test_input1 (\n" +
                            "    id INT IDENTITY(1,1) NOT NULL PRIMARY KEY,\n" +
                            "    name NVARCHAR(255) NOT NULL DEFAULT 'flink',\n" +
                            "    description NVARCHAR(512)\n" +
                            ");");
            stat.execute("if exists(select 1 from sys.databases where name='test' and is_cdc_enabled=0)\n" +
                    "begin\n" +
                    "    exec sys.sp_cdc_enable_db\n" +
                    "end");
            stat.execute("IF EXISTS(SELECT 1 FROM sys.tables WHERE name='test_input1' AND is_tracked_by_cdc = 0)\n" +
                    "BEGIN\n" +
                    "    EXEC sys.sp_cdc_enable_table\n" +
                    "        @source_schema = 'dbo', -- source_schema\n" +
                    "        @source_name = 'test_input1', -- table_name\n" +
                    "        @capture_instance = NULL, -- capture_instance\n" +
                    "        @supports_net_changes = '1', -- capture_instance\n" +
                    "        @index_name = NULL,  -- \n" +
                    "        @captured_column_list  = NULL, -- \n" +
                    "        @filegroup_name = 'PRIMARY', -- \n" +
                    "        @role_name = NULL -- role_name\n" +
                    "END");
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
                    + "       description VARCHAR(512)\n,"
                    + "       tag VARCHAR(255)\n"
                    + ")\n"
                    + "PRIMARY KEY(id)\n"
                    + "DISTRIBUTED by HASH(id) PROPERTIES (\"replication_num\" = \"1\");");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @AfterClass
    public static void teardown() {
        if (SQLSERVER_CONTAINER != null) {
            SQLSERVER_CONTAINER.stop();
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
    public void testSqlserverUpdateAndDelete() throws Exception {
        submitSQLJob(sqlFile, jdbcJar, sqlserverJar, mssqlJar, mysqlJar);
        waitUntilJobRunning(Duration.ofSeconds(10));

        // generate input
        try (Connection conn =
                DriverManager.getConnection(SQLSERVER_CONTAINER.getJdbcUrl(), SQLSERVER_CONTAINER.getUsername(),
                        SQLSERVER_CONTAINER.getPassword());
                Statement stat = conn.createStatement()) {
            stat.execute("USE test;");
            stat.execute(
                    "SET IDENTITY_INSERT test_input1 ON;" +
                            "INSERT INTO test_input1 (id, name, description) "
                            + "VALUES (1, 'jacket','water resistent white wind breaker');" +
                            "SET IDENTITY_INSERT test_input1 OFF;");
        } catch (SQLException e) {
            LOG.error("Update table for CDC failed.", e);
            throw e;
        }

        JdbcProxy proxy =
                new JdbcProxy(STAR_ROCKS.getJdbcUrl(), STAR_ROCKS.getUsername(),
                        STAR_ROCKS.getPassword(),
                        STAR_ROCKS.getDriverClassName());
        List<String> expectResult =
                Arrays.asList("1,jacket,water resistent white wind breaker");
        proxy.checkResultWithTimeout(
                expectResult,
                "test_output1",
                3,
                60000L);
    }
}