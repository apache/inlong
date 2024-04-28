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
import org.apache.inlong.sort.tests.utils.MSSQLServerContainer;
import org.apache.inlong.sort.tests.utils.StarRocksContainer;
import org.apache.inlong.sort.tests.utils.TestUtils;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

import static org.apache.inlong.sort.tests.utils.StarRocksManager.INTER_CONTAINER_STAR_ROCKS_ALIAS;
import static org.apache.inlong.sort.tests.utils.StarRocksManager.STAR_ROCKS_LOG;
import static org.apache.inlong.sort.tests.utils.StarRocksManager.getNewStarRocksImageName;
import static org.apache.inlong.sort.tests.utils.StarRocksManager.initializeStarRocksTable;

public class Sqlserver2StarRocksTest extends FlinkContainerTestEnvJRE8 {

    private static final Logger LOG = LoggerFactory.getLogger(Sqlserver2StarRocksTest.class);

    private static final Path sqlserverJar = TestUtils.getResource("sort-connector-sqlserver-cdc.jar");
    private static final Path jdbcJar = TestUtils.getResource("sort-connector-starrocks.jar");

    private static final Path mysqlJar = TestUtils.getResource("mysql-driver.jar");

    private static final String sqlFile;

    static {
        try {
            sqlFile = Paths.get(Sqlserver2StarRocksTest.class.getResource("/flinkSql/sqlserver_test.sql").toURI())
                    .toString();
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    @ClassRule
    public static StarRocksContainer STAR_ROCKS =
            (StarRocksContainer) new StarRocksContainer(getNewStarRocksImageName())
                    .withExposedPorts(9030, 8030, 8040)
                    .withNetwork(NETWORK)
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
        initializeStarRocksTable(STAR_ROCKS);
    }

    private void initializeSqlserverTable() {
        try {
            // Waiting for MSSQL Agent started.
            LOG.info("Sleep until the MSSQL Agent is started...");
            Thread.sleep(20 * 1000);
            LOG.info("Now continue initialize task...");
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
        submitSQLJob(sqlFile, jdbcJar, sqlserverJar, mysqlJar);
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
            stat.execute(
                    "SET IDENTITY_INSERT test_input1 ON;" +
                            "INSERT INTO test_input1 (id, name, description) " +
                            "VALUES (2,'scooter','Big 2-wheel scooter ');" +
                            "SET IDENTITY_INSERT test_input1 OFF;");
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