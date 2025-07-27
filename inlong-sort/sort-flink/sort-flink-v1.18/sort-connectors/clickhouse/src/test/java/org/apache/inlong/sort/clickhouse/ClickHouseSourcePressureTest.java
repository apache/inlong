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

package org.apache.inlong.sort.clickhouse;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.inlong.sort.clickhouse.source.ClickHouseSource;
import org.apache.inlong.sort.clickhouse.source.ClickHouseSourceConfig;
import com.ververica.cdc.connectors.base.options.StartupOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class ClickHouseSourcePressureTest {
    private static final Logger LOG = LoggerFactory.getLogger(ClickHouseSourcePressureTest.class);
    private static final String CLICKHOUSE_URL = "jdbc:clickhouse://localhost:8123/default";
    private static final String USERNAME = "default";
    private static final String PASSWORD = "";
    private static final int DEFAULT_PARALLELISM = 2;
    private static final int TEST_DATA_SIZE = 100000;
    private static final long METRIC_REPORT_INTERVAL = 1000;

    public static void main(String[] args) {
        try {
            Configuration config = parseArgs(args);
            createTableAndInsertData(config);
            StreamExecutionEnvironment env = createExecutionEnvironment(config);
            ClickHouseSourceConfig sourceConfig = buildClickHouseSourceConfig(config);
            SourceFunction<String> source = new ClickHouseSource(sourceConfig);
            DataStreamSource<String> sourceStream = (DataStreamSource<String>) env.addSource(source)
                    .name("ClickHouseSource")
                    .uid("clickhouse-source-uid");
            sourceStream
                    .map(new PerformanceMonitor())
                    .name("PerformanceMonitor")
                    .uid("performance-monitor-uid")
                    .name("SinkPrint")
                    .uid("sink-print-uid");
            env.execute("Enhanced ClickHouse Source Pressure Test");
        } catch (Exception e) {
            LOG.error("Job execution failed", e);
            System.exit(1);
        }
    }

    private static Configuration parseArgs(String[] args) {
        Configuration config = new Configuration();
        config.setInteger("parallelism", DEFAULT_PARALLELISM);
        config.setString("clickhouse.url", CLICKHOUSE_URL);
        config.setString("clickhouse.username", USERNAME);
        config.setString("clickhouse.password", PASSWORD);
        return config;
    }

    private static StreamExecutionEnvironment createExecutionEnvironment(Configuration config) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        int parallelism = config.getInteger("parallelism", DEFAULT_PARALLELISM);
        env.setParallelism(parallelism);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                5,
                Time.of(30, TimeUnit.SECONDS)
        ));
        env.enableCheckpointing(5000);
        env.getConfig().setGlobalJobParameters(config);
        return env;
    }

    private static ClickHouseSourceConfig buildClickHouseSourceConfig(Configuration config) {
        Properties dbzProperties = new Properties();
        dbzProperties.setProperty("database.history.store.only.monitored.tables.ddl", "true");
        dbzProperties.setProperty("database.history.skip.unparseable.ddl", "true");
        dbzProperties.setProperty("snapshot.locking.mode", "minimal");
        int parallelism = config.getInteger("parallelism", DEFAULT_PARALLELISM);
        return new ClickHouseSourceConfig(
                StartupOptions.initial(),
                Collections.singletonList("default"),
                Collections.singletonList("my_table"),
                5000,
                1,
                1.5,
                0.5,
                false,
                dbzProperties,
                "com.clickhouse.jdbc.ClickHouseDriver",
                "localhost",
                8123,
                "default",
                "",
                20000,
                "UTC",
                Duration.ofSeconds(30),
                5,
                parallelism,
                "id",
                null,
                null,
                "SELECT * FROM my_table WHERE id > ${last_offset} ORDER BY id ASC LIMIT 5000"
        );
    }

    private static void createTableAndInsertData(Configuration config) {
        String url = config.getString("clickhouse.url", CLICKHOUSE_URL);
        String username = config.getString("clickhouse.username", USERNAME);
        String password = config.getString("clickhouse.password", PASSWORD);

        try (Connection connection = getClickHouseConnection(url, username, password)) {
            if (!tableExists(connection, "my_table")) {
                createTable(connection);
                insertTestData(connection, TEST_DATA_SIZE);
                LOG.info("Successfully created table and inserted {} test records", TEST_DATA_SIZE);
            } else {
                int recordCount = getRecordCount(connection);
                if (recordCount < TEST_DATA_SIZE) {
                    truncateTable(connection);
                    insertTestData(connection, TEST_DATA_SIZE);
                    LOG.info("Table already exists but with insufficient data. Inserted {} records", TEST_DATA_SIZE);
                } else {
                    LOG.info("Table already exists with {} records. Skipping data insertion.", recordCount);
                }
            }
        } catch (SQLException e) {
            LOG.error("Failed to initialize test table", e);
            throw new RuntimeException("Test table initialization failed", e);
        }
    }

    private static Connection getClickHouseConnection(String url, String username, String password) throws SQLException {
        Properties props = new Properties();
        props.setProperty("user", username);
        props.setProperty("password", password);
        props.setProperty("connectionTimeout", "30000");
        props.setProperty("socketTimeout", "60000");
        return DriverManager.getConnection(url, props);
    }

    private static boolean tableExists(Connection connection, String tableName) throws SQLException {
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT name FROM system.tables WHERE database = 'default' AND name = '" + tableName + "'")) {
            return rs.next();
        }
    }

    private static void createTable(Connection connection) throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            String createTableSQL =
                    "CREATE TABLE default.my_table (" +
                            "    id Int64," +
                            "    name String," +
                            "    create_time DateTime," +
                            "    PRIMARY KEY (id)" +
                            ") ENGINE = MergeTree() " +
                            "ORDER BY id " +
                            "SETTINGS index_granularity = 8192";
            stmt.execute(createTableSQL);
        }
    }

    private static void insertTestData(Connection connection, int recordCount) throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            String insertSQL =
                    "INSERT INTO default.my_table (id, name, create_time) " +
                            "SELECT number, 'test_' || toString(number), now() " +
                            "FROM numbers(" + recordCount + ")";
            stmt.execute(insertSQL);
        }
    }

    private static int getRecordCount(Connection connection) throws SQLException {
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM my_table")) {
            if (rs.next()) {
                return rs.getInt(1);
            }
            return 0;
        }
    }

    private static void truncateTable(Connection connection) throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("TRUNCATE TABLE my_table");
        }
    }

    public static class PerformanceMonitor implements MapFunction<String, String> {
        private static final long serialVersionUID = 1L;
        private transient long count;
        private transient long startTime;
        private transient long lastReportTime;
        private transient long totalBytes;

        @Override
        public String map(String value) throws Exception {
            count++;
            totalBytes += value.getBytes().length;
            long currentTime = System.currentTimeMillis();
            if (currentTime - lastReportTime >= METRIC_REPORT_INTERVAL) {
                long elapsedTime = currentTime - startTime;
                long recordsPerSecond = (long) ((count / (elapsedTime / 1000.0)));
                long throughput = (long) ((totalBytes / (elapsedTime / 1000.0)) / 1024);
                LOG.info("Processed {} records | Throughput: {} records/sec, {} KB/s",
                        count, recordsPerSecond, throughput);
                System.out.println("Processed " + count + " records | Throughput: " + recordsPerSecond + " records/sec, " + throughput + " KB/s");
                startTime = currentTime;
                lastReportTime = currentTime;
                count = 0;
                totalBytes = 0;
            }
            return value;
        }
    }
}