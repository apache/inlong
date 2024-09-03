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

package org.apache.inlong.audit.sink;

import org.apache.inlong.audit.channel.DataQueue;
import org.apache.inlong.audit.config.Configuration;
import org.apache.inlong.audit.entities.SinkConfig;
import org.apache.inlong.audit.entities.StatData;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.apache.inlong.audit.config.ConfigConstants.CACHE_PREP_STMTS;
import static org.apache.inlong.audit.config.ConfigConstants.DEFAULT_AUDIT_DATA_TEMP_STORAGE_DAYS;
import static org.apache.inlong.audit.config.ConfigConstants.DEFAULT_CACHE_PREP_STMTS;
import static org.apache.inlong.audit.config.ConfigConstants.DEFAULT_CHECK_PARTITION_INTERVAL_HOURS;
import static org.apache.inlong.audit.config.ConfigConstants.DEFAULT_CONNECTION_TIMEOUT;
import static org.apache.inlong.audit.config.ConfigConstants.DEFAULT_DATASOURCE_POOL_SIZE;
import static org.apache.inlong.audit.config.ConfigConstants.DEFAULT_ENABLE_MANAGE_PARTITIONS;
import static org.apache.inlong.audit.config.ConfigConstants.DEFAULT_PREP_STMT_CACHE_SIZE;
import static org.apache.inlong.audit.config.ConfigConstants.DEFAULT_PREP_STMT_CACHE_SQL_LIMIT;
import static org.apache.inlong.audit.config.ConfigConstants.DEFAULT_QUEUE_PULL_TIMEOUT;
import static org.apache.inlong.audit.config.ConfigConstants.DEFAULT_SOURCE_DB_SINK_BATCH;
import static org.apache.inlong.audit.config.ConfigConstants.DEFAULT_SOURCE_DB_SINK_INTERVAL;
import static org.apache.inlong.audit.config.ConfigConstants.KEY_AUDIT_DATA_TEMP_STORAGE_DAYS;
import static org.apache.inlong.audit.config.ConfigConstants.KEY_CACHE_PREP_STMTS;
import static org.apache.inlong.audit.config.ConfigConstants.KEY_CHECK_PARTITION_INTERVAL_HOURS;
import static org.apache.inlong.audit.config.ConfigConstants.KEY_DATASOURCE_CONNECTION_TIMEOUT;
import static org.apache.inlong.audit.config.ConfigConstants.KEY_DATASOURCE_POOL_SIZE;
import static org.apache.inlong.audit.config.ConfigConstants.KEY_ENABLE_MANAGE_PARTITIONS;
import static org.apache.inlong.audit.config.ConfigConstants.KEY_PREP_STMT_CACHE_SIZE;
import static org.apache.inlong.audit.config.ConfigConstants.KEY_PREP_STMT_CACHE_SQL_LIMIT;
import static org.apache.inlong.audit.config.ConfigConstants.KEY_QUEUE_PULL_TIMEOUT;
import static org.apache.inlong.audit.config.ConfigConstants.KEY_SOURCE_DB_SINK_BATCH;
import static org.apache.inlong.audit.config.ConfigConstants.KEY_SOURCE_DB_SINK_INTERVAL;
import static org.apache.inlong.audit.config.ConfigConstants.PREP_STMT_CACHE_SIZE;
import static org.apache.inlong.audit.config.ConfigConstants.PREP_STMT_CACHE_SQL_LIMIT;
import static org.apache.inlong.audit.config.SqlConstants.DEFAULT_AUDIT_DATA_TEMP_ADD_PARTITION_SQL;
import static org.apache.inlong.audit.config.SqlConstants.DEFAULT_AUDIT_DATA_TEMP_CHECK_PARTITION_SQL;
import static org.apache.inlong.audit.config.SqlConstants.DEFAULT_AUDIT_DATA_TEMP_DELETE_PARTITION_SQL;
import static org.apache.inlong.audit.config.SqlConstants.KEY_AUDIT_DATA_TEMP_ADD_PARTITION_SQL;
import static org.apache.inlong.audit.config.SqlConstants.KEY_AUDIT_DATA_TEMP_CHECK_PARTITION_SQL;
import static org.apache.inlong.audit.config.SqlConstants.KEY_AUDIT_DATA_TEMP_DELETE_PARTITION_SQL;

/**
 * Jdbc sink
 */
public class JdbcSink implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(JdbcSink.class);
    private final ScheduledExecutorService sinkTimer = Executors.newSingleThreadScheduledExecutor();
    private final ScheduledExecutorService partitionManagerTimer = Executors.newSingleThreadScheduledExecutor();
    private final DataQueue dataQueue;
    private final int insertBatch;
    private final int pullTimeOut;
    private final SinkConfig sinkConfig;
    private DataSource dataSource;

    private final DateTimeFormatter FORMATTER_YYMMDDHH = DateTimeFormatter.ofPattern("yyyyMMdd");
    private final DateTimeFormatter FORMATTER_YY_MM_DD_HH = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private final String checkPartitionSql;

    public JdbcSink(DataQueue dataQueue, SinkConfig sinkConfig) {
        this.dataQueue = dataQueue;
        this.sinkConfig = sinkConfig;

        insertBatch = Configuration.getInstance().get(KEY_SOURCE_DB_SINK_BATCH,
                DEFAULT_SOURCE_DB_SINK_BATCH);

        pullTimeOut = Configuration.getInstance().get(KEY_QUEUE_PULL_TIMEOUT,
                DEFAULT_QUEUE_PULL_TIMEOUT);
        checkPartitionSql = Configuration.getInstance().get(KEY_AUDIT_DATA_TEMP_CHECK_PARTITION_SQL,
                DEFAULT_AUDIT_DATA_TEMP_CHECK_PARTITION_SQL);

    }

    /**
     * start
     */
    public void start() {
        createDataSource();

        sinkTimer.scheduleWithFixedDelay(this::process,
                0,
                Configuration.getInstance().get(KEY_SOURCE_DB_SINK_INTERVAL,
                        DEFAULT_SOURCE_DB_SINK_INTERVAL),
                TimeUnit.MILLISECONDS);
        if (Configuration.getInstance().get(KEY_ENABLE_MANAGE_PARTITIONS,
                DEFAULT_ENABLE_MANAGE_PARTITIONS)) {
            // Create MySQL data partition of today
            addPartition(0);

            partitionManagerTimer.scheduleWithFixedDelay(this::managePartitions,
                    0,
                    Configuration.getInstance().get(KEY_CHECK_PARTITION_INTERVAL_HOURS,
                            DEFAULT_CHECK_PARTITION_INTERVAL_HOURS),
                    TimeUnit.HOURS);
        }
    }

    /**
     * Process
     */
    private void process() {
        try (Connection connection = dataSource.getConnection();
                PreparedStatement preparedStatement = connection.prepareStatement(sinkConfig.getInsertSql())) {
            if (connection.isClosed()) {
                createDataSource();
            }
            int counter = 0;
            StatData data = dataQueue.pull(pullTimeOut, TimeUnit.MILLISECONDS);
            while (data != null) {
                preparedStatement.setString(1, data.getLogTs());
                preparedStatement.setString(2, data.getInlongGroupId());
                preparedStatement.setString(3, data.getInlongStreamId());
                preparedStatement.setString(4, data.getAuditId());
                preparedStatement.setString(5, data.getAuditTag());
                preparedStatement.setLong(6, data.getCount());
                preparedStatement.setLong(7, data.getSize());
                preparedStatement.setLong(8, data.getDelay());
                preparedStatement.addBatch();

                if (++counter >= insertBatch) {
                    preparedStatement.executeBatch();
                    preparedStatement.clearBatch();
                    counter = 0;
                }
                data = dataQueue.pull(pullTimeOut, TimeUnit.MILLISECONDS);
            }
            if (counter > 0) {
                preparedStatement.executeBatch();
                preparedStatement.clearBatch();
            }
        } catch (Exception exception) {
            LOGGER.error("Process exception!", exception);
        }
    }

    /**
     * Create data source
     */
    protected void createDataSource() {
        HikariConfig config = new HikariConfig();
        config.setDriverClassName(sinkConfig.getDriverClassName());
        config.setJdbcUrl(sinkConfig.getJdbcUrl());
        config.setUsername(sinkConfig.getUserName());
        config.setPassword(sinkConfig.getPassword());
        config.setConnectionTimeout(Configuration.getInstance().get(KEY_DATASOURCE_CONNECTION_TIMEOUT,
                DEFAULT_CONNECTION_TIMEOUT));
        config.addDataSourceProperty(CACHE_PREP_STMTS,
                Configuration.getInstance().get(KEY_CACHE_PREP_STMTS, DEFAULT_CACHE_PREP_STMTS));
        config.addDataSourceProperty(PREP_STMT_CACHE_SIZE,
                Configuration.getInstance().get(KEY_PREP_STMT_CACHE_SIZE, DEFAULT_PREP_STMT_CACHE_SIZE));
        config.addDataSourceProperty(PREP_STMT_CACHE_SQL_LIMIT,
                Configuration.getInstance().get(KEY_PREP_STMT_CACHE_SQL_LIMIT, DEFAULT_PREP_STMT_CACHE_SQL_LIMIT));
        config.setMaximumPoolSize(
                Configuration.getInstance().get(KEY_DATASOURCE_POOL_SIZE,
                        DEFAULT_DATASOURCE_POOL_SIZE));
        dataSource = new HikariDataSource(config);
    }

    private void managePartitions() {
        // Create MySQL data partition of tomorrow
        addPartition(1);

        deletePartition();
    }

    private String formatPartitionName(LocalDate date) {
        return "p" + date.format(FORMATTER_YYMMDDHH);
    }

    private void addPartition(long daysToAdd) {
        String partitionName = formatPartitionName(LocalDate.now().plusDays(daysToAdd));
        if (isPartitionExists(partitionName)) {
            LOGGER.info("Partition [{}] is exist, dont`t need add this partition.", partitionName);
            return;
        }
        String partitionValue = LocalDate.now().plusDays(daysToAdd + 1).format(FORMATTER_YY_MM_DD_HH);
        String addPartitionSQL = String.format(
                Configuration.getInstance().get(KEY_AUDIT_DATA_TEMP_ADD_PARTITION_SQL,
                        DEFAULT_AUDIT_DATA_TEMP_ADD_PARTITION_SQL),
                partitionName, partitionValue);
        executeUpdate(addPartitionSQL);
    }

    private void deletePartition() {
        int daysToSubtract = Configuration.getInstance().get(KEY_AUDIT_DATA_TEMP_STORAGE_DAYS,
                DEFAULT_AUDIT_DATA_TEMP_STORAGE_DAYS);
        String partitionName = formatPartitionName(LocalDate.now().minusDays(daysToSubtract));
        if (!isPartitionExists(partitionName)) {
            LOGGER.info("Partition [{}] is not exist, dont`t need delete this partition.", partitionName);
            return;
        }
        String deletePartitionSQL = String.format(
                Configuration.getInstance().get(KEY_AUDIT_DATA_TEMP_DELETE_PARTITION_SQL,
                        DEFAULT_AUDIT_DATA_TEMP_DELETE_PARTITION_SQL),
                partitionName);
        executeUpdate(deletePartitionSQL);
    }

    private void executeUpdate(String updateSQL) {
        try (Connection connection = dataSource.getConnection();
                PreparedStatement preparedStatement = connection.prepareStatement(updateSQL)) {
            preparedStatement.executeUpdate();
            LOGGER.info("Execute update [{}] success!", updateSQL);
        } catch (Exception exception) {
            LOGGER.error("Execute update [{}] has exception!", updateSQL, exception);
        }
    }

    private boolean isPartitionExists(String partitionName) {
        try (Connection connection = dataSource.getConnection();
                PreparedStatement statement = connection.prepareStatement(checkPartitionSql)) {
            statement.setString(1, partitionName);

            try (ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next()) {
                    return resultSet.getInt("count") > 0;
                }
            } catch (SQLException sqlException) {
                LOGGER.error("An error occurred while checking partition [{}] existence:", partitionName, sqlException);
            }
        } catch (Exception exception) {
            LOGGER.error("An exception occurred while checking partition [{}]existence:", partitionName, exception);
        }
        return false;
    }

    public void destroy() {
        sinkTimer.shutdown();
    }

    @Override
    public void close() throws Exception {

    }
}
