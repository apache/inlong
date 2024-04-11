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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.apache.inlong.audit.config.ConfigConstants.CACHE_PREP_STMTS;
import static org.apache.inlong.audit.config.ConfigConstants.DEFAULT_CACHE_PREP_STMTS;
import static org.apache.inlong.audit.config.ConfigConstants.DEFAULT_CONNECTION_TIMEOUT;
import static org.apache.inlong.audit.config.ConfigConstants.DEFAULT_DATASOURCE_POOL_SIZE;
import static org.apache.inlong.audit.config.ConfigConstants.DEFAULT_PREP_STMT_CACHE_SIZE;
import static org.apache.inlong.audit.config.ConfigConstants.DEFAULT_PREP_STMT_CACHE_SQL_LIMIT;
import static org.apache.inlong.audit.config.ConfigConstants.DEFAULT_QUEUE_PULL_TIMEOUT;
import static org.apache.inlong.audit.config.ConfigConstants.DEFAULT_SOURCE_DB_SINK_BATCH;
import static org.apache.inlong.audit.config.ConfigConstants.DEFAULT_SOURCE_DB_SINK_INTERVAL;
import static org.apache.inlong.audit.config.ConfigConstants.KEY_CACHE_PREP_STMTS;
import static org.apache.inlong.audit.config.ConfigConstants.KEY_DATASOURCE_CONNECTION_TIMEOUT;
import static org.apache.inlong.audit.config.ConfigConstants.KEY_DATASOURCE_POOL_SIZE;
import static org.apache.inlong.audit.config.ConfigConstants.KEY_PREP_STMT_CACHE_SIZE;
import static org.apache.inlong.audit.config.ConfigConstants.KEY_PREP_STMT_CACHE_SQL_LIMIT;
import static org.apache.inlong.audit.config.ConfigConstants.KEY_QUEUE_PULL_TIMEOUT;
import static org.apache.inlong.audit.config.ConfigConstants.KEY_SOURCE_DB_SINK_BATCH;
import static org.apache.inlong.audit.config.ConfigConstants.KEY_SOURCE_DB_SINK_INTERVAL;
import static org.apache.inlong.audit.config.ConfigConstants.PREP_STMT_CACHE_SIZE;
import static org.apache.inlong.audit.config.ConfigConstants.PREP_STMT_CACHE_SQL_LIMIT;

/**
 * Jdbc sink
 */
public class JdbcSink implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(JdbcSink.class);
    private final ScheduledExecutorService sinkTimer = Executors.newSingleThreadScheduledExecutor();
    private final DataQueue dataQueue;
    private final int insertBatch;
    private final int pullTimeOut;
    private final SinkConfig sinkConfig;
    private DataSource dataSource;

    public JdbcSink(DataQueue dataQueue, SinkConfig sinkConfig) {
        this.dataQueue = dataQueue;
        this.sinkConfig = sinkConfig;

        insertBatch = Configuration.getInstance().get(KEY_SOURCE_DB_SINK_BATCH,
                DEFAULT_SOURCE_DB_SINK_BATCH);

        pullTimeOut = Configuration.getInstance().get(KEY_QUEUE_PULL_TIMEOUT,
                DEFAULT_QUEUE_PULL_TIMEOUT);
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
        } catch (Exception e) {
            LOGGER.error("Process exception! {}", e.getMessage());
        }
    }

    /**
     * Create data source
     */
    protected void createDataSource() {
        HikariConfig config = new HikariConfig();
        config.setDriverClassName(sinkConfig.getDriverClassName());
        config.setJdbcUrl(sinkConfig.getJdbcUrl());
        config.setUsername(sinkConfig.getUsername());
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

    public void destroy() {
        sinkTimer.shutdown();
    }

    @Override
    public void close() throws Exception {

    }
}
