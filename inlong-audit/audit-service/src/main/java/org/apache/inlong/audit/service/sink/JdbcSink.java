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

package org.apache.inlong.audit.service.sink;

import org.apache.inlong.audit.service.channel.DataQueue;
import org.apache.inlong.audit.service.config.Configuration;
import org.apache.inlong.audit.service.entities.SinkConfig;
import org.apache.inlong.audit.service.entities.StatData;
import org.apache.inlong.audit.service.utils.JdbcUtils;

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

import static org.apache.inlong.audit.service.config.ConfigConstants.DEFAULT_QUEUE_PULL_TIMEOUT;
import static org.apache.inlong.audit.service.config.ConfigConstants.DEFAULT_SOURCE_DB_SINK_BATCH;
import static org.apache.inlong.audit.service.config.ConfigConstants.DEFAULT_SOURCE_DB_SINK_INTERVAL;
import static org.apache.inlong.audit.service.config.ConfigConstants.KEY_QUEUE_PULL_TIMEOUT;
import static org.apache.inlong.audit.service.config.ConfigConstants.KEY_SOURCE_DB_SINK_BATCH;
import static org.apache.inlong.audit.service.config.ConfigConstants.KEY_SOURCE_DB_SINK_INTERVAL;

/**
 * Jdbc sink
 */
public class JdbcSink implements AutoCloseable, AuditSink {

    private static final Logger LOGGER = LoggerFactory.getLogger(JdbcSink.class);
    private final ScheduledExecutorService sinkTimer = Executors.newSingleThreadScheduledExecutor();
    private final DataQueue dataQueue;
    private final int insertBatch;
    private final int pullTimeOut;
    private final SinkConfig sinkConfig;
    private DataSource dataSource;
    private final Configuration configuration;

    public JdbcSink(DataQueue dataQueue, SinkConfig sinkConfig) {
        configuration = Configuration.getInstance();
        this.dataQueue = dataQueue;
        this.sinkConfig = sinkConfig;

        insertBatch = configuration.get(KEY_SOURCE_DB_SINK_BATCH,
                DEFAULT_SOURCE_DB_SINK_BATCH);

        pullTimeOut = configuration.get(KEY_QUEUE_PULL_TIMEOUT,
                DEFAULT_QUEUE_PULL_TIMEOUT);

    }

    /**
     * start
     */
    public void start() {
        createDataSource();
        sinkTimer.scheduleWithFixedDelay(this::process,
                0,
                configuration.get(KEY_SOURCE_DB_SINK_INTERVAL,
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
        } catch (Exception exception) {
            LOGGER.error("Process exception!", exception);
        }
    }

    /**
     * Create data source
     */
    protected void createDataSource() {
        HikariConfig hikariConfig = JdbcUtils.buildHikariConfig(
                sinkConfig.getDriverClassName(),
                sinkConfig.getJdbcUrl(),
                sinkConfig.getUserName(),
                sinkConfig.getPassword());
        dataSource = new HikariDataSource(hikariConfig);
    }

    public void destroy() {
        sinkTimer.shutdown();
    }

    @Override
    public void close() {

    }
}
