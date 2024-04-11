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

package org.apache.inlong.audit.service;

import org.apache.inlong.audit.cache.HalfHourCache;
import org.apache.inlong.audit.cache.HourCache;
import org.apache.inlong.audit.cache.TenMinutesCache;
import org.apache.inlong.audit.channel.DataQueue;
import org.apache.inlong.audit.config.Configuration;
import org.apache.inlong.audit.entities.AuditCycle;
import org.apache.inlong.audit.entities.JdbcConfig;
import org.apache.inlong.audit.entities.SinkConfig;
import org.apache.inlong.audit.entities.SourceConfig;
import org.apache.inlong.audit.selector.api.Selector;
import org.apache.inlong.audit.selector.api.SelectorConfig;
import org.apache.inlong.audit.selector.api.SelectorFactory;
import org.apache.inlong.audit.sink.CacheSink;
import org.apache.inlong.audit.sink.JdbcSink;
import org.apache.inlong.audit.source.JdbcSource;
import org.apache.inlong.audit.utils.JdbcUtils;
import org.apache.inlong.common.util.NetworkUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

import static org.apache.inlong.audit.config.ConfigConstants.DEFAULT_DATA_QUEUE_SIZE;
import static org.apache.inlong.audit.config.ConfigConstants.DEFAULT_SELECTOR_FOLLOWER_LISTEN_CYCLE_MS;
import static org.apache.inlong.audit.config.ConfigConstants.DEFAULT_SELECTOR_SERVICE_ID;
import static org.apache.inlong.audit.config.ConfigConstants.DEFAULT_SUMMARY_DAILY_STAT_BACK_TIMES;
import static org.apache.inlong.audit.config.ConfigConstants.DEFAULT_SUMMARY_REALTIME_STAT_BACK_TIMES;
import static org.apache.inlong.audit.config.ConfigConstants.KEY_DATA_QUEUE_SIZE;
import static org.apache.inlong.audit.config.ConfigConstants.KEY_SELECTOR_FOLLOWER_LISTEN_CYCLE_MS;
import static org.apache.inlong.audit.config.ConfigConstants.KEY_SELECTOR_SERVICE_ID;
import static org.apache.inlong.audit.config.ConfigConstants.KEY_SUMMARY_DAILY_STAT_BACK_TIMES;
import static org.apache.inlong.audit.config.ConfigConstants.KEY_SUMMARY_REALTIME_STAT_BACK_TIMES;
import static org.apache.inlong.audit.config.SqlConstants.DEFAULT_CLICKHOUSE_SOURCE_QUERY_SQL;
import static org.apache.inlong.audit.config.SqlConstants.DEFAULT_MYSQL_SINK_INSERT_DAY_SQL;
import static org.apache.inlong.audit.config.SqlConstants.DEFAULT_MYSQL_SINK_INSERT_TEMP_SQL;
import static org.apache.inlong.audit.config.SqlConstants.DEFAULT_MYSQL_SOURCE_QUERY_TEMP_SQL;
import static org.apache.inlong.audit.config.SqlConstants.KEY_CLICKHOUSE_SOURCE_QUERY_SQL;
import static org.apache.inlong.audit.config.SqlConstants.KEY_MYSQL_SINK_INSERT_DAY_SQL;
import static org.apache.inlong.audit.config.SqlConstants.KEY_MYSQL_SINK_INSERT_TEMP_SQL;
import static org.apache.inlong.audit.config.SqlConstants.KEY_MYSQL_SOURCE_QUERY_TEMP_SQL;

/**
 * Etl service aggregate the data from the data source and store the aggregated data to the target storage.
 */
public class EtlService {

    private static final Logger LOGGER = LoggerFactory.getLogger(EtlService.class);
    private JdbcSource mysqlSourceOfTemp;
    private JdbcSource mysqlSourceOfTenMinutesCache;
    private JdbcSource mysqlSourceOfHalfHourCache;
    private JdbcSource mysqlSourceOfHourCache;
    private JdbcSink mysqlSinkOfDay;
    private JdbcSource clickhouseSource;
    private JdbcSink mysqlSinkOfTemp;
    private CacheSink cacheSinkOfTenMinutesCache;
    private CacheSink cacheSinkOfHalfHourCache;
    private CacheSink cacheSinkOfHourCache;
    private final int queueSize;
    private final int statBackTimes;
    private static Selector selector;
    private boolean running = true;

    public EtlService() {
        queueSize = Configuration.getInstance().get(KEY_DATA_QUEUE_SIZE,
                DEFAULT_DATA_QUEUE_SIZE);
        statBackTimes = Configuration.getInstance().get(KEY_SUMMARY_REALTIME_STAT_BACK_TIMES,
                DEFAULT_SUMMARY_REALTIME_STAT_BACK_TIMES);
    }

    /**
     * Start the etl service.
     */
    public void start() {
        mysqlToMysqlOfDay();
        mysqlToTenMinutesCache();
        mysqlToHalfHourCache();
        mysqlToHourCache();

        initSelector();
        waitToBeLeader();
    }

    /**
     * Aggregate data from mysql data source and store the aggregated data in the target mysql table.
     * The audit data cycle is days,and stored in table of day.
     */
    private void mysqlToMysqlOfDay() {
        DataQueue dataQueue = new DataQueue(queueSize);

        mysqlSourceOfTemp = new JdbcSource(dataQueue, buildMysqlSourceConfig(AuditCycle.DAY,
                Configuration.getInstance().get(KEY_SUMMARY_DAILY_STAT_BACK_TIMES,
                        DEFAULT_SUMMARY_DAILY_STAT_BACK_TIMES)));
        mysqlSourceOfTemp.start();

        SinkConfig sinkConfig = buildMysqlSinkConfig(Configuration.getInstance().get(KEY_MYSQL_SINK_INSERT_DAY_SQL,
                DEFAULT_MYSQL_SINK_INSERT_DAY_SQL));
        mysqlSinkOfDay = new JdbcSink(dataQueue, sinkConfig);
        mysqlSinkOfDay.start();
    }

    /**
     * Aggregate data from mysql data source and store in local cache for openapi.
     */
    private void mysqlToTenMinutesCache() {
        DataQueue dataQueue = new DataQueue(queueSize);
        mysqlSourceOfTenMinutesCache =
                new JdbcSource(dataQueue, buildMysqlSourceConfig(AuditCycle.MINUTE_10, statBackTimes));
        mysqlSourceOfTenMinutesCache.start();

        cacheSinkOfTenMinutesCache = new CacheSink(dataQueue, TenMinutesCache.getInstance().getCache());
        cacheSinkOfTenMinutesCache.start();
    }

    /**
     * Aggregate data from mysql data source and store in local cache for openapi.
     */
    private void mysqlToHalfHourCache() {
        DataQueue dataQueue = new DataQueue(queueSize);
        mysqlSourceOfHalfHourCache =
                new JdbcSource(dataQueue, buildMysqlSourceConfig(AuditCycle.MINUTE_30, statBackTimes));
        mysqlSourceOfHalfHourCache.start();

        cacheSinkOfHalfHourCache = new CacheSink(dataQueue, HalfHourCache.getInstance().getCache());
        cacheSinkOfHalfHourCache.start();
    }

    /**
     * Aggregate data from mysql data source and store in local cache for openapi.
     */
    private void mysqlToHourCache() {
        DataQueue dataQueue = new DataQueue(queueSize);
        mysqlSourceOfHourCache = new JdbcSource(dataQueue, buildMysqlSourceConfig(AuditCycle.HOUR, statBackTimes));
        mysqlSourceOfHourCache.start();

        cacheSinkOfHourCache = new CacheSink(dataQueue, HourCache.getInstance().getCache());
        cacheSinkOfHourCache.start();
    }

    /**
     * Aggregate data from clickhouse data source and store the aggregated data in the target mysql table.
     * The default audit data cycle is 5 minutes,and stored in a temporary table.
     */
    private void clickhouseToMysql() {
        DataQueue dataQueue = new DataQueue(queueSize);

        clickhouseSource = new JdbcSource(dataQueue, buildClickhouseSourceConfig());
        clickhouseSource.start();

        SinkConfig sinkConfig = buildMysqlSinkConfig(Configuration.getInstance().get(KEY_MYSQL_SINK_INSERT_TEMP_SQL,
                DEFAULT_MYSQL_SINK_INSERT_TEMP_SQL));
        mysqlSinkOfTemp = new JdbcSink(dataQueue, sinkConfig);
        mysqlSinkOfTemp.start();
    }

    /**
     * Build the configurations of mysql sink.
     *
     * @param insertSql
     * @return
     */
    private SinkConfig buildMysqlSinkConfig(String insertSql) {
        JdbcConfig jdbcConfig = JdbcUtils.buildMysqlConfig();
        return new SinkConfig(
                insertSql,
                jdbcConfig.getDriverClass(),
                jdbcConfig.getJdbcUrl(),
                jdbcConfig.getUserName(),
                jdbcConfig.getPassword());
    }

    /**
     * Build the configurations of mysql source.
     *
     * @return
     */
    private SourceConfig buildMysqlSourceConfig(AuditCycle auditCycle, int statBackTimes) {
        JdbcConfig jdbcConfig = JdbcUtils.buildMysqlConfig();
        return new SourceConfig(auditCycle,
                Configuration.getInstance().get(KEY_MYSQL_SOURCE_QUERY_TEMP_SQL,
                        DEFAULT_MYSQL_SOURCE_QUERY_TEMP_SQL),
                statBackTimes,
                jdbcConfig.getDriverClass(),
                jdbcConfig.getJdbcUrl(),
                jdbcConfig.getUserName(),
                jdbcConfig.getPassword());
    }

    /**
     * Build the configurations of clickhouse source.
     *
     * @return
     */
    private SourceConfig buildClickhouseSourceConfig() {
        JdbcConfig jdbcConfig = JdbcUtils.buildClickhouseConfig();
        return new SourceConfig(AuditCycle.MINUTE_5,
                Configuration.getInstance().get(KEY_CLICKHOUSE_SOURCE_QUERY_SQL,
                        DEFAULT_CLICKHOUSE_SOURCE_QUERY_SQL),
                Configuration.getInstance().get(KEY_SUMMARY_REALTIME_STAT_BACK_TIMES,
                        DEFAULT_SUMMARY_REALTIME_STAT_BACK_TIMES),
                jdbcConfig.getDriverClass(),
                jdbcConfig.getJdbcUrl(),
                jdbcConfig.getUserName(),
                jdbcConfig.getPassword());
    }

    /**
     * Init selector
     */
    private void initSelector() {
        JdbcConfig jdbcConfig = JdbcUtils.buildMysqlConfig();
        String leaderId = NetworkUtils.getLocalIp() + "-" + UUID.randomUUID();
        LOGGER.info("Init selector. Leader id is :{}", leaderId);
        if (selector == null) {
            SelectorConfig electorConfig = new SelectorConfig(
                    Configuration.getInstance().get(KEY_SELECTOR_SERVICE_ID, DEFAULT_SELECTOR_SERVICE_ID),
                    leaderId,
                    jdbcConfig.getJdbcUrl(),
                    jdbcConfig.getUserName(), jdbcConfig.getPassword(), jdbcConfig.getDriverClass());

            selector = SelectorFactory.getNewElector(electorConfig);
            try {
                selector.init();
            } catch (Exception e) {
                LOGGER.error("Init selector has exception:", e);
            }
        }
    }

    /**
     * Wait to be leader
     */
    public void waitToBeLeader() {
        while (running) {
            try {
                Thread.sleep(Configuration.getInstance().get(KEY_SELECTOR_FOLLOWER_LISTEN_CYCLE_MS,
                        DEFAULT_SELECTOR_FOLLOWER_LISTEN_CYCLE_MS));
            } catch (Exception e) {
                LOGGER.error("Wait to be Leader has exception! lost Leadership!", e);
            }

            if (selector.isLeader()) {
                LOGGER.info("I get Leadership! Begin to aggregate clickhouse data to mysql");
                clickhouseToMysql();
                return;
            }
        }
    }

    /**
     * Stop the etl service,and destroy related resources.
     */
    public void stop() {
        running = false;
        mysqlSourceOfTemp.destroy();
        mysqlSinkOfDay.destroy();

        clickhouseSource.destroy();
        mysqlSinkOfTemp.destroy();

        mysqlSourceOfTenMinutesCache.destroy();
        mysqlSourceOfHalfHourCache.destroy();
        mysqlSourceOfHourCache.destroy();

        cacheSinkOfTenMinutesCache.destroy();
        cacheSinkOfHalfHourCache.destroy();
        cacheSinkOfHourCache.destroy();

        selector.close();
    }
}
