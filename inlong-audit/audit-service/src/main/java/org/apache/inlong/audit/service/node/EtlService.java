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

package org.apache.inlong.audit.service.node;

import org.apache.inlong.audit.service.cache.HalfHourCache;
import org.apache.inlong.audit.service.cache.HourCache;
import org.apache.inlong.audit.service.cache.TenMinutesCache;
import org.apache.inlong.audit.service.channel.DataQueue;
import org.apache.inlong.audit.service.config.Configuration;
import org.apache.inlong.audit.service.entities.AuditCycle;
import org.apache.inlong.audit.service.entities.JdbcConfig;
import org.apache.inlong.audit.service.entities.SinkConfig;
import org.apache.inlong.audit.service.entities.SourceConfig;
import org.apache.inlong.audit.service.entities.StatData;
import org.apache.inlong.audit.service.sink.AuditSink;
import org.apache.inlong.audit.service.sink.CacheSink;
import org.apache.inlong.audit.service.sink.JdbcSink;
import org.apache.inlong.audit.service.source.JdbcSource;
import org.apache.inlong.audit.service.utils.JdbcUtils;

import com.github.benmanes.caffeine.cache.Cache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;

import static org.apache.inlong.audit.service.config.ConfigConstants.DEFAULT_DATA_QUEUE_SIZE;
import static org.apache.inlong.audit.service.config.ConfigConstants.DEFAULT_ENABLE_STAT_AUDIT_DAY;
import static org.apache.inlong.audit.service.config.ConfigConstants.DEFAULT_ENABLE_STAT_AUDIT_HOUR;
import static org.apache.inlong.audit.service.config.ConfigConstants.DEFAULT_ENABLE_STAT_AUDIT_MINUTE_10;
import static org.apache.inlong.audit.service.config.ConfigConstants.DEFAULT_ENABLE_STAT_AUDIT_MINUTE_30;
import static org.apache.inlong.audit.service.config.ConfigConstants.DEFAULT_SELECTOR_SERVICE_ID;
import static org.apache.inlong.audit.service.config.ConfigConstants.DEFAULT_SUMMARY_DAILY_STAT_BACK_TIMES;
import static org.apache.inlong.audit.service.config.ConfigConstants.DEFAULT_SUMMARY_REALTIME_STAT_BACK_TIMES;
import static org.apache.inlong.audit.service.config.ConfigConstants.KEY_DATA_QUEUE_SIZE;
import static org.apache.inlong.audit.service.config.ConfigConstants.KEY_ENABLE_STAT_AUDIT_DAY;
import static org.apache.inlong.audit.service.config.ConfigConstants.KEY_ENABLE_STAT_AUDIT_HOUR;
import static org.apache.inlong.audit.service.config.ConfigConstants.KEY_ENABLE_STAT_AUDIT_MINUTE_10;
import static org.apache.inlong.audit.service.config.ConfigConstants.KEY_ENABLE_STAT_AUDIT_MINUTE_30;
import static org.apache.inlong.audit.service.config.ConfigConstants.KEY_SELECTOR_SERVICE_ID;
import static org.apache.inlong.audit.service.config.ConfigConstants.KEY_SUMMARY_DAILY_STAT_BACK_TIMES;
import static org.apache.inlong.audit.service.config.ConfigConstants.KEY_SUMMARY_REALTIME_STAT_BACK_TIMES;
import static org.apache.inlong.audit.service.config.SqlConstants.DEFAULT_MYSQL_SINK_INSERT_DAY_SQL;
import static org.apache.inlong.audit.service.config.SqlConstants.DEFAULT_MYSQL_SINK_INSERT_TEMP_SQL;
import static org.apache.inlong.audit.service.config.SqlConstants.DEFAULT_MYSQL_SOURCE_QUERY_TEMP_SQL;
import static org.apache.inlong.audit.service.config.SqlConstants.DEFAULT_SOURCE_STAT_SQL;
import static org.apache.inlong.audit.service.config.SqlConstants.KEY_MYSQL_SINK_INSERT_DAY_SQL;
import static org.apache.inlong.audit.service.config.SqlConstants.KEY_MYSQL_SINK_INSERT_TEMP_SQL;
import static org.apache.inlong.audit.service.config.SqlConstants.KEY_MYSQL_SOURCE_QUERY_TEMP_SQL;
import static org.apache.inlong.audit.service.config.SqlConstants.KEY_SOURCE_STAT_SQL;

/**
 * Etl service aggregate the data from the data source and store the aggregated data to the target storage.
 */
public class EtlService {

    private static final Logger LOGGER = LoggerFactory.getLogger(EtlService.class);

    // Statistics of original audit data
    private final List<JdbcSource> originalSources = new LinkedList<>();
    private final int queueSize;
    private final String serviceId;
    private final Configuration configuration;

    private final List<JdbcSource> dataFlowSources = new LinkedList<>();
    private final List<AuditSink> dataFlowSinks = new LinkedList<>();

    public EtlService() {
        configuration = Configuration.getInstance();
        queueSize = configuration.get(KEY_DATA_QUEUE_SIZE,
                DEFAULT_DATA_QUEUE_SIZE);
        serviceId = configuration.get(KEY_SELECTOR_SERVICE_ID, DEFAULT_SELECTOR_SERVICE_ID);
    }

    public void start() {
        int statBackTimes = configuration.get(KEY_SUMMARY_REALTIME_STAT_BACK_TIMES,
                DEFAULT_SUMMARY_REALTIME_STAT_BACK_TIMES);

        if (configuration.get(KEY_ENABLE_STAT_AUDIT_MINUTE_10, DEFAULT_ENABLE_STAT_AUDIT_MINUTE_10)) {
            startDataFlow(AuditCycle.MINUTE_10, statBackTimes, TenMinutesCache.getInstance().getCache());
            LOGGER.info("Start data flow for minute 10");
        }

        if (configuration.get(KEY_ENABLE_STAT_AUDIT_MINUTE_30, DEFAULT_ENABLE_STAT_AUDIT_MINUTE_30)) {
            startDataFlow(AuditCycle.MINUTE_30, statBackTimes, HalfHourCache.getInstance().getCache());
            LOGGER.info("Start data flow for minute 30");
        }

        if (configuration.get(KEY_ENABLE_STAT_AUDIT_HOUR, DEFAULT_ENABLE_STAT_AUDIT_HOUR)) {
            startDataFlow(AuditCycle.HOUR, statBackTimes, HourCache.getInstance().getCache());
            LOGGER.info("Start data flow for hour");
        }

        if (configuration.get(KEY_ENABLE_STAT_AUDIT_DAY, DEFAULT_ENABLE_STAT_AUDIT_DAY)) {
            statBackTimes = configuration.get(KEY_SUMMARY_DAILY_STAT_BACK_TIMES, DEFAULT_SUMMARY_DAILY_STAT_BACK_TIMES);
            startDataFlow(AuditCycle.DAY, statBackTimes, null);
        }
    }

    private void startDataFlow(AuditCycle cycle, int backTimes, Cache<String, StatData> cache) {
        DataQueue dataQueue = new DataQueue(queueSize);
        JdbcSource source = new JdbcSource(dataQueue, buildMysqlSourceConfig(cycle, backTimes));
        source.start();
        dataFlowSources.add(source);

        AuditSink sink;
        if (cache != null) {
            sink = new CacheSink(dataQueue, cache);
        } else {
            SinkConfig sinkConfig = buildMysqlSinkConfig(configuration.get(KEY_MYSQL_SINK_INSERT_DAY_SQL,
                    DEFAULT_MYSQL_SINK_INSERT_DAY_SQL));
            sink = new JdbcSink(dataQueue, sinkConfig);
        }
        sink.start();
        dataFlowSinks.add(sink);
    }

    public void auditSourceToMysql() {
        DataQueue dataQueue = new DataQueue(queueSize);
        List<JdbcConfig> sourceList = ConfigService.getInstance().getAuditSourceByServiceId(serviceId);
        for (JdbcConfig jdbcConfig : sourceList) {
            JdbcSource jdbcSource = new JdbcSource(dataQueue, buildAuditJdbcSourceConfig(jdbcConfig));
            jdbcSource.start();
            originalSources.add(jdbcSource);
            LOGGER.info("Audit source to mysql jdbc config:{}", jdbcConfig);
        }

        SinkConfig sinkConfig = buildMysqlSinkConfig(configuration.get(KEY_MYSQL_SINK_INSERT_TEMP_SQL,
                DEFAULT_MYSQL_SINK_INSERT_TEMP_SQL));
        JdbcSink sink = new JdbcSink(dataQueue, sinkConfig);
        sink.start();
        dataFlowSinks.add(sink);
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
                configuration.get(KEY_MYSQL_SOURCE_QUERY_TEMP_SQL,
                        DEFAULT_MYSQL_SOURCE_QUERY_TEMP_SQL),
                statBackTimes,
                jdbcConfig.getDriverClass(),
                jdbcConfig.getJdbcUrl(),
                jdbcConfig.getUserName(),
                jdbcConfig.getPassword());
    }

    /**
     * Build the configurations of audit source.
     *
     * @return
     */
    private SourceConfig buildAuditJdbcSourceConfig(JdbcConfig jdbcConfig) {
        return new SourceConfig(AuditCycle.MINUTE_5,
                configuration.get(KEY_SOURCE_STAT_SQL,
                        DEFAULT_SOURCE_STAT_SQL),
                configuration.get(KEY_SUMMARY_REALTIME_STAT_BACK_TIMES,
                        DEFAULT_SUMMARY_REALTIME_STAT_BACK_TIMES),
                jdbcConfig.getDriverClass(),
                jdbcConfig.getJdbcUrl(),
                jdbcConfig.getUserName(),
                jdbcConfig.getPassword(),
                true);
    }

    /**
     * Stop the etl service,and destroy related resources.
     */
    public void stop() {
        for (JdbcSource source : originalSources) {
            source.destroy();
        }
        for (JdbcSource source : dataFlowSources) {
            source.destroy();
        }
        for (AuditSink sink : dataFlowSinks) {
            sink.destroy();
        }
    }
}
