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

package org.apache.inlong.audit.service.source;

import org.apache.inlong.audit.service.channel.DataQueue;
import org.apache.inlong.audit.service.config.Configuration;
import org.apache.inlong.audit.service.entities.SourceConfig;
import org.apache.inlong.audit.service.entities.StartEndTime;
import org.apache.inlong.audit.service.entities.StatData;
import org.apache.inlong.audit.service.node.ConfigService;
import org.apache.inlong.audit.service.utils.CacheUtils;
import org.apache.inlong.audit.service.utils.JdbcUtils;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.apache.inlong.audit.consts.ConfigConstants.DEFAULT_AUDIT_TAG;
import static org.apache.inlong.audit.service.config.ConfigConstants.DATE_FORMAT;
import static org.apache.inlong.audit.service.config.ConfigConstants.DEFAULT_SOURCE_DB_STAT_INTERVAL;
import static org.apache.inlong.audit.service.config.ConfigConstants.DEFAULT_STAT_BACK_INITIAL_OFFSET;
import static org.apache.inlong.audit.service.config.ConfigConstants.DEFAULT_STAT_THREAD_POOL_SIZE;
import static org.apache.inlong.audit.service.config.ConfigConstants.KEY_SOURCE_DB_STAT_INTERVAL;
import static org.apache.inlong.audit.service.config.ConfigConstants.KEY_STAT_BACK_INITIAL_OFFSET;
import static org.apache.inlong.audit.service.config.ConfigConstants.KEY_STAT_THREAD_POOL_SIZE;
import static org.apache.inlong.audit.service.entities.AuditCycle.DAY;
import static org.apache.inlong.audit.service.entities.AuditCycle.HOUR;
/**
 * Jdbc source
 */
@Data
public class JdbcSource {

    private static final Logger LOGGER = LoggerFactory.getLogger(JdbcSource.class);
    private final ConcurrentHashMap<Integer, ScheduledExecutorService> statTimers = new ConcurrentHashMap<>();
    private DataQueue dataQueue;
    private int querySqlTimeout;
    private DataSource dataSource;
    private String querySql;
    private SourceConfig sourceConfig;
    private static final int MAX_MINUTE = 60;

    public JdbcSource(DataQueue dataQueue, SourceConfig sourceConfig) {
        this.dataQueue = dataQueue;
        this.sourceConfig = sourceConfig;
    }

    /**
     * Init
     */
    public void init() {
        createDataSource();
    }

    public void start() {
        init();
        int statInterval = Configuration.getInstance().get(KEY_SOURCE_DB_STAT_INTERVAL,
                DEFAULT_SOURCE_DB_STAT_INTERVAL);
        if (sourceConfig.getAuditCycle() == DAY) {
            statInterval = HOUR.getValue();
        }
        int offset = Configuration.getInstance().get(KEY_STAT_BACK_INITIAL_OFFSET,
                DEFAULT_STAT_BACK_INITIAL_OFFSET);
        for (int statBackTime = 0; statBackTime < sourceConfig.getStatBackTimes(); statBackTime++) {
            ScheduledExecutorService timer =
                    statTimers.computeIfAbsent(statBackTime, k -> Executors.newSingleThreadScheduledExecutor());
            timer.scheduleWithFixedDelay(new StatServer(offset++),
                    0,
                    statInterval + statBackTime, TimeUnit.MINUTES);
        }
    }

    ;

    /**
     * Get stat cycle of minute
     *
     * @param hoursAgo
     * @param dataCycle
     * @return
     */
    public List<StartEndTime> getStatCycleOfMinute(int hoursAgo, int dataCycle) {
        List<StartEndTime> statCycleList = new LinkedList<>();
        for (int minute = 0; minute < MAX_MINUTE; minute = minute + dataCycle) {
            Calendar calendar = Calendar.getInstance();
            calendar.add(Calendar.HOUR_OF_DAY, -hoursAgo);

            calendar.set(Calendar.MINUTE, minute);
            calendar.set(Calendar.SECOND, 0);
            SimpleDateFormat dateFormat = new SimpleDateFormat(DATE_FORMAT);
            StartEndTime statCycle = new StartEndTime();
            statCycle.setStartTime(dateFormat.format(calendar.getTime()));

            calendar.set(Calendar.MINUTE, minute + dataCycle);
            calendar.set(Calendar.SECOND, 0);
            statCycle.setEndTime(dateFormat.format(calendar.getTime()));
            statCycleList.add(statCycle);
        }
        return statCycleList;
    }

    /**
     * Get stat cycle of day
     *
     * @param daysAgo
     * @return
     */
    public List<StartEndTime> getStatCycleOfDay(int daysAgo) {
        StartEndTime statCycle = new StartEndTime();
        Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.DATE, -daysAgo);
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);

        SimpleDateFormat dateFormat = new SimpleDateFormat(DATE_FORMAT);
        statCycle.setStartTime(dateFormat.format(calendar.getTime()));

        calendar.set(Calendar.HOUR_OF_DAY, 23);
        calendar.set(Calendar.MINUTE, 59);
        statCycle.setEndTime(dateFormat.format(calendar.getTime()));
        return new ArrayList<StartEndTime>() {

            {
                add(statCycle);
            }
        };
    }

    /**
     * Create data source
     */
    protected void createDataSource() {
        HikariConfig hikariConfig = JdbcUtils.buildHikariConfig(
                sourceConfig.getDriverClassName(),
                sourceConfig.getJdbcUrl(),
                sourceConfig.getUserName(),
                sourceConfig.getPassword());
        dataSource = new HikariDataSource(hikariConfig);
    }

    /**
     * Destory
     */
    public void destroy() {
        for (Map.Entry<Integer, ScheduledExecutorService> timer : statTimers.entrySet()) {
            timer.getValue().shutdown();
        }
    }

    /**
     * Stat server
     */
    class StatServer implements Runnable, AutoCloseable {

        private final int statBackTimes;
        private final ExecutorService executor =
                Executors.newFixedThreadPool(
                        Configuration.getInstance().get(KEY_STAT_THREAD_POOL_SIZE, DEFAULT_STAT_THREAD_POOL_SIZE));

        public StatServer(int statBackTimes) {
            this.statBackTimes = statBackTimes;
        }

        public void run() {
            long currentTimestamp = System.currentTimeMillis();
            LOGGER.info("Stat source data at {},stat back times:{}", currentTimestamp, statBackTimes);

            statByStep();

            long timeCost = System.currentTimeMillis() - currentTimestamp;
            LOGGER.info("Stat source data cost time:{}ms,stat back times:{}", timeCost, statBackTimes);
        }

        /**
         * Stat by step
         */
        public void statByStep() {
            List<String> auditIds = ConfigService.getInstance().getAuditIds();
            if (auditIds.isEmpty()) {
                LOGGER.info("No audit id need to stat!");
                return;
            }
            List<CompletableFuture<Void>> futures = new ArrayList<>();
            for (String auditId : auditIds) {
                CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                    aggregate(auditId);
                }, executor);
                futures.add(future);
            }
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()])).join();
        }

        /**
         * Aggregate
         *
         * @param auditId
         */
        public void aggregate(String auditId) {
            List<StartEndTime> statCycleList = sourceConfig.getAuditCycle() == DAY ? getStatCycleOfDay(statBackTimes)
                    : getStatCycleOfMinute(statBackTimes, sourceConfig.getAuditCycle().getValue());
            for (StartEndTime statCycle : statCycleList) {
                long currentTimestamp = System.currentTimeMillis();
                query(statCycle.getStartTime(), statCycle.getEndTime(), auditId);
                long timeCost = System.currentTimeMillis() - currentTimestamp;
                LOGGER.info("[{}]-[{}],{},stat back times:{},audit id:{},cost:{}ms",
                        statCycle.getStartTime(), statCycle.getEndTime(),
                        sourceConfig.getAuditCycle(),
                        statBackTimes, auditId, timeCost);
            }
        }

        /**
         * Query
         *
         * @param startTime
         * @param endTime
         * @param auditId
         */
        public void query(String startTime, String endTime, String auditId) {
            try (Connection connection = dataSource.getConnection();
                    PreparedStatement pstat = connection.prepareStatement(sourceConfig.getQuerySql())) {
                if (connection.isClosed()) {
                    createDataSource();
                }
                pstat.setString(1, startTime);
                pstat.setString(2, endTime);
                pstat.setString(3, auditId);
                if (sourceConfig.isNeedJoin()) {
                    pstat.setString(4, startTime);
                    pstat.setString(5, endTime);
                    pstat.setString(6, auditId);
                }
                try (ResultSet resultSet = pstat.executeQuery()) {
                    while (resultSet.next()) {
                        StatData data = new StatData();
                        data.setLogTs(startTime);
                        data.setInlongGroupId(resultSet.getString(1));
                        data.setInlongStreamId(resultSet.getString(2));
                        data.setAuditId(resultSet.getString(3));
                        String auditTag = resultSet.getString(4);
                        data.setAuditTag(StringUtils.isBlank(auditTag) ? DEFAULT_AUDIT_TAG : auditTag);
                        long count = resultSet.getLong(5);
                        data.setCount(count);
                        data.setSize(resultSet.getLong(6));
                        data.setDelay(CacheUtils.calculateAverageDelay(count, resultSet.getLong(7)));
                        dataQueue.push(data);
                    }
                } catch (SQLException sqlException) {
                    LOGGER.error("Query has SQL exception! ", sqlException);
                }
            } catch (Exception exception) {
                LOGGER.error("Query has exception! ", exception);
            }
        }

        @Override
        public void close() throws Exception {

        }
    }
}
