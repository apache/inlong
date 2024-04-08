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

package org.apache.inlong.audit.source;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.Data;
import org.apache.inlong.audit.channel.DataQueue;
import org.apache.inlong.audit.config.Configuration;
import org.apache.inlong.audit.entities.SourceConfig;
import org.apache.inlong.audit.entities.StartEndTime;
import org.apache.inlong.audit.entities.StatData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.apache.inlong.audit.config.ConfigConstants.CACHE_PREP_STMTS;
import static org.apache.inlong.audit.config.ConfigConstants.DEFAULT_AUDIT_IDS;
import static org.apache.inlong.audit.config.ConfigConstants.DEFAULT_CACHE_PREP_STMTS;
import static org.apache.inlong.audit.config.ConfigConstants.DEFAULT_CONNECTION_TIMEOUT;
import static org.apache.inlong.audit.config.ConfigConstants.DEFAULT_DATASOURCE_POOL_SIZE;
import static org.apache.inlong.audit.config.ConfigConstants.DEFAULT_PREP_STMT_CACHE_SIZE;
import static org.apache.inlong.audit.config.ConfigConstants.DEFAULT_PREP_STMT_CACHE_SQL_LIMIT;
import static org.apache.inlong.audit.config.ConfigConstants.DEFAULT_SOURCE_DB_STAT_INTERVAL;
import static org.apache.inlong.audit.config.ConfigConstants.KEY_AUDIT_IDS;
import static org.apache.inlong.audit.config.ConfigConstants.KEY_CACHE_PREP_STMTS;
import static org.apache.inlong.audit.config.ConfigConstants.KEY_DATASOURCE_CONNECTION_TIMEOUT;
import static org.apache.inlong.audit.config.ConfigConstants.KEY_DATASOURCE_POOL_SIZE;
import static org.apache.inlong.audit.config.ConfigConstants.KEY_PREP_STMT_CACHE_SIZE;
import static org.apache.inlong.audit.config.ConfigConstants.KEY_PREP_STMT_CACHE_SQL_LIMIT;
import static org.apache.inlong.audit.config.ConfigConstants.KEY_SOURCE_DB_STAT_INTERVAL;
import static org.apache.inlong.audit.config.ConfigConstants.PREP_STMT_CACHE_SIZE;
import static org.apache.inlong.audit.config.ConfigConstants.PREP_STMT_CACHE_SQL_LIMIT;
import static org.apache.inlong.audit.entities.AuditCycle.DAY;
import static org.apache.inlong.audit.entities.AuditCycle.HOUR;

/**
 * Jdbc source
 */
@Data
public class JdbcSource {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcSource.class);
    private final ConcurrentHashMap<Integer, ScheduledExecutorService> statTimers = new ConcurrentHashMap<>();
    private DataQueue dataQueue;
    private List<String> auditIds;
    private int querySqlTimeout;
    private DataSource dataSource;
    private String querySql;
    private SourceConfig sourceConfig;

    private static final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";
    private static final int MAX_MINUTE = 60;

    public JdbcSource(DataQueue dataQueue, SourceConfig sourceConfig) {
        this.dataQueue = dataQueue;
        this.sourceConfig = sourceConfig;
        auditIds = Arrays.asList(Configuration.getInstance().get(KEY_AUDIT_IDS, DEFAULT_AUDIT_IDS).split(";"));
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
        for (int statBackTime = 1; statBackTime < sourceConfig.getStatBackTimes(); statBackTime++) {
            ScheduledExecutorService timer =
                    statTimers.computeIfAbsent(statBackTime, k -> Executors.newSingleThreadScheduledExecutor());
            timer.scheduleWithFixedDelay(new StatServer(statBackTime),
                    statBackTime,
                    statInterval, TimeUnit.MINUTES);
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

            calendar.set(Calendar.MINUTE, minute + dataCycle - 1);
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
        HikariConfig config = new HikariConfig();
        config.setDriverClassName(sourceConfig.getDriverClassName());
        config.setJdbcUrl(sourceConfig.getJdbcUrl());
        config.setUsername(sourceConfig.getUsername());
        config.setPassword(sourceConfig.getPassword());
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
    class StatServer implements Runnable {

        private final int statBackTimes;

        public StatServer(int statBackTimes) {
            this.statBackTimes = statBackTimes;
        }

        public void run() {
            long currentTimestamp = System.currentTimeMillis();
            LOG.info("Stat source data at {},stat back times:{}", currentTimestamp, statBackTimes);

            statByStep();

            long timeCost = System.currentTimeMillis() - currentTimestamp;
            LOG.info("Stat source data cost time:{}ms,stat back times:{}", timeCost, statBackTimes);
        }

        /**
         * Stat by step
         */
        public void statByStep() {
            List<CompletableFuture<String>> futures = new ArrayList<>();
            for (String auditId : auditIds) {
                CompletableFuture<String> future = CompletableFuture.supplyAsync(() -> {
                    aggregate(auditId);
                    return "Task " + auditId + " completed";
                });
                futures.add(future);
            }
            CompletableFuture[] futureArray = futures.toArray(new CompletableFuture[futures.size()]);
            CompletableFuture<Void> allFutures = CompletableFuture.allOf(futureArray);
            allFutures.join();
        }

        /**
         * Aggregate
         * @param auditId
         */
        public void aggregate(String auditId) {
            List<StartEndTime> statCycleList = sourceConfig.getAuditCycle() == DAY ? getStatCycleOfDay(statBackTimes)
                    : getStatCycleOfMinute(statBackTimes, sourceConfig.getAuditCycle().getValue());
            for (StartEndTime statCycle : statCycleList) {
                long currentTimestamp = System.currentTimeMillis();
                query(statCycle.getStartTime(), statCycle.getEndTime(), auditId);
                long timeCost = System.currentTimeMillis() - currentTimestamp;
                LOG.info("[{}]-[{}],{},stat back times:{},audit id:{},cost:{}ms",
                        statCycle.getStartTime(), statCycle.getEndTime(),
                        sourceConfig.getAuditCycle(),
                        statBackTimes, auditId, timeCost);
            }
        }

        /**
         * Query
         * @param startTime
         * @param endTime
         * @param auditId
         */
        public void query(String startTime, String endTime, String auditId) {
            try {
                Connection connection = dataSource.getConnection();
                if (connection == null || connection.isClosed()) {
                    createDataSource();
                }
                PreparedStatement pstat = connection.prepareStatement(sourceConfig.getQuerySql());
                pstat.setString(1, startTime);
                pstat.setString(2, endTime);
                pstat.setString(3, auditId);
                ResultSet resultSet = pstat.executeQuery();
                while (resultSet.next()) {
                    String inlongGroupID = resultSet.getString(1);
                    String InlongStreamID = resultSet.getString(2);
                    String AuditId = resultSet.getString(3);
                    String AuditTag = resultSet.getString(4);
                    long count = resultSet.getLong(5);
                    long size = resultSet.getLong(6);
                    long delay = resultSet.getLong(7);
                    StatData data = new StatData();
                    data.setLogTs(startTime);
                    data.setInlongGroupId(inlongGroupID);
                    data.setInlongStreamId(InlongStreamID);
                    data.setAuditId(AuditId);
                    data.setAuditTag(AuditTag);
                    data.setCount(count);
                    data.setSize(size);
                    data.setDelay(delay);
                    dataQueue.push(data);
                }
                resultSet.close();
                pstat.close();
                connection.close();
            } catch (Exception exception) {
                LOG.error("Query has exception! ", exception);
            }
        }
    }
}
