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

package org.apache.inlong.audit.service.cache;

import org.apache.inlong.audit.service.config.ConfigConstants;
import org.apache.inlong.audit.service.config.Configuration;
import org.apache.inlong.audit.service.entities.JdbcConfig;
import org.apache.inlong.audit.service.entities.StatData;
import org.apache.inlong.audit.service.node.ConfigService;
import org.apache.inlong.audit.service.utils.AuditUtils;
import org.apache.inlong.audit.service.utils.CacheUtils;

import org.apache.commons.dbcp.BasicDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.apache.inlong.audit.consts.OpenApiConstants.DEFAULT_API_THREAD_POOL_SIZE;
import static org.apache.inlong.audit.consts.OpenApiConstants.KEY_API_THREAD_POOL_SIZE;
import static org.apache.inlong.audit.service.config.SqlConstants.DEFAULT_RECONCILIATION_DISTINCT_SQL;
import static org.apache.inlong.audit.service.config.SqlConstants.DEFAULT_RECONCILIATION_SQL;
import static org.apache.inlong.audit.service.config.SqlConstants.DEFAULT_SOURCE_QUERY_IDS_SQL;
import static org.apache.inlong.audit.service.config.SqlConstants.DEFAULT_SOURCE_QUERY_IPS_SQL;
import static org.apache.inlong.audit.service.config.SqlConstants.DEFAULT_SOURCE_QUERY_MINUTE_SQL;
import static org.apache.inlong.audit.service.config.SqlConstants.KEY_RECONCILIATION_DISTINCT_SQL;
import static org.apache.inlong.audit.service.config.SqlConstants.KEY_RECONCILIATION_SQL;
import static org.apache.inlong.audit.service.config.SqlConstants.KEY_SOURCE_QUERY_IDS_SQL;
import static org.apache.inlong.audit.service.config.SqlConstants.KEY_SOURCE_QUERY_IPS_SQL;
import static org.apache.inlong.audit.service.config.SqlConstants.KEY_SOURCE_QUERY_MINUTE_SQL;

/**
 * Real time query data from audit source.
 */
public class RealTimeQuery {

    private static final Logger LOGGER = LoggerFactory.getLogger(RealTimeQuery.class);
    private static volatile RealTimeQuery realTimeQuery = null;

    private final List<BasicDataSource> dataSourceList = new LinkedList<>();

    private final String queryLogTsSql;
    private final String queryIdsByIpSql;
    private final String queryReportIpsSql;
    private final ExecutorService executor =
            Executors.newFixedThreadPool(
                    Configuration.getInstance().get(KEY_API_THREAD_POOL_SIZE, DEFAULT_API_THREAD_POOL_SIZE));

    private RealTimeQuery() {

        List<JdbcConfig> jdbcConfigList = ConfigService.getInstance().getAllAuditSource();
        for (JdbcConfig jdbcConfig : jdbcConfigList) {
            BasicDataSource dataSource = new BasicDataSource();
            dataSource.setDriverClassName(jdbcConfig.getDriverClass());
            dataSource.setUrl(jdbcConfig.getJdbcUrl());
            dataSource.setUsername(jdbcConfig.getUserName());
            dataSource.setPassword(jdbcConfig.getPassword());
            dataSource
                    .setInitialSize(Configuration.getInstance().get(ConfigConstants.KEY_DATASOURCE_MIN_IDLE_CONNECTIONS,
                            ConfigConstants.DEFAULT_DATASOURCE_MIX_IDLE_CONNECTIONS));
            dataSource
                    .setMaxActive(Configuration.getInstance().get(ConfigConstants.KEY_DATASOURCE_MAX_TOTAL_CONNECTIONS,
                            ConfigConstants.DEFAULT_DATASOURCE_MAX_TOTAL_CONNECTIONS));
            dataSource.setMaxIdle(Configuration.getInstance().get(ConfigConstants.KEY_DATASOURCE_MAX_IDLE_CONNECTIONS,
                    ConfigConstants.DEFAULT_DATASOURCE_MAX_IDLE_CONNECTIONS));
            dataSource.setMinIdle(Configuration.getInstance().get(ConfigConstants.KEY_DATASOURCE_MIN_IDLE_CONNECTIONS,
                    ConfigConstants.DEFAULT_DATASOURCE_MIX_IDLE_CONNECTIONS));
            dataSource.setTestOnBorrow(true);
            dataSource.setValidationQuery("SELECT 1");
            dataSource
                    .setTimeBetweenEvictionRunsMillis(
                            Configuration.getInstance().get(ConfigConstants.KEY_DATASOURCE_DETECT_INTERVAL_MS,
                                    ConfigConstants.DEFAULT_DATASOURCE_DETECT_INTERVAL_MS));

            dataSourceList.add(dataSource);
        }

        queryLogTsSql = Configuration.getInstance().get(KEY_SOURCE_QUERY_MINUTE_SQL,
                DEFAULT_SOURCE_QUERY_MINUTE_SQL);
        queryIdsByIpSql = Configuration.getInstance().get(KEY_SOURCE_QUERY_IDS_SQL,
                DEFAULT_SOURCE_QUERY_IDS_SQL);
        queryReportIpsSql = Configuration.getInstance().get(KEY_SOURCE_QUERY_IPS_SQL,
                DEFAULT_SOURCE_QUERY_IPS_SQL);
    }

    public static RealTimeQuery getInstance() {
        if (realTimeQuery == null) {
            synchronized (Configuration.class) {
                if (realTimeQuery == null) {
                    realTimeQuery = new RealTimeQuery();
                }
            }
        }
        return realTimeQuery;
    }

    /**
     * Query the audit data of log time.
     *
     * @param startTime
     * @param endTime
     * @param inlongGroupId
     * @param inlongStreamId
     * @param auditId
     * @return
     */
    public List<StatData> queryLogTs(String startTime, String endTime, String inlongGroupId,
            String inlongStreamId, String auditId) {
        long currentTime = System.currentTimeMillis();
        List<StatData> statDataList = new CopyOnWriteArrayList<>();
        if (dataSourceList.isEmpty()) {
            return statDataList;
        }
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (DataSource dataSource : dataSourceList) {
            CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                List<StatData> statDataListTemp =
                        doQueryLogTs(dataSource, startTime, endTime, inlongGroupId, inlongStreamId, auditId);
                statDataList.addAll(statDataListTemp);
            }, executor);
            futures.add(future);
        }
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        LOGGER.info("Query log ts by params: {} {} {} {} {}, total cost {} ms", startTime, endTime, inlongGroupId,
                inlongStreamId, auditId, System.currentTimeMillis() - currentTime);
        return filterMaxAuditVersion(statDataList);
    }

    /**
     * @param allStatData
     * @return
     */
    public List<StatData> filterMaxAuditVersion(List<StatData> allStatData) {
        HashMap<String, List<StatData>> allData = new HashMap<>();
        for (StatData statData : allStatData) {
            String dataKey = CacheUtils.buildCacheKey(
                    statData.getLogTs(),
                    statData.getInlongGroupId(),
                    statData.getInlongStreamId(),
                    statData.getAuditId(),
                    statData.getAuditTag());
            List<StatData> statDataList = allData.computeIfAbsent(dataKey, k -> new LinkedList<>());
            statDataList.add(statData);
        }
        List<StatData> result = new LinkedList<>();
        for (Map.Entry<String, List<StatData>> entry : allData.entrySet()) {
            long maxAuditVersion = Long.MIN_VALUE;
            for (StatData maxData : entry.getValue()) {
                maxAuditVersion = Math.max(maxData.getAuditVersion(), maxAuditVersion);
            }
            for (StatData statData : entry.getValue()) {
                if (statData.getAuditVersion() == maxAuditVersion) {
                    result.add(statData);
                    break;
                }
            }
        }
        return result;
    }

    /**
     * Do query the audit data of log time.
     *
     * @param dataSource
     * @param startTime
     * @param endTime
     * @param inlongGroupId
     * @param inlongStreamId
     * @param auditId
     * @return
     */
    private List<StatData> doQueryLogTs(DataSource dataSource, String startTime, String endTime, String inlongGroupId,
            String inlongStreamId, String auditId) {
        long currentTime = System.currentTimeMillis();
        List<StatData> result = new LinkedList<>();
        try (Connection connection = dataSource.getConnection();
                PreparedStatement pstat = connection.prepareStatement(queryLogTsSql)) {
            pstat.setString(1, startTime);
            pstat.setString(2, endTime);
            pstat.setString(3, inlongGroupId);
            pstat.setString(4, inlongStreamId);
            pstat.setString(5, auditId);
            try (ResultSet resultSet = pstat.executeQuery()) {
                while (resultSet.next()) {
                    StatData data = new StatData();
                    data.setLogTs(resultSet.getString(1));
                    data.setInlongGroupId(resultSet.getString(2));
                    data.setInlongStreamId(resultSet.getString(3));
                    data.setAuditId(resultSet.getString(4));
                    data.setAuditTag(resultSet.getString(5));
                    long count = resultSet.getLong(6);
                    data.setCount(count);
                    data.setSize(resultSet.getLong(7));
                    data.setDelay(CacheUtils.calculateAverageDelay(count, resultSet.getLong(8)));
                    data.setAuditVersion(resultSet.getLong(9));
                    result.add(data);
                }
            } catch (SQLException sqlException) {
                LOGGER.error("Query log time has SQL exception!, datasource={} ", dataSource, sqlException);
            }
        } catch (Exception exception) {
            LOGGER.error("Query log time has exception!, datasource={} ", dataSource, exception);
        }
        LOGGER.info("Query log ts by params: {} {} {} {} {}, cost {} ms", startTime, endTime, inlongGroupId,
                inlongStreamId, auditId, System.currentTimeMillis() - currentTime);
        return result;
    }

    /**
     * Query InLong group id by report ip.
     *
     * @param startTime
     * @param endTime
     * @param ip
     * @param auditId
     * @return
     */
    public List<StatData> queryIdsByIp(String startTime, String endTime, String ip, String auditId) {
        List<StatData> statDataList = new LinkedList<>();
        for (DataSource dataSource : dataSourceList) {
            statDataList = doQueryIdsByIp(dataSource, startTime, endTime, ip, auditId);
            if (!statDataList.isEmpty()) {
                break;
            }
        }
        LOGGER.info("Query ids by params:{} {} {} {}, result size:{} ", startTime,
                endTime, ip, auditId, statDataList.size());
        return statDataList;
    }

    /**
     * Do query InLong group id by report ip.
     *
     * @param dataSource
     * @param startTime
     * @param endTime
     * @param ip
     * @param auditId
     * @return
     */
    private List<StatData> doQueryIdsByIp(DataSource dataSource, String startTime, String endTime, String ip,
            String auditId) {
        List<StatData> result = new LinkedList<>();
        try (Connection connection = dataSource.getConnection();
                PreparedStatement pstat = connection.prepareStatement(queryIdsByIpSql)) {
            pstat.setString(1, startTime);
            pstat.setString(2, endTime);
            pstat.setString(3, auditId);
            pstat.setString(4, ip);
            try (ResultSet resultSet = pstat.executeQuery()) {
                while (resultSet.next()) {
                    StatData data = new StatData();
                    data.setInlongGroupId(resultSet.getString(1));
                    data.setInlongStreamId(resultSet.getString(2));
                    data.setAuditId(resultSet.getString(3));
                    data.setAuditTag(resultSet.getString(4));
                    long count = resultSet.getLong(5);
                    data.setCount(count);
                    data.setSize(resultSet.getLong(6));
                    data.setDelay(CacheUtils.calculateAverageDelay(count, resultSet.getLong(7)));
                    result.add(data);
                }
            } catch (SQLException sqlException) {
                LOGGER.error("Query inLongGroupIds has SQL exception!, datasource={} ", dataSource, sqlException);
            }
        } catch (Exception exception) {
            LOGGER.error("Query inLongGroupIds has exception!, datasource={} ", dataSource, exception);
        }
        return result;
    }

    /**
     * Query report ips.
     *
     * @param startTime
     * @param endTime
     * @param inlongGroupId
     * @param inlongStreamId
     * @param auditId
     * @return
     */
    public List<StatData> queryIpsById(String startTime, String endTime, String inlongGroupId,
            String inlongStreamId, String auditId) {
        List<StatData> statDataList = new LinkedList<>();
        for (DataSource dataSource : dataSourceList) {
            statDataList = doQueryIpsById(dataSource, startTime, endTime, inlongGroupId, inlongStreamId, auditId);
            if (!statDataList.isEmpty()) {
                break;
            }
        }
        LOGGER.info("Query ips by params:{} {} {} {} {}, result size:{} ",
                startTime, endTime, inlongGroupId, inlongStreamId, auditId, statDataList.size());
        return statDataList;
    }

    /**
     * Do query report ips.
     *
     * @param dataSource
     * @param startTime
     * @param endTime
     * @param inlongGroupId
     * @param inlongStreamId
     * @param auditId
     * @return
     */
    private List<StatData> doQueryIpsById(DataSource dataSource, String startTime, String endTime,
            String inlongGroupId,
            String inlongStreamId, String auditId) {
        List<StatData> result = new LinkedList<>();
        try (Connection connection = dataSource.getConnection();
                PreparedStatement pstat = connection.prepareStatement(queryReportIpsSql)) {
            pstat.setString(1, startTime);
            pstat.setString(2, endTime);
            pstat.setString(3, inlongGroupId);
            pstat.setString(4, inlongStreamId);
            pstat.setString(5, auditId);
            try (ResultSet resultSet = pstat.executeQuery()) {
                while (resultSet.next()) {
                    StatData data = new StatData();
                    data.setIp(resultSet.getString(1));
                    long count = resultSet.getLong(2);
                    data.setCount(count);
                    data.setSize(resultSet.getLong(3));
                    data.setDelay(CacheUtils.calculateAverageDelay(count, resultSet.getLong(4)));
                    result.add(data);
                }
            } catch (SQLException sqlException) {
                LOGGER.error("Query ips has SQL exception!, datasource={} ", dataSource, sqlException);
            }
        } catch (Exception exception) {
            LOGGER.error("Query ips has exception! ", exception);
        }
        return result;
    }

    public List<StatData> queryAuditData(String startTime, String endTime,
            String inlongGroupId, String inlongStreamId, String auditId,
            String auditTag, boolean distinct) {
        long currentTime = System.currentTimeMillis();
        List<StatData> statDataList = new CopyOnWriteArrayList<>();
        if (dataSourceList.isEmpty()) {
            return null;
        }
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (DataSource dataSource : dataSourceList) {
            CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                StatData statDataListTemp =
                        doQueryAuditData(dataSource, startTime, endTime, inlongGroupId, inlongStreamId, auditId,
                                auditTag, distinct);
                if (statDataListTemp != null) {
                    statDataList.add(statDataListTemp);
                }
            }, executor);
            futures.add(future);
        }
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        LOGGER.info("Query audit data by params: {} {} {} {} {}, total cost {} ms", startTime, endTime, inlongGroupId,
                inlongStreamId, auditId, System.currentTimeMillis() - currentTime);
        return statDataList;

    }

    public StatData doQueryAuditData(DataSource dataSource, String startTime, String endTime,
            String inlongGroupId, String inlongStreamId, String auditId,
            String auditTag, boolean distinct) {
        List<StatData> result = new LinkedList<>();
        String querySQL = distinct
                ? Configuration.getInstance().get(KEY_RECONCILIATION_DISTINCT_SQL, DEFAULT_RECONCILIATION_DISTINCT_SQL)
                : Configuration.getInstance().get(KEY_RECONCILIATION_SQL, DEFAULT_RECONCILIATION_SQL);

        try (Connection connection = dataSource.getConnection();
                PreparedStatement pstat = connection.prepareStatement(querySQL)) {

            pstat.setString(1, startTime);
            pstat.setString(2, endTime);
            pstat.setString(3, auditId);
            pstat.setString(4, inlongGroupId);
            pstat.setString(5, inlongStreamId);
            pstat.setString(6, auditTag);
            try (ResultSet resultSet = pstat.executeQuery()) {
                while (resultSet.next()) {
                    StatData data = new StatData();
                    data.setAuditVersion(resultSet.getLong(1));
                    data.setCount(resultSet.getLong(2));
                    data.setLogTs(startTime);
                    data.setInlongGroupId(inlongGroupId);
                    data.setInlongStreamId(inlongStreamId);
                    data.setAuditId(auditId);
                    data.setAuditTag(auditTag);
                    result.add(data);
                }
            } catch (SQLException sqlException) {
                LOGGER.error("Query ips has SQL exception!, datasource={} ", dataSource, sqlException);
            }
        } catch (Exception exception) {
            LOGGER.error("Query audit data has exception! ", exception);
        }
        return AuditUtils.getMaxAuditVersionAuditData(result);
    }
}
