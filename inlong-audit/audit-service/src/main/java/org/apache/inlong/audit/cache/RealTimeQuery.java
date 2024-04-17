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

package org.apache.inlong.audit.cache;

import org.apache.inlong.audit.config.Configuration;
import org.apache.inlong.audit.entities.JdbcConfig;
import org.apache.inlong.audit.entities.StatData;
import org.apache.inlong.audit.service.ConfigService;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;

import static org.apache.inlong.audit.config.ConfigConstants.CACHE_PREP_STMTS;
import static org.apache.inlong.audit.config.ConfigConstants.DEFAULT_CACHE_PREP_STMTS;
import static org.apache.inlong.audit.config.ConfigConstants.DEFAULT_CONNECTION_TIMEOUT;
import static org.apache.inlong.audit.config.ConfigConstants.DEFAULT_DATASOURCE_POOL_SIZE;
import static org.apache.inlong.audit.config.ConfigConstants.DEFAULT_PREP_STMT_CACHE_SIZE;
import static org.apache.inlong.audit.config.ConfigConstants.DEFAULT_PREP_STMT_CACHE_SQL_LIMIT;
import static org.apache.inlong.audit.config.ConfigConstants.KEY_CACHE_PREP_STMTS;
import static org.apache.inlong.audit.config.ConfigConstants.KEY_DATASOURCE_CONNECTION_TIMEOUT;
import static org.apache.inlong.audit.config.ConfigConstants.KEY_DATASOURCE_POOL_SIZE;
import static org.apache.inlong.audit.config.ConfigConstants.KEY_PREP_STMT_CACHE_SIZE;
import static org.apache.inlong.audit.config.ConfigConstants.KEY_PREP_STMT_CACHE_SQL_LIMIT;
import static org.apache.inlong.audit.config.ConfigConstants.PREP_STMT_CACHE_SIZE;
import static org.apache.inlong.audit.config.ConfigConstants.PREP_STMT_CACHE_SQL_LIMIT;
import static org.apache.inlong.audit.config.SqlConstants.DEFAULT_SOURCE_QUERY_IDS_SQL;
import static org.apache.inlong.audit.config.SqlConstants.DEFAULT_SOURCE_QUERY_IPS_SQL;
import static org.apache.inlong.audit.config.SqlConstants.DEFAULT_SOURCE_QUERY_MINUTE_SQL;
import static org.apache.inlong.audit.config.SqlConstants.KEY_SOURCE_QUERY_IDS_SQL;
import static org.apache.inlong.audit.config.SqlConstants.KEY_SOURCE_QUERY_IPS_SQL;
import static org.apache.inlong.audit.config.SqlConstants.KEY_SOURCE_QUERY_MINUTE_SQL;

/**
 * Real time query data from audit source.
 */
public class RealTimeQuery {

    private static final Logger LOGGER = LoggerFactory.getLogger(RealTimeQuery.class);
    private static volatile RealTimeQuery realTimeQuery = null;

    private final List<DataSource> dataSourceList = new LinkedList<>();

    private final String queryLogTsSql;
    private final String queryIdsByIpSql;
    private final String queryReportIpsSql;

    private RealTimeQuery() {
        List<JdbcConfig> jdbcConfigList = ConfigService.getInstance().getAllAuditSource();
        for (JdbcConfig jdbcConfig : jdbcConfigList) {
            assert false;
            dataSourceList.add(createDataSource(jdbcConfig));
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
     * Create data source.
     */
    private DataSource createDataSource(JdbcConfig jdbcConfig) {
        HikariConfig config = new HikariConfig();
        config.setDriverClassName(jdbcConfig.getDriverClass());
        config.setJdbcUrl(jdbcConfig.getJdbcUrl());
        config.setUsername(jdbcConfig.getUserName());
        config.setPassword(jdbcConfig.getPassword());
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
        return new HikariDataSource(config);
    }

    /**
     * Query data which log time.
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
        List<StatData> statDataList = new LinkedList<>();
        for (DataSource dataSource : dataSourceList) {
            statDataList =
                    doQueryLogTs(dataSource, startTime, endTime, inlongGroupId, inlongStreamId, auditId);
            if (!statDataList.isEmpty()) {
                break;
            }
            LOGGER.info("Change another audit source to query data! Params is: {} {} {} {} {}",
                    startTime, endTime, inlongGroupId, inlongStreamId, auditId);
        }
        return statDataList;
    }

    /**
     * Do query log time.
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
                    data.setCount(resultSet.getLong(6));
                    data.setSize(resultSet.getLong(7));
                    data.setDelay(resultSet.getLong(8));
                    result.add(data);
                }
            } catch (SQLException sqlException) {
                LOGGER.error("Query has SQL exception! ", sqlException);
            }
        } catch (Exception exception) {
            LOGGER.error("Query has exception! ", exception);
        }
        return result;
    }

    /**
     * Query inlong group id by report ip.
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
            LOGGER.info("Change another audit source to query data! Params is: {} {} {} {}",
                    startTime, endTime, ip, auditId);
        }
        return statDataList;
    }

    /**
     * Do query inlong group id by report ip.
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
            LOGGER.info("doQueryIdsByIp {}", pstat.toString());
            try (ResultSet resultSet = pstat.executeQuery()) {
                while (resultSet.next()) {
                    StatData data = new StatData();
                    data.setInlongGroupId(resultSet.getString(1));
                    data.setInlongStreamId(resultSet.getString(2));
                    data.setAuditId(resultSet.getString(3));
                    data.setAuditTag(resultSet.getString(4));
                    data.setCount(resultSet.getLong(5));
                    data.setSize(resultSet.getLong(6));
                    data.setDelay(resultSet.getLong(7));
                    result.add(data);
                }
            } catch (SQLException sqlException) {
                LOGGER.error("Query has SQL exception! ", sqlException);
            }
        } catch (Exception exception) {
            LOGGER.error("Query has exception! ", exception);
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
    public List<StatData> queryReportIps(String startTime, String endTime, String inlongGroupId,
            String inlongStreamId, String auditId) {
        List<StatData> statDataList = new LinkedList<>();
        for (DataSource dataSource : dataSourceList) {
            statDataList = doQueryReportIps(dataSource, startTime, endTime, inlongGroupId, inlongStreamId, auditId);
            if (!statDataList.isEmpty()) {
                break;
            }
        }
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
    private List<StatData> doQueryReportIps(DataSource dataSource, String startTime, String endTime,
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
                    data.setCount(resultSet.getLong(2));
                    data.setSize(resultSet.getLong(3));
                    data.setDelay(resultSet.getLong(4));
                    result.add(data);
                }
            } catch (SQLException sqlException) {
                LOGGER.error("Query has SQL exception! ", sqlException);
            }
        } catch (Exception exception) {
            LOGGER.error("Query has exception! ", exception);
        }
        return result;
    }
}
