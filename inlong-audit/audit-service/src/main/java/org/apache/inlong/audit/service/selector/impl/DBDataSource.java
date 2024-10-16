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

package org.apache.inlong.audit.service.selector.impl;

import org.apache.inlong.audit.service.config.ConfigConstants;
import org.apache.inlong.audit.service.config.SqlConstants;
import org.apache.inlong.audit.service.selector.api.SelectorConfig;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.MessageFormat;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * DB data source
 */
public class DBDataSource {

    private static final Logger LOGGER = LoggerFactory.getLogger(DBDataSource.class);
    private String selectorSql = SqlConstants.SELECTOR_SQL;
    private String replaceLeaderSql = SqlConstants.REPLACE_LEADER_SQL;
    private String reLeaseSql = SqlConstants.RELEASE_SQL;
    private String isLeaderSql = SqlConstants.IS_LEADER_SQL;
    private String searchCurrentLeaderSql = SqlConstants.SEARCH_CURRENT_LEADER_SQL;
    private final SelectorConfig selectorConfig;
    private HikariDataSource datasource;
    public AtomicInteger getConnectionFailTimes;

    public DBDataSource(SelectorConfig selectorConfig) {
        this.selectorConfig = selectorConfig;
        this.getConnectionFailTimes = new AtomicInteger(0);
    }

    /**
     * init
     *
     * @param needFormatSql
     * @throws Exception
     */
    public void init(boolean needFormatSql) throws Exception {
        try {
            if (!selectorConfig.isUseDefaultLeader()) {
                initDataSource();
                if (needFormatSql) {
                    formatSql(selectorConfig.getSelectorDbName(), selectorConfig.getServiceId(),
                            selectorConfig.getLeaderId());
                }
            }
        } catch (Exception exception) {
            LOGGER.error(exception.getMessage());
            throw exception;
        }
    }

    /**
     * init data source
     *
     * @throws Exception
     */
    public void initDataSource() throws Exception {
        boolean initSucc = false;
        int initCount = 0;

        while (!initSucc && initCount < ConfigConstants.MAX_INIT_COUNT) {
            try {
                ++initCount;
                if (datasource == null || datasource.isClosed()) {
                    HikariConfig config = new HikariConfig();
                    config.setDriverClassName(selectorConfig.getDbDriver());
                    LOGGER.info("Init dataSource:{}", selectorConfig.getDbUrl());
                    config.setJdbcUrl(selectorConfig.getDbUrl());
                    config.setUsername(selectorConfig.getDbUser());
                    config.setPassword(selectorConfig.getDbPasswd());
                    config.setMaximumPoolSize(selectorConfig.getMaximumPoolSize());
                    config.setAutoCommit(true);
                    config.setConnectionTimeout((long) selectorConfig.getConnectionTimeout());
                    config.setMaxLifetime((long) selectorConfig.getMaxLifetime());
                    config.addDataSourceProperty(ConfigConstants.CACHE_PREP_STMTS, selectorConfig.getCachePrepStmts());
                    config.addDataSourceProperty(ConfigConstants.PREP_STMT_CACHE_SIZE,
                            selectorConfig.getPrepStmtCacheSize());
                    config.addDataSourceProperty(ConfigConstants.PREP_STMT_CACHE_SQL_LIMIT,
                            selectorConfig.getPrepStmtCacheSqlLimit());
                    config.setConnectionTestQuery(SqlConstants.SELECT_TEST_SQL);
                    datasource = new HikariDataSource(config);
                }

                initSucc = true;
            } catch (Exception exception) {
                LOGGER.error("DB url:{},user name:{},password:{},exception:{}",
                        selectorConfig.getDbUrl(),
                        selectorConfig.getDbUser(),
                        selectorConfig.getDbPasswd(),
                        exception.getMessage());
            }
        }

        if (!initSucc) {
            throw new Exception("## DBDataSource init Fail！");
        }
    }

    /**
     * close
     */
    public void close() {
        datasource.close();
    }

    /**
     * Execute update
     *
     * @param sql
     * @return
     */
    private int executeUpdate(String sql) {
        int result = 0;
        try {
            if ((null == datasource) || (datasource.isClosed())) {
                initDataSource();
            }

            try (Connection connection = datasource.getConnection()) {
                try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
                    result = pstmt.executeUpdate();
                } catch (Exception executeUpdatEexception) {
                    LOGGER.error("Exception :{}", executeUpdatEexception.getMessage());
                }
            } catch (Exception pstmtEexception) {
                LOGGER.error("Exception :{}", pstmtEexception.getMessage());
            }
            getConnectionFailTimes.set(0);
        } catch (Exception exception) {
            getConnectionFailTimes.addAndGet(1);
            LOGGER.warn("Get Connection fail. {}", exception.getMessage());
        }
        return result;
    }

    /**
     * Leader selector
     */
    public void leaderSelector() {
        if (!selectorConfig.isUseDefaultLeader()) {
            try {
                int result = executeUpdate(selectorSql);
                if (result == 2) {
                    LOGGER.info("{} become the leader", selectorConfig.getLeaderId());
                } else if (result == 1) {
                    LOGGER.info("{} waiting to be the leader", selectorConfig.getLeaderId());
                }
            } catch (Exception exception) {
                LOGGER.error("Exception: {} ,sql:{}", exception.getMessage(), selectorSql);
            }

        }
    }

    /**
     * Replace leader
     *
     * @param replaceLeaderId
     */
    public void replaceLeader(String replaceLeaderId) {
        replaceLeaderSql = replaceLeaderSql.replaceAll("#", replaceLeaderId);

        try {
            int result = executeUpdate(replaceLeaderSql);
            if (result > 0) {
                LOGGER.info("Replace leader success.sql:{}", replaceLeaderSql);
            } else {
                LOGGER.warn("Replace leader failed. sql:" + replaceLeaderSql);
            }

        } catch (Exception exception) {
            LOGGER.error("Exception :{} ", exception.getMessage());
        }
    }

    /**
     * Release leader
     */
    public void releaseLeader() {
        try {
            int result = executeUpdate(reLeaseSql);
            LOGGER.info("ReleaseLeader sql:{}", reLeaseSql);
            if (result == 1) {
                LOGGER.info("{} release the leader success", selectorConfig.getLeaderId());
            }
        } catch (Exception exception) {
            LOGGER.error("ReLease sql:{},exception {}:,", reLeaseSql, exception.getMessage());
        }

    }

    /**
     * Get current leader
     *
     * @return
     */
    public String getCurrentLeader() {
        if (selectorConfig.isUseDefaultLeader()) {
            return selectorConfig.getDefaultLeaderId();
        } else {
            String leaderId = "";

            try {
                if (null == datasource || datasource.isClosed()) {
                    LOGGER.warn("DataSource is closed init is again");
                    initDataSource();
                }
                try (Connection connection = datasource.getConnection()) {
                    try (PreparedStatement pstmt = connection.prepareStatement(searchCurrentLeaderSql)) {
                        ResultSet resultSet = pstmt.executeQuery();
                        if (resultSet.next()) {
                            leaderId = resultSet.getString("leader");
                        }
                    } catch (Exception exception) {
                        LOGGER.error("Exception {}", exception.getMessage());
                    }
                } catch (Throwable connectionException) {
                    LOGGER.error("Exception {}", connectionException.getMessage());
                }
            } catch (Exception datasourceException) {
                LOGGER.error("Exception {}", datasourceException.getMessage());
            }

            return leaderId;
        }
    }

    /**
     * Judge DB data source whether to closed
     *
     * @return
     */
    public boolean isDBDataSourceClosed() {
        if (this.datasource != null) {
            try {
                Connection con = datasource.getConnection();
                if (con != null) {
                    con.close();
                }
                return false;
            } catch (Exception exception) {
                LOGGER.error("Exception {}", exception.getMessage());
                return true;
            }
        }
        return true;
    }

    /**
     * Format sql
     *
     * @param params
     */
    public void formatSql(String... params) {
        selectorSql = MessageFormat.format(selectorSql, params);
        selectorSql = selectorSql.replaceAll("#", selectorConfig.getLeaderTimeout() + "");
        LOGGER.info(selectorSql);
        replaceLeaderSql = MessageFormat.format(replaceLeaderSql, params);
        LOGGER.info(replaceLeaderSql);
        reLeaseSql = MessageFormat.format(reLeaseSql, params);
        LOGGER.info("ReLeaseSql:{}", reLeaseSql);
        isLeaderSql = MessageFormat.format(isLeaderSql, params);
        LOGGER.info(isLeaderSql);
        searchCurrentLeaderSql = MessageFormat.format(searchCurrentLeaderSql, params);
        LOGGER.info(searchCurrentLeaderSql);
    }
}
