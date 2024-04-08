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

package elector.impl;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import elector.api.SelectorConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.MessageFormat;
import java.util.concurrent.atomic.AtomicInteger;

import static config.ConfigConstants.CACHE_PREP_STMTS;
import static config.ConfigConstants.MAX_INIT_COUNT;
import static config.ConfigConstants.PREP_STMT_CACHE_SIZE;
import static config.ConfigConstants.PREP_STMT_CACHE_SQL_LIMIT;
import static config.SqlConstants.IS_LEADER_SQL;
import static config.SqlConstants.RELEASE_SQL;
import static config.SqlConstants.REPLACE_LEADER_SQL;
import static config.SqlConstants.SEARCH_CURRENT_LEADER_SQL;
import static config.SqlConstants.SELECTOR_SQL;
import static config.SqlConstants.SELECT_TEST_SQL;

/**
 * DB data source
 */
public class DBDataSource {

    private static final Logger logger = LoggerFactory.getLogger(DBDataSource.class);
    private String selectorSql = SELECTOR_SQL;
    private String replaceLeaderSql = REPLACE_LEADER_SQL;
    private String reLeaseSql = RELEASE_SQL;
    private String isLeaderSql = IS_LEADER_SQL;
    private String searchCurrentLeaderSql = SEARCH_CURRENT_LEADER_SQL;
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
                    formatSql(selectorConfig.getElectorDbName(), selectorConfig.getServiceId(),
                            selectorConfig.getLeaderId());
                }
            }
        } catch (Exception exception) {
            logger.error(exception.getMessage());
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

        while (!initSucc && initCount < MAX_INIT_COUNT) {
            try {
                ++initCount;
                if (datasource == null || datasource.isClosed()) {
                    HikariConfig config = new HikariConfig();
                    config.setDriverClassName(selectorConfig.getDbDriver());
                    logger.info("Init dataSource:{}", selectorConfig.getDbUrl());
                    config.setJdbcUrl(selectorConfig.getDbUrl());
                    config.setUsername(selectorConfig.getDbUser());
                    config.setPassword(selectorConfig.getDbPasswd());
                    config.setMaximumPoolSize(selectorConfig.getMaximumPoolSize());
                    config.setAutoCommit(true);
                    config.setConnectionTimeout((long) selectorConfig.getConnectionTimeout());
                    config.setMaxLifetime((long) selectorConfig.getMaxLifetime());
                    config.addDataSourceProperty(CACHE_PREP_STMTS, selectorConfig.getCachePrepStmts());
                    config.addDataSourceProperty(PREP_STMT_CACHE_SIZE, selectorConfig.getPrepStmtCacheSize());
                    config.addDataSourceProperty(PREP_STMT_CACHE_SQL_LIMIT,
                            selectorConfig.getPrepStmtCacheSqlLimit());
                    config.setConnectionTestQuery(SELECT_TEST_SQL);
                    datasource = new HikariDataSource(config);
                }

                initSucc = true;
            } catch (Exception exception) {
                logger.error("DB url:{},user name:{},password:{},exception:{}",
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
                    logger.error("Exception :{}", executeUpdatEexception.getMessage());
                }
            } catch (Exception pstmtEexception) {
                logger.error("Exception :{}", pstmtEexception.getMessage());
            }
            getConnectionFailTimes.set(0);
        } catch (Exception exception) {
            getConnectionFailTimes.addAndGet(1);
            logger.warn("Get Connection fail. {}", exception.getMessage());
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
                    logger.info("{} get the leader", selectorConfig.getLeaderId());
                } else if (result == 1) {
                    logger.info("{} do not get the leader", selectorConfig.getLeaderId());
                }
            } catch (Exception exception) {
                logger.error("Exception: {} ,sql:{}", exception.getMessage(), selectorSql);
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
                logger.info("Replace leader success.sql:{}", replaceLeaderSql);
            } else {
                logger.warn("Replace leader failed. sql:" + replaceLeaderSql);
            }

        } catch (Exception exception) {
            logger.error("Exception :{} ", exception.getMessage());
        }
    }

    /**
     * Release leader
     */
    public void releaseLeader() {
        try {
            int result = executeUpdate(reLeaseSql);
            logger.info("ReleaseLeader sql:{}", reLeaseSql);
            if (result == 1) {
                logger.info("{} release the leader success", selectorConfig.getLeaderId());
            }
        } catch (Exception exception) {
            logger.error("ReLease sql:{},exception {}:,", reLeaseSql, exception.getMessage());
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
                    logger.warn("DataSource is closed init is again");
                    initDataSource();
                }
                try (Connection connection = datasource.getConnection()) {
                    try (PreparedStatement pstmt = connection.prepareStatement(searchCurrentLeaderSql)) {
                        ResultSet resultSet = pstmt.executeQuery();
                        if (resultSet.next()) {
                            leaderId = resultSet.getString("leader");
                        }
                    } catch (Exception exception) {
                        logger.error("Exception {}", exception.getMessage());
                    }
                } catch (Throwable connectionException) {
                    logger.error("Exception {}", connectionException.getMessage());
                }
            } catch (Exception datasourceException) {
                logger.error("Exception {}", datasourceException.getMessage());
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
                logger.error("Exception {}", exception.getMessage());
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
        logger.info(selectorSql);
        replaceLeaderSql = MessageFormat.format(replaceLeaderSql, params);
        logger.info(replaceLeaderSql);
        reLeaseSql = MessageFormat.format(reLeaseSql, params);
        logger.info("ReLeaseSql:{}", reLeaseSql);
        isLeaderSql = MessageFormat.format(isLeaderSql, params);
        logger.info(isLeaderSql);
        searchCurrentLeaderSql = MessageFormat.format(searchCurrentLeaderSql, params);
        logger.info(searchCurrentLeaderSql);
    }
}
