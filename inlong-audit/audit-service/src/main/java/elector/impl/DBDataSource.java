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
import elector.api.ElectorConfig;
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

    private static final Logger logger = LoggerFactory.getLogger(DBDataSource.class);
    private String electorSql =
            "insert ignore into {0} (service_id, leader_id, last_seen_active) values (''{1}'', ''{2}'', now()) on duplicate key update leader_id = if(last_seen_active < now() - interval # second, values(leader_id), leader_id),last_seen_active = if(leader_id = values(leader_id), values(last_seen_active), last_seen_active)";
    private String replaceLeaderSql =
            "replace into {0} (\n   service_id, leader_id, last_seen_active )\nvalues (''{1}'', ''#'', now())";
    private String reLeaseSql = "delete from {0} where service_id=''{1}'' and leader_id= ''{2}''";
    private String isLeaderSql = "select count(*) as is_leader from {0} where service_id=''{1}'' and leader_id=''{2}''";
    private String searchCurrentLeaderSql = "select leader_id as leader from {0} where service_id=''{1}''";
    private ElectorConfig electorConfig;
    private HikariDataSource datasource;
    public AtomicInteger getConnectionFailTimes;

    public DBDataSource(ElectorConfig electorConfig) {
        this.electorConfig = electorConfig;
        this.getConnectionFailTimes = new AtomicInteger(0);
    }

    /**
     * init
     *
     * @param reBuildDataSource
     * @throws Exception
     */
    public void init(boolean reBuildDataSource) throws Exception {
        try {
            if (!electorConfig.isUseDefaultLeader()) {
                initDataSource();
                if (!reBuildDataSource) {
                    formatSql(electorConfig.getElectorDbName(), electorConfig.getServiceId(),
                            electorConfig.getLeaderId());
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

        while (!initSucc && initCount < 2) {
            try {
                ++initCount;
                if (datasource == null || datasource.isClosed()) {
                    HikariConfig config = new HikariConfig();
                    config.setDriverClassName(electorConfig.getDbDriver());
                    logger.info("## init dataSource:" + electorConfig.getDbUrl());
                    config.setJdbcUrl(electorConfig.getDbUrl());
                    config.setUsername(electorConfig.getDbUser());
                    config.setPassword(electorConfig.getDbPasswd());
                    config.setMaximumPoolSize(electorConfig.getMaximumPoolSize());
                    config.setAutoCommit(true);
                    config.setConnectionTimeout((long) electorConfig.getConnectionTimeout());
                    config.setMaxLifetime((long) electorConfig.getMaxLifetime());
                    config.addDataSourceProperty("cachePrepStmts", electorConfig.getCachePrepStmts());
                    config.addDataSourceProperty("prepStmtCacheSize", electorConfig.getPrepStmtCacheSize());
                    config.addDataSourceProperty("prepStmtCacheSqlLimit",
                            electorConfig.getPrepStmtCacheSqlLimit());
                    config.setConnectionTestQuery("SELECT 1");
                    datasource = new HikariDataSource(config);
                }

                initSucc = true;
            } catch (Exception exception) {
                logger.error(exception.getMessage());
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

            Connection connection = datasource.getConnection();
            try {
                PreparedStatement pstmt = connection.prepareStatement(sql);
                try {
                    result = pstmt.executeUpdate();
                } catch (Exception executeUpdatEexception) {
                    logger.error("Exception :{}", executeUpdatEexception.getMessage());
                } finally {
                    if (pstmt != null) {
                        pstmt.close();
                    }
                }
            } catch (Exception pstmtEexception) {
                logger.error("Exception :{}", pstmtEexception.getMessage());
            } finally {
                if (connection != null) {
                    connection.close();
                }
            }
            getConnectionFailTimes.set(0);
        } catch (Exception e) {
            getConnectionFailTimes.addAndGet(1);
            logger.warn("## get Connection fail ...");
        }
        return result;
    }

    /**
     * Leader selector
     */
    public void leaderSelector() {
        if (!electorConfig.isUseDefaultLeader()) {
            try {
                int result = executeUpdate(electorSql);
                if (result == 2) {
                    logger.info(electorConfig.getLeaderId() + " get the leader");
                } else if (result == 1) {
                    logger.info(electorConfig.getLeaderId() + " do not get the leader");
                }
            } catch (Exception wxception) {
                logger.error("Exception: {} ,sql:{}", wxception.getMessage(), electorSql);
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
                logger.info("## replace leader succ sql:" + replaceLeaderSql);
            } else {
                logger.warn("## replace leader fail sql:" + replaceLeaderSql);
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
            logger.info("ReleaseLeader sql:" + reLeaseSql);
            if (result == 1) {
                logger.info(electorConfig.getLeaderId() + " release the leader success");
            }
        } catch (Exception exception) {
            logger.error("Exception {}:,sql:{}", exception.getMessage(), reLeaseSql);
        }

    }

    /**
     * Get current leader
     *
     * @return
     */
    public String getCurrentLeader() {
        if (electorConfig.isUseDefaultLeader()) {
            return electorConfig.getDefaultLeaderId();
        } else {
            String leaderId = "";

            try {
                if (null == datasource || datasource.isClosed()) {
                    logger.warn("## dataSource is closed init is again");
                    initDataSource();
                }
                Connection connection = datasource.getConnection();
                try {
                    PreparedStatement pstmt = connection.prepareStatement(searchCurrentLeaderSql);
                    try {
                        ResultSet resultSet = pstmt.executeQuery();
                        if (resultSet.next()) {
                            leaderId = resultSet.getString("leader");
                        }
                    } catch (Exception exception) {
                        logger.error("Exception {}", exception.getMessage());
                    } finally {
                        if (pstmt != null) {
                            pstmt.close();
                        }
                    }
                } catch (Throwable connectionException) {
                    logger.error("Exception {}", connectionException.getMessage());
                } finally {
                    if (connection != null) {
                        connection.close();
                    }
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
        } else {
            return true;
        }
    }

    /**
     * Format sql
     *
     * @param params
     */
    public void formatSql(String... params) {
        electorSql = MessageFormat.format(electorSql, params);
        electorSql = electorSql.replaceAll("#", electorConfig.getLeaderTimeout() + "");
        logger.info(electorSql);
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
