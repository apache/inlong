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
import org.apache.inlong.audit.service.utils.AuditUtils;
import org.apache.inlong.audit.service.utils.JdbcUtils;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import static org.apache.inlong.audit.service.config.AuditColumn.COLUMN_AUDIT_ID;
import static org.apache.inlong.audit.service.config.AuditColumn.COLUMN_AUDIT_TAG;
import static org.apache.inlong.audit.service.config.AuditColumn.COLUMN_CNT;
import static org.apache.inlong.audit.service.config.AuditColumn.COLUMN_DELAY;
import static org.apache.inlong.audit.service.config.AuditColumn.COLUMN_GROUP_ID;
import static org.apache.inlong.audit.service.config.AuditColumn.COLUMN_LOG_TS;
import static org.apache.inlong.audit.service.config.AuditColumn.COLUMN_SIZE;
import static org.apache.inlong.audit.service.config.AuditColumn.COLUMN_STREAM_ID;
import static org.apache.inlong.audit.service.config.SqlConstants.DEFAULT_MYSQL_SOURCE_QUERY_DAY_SQL;
import static org.apache.inlong.audit.service.config.SqlConstants.KEY_MYSQL_SOURCE_QUERY_DAY_SQL;
import static org.apache.inlong.audit.service.config.SqlConstants.WILDCARD_STREAM_ID;

/**
 * Cache Of day ,for day openapi
 */
public class DayCache implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(DayCache.class);
    private static volatile DayCache dayCache = null;
    private DataSource dataSource;

    private DayCache() {
        createDataSource();
    }

    /**
     * Get instance
     *
     * @return
     */
    public static DayCache getInstance() {
        if (dayCache == null) {
            synchronized (Configuration.class) {
                if (dayCache == null) {
                    dayCache = new DayCache();
                }
            }
        }
        return dayCache;
    }

    /**
     * Get data
     *
     * @param startTime
     * @param endTime
     * @param inlongGroupId
     * @param inlongStreamId
     * @param auditId
     * @return
     */
    public List<StatData> getData(String startTime, String endTime, String inlongGroupId,
            String inlongStreamId, String auditId) {
        List<StatData> result = new LinkedList<>();
        String querySQL = Configuration.getInstance().get(KEY_MYSQL_SOURCE_QUERY_DAY_SQL,
                DEFAULT_MYSQL_SOURCE_QUERY_DAY_SQL);
        List<String> paramList = new ArrayList<>();
        if (WILDCARD_STREAM_ID.equals(inlongStreamId)) {
            querySQL = AuditUtils.removeStreamIdCondition(querySQL);
            querySQL = AuditUtils.removeStreamIdColumn(querySQL);
            paramList.add(startTime);
            paramList.add(endTime);
            paramList.add(inlongGroupId);
            paramList.add(auditId);
        } else {
            paramList.add(startTime);
            paramList.add(endTime);
            paramList.add(inlongGroupId);
            paramList.add(inlongStreamId);
            paramList.add(auditId);
        }
        try (Connection connection = dataSource.getConnection();
                PreparedStatement pstat = connection.prepareStatement(querySQL)) {
            if (connection.isClosed()) {
                createDataSource();
            }
            for (int i = 0; i < paramList.size(); i++) {
                pstat.setString(i + 1, paramList.get(i));
            }
            try (ResultSet resultSet = pstat.executeQuery()) {
                while (resultSet.next()) {
                    StatData data = new StatData();
                    data.setLogTs(resultSet.getString(COLUMN_LOG_TS));
                    data.setInlongGroupId(resultSet.getString(COLUMN_GROUP_ID));
                    data.setAuditId(resultSet.getString(COLUMN_AUDIT_ID));
                    data.setAuditTag(resultSet.getString(COLUMN_AUDIT_TAG));
                    data.setCount(resultSet.getLong(COLUMN_CNT));
                    data.setSize(resultSet.getLong(COLUMN_SIZE));
                    data.setDelay(resultSet.getLong(COLUMN_DELAY));

                    if (WILDCARD_STREAM_ID.equals(inlongStreamId)) {
                        data.setInlongStreamId(WILDCARD_STREAM_ID);
                    } else {
                        data.setInlongStreamId(resultSet.getString(COLUMN_STREAM_ID));
                    }

                    result.add(data);
                }
            } catch (SQLException sqlException) {
                LOGGER.error("Query has SQL exception! ", sqlException);
            }
        } catch (Exception exception) {
            LOGGER.error("Query has exception! ", exception);
        }

        if (result.isEmpty()) {
            result.add(new StatData(startTime, inlongGroupId, inlongStreamId, auditId));
        }

        return WILDCARD_STREAM_ID.equals(inlongStreamId)
                ? AuditUtils.aggregateStatData(result, inlongStreamId)
                : result;
    }

    /**
     * Create data source
     */
    private void createDataSource() {
        JdbcConfig jdbcConfig = JdbcUtils.buildMysqlConfig();

        HikariConfig config = new HikariConfig();
        config.setDriverClassName(jdbcConfig.getDriverClass());
        config.setJdbcUrl(jdbcConfig.getJdbcUrl());
        config.setUsername(jdbcConfig.getUserName());
        config.setPassword(jdbcConfig.getPassword());
        config.setConnectionTimeout(Configuration.getInstance().get(ConfigConstants.KEY_DATASOURCE_CONNECTION_TIMEOUT,
                ConfigConstants.DEFAULT_CONNECTION_TIMEOUT));
        config.addDataSourceProperty(ConfigConstants.CACHE_PREP_STMTS,
                Configuration.getInstance().get(ConfigConstants.KEY_CACHE_PREP_STMTS,
                        ConfigConstants.DEFAULT_CACHE_PREP_STMTS));
        config.addDataSourceProperty(ConfigConstants.PREP_STMT_CACHE_SIZE,
                Configuration.getInstance().get(ConfigConstants.KEY_PREP_STMT_CACHE_SIZE,
                        ConfigConstants.DEFAULT_PREP_STMT_CACHE_SIZE));
        config.addDataSourceProperty(ConfigConstants.PREP_STMT_CACHE_SQL_LIMIT,
                Configuration.getInstance().get(ConfigConstants.KEY_PREP_STMT_CACHE_SQL_LIMIT,
                        ConfigConstants.DEFAULT_PREP_STMT_CACHE_SQL_LIMIT));
        config.setMaximumPoolSize(
                Configuration.getInstance().get(ConfigConstants.KEY_DATASOURCE_POOL_SIZE,
                        ConfigConstants.DEFAULT_DATASOURCE_POOL_SIZE));
        dataSource = new HikariDataSource(config);
    }

    @Override
    public void close() throws Exception {

    }
}
