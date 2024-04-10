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
import org.apache.inlong.audit.entities.StatData;

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
import java.util.Objects;

import static org.apache.inlong.audit.config.ConfigConstants.CACHE_PREP_STMTS;
import static org.apache.inlong.audit.config.ConfigConstants.DEFAULT_CACHE_PREP_STMTS;
import static org.apache.inlong.audit.config.ConfigConstants.DEFAULT_CONNECTION_TIMEOUT;
import static org.apache.inlong.audit.config.ConfigConstants.DEFAULT_DATASOURCE_POOL_SIZE;
import static org.apache.inlong.audit.config.ConfigConstants.DEFAULT_PREP_STMT_CACHE_SIZE;
import static org.apache.inlong.audit.config.ConfigConstants.DEFAULT_PREP_STMT_CACHE_SQL_LIMIT;
import static org.apache.inlong.audit.config.ConfigConstants.KEY_CACHE_PREP_STMTS;
import static org.apache.inlong.audit.config.ConfigConstants.KEY_DATASOURCE_CONNECTION_TIMEOUT;
import static org.apache.inlong.audit.config.ConfigConstants.KEY_DATASOURCE_POOL_SIZE;
import static org.apache.inlong.audit.config.ConfigConstants.KEY_DEFAULT_MYSQL_DRIVER;
import static org.apache.inlong.audit.config.ConfigConstants.KEY_MYSQL_DRIVER;
import static org.apache.inlong.audit.config.ConfigConstants.KEY_MYSQL_JDBC_URL;
import static org.apache.inlong.audit.config.ConfigConstants.KEY_MYSQL_PASSWORD;
import static org.apache.inlong.audit.config.ConfigConstants.KEY_MYSQL_USERNAME;
import static org.apache.inlong.audit.config.ConfigConstants.KEY_PREP_STMT_CACHE_SIZE;
import static org.apache.inlong.audit.config.ConfigConstants.KEY_PREP_STMT_CACHE_SQL_LIMIT;
import static org.apache.inlong.audit.config.ConfigConstants.PREP_STMT_CACHE_SIZE;
import static org.apache.inlong.audit.config.ConfigConstants.PREP_STMT_CACHE_SQL_LIMIT;
import static org.apache.inlong.audit.config.SqlConstants.DEFAULT_MYSQL_SOURCE_QUERY_DAY_SQL;
import static org.apache.inlong.audit.config.SqlConstants.KEY_MYSQL_SOURCE_QUERY_DAY_SQL;

/**
 * Cache Of day ,for day openapi
 */
public class DayCache implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(DayCache.class);
    private static volatile DayCache dayCache = null;
    private DataSource dataSource;

    private final String querySql;

    private DayCache() {
        createDataSource();
        querySql = Configuration.getInstance().get(KEY_MYSQL_SOURCE_QUERY_DAY_SQL,
                DEFAULT_MYSQL_SOURCE_QUERY_DAY_SQL);
    }

    /**
     * Get instance
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
     * @param startTime
     * @param endTime
     * @param inlongGroupId
     * @param inlongStreamId
     * @param auditId
     * @param auditTag
     * @return
     */
    public List<StatData> getData(String startTime, String endTime, String inlongGroupId,
            String inlongStreamId, String auditId, String auditTag) {
        List<StatData> result = new LinkedList<>();
        try (Connection connection = dataSource.getConnection();
                PreparedStatement pstat = connection.prepareStatement(querySql)) {
            if (connection.isClosed()) {
                createDataSource();
            }
            pstat.setString(1, startTime);
            pstat.setString(2, endTime);
            pstat.setString(3, inlongGroupId);
            pstat.setString(4, inlongStreamId);
            pstat.setString(5, auditId);
            try (ResultSet resultSet = pstat.executeQuery()) {
                while (resultSet.next()) {
                    StatData data = new StatData();
                    data.setLogTs(startTime);
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
                LOG.error("Query has SQL exception! ", sqlException);
            }
        } catch (Exception exception) {
            LOG.error("Query has exception! ", exception);
        }
        return result;
    }

    /**
     * Create data source
     */
    private void createDataSource() {
        String driver = Configuration.getInstance().get(KEY_MYSQL_DRIVER, KEY_DEFAULT_MYSQL_DRIVER);
        String jdbcUrl = Configuration.getInstance().get(KEY_MYSQL_JDBC_URL);
        String userName = Configuration.getInstance().get(KEY_MYSQL_USERNAME);
        String passWord = Configuration.getInstance().get(KEY_MYSQL_PASSWORD);
        assert (Objects.nonNull(driver)
                && Objects.nonNull(jdbcUrl)
                && Objects.nonNull(userName)
                && Objects.nonNull(passWord));

        HikariConfig config = new HikariConfig();
        config.setDriverClassName(driver);
        config.setJdbcUrl(jdbcUrl);
        config.setUsername(userName);
        config.setPassword(passWord);
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

    @Override
    public void close() throws Exception {

    }
}
