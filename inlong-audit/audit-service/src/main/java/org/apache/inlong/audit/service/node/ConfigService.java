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

import org.apache.inlong.audit.service.config.Configuration;
import org.apache.inlong.audit.service.entities.JdbcConfig;
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
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.apache.inlong.audit.service.config.ConfigConstants.CACHE_PREP_STMTS;
import static org.apache.inlong.audit.service.config.ConfigConstants.DEFAULT_CACHE_PREP_STMTS;
import static org.apache.inlong.audit.service.config.ConfigConstants.DEFAULT_CONFIG_UPDATE_INTERVAL_SECONDS;
import static org.apache.inlong.audit.service.config.ConfigConstants.DEFAULT_CONNECTION_TIMEOUT;
import static org.apache.inlong.audit.service.config.ConfigConstants.DEFAULT_DATASOURCE_POOL_SIZE;
import static org.apache.inlong.audit.service.config.ConfigConstants.DEFAULT_PREP_STMT_CACHE_SIZE;
import static org.apache.inlong.audit.service.config.ConfigConstants.DEFAULT_PREP_STMT_CACHE_SQL_LIMIT;
import static org.apache.inlong.audit.service.config.ConfigConstants.DEFAULT_SELECTOR_SERVICE_ID;
import static org.apache.inlong.audit.service.config.ConfigConstants.KEY_CACHE_PREP_STMTS;
import static org.apache.inlong.audit.service.config.ConfigConstants.KEY_CONFIG_UPDATE_INTERVAL_SECONDS;
import static org.apache.inlong.audit.service.config.ConfigConstants.KEY_DATASOURCE_CONNECTION_TIMEOUT;
import static org.apache.inlong.audit.service.config.ConfigConstants.KEY_DATASOURCE_POOL_SIZE;
import static org.apache.inlong.audit.service.config.ConfigConstants.KEY_PREP_STMT_CACHE_SIZE;
import static org.apache.inlong.audit.service.config.ConfigConstants.KEY_PREP_STMT_CACHE_SQL_LIMIT;
import static org.apache.inlong.audit.service.config.ConfigConstants.KEY_SELECTOR_SERVICE_ID;
import static org.apache.inlong.audit.service.config.ConfigConstants.PREP_STMT_CACHE_SIZE;
import static org.apache.inlong.audit.service.config.ConfigConstants.PREP_STMT_CACHE_SQL_LIMIT;
import static org.apache.inlong.audit.service.config.SqlConstants.DEFAULT_MYSQL_QUERY_AUDIT_ID_SQL;
import static org.apache.inlong.audit.service.config.SqlConstants.DEFAULT_MYSQL_QUERY_AUDIT_SOURCE_SQL;
import static org.apache.inlong.audit.service.config.SqlConstants.KEY_MYSQL_QUERY_AUDIT_ID_SQL;
import static org.apache.inlong.audit.service.config.SqlConstants.KEY_MYSQL_QUERY_AUDIT_SOURCE_SQL;

/**
 * ConfigService periodically pull the configuration of audit id and audit source from DB.
 */
public class ConfigService {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConfigService.class);

    private static volatile ConfigService configService = null;
    private CopyOnWriteArrayList<String> auditIds = new CopyOnWriteArrayList<>();
    protected final ScheduledExecutorService updateTimer = Executors.newSingleThreadScheduledExecutor();
    private DataSource dataSource;
    private ConcurrentHashMap<String, List<JdbcConfig>> auditSources = new ConcurrentHashMap<>();
    private final String queryAuditIdSql;
    private final String queryAuditSourceSql;

    /**
     * If there is no audit item configured in the service configuration table audit_id_config, the default audit item is used.
     * Audit id 3 means that the Agent receives successfully.
     * Audit id 4 means that the Agent sends successfully.
     * Audit id 5 means that the DataProxy receives successfully.
     * Audit id 6 means that the DataProxy sends successfully.
     */
    private final String DEFAULT_AUDIT_IDS = "3;4;5;6";

    public static ConfigService getInstance() {
        if (configService == null) {
            synchronized (ConfigService.class) {
                if (configService == null) {
                    configService = new ConfigService();
                }
            }
        }
        return configService;
    }

    private ConfigService() {
        queryAuditIdSql = Configuration.getInstance().get(KEY_MYSQL_QUERY_AUDIT_ID_SQL,
                DEFAULT_MYSQL_QUERY_AUDIT_ID_SQL);
        queryAuditSourceSql = Configuration.getInstance().get(KEY_MYSQL_QUERY_AUDIT_SOURCE_SQL,
                DEFAULT_MYSQL_QUERY_AUDIT_SOURCE_SQL);
    }

    public void start() {
        createDataSource();
        updateTimer.scheduleWithFixedDelay(new Runnable() {

            @Override
            public void run() {
                updateAuditIds();
                updateAuditSource();
            }
        }, 0,
                Configuration.getInstance().get(KEY_CONFIG_UPDATE_INTERVAL_SECONDS,
                        DEFAULT_CONFIG_UPDATE_INTERVAL_SECONDS),
                TimeUnit.SECONDS);
    }

    /**
     * Create data source.
     */
    private void createDataSource() {
        JdbcConfig jdbcConfig = JdbcUtils.buildMysqlConfig();
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
        dataSource = new HikariDataSource(config);
    }

    /**
     * Update audit ids.
     */
    private void updateAuditIds() {
        try (Connection connection = dataSource.getConnection();
                PreparedStatement pstat = connection.prepareStatement(queryAuditIdSql)) {
            if (connection.isClosed()) {
                createDataSource();
            }

            try (ResultSet resultSet = pstat.executeQuery()) {
                CopyOnWriteArrayList<String> auditIdsTemp = new CopyOnWriteArrayList<>();
                while (resultSet.next()) {
                    String auditId = resultSet.getString(1);
                    LOGGER.info("Update audit id {}", auditId);
                    auditIdsTemp.add(auditId);
                }
                if (!auditIdsTemp.isEmpty()) {
                    auditIds = auditIdsTemp;
                }
            } catch (SQLException sqlException) {
                LOGGER.error("Query has SQL exception! ", sqlException);
            }

        } catch (Exception exception) {
            LOGGER.error("Query has exception! ", exception);
        }

        /**
         * If there is no audit item configured in the service configuration table audit_id_config, the default audit item will be used.
         * audit id 3 means that the Agent receives successfully.
         * audit id 4 means that the Agent sends successfully.
         * audit id 5 means that the DataProxy receives successfully.
         * audit id 6 means that the DataProxy sends successfully.
         */
        if (auditIds.isEmpty()) {
            auditIds.addAll(Arrays.asList(DEFAULT_AUDIT_IDS.split(";")));
            LOGGER.info("The default audit item is used: {}", DEFAULT_AUDIT_IDS);
        }
    }

    /**
     * Update audit source.
     */
    private void updateAuditSource() {
        try (Connection connection = dataSource.getConnection();
                PreparedStatement pstat = connection.prepareStatement(queryAuditSourceSql)) {
            if (connection.isClosed()) {
                createDataSource();
            }
            try (ResultSet resultSet = pstat.executeQuery()) {
                ConcurrentHashMap<String, List<JdbcConfig>> auditSourcesTemp = new ConcurrentHashMap<>();
                while (resultSet.next()) {
                    JdbcConfig data = new JdbcConfig(resultSet.getString(1),
                            resultSet.getString(2),
                            resultSet.getString(3),
                            resultSet.getString(4));
                    String serviceId = resultSet.getString(5);
                    List<JdbcConfig> config = auditSourcesTemp.computeIfAbsent(serviceId, k -> new LinkedList<>());
                    config.add(data);
                    LOGGER.info("Update audit source service id = {}, jdbc config = {}", serviceId, data);
                }
                if (!auditSourcesTemp.isEmpty()) {
                    auditSources = auditSourcesTemp;
                }
            } catch (SQLException sqlException) {
                LOGGER.error("Query has SQL exception! ", sqlException);
            }
        } catch (Exception exception) {
            LOGGER.error("Query has exception! ", exception);
        }

        /**
         * If the audit data source is not configured in the service configuration table audit_source_config,
         * use the same MySQL configuration as the service.
         */
        if (auditSources.isEmpty()) {
            auditSources.put(Configuration.getInstance().get(KEY_SELECTOR_SERVICE_ID, DEFAULT_SELECTOR_SERVICE_ID),
                    Collections.singletonList(JdbcUtils.buildMysqlConfig()));
            LOGGER.info("The default audit data source is used,the same as the audit service.");
        }
    }

    /**
     * Get audit ids.
     *
     * @return
     */
    public List<String> getAuditIds() {
        return auditIds = auditIds == null ? new CopyOnWriteArrayList<>() : auditIds;
    }

    /**
     * Get all audit source.
     *
     * @return
     */
    public List<JdbcConfig> getAllAuditSource() {
        List<JdbcConfig> sourceList = new LinkedList<>();
        for (Map.Entry<String, List<JdbcConfig>> entry : auditSources.entrySet()) {
            sourceList.addAll(entry.getValue());
        }
        return sourceList;
    }

    /**
     * Get audit source by service id.
     *
     * @param serviceId
     * @return
     */
    public List<JdbcConfig> getAuditSourceByServiceId(String serviceId) {
        return auditSources.get(serviceId);
    }
}
