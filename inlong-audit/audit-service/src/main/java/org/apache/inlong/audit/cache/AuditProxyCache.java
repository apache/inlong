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
import org.apache.inlong.audit.entity.AuditProxy;
import org.apache.inlong.audit.utils.JdbcUtils;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.commons.dbcp.BasicDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.apache.inlong.audit.config.ConfigConstants.DEFAULT_DATASOURCE_DETECT_INTERVAL_MS;
import static org.apache.inlong.audit.config.ConfigConstants.DEFAULT_DATASOURCE_MAX_IDLE_CONNECTIONS;
import static org.apache.inlong.audit.config.ConfigConstants.DEFAULT_DATASOURCE_MAX_TOTAL_CONNECTIONS;
import static org.apache.inlong.audit.config.ConfigConstants.DEFAULT_DATASOURCE_MIX_IDLE_CONNECTIONS;
import static org.apache.inlong.audit.config.ConfigConstants.KEY_DATASOURCE_DETECT_INTERVAL_MS;
import static org.apache.inlong.audit.config.ConfigConstants.KEY_DATASOURCE_MAX_IDLE_CONNECTIONS;
import static org.apache.inlong.audit.config.ConfigConstants.KEY_DATASOURCE_MAX_TOTAL_CONNECTIONS;
import static org.apache.inlong.audit.config.ConfigConstants.KEY_DATASOURCE_MIN_IDLE_CONNECTIONS;
import static org.apache.inlong.audit.config.OpenApiConstants.DEFAULT_API_CACHE_EXPIRED_MINUTES;
import static org.apache.inlong.audit.config.OpenApiConstants.DEFAULT_API_CACHE_MAX_SIZE;
import static org.apache.inlong.audit.config.OpenApiConstants.KEY_API_CACHE_EXPIRED_MINUTES;
import static org.apache.inlong.audit.config.OpenApiConstants.KEY_API_CACHE_MAX_SIZE;
import static org.apache.inlong.audit.config.SqlConstants.DEFAULT_AUDIT_HEARTBEAT_QUERY_ALL_SQL;
import static org.apache.inlong.audit.config.SqlConstants.DEFAULT_AUDIT_PROXY_HEARTBEAT_QUERY_COMPONENT_SQL;
import static org.apache.inlong.audit.config.SqlConstants.DEFAULT_AUDIT_PROXY_HOST_QUERY_ALL_SQL;
import static org.apache.inlong.audit.config.SqlConstants.DEFAULT_AUDIT_PROXY_HOST_QUERY_COMPONENT_SQL;
import static org.apache.inlong.audit.config.SqlConstants.KEY_AUDIT_PROXY_HEARTBEAT_QUERY_ALL_SQL;
import static org.apache.inlong.audit.config.SqlConstants.KEY_AUDIT_PROXY_HEARTBEAT_QUERY_COMPONENT_SQL;
import static org.apache.inlong.audit.config.SqlConstants.KEY_AUDIT_PROXY_HOST_QUERY_ALL_SQL;
import static org.apache.inlong.audit.config.SqlConstants.KEY_AUDIT_PROXY_HOST_QUERY_COMPONENT_SQL;

public class AuditProxyCache {

    private static final Logger LOGGER = LoggerFactory.getLogger(AuditProxyCache.class);
    private static final AuditProxyCache instance = new AuditProxyCache();
    private final Cache<String, HashSet<AuditProxy>> cache;
    protected final ScheduledExecutorService monitorTimer = Executors.newSingleThreadScheduledExecutor();
    private final BasicDataSource dataSource = new BasicDataSource();
    private final String queryAllAuditProxyHostSQL;
    private final String queryAuditProxyHostByComponentSQL;

    private final String queryAuditProxyHeartbeatSQL;
    private final String queryAuditProxyHeartbeatByComponentSQL;

    private AuditProxyCache() {
        cache = Caffeine.newBuilder()
                .maximumSize(Configuration.getInstance().get(KEY_API_CACHE_MAX_SIZE,
                        DEFAULT_API_CACHE_MAX_SIZE))
                .expireAfterWrite(Configuration.getInstance().get(KEY_API_CACHE_EXPIRED_MINUTES,
                        DEFAULT_API_CACHE_EXPIRED_MINUTES), TimeUnit.MINUTES)
                .build();
        queryAuditProxyHostByComponentSQL = Configuration.getInstance().get(KEY_AUDIT_PROXY_HOST_QUERY_COMPONENT_SQL,
                DEFAULT_AUDIT_PROXY_HOST_QUERY_COMPONENT_SQL);

        queryAllAuditProxyHostSQL = Configuration.getInstance().get(KEY_AUDIT_PROXY_HOST_QUERY_ALL_SQL,
                DEFAULT_AUDIT_PROXY_HOST_QUERY_ALL_SQL);
        queryAuditProxyHeartbeatSQL = Configuration.getInstance().get(KEY_AUDIT_PROXY_HEARTBEAT_QUERY_ALL_SQL,
                DEFAULT_AUDIT_HEARTBEAT_QUERY_ALL_SQL);
        queryAuditProxyHeartbeatByComponentSQL =
                Configuration.getInstance().get(KEY_AUDIT_PROXY_HEARTBEAT_QUERY_COMPONENT_SQL,
                        DEFAULT_AUDIT_PROXY_HEARTBEAT_QUERY_COMPONENT_SQL);

        initDataSource();
        monitorTimer.scheduleWithFixedDelay(new Runnable() {

            @Override
            public void run() {
                update();
            }
        }, 0, 1, TimeUnit.MINUTES);
    }

    private void initDataSource() {
        JdbcConfig jdbcConfig = JdbcUtils.buildMysqlConfig();
        dataSource.setDriverClassName(jdbcConfig.getDriverClass());
        dataSource.setUrl(jdbcConfig.getJdbcUrl());
        dataSource.setUsername(jdbcConfig.getUserName());
        dataSource.setPassword(jdbcConfig.getPassword());
        dataSource.setInitialSize(Configuration.getInstance().get(KEY_DATASOURCE_MIN_IDLE_CONNECTIONS,
                DEFAULT_DATASOURCE_MIX_IDLE_CONNECTIONS));
        dataSource.setMaxActive(Configuration.getInstance().get(KEY_DATASOURCE_MAX_TOTAL_CONNECTIONS,
                DEFAULT_DATASOURCE_MAX_TOTAL_CONNECTIONS));
        dataSource.setMaxIdle(Configuration.getInstance().get(KEY_DATASOURCE_MAX_IDLE_CONNECTIONS,
                DEFAULT_DATASOURCE_MAX_IDLE_CONNECTIONS));
        dataSource.setMinIdle(Configuration.getInstance().get(KEY_DATASOURCE_MIN_IDLE_CONNECTIONS,
                DEFAULT_DATASOURCE_MIX_IDLE_CONNECTIONS));
        dataSource.setTestOnBorrow(true);
        dataSource.setValidationQuery("SELECT 1");
        dataSource
                .setTimeBetweenEvictionRunsMillis(Configuration.getInstance().get(KEY_DATASOURCE_DETECT_INTERVAL_MS,
                        DEFAULT_DATASOURCE_DETECT_INTERVAL_MS));
    }

    public static AuditProxyCache getInstance() {
        return instance;
    }

    public List<AuditProxy> getData(String component) {
        HashSet<AuditProxy> result = cache.getIfPresent(component);
        if (result != null) {
            return new ArrayList<>(result);
        }
        result = queryAuditProxyInfo(component);
        if (result.isEmpty()) {
            result = queryAuditProxyHeartbeat(component);
        }
        if (!result.isEmpty()) {
            cache.put(component, result);
        }
        return new ArrayList<>(result);
    }

    private void update() {
        Map<String, HashSet<AuditProxy>> auditProxyInfo = queryAllAuditProxyInfo();
        if (auditProxyInfo.isEmpty()) {
            auditProxyInfo = queryAuditProxyHeartbeat();
        }
        if (auditProxyInfo.isEmpty()) {
            return;
        }
        for (Map.Entry<String, HashSet<AuditProxy>> entry : auditProxyInfo.entrySet()) {
            try {
                cache.put(entry.getKey(), entry.getValue());
            } catch (Exception e) {
                LOGGER.error("Put data into audit proxy cache has exception! ", e);
                // Decide whether to break or continue based on your requirement
                break;
            }
        }
    }

    private Map<String, HashSet<AuditProxy>> queryAllAuditProxyInfo() {
        Map<String, HashSet<AuditProxy>> result = new HashMap<>();
        try (Connection connection = dataSource.getConnection();
                PreparedStatement statement = connection.prepareStatement(queryAllAuditProxyHostSQL);
                ResultSet resultSet = statement.executeQuery()) {
            while (resultSet.next()) {
                String component = resultSet.getString("component");
                AuditProxy auditProxyInfo = new AuditProxy(resultSet.getString("host"), resultSet.getInt("port"));
                result.computeIfAbsent(component, k -> new HashSet<>()).add(auditProxyInfo);
            }
        } catch (Exception exception) {
            LOGGER.error("Query audit proxy info has exception! ", exception);
        }
        return result;
    }

    private HashSet<AuditProxy> queryAuditProxyInfo(String component) {
        HashSet<AuditProxy> result = new HashSet<>();
        try (Connection connection = dataSource.getConnection();
                PreparedStatement statement = connection.prepareStatement(queryAuditProxyHostByComponentSQL)) {
            statement.setString(1, component);
            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    AuditProxy auditProxyInfo = new AuditProxy(resultSet.getString("host"), resultSet.getInt("port"));
                    result.add(auditProxyInfo);
                }
            } catch (SQLException sqlException) {
                LOGGER.error("Query audit proxy info by {} has SQL exception ", component, sqlException);
            }
        } catch (Exception exception) {
            LOGGER.error("Query audit proxy info by {} has  exception ", component, exception);
        }
        return result;
    }

    private Map<String, HashSet<AuditProxy>> queryAuditProxyHeartbeat() {
        Map<String, HashSet<AuditProxy>> result = new HashMap<>();
        try (Connection connection = dataSource.getConnection();
                PreparedStatement statement = connection.prepareStatement(queryAuditProxyHeartbeatSQL);
                ResultSet resultSet = statement.executeQuery()) {
            while (resultSet.next()) {
                String component = resultSet.getString("component");
                AuditProxy auditProxyInfo = new AuditProxy(resultSet.getString("host"), resultSet.getInt("port"));
                result.computeIfAbsent(component, k -> new HashSet<>()).add(auditProxyInfo);
            }
        } catch (Exception exception) {
            LOGGER.error("Query audit proxy info has exception! ", exception);
        }
        return result;
    }

    private HashSet<AuditProxy> queryAuditProxyHeartbeat(String component) {
        HashSet<AuditProxy> result = new HashSet<>();
        try (Connection connection = dataSource.getConnection();
                PreparedStatement statement = connection.prepareStatement(queryAuditProxyHeartbeatByComponentSQL)) {
            statement.setString(1, component);
            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    AuditProxy auditProxyInfo = new AuditProxy(resultSet.getString("host"), resultSet.getInt("port"));
                    result.add(auditProxyInfo);
                }
            } catch (SQLException sqlException) {
                LOGGER.error("Query audit proxy info by {} has SQL exception ", component, sqlException);
            }
        } catch (Exception exception) {
            LOGGER.error("Query audit proxy info by {} has  exception ", component, exception);
        }
        return result;
    }
}
