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

import org.apache.inlong.audit.entity.AuditRoute;
import org.apache.inlong.audit.service.config.Configuration;
import org.apache.inlong.audit.service.config.SqlConstants;
import org.apache.inlong.audit.service.datasource.AuditDataSource;
import org.apache.inlong.audit.service.entities.JdbcConfig;
import org.apache.inlong.audit.service.utils.JdbcUtils;
import org.apache.inlong.audit.utils.NamedThreadFactory;
import org.apache.inlong.audit.utils.RouteUtils;

import lombok.Getter;
import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class AuditRouteCache {

    private static final Logger LOGGER = LoggerFactory.getLogger(AuditRouteCache.class);

    private static final int PERIOD_MS = 60 * 1000; // 1 minute
    private static final String ADDRESS = "address";
    private static final String AUDIT_ID_INCLUDE = "audit_id_include";
    private static final String GROUP_ID_INCLUDE = "inlong_group_id_include";
    private static final String GROUP_ID_EXCLUDE = "inlong_group_id_exclude";

    @Getter
    private static final AuditRouteCache instance = new AuditRouteCache();

    private volatile ConcurrentHashMap<String, List<AuditRoute>> auditRouteCache = new ConcurrentHashMap<>();
    private AuditDataSource auditDataSource;

    private final ScheduledExecutorService timerExecutor =
            Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("update-audit-route"));

    private AuditRouteCache() {
    }

    public void init() {

        String querySQL =
                Configuration.getInstance().get(SqlConstants.KEY_QUERY_AUDIT_ROUTE_SQL,
                        SqlConstants.DEFAULT_QUERY_AUDIT_ROUTE_SQL);
        try {
            auditDataSource = createDataSource();
        } catch (Exception e) {
            LOGGER.error("Failed to create DataSource in AuditRouteCache init", e);
        }

        timerExecutor.scheduleWithFixedDelay(() -> {
            try {
                updateAuditRouteCache(querySQL);
            } catch (Exception e) {
                LOGGER.error("Exception occurred during audit route cache update", e);
            }
        }, 0, PERIOD_MS, TimeUnit.MILLISECONDS);
    }

    private AuditDataSource createDataSource() {
        JdbcConfig jdbcConfig = JdbcUtils.buildMysqlConfig();

        BasicDataSource dataSource = new BasicDataSource();
        dataSource.setDriverClassName(jdbcConfig.getDriverClass());
        dataSource.setUrl(jdbcConfig.getJdbcUrl());
        dataSource.setUsername(jdbcConfig.getUserName());
        dataSource.setPassword(jdbcConfig.getPassword());
        dataSource.setValidationQuery("SELECT 1");

        return new AuditDataSource(jdbcConfig.getJdbcUrl(), dataSource);
    }

    private void updateAuditRouteCache(String querySQL) {
        ConcurrentHashMap<String, List<AuditRoute>> auditRoutes = new ConcurrentHashMap<>();

        try (Connection connection = auditDataSource.getDataSource().getConnection();
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(querySQL)) {

            while (resultSet.next()) {
                String address = resultSet.getString(ADDRESS);
                if (address == null || address.trim().isEmpty()) {
                    continue;
                }

                String auditId = StringUtils.trimToNull(resultSet.getString(AUDIT_ID_INCLUDE));
                String includeGroupId = StringUtils.trimToNull(resultSet.getString(GROUP_ID_INCLUDE));
                String excludeGroupId = StringUtils.trimToNull(resultSet.getString(GROUP_ID_EXCLUDE));

                if (!isValidRegexOrLog(auditId)
                        || !isValidRegexOrLog(includeGroupId)
                        || !isValidRegexOrLog(excludeGroupId)) {
                    LOGGER.error(
                            "Skipping invalid regex entry: address={}, auditId={}, includeGroupId={}, excludeGroupId={}",
                            address, auditId, includeGroupId, excludeGroupId);
                    continue;
                }

                AuditRoute data = new AuditRoute();
                data.setAddress(address);
                data.setAuditId(auditId);
                data.setInlongGroupIdsInclude(includeGroupId);
                data.setInlongGroupIdsExclude(excludeGroupId);

                auditRoutes.computeIfAbsent(address, key -> new ArrayList<>()).add(data);
            }
        } catch (SQLException e) {
            LOGGER.error("Failed to update audit route cache", e);
            return;
        }

        if (!auditRoutes.isEmpty()) {
            auditRouteCache = auditRoutes;
            LOGGER.info("AuditRouteCache update success. Cache size={}, Query size={}", auditRouteCache.size(),
                    auditRoutes.size());
        } else {
            LOGGER.warn("Audit route list is empty; cache not updated.");
        }
    }

    private static boolean isValidRegexOrLog(String fieldName) {
        if (fieldName == null) {
            return true;
        }
        if (!RouteUtils.isValidRegex(fieldName)) {
            LOGGER.error("Invalid regex for field: {}", fieldName);
            return false;
        }
        return true;
    }

    public void shutdown() {
        try {
            timerExecutor.shutdown();
            if (!timerExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                timerExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOGGER.error("Interrupted while shutting down AuditRouteCache timerExecutor", e);
        }
    }

    public List<AuditRoute> getData(String host) {
        List<AuditRoute> routes = auditRouteCache.get(host);
        return routes == null ? Collections.emptyList() : routes;
    }
}
