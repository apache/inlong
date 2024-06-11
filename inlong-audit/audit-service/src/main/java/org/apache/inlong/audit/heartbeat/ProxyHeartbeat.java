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

package org.apache.inlong.audit.heartbeat;

import org.apache.inlong.audit.cache.AuditProxyCache;
import org.apache.inlong.audit.config.Configuration;
import org.apache.inlong.audit.entities.JdbcConfig;
import org.apache.inlong.audit.utils.JdbcUtils;

import org.apache.commons.dbcp.BasicDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import static org.apache.inlong.audit.config.ConfigConstants.DEFAULT_DATASOURCE_DETECT_INTERVAL_MS;
import static org.apache.inlong.audit.config.ConfigConstants.DEFAULT_DATASOURCE_MAX_IDLE_CONNECTIONS;
import static org.apache.inlong.audit.config.ConfigConstants.DEFAULT_DATASOURCE_MAX_TOTAL_CONNECTIONS;
import static org.apache.inlong.audit.config.ConfigConstants.DEFAULT_DATASOURCE_MIX_IDLE_CONNECTIONS;
import static org.apache.inlong.audit.config.ConfigConstants.KEY_DATASOURCE_DETECT_INTERVAL_MS;
import static org.apache.inlong.audit.config.ConfigConstants.KEY_DATASOURCE_MAX_IDLE_CONNECTIONS;
import static org.apache.inlong.audit.config.ConfigConstants.KEY_DATASOURCE_MAX_TOTAL_CONNECTIONS;
import static org.apache.inlong.audit.config.ConfigConstants.KEY_DATASOURCE_MIN_IDLE_CONNECTIONS;
import static org.apache.inlong.audit.config.SqlConstants.DEFAULT_AUDIT_PROXY_HEARTBEAT_SQL;
import static org.apache.inlong.audit.config.SqlConstants.KEY_AUDIT_PROXY_HEARTBEAT_SQL;

public class ProxyHeartbeat {

    private static final Logger LOGGER = LoggerFactory.getLogger(AuditProxyCache.class);
    private static final ProxyHeartbeat instance = new ProxyHeartbeat();
    private final BasicDataSource dataSource = new BasicDataSource();
    private final String heartbeatSQL;

    ProxyHeartbeat() {
        heartbeatSQL =
                Configuration.getInstance().get(KEY_AUDIT_PROXY_HEARTBEAT_SQL, DEFAULT_AUDIT_PROXY_HEARTBEAT_SQL);
        initDataSource();
    }

    private void initDataSource() {
        JdbcConfig jdbcConfig = JdbcUtils.buildMysqlConfig();
        setDataSourceConfig(jdbcConfig);
        dataSource.setTestOnBorrow(true);
        dataSource.setValidationQuery("SELECT 1");
        dataSource.setTimeBetweenEvictionRunsMillis(Configuration.getInstance().get(KEY_DATASOURCE_DETECT_INTERVAL_MS,
                DEFAULT_DATASOURCE_DETECT_INTERVAL_MS));
    }

    private void setDataSourceConfig(JdbcConfig jdbcConfig) {
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
    }

    public static ProxyHeartbeat getInstance() {
        return instance;
    }

    public void heartbeat(String component, String host, int port) {
        try (Connection connection = dataSource.getConnection();
                PreparedStatement statement = connection.prepareStatement(heartbeatSQL)) {
            statement.setString(1, component);
            statement.setString(2, host);
            statement.setInt(3, port);
            statement.executeUpdate();
        } catch (SQLException exception) {
            LOGGER.error("Heartbeat {} {} {} has exception ", component, host, port, exception);
        }
    }
}
