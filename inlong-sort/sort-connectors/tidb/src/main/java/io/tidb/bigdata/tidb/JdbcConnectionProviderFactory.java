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

package io.tidb.bigdata.tidb;

import static java.util.Objects.requireNonNull;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class JdbcConnectionProviderFactory {

    public static JdbcConnectionProvider createJdbcConnectionProvider(ClientConfig config) {
        try {
            Class<?> provideClass = Class.forName(config.getJdbcConnectionProviderImpl());
            return (JdbcConnectionProvider) provideClass.getConstructor(ClientConfig.class)
                    .newInstance(config);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    public abstract static class JdbcConnectionProvider implements AutoCloseable {

        protected final String url;
        protected final String username;
        protected final String password;

        public JdbcConnectionProvider(ClientConfig config) {
            this.url = requireNonNull(config.getDatabaseUrl(), "database url can not be null");
            this.username = requireNonNull(config.getUsername(), "username can not be null");
            this.password = config.getPassword();
        }

        abstract Connection getConnection() throws SQLException;
    }

    public static class BasicJdbcConnectionProvider extends JdbcConnectionProvider {

        public BasicJdbcConnectionProvider(ClientConfig config) {
            super(config);
        }

        @Override
        Connection getConnection() throws SQLException {
            return DriverManager.getConnection(url, username, password);
        }

        @Override
        public void close() throws Exception {

        }
    }

    public static class HikariDataSourceJdbcConnectionProvider extends JdbcConnectionProvider {

        private final HikariDataSource dataSource;

        public HikariDataSourceJdbcConnectionProvider(ClientConfig config) {
            super(config);
            HikariConfig hikariConfig = new HikariConfig();
            hikariConfig.setJdbcUrl(url);
            hikariConfig.setUsername(username);
            hikariConfig.setPassword(password);
            hikariConfig.setDriverClassName(config.getDriverName());
            hikariConfig.setMaximumPoolSize(config.getMaximumPoolSize());
            hikariConfig.setMinimumIdle(config.getMinimumIdleSize());
            this.dataSource = new HikariDataSource(hikariConfig);
        }

        @Override
        public Connection getConnection() throws SQLException {
            return dataSource.getConnection();
        }

        @Override
        public void close() throws Exception {
            dataSource.close();
        }
    }

}
