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

package org.apache.inlong.sort.cdc.postgres.connection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Simple JDBC connection provider.
 */
public class PostgreSQLJdbcConnectionProvider implements PostgreSQLJdbcConnectionIProvider, Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(PostgreSQLJdbcConnectionProvider.class);

    private static final long serialVersionUID = 1L;

    private final PostgreSQLJdbcConnectionOptions jdbcOptions;

    private transient volatile Connection connection;

    public PostgreSQLJdbcConnectionProvider(PostgreSQLJdbcConnectionOptions jdbcOptions) {
        this.jdbcOptions = jdbcOptions;
    }

    @Override
    public Connection getConnection() throws SQLException, ClassNotFoundException {
        if (connection == null) {
            synchronized (this) {
                if (connection == null) {
                    Class.forName(jdbcOptions.getDriverName());
                    if (jdbcOptions.getUsername().isPresent()) {
                        connection = DriverManager.getConnection(jdbcOptions.getDbURL(),
                                jdbcOptions.getUsername().orElse(null), jdbcOptions.getPassword().orElse(null));
                    } else {
                        connection = DriverManager.getConnection(jdbcOptions.getDbURL());
                    }
                }
            }
        }
        return connection;
    }

    @Override
    public Connection reestablishConnection() throws SQLException, ClassNotFoundException {
        close();
        connection = getConnection();
        return connection;
    }

    @Override
    public void close() {
        if (connection == null) {
            return;
        }
        try {
            connection.close();
        } catch (SQLException e) {
            LOG.error("JDBC connection close failed.", e);
        } finally {
            connection = null;
        }
    }
}
