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

package org.apache.inlong.sort.cdc.dm.source;

import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.JdbcConnection;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class DMConnection extends JdbcConnection {

    private static final String QUOTED_CHARACTER = "\"";

    private static final Properties DEFAULT_JDBC_PROPERTIES = initializeDefaultJdbcProperties();

    private static final String DM_URL_PATTERN =
            "jdbc:dm://${hostname}:${port}/${database}";

    public DMConnection(
            String hostname,
            Integer port,
            String user,
            String password,
            Duration timeout,
            String jdbcDriver,
            Properties jdbcProperties,
            ClassLoader classLoader) {
        super(
                config(hostname, port, user, password, timeout),
                factory(jdbcDriver, jdbcProperties, classLoader),
                QUOTED_CHARACTER,
                QUOTED_CHARACTER);
    }

    private static Properties initializeDefaultJdbcProperties() {
        Properties defaultJdbcProperties = new Properties();
        defaultJdbcProperties.setProperty("useInformationSchema", "true");
        defaultJdbcProperties.setProperty("nullCatalogMeansCurrent", "false");
        defaultJdbcProperties.setProperty("useUnicode", "true");
        defaultJdbcProperties.setProperty("zeroDateTimeBehavior", "convertToNull");
        defaultJdbcProperties.setProperty("characterEncoding", "UTF-8");
        defaultJdbcProperties.setProperty("characterSetResults", "UTF-8");
        return defaultJdbcProperties;
    }

    private static JdbcConfiguration config(
            String hostname, Integer port, String user, String password, Duration timeout) {
        return JdbcConfiguration.create()
                .with("hostname", hostname)
                .with("port", port)
                .with("user", user)
                .with("password", password)
                .with("connectTimeout", timeout)
                .build();
    }

    private static JdbcConnection.ConnectionFactory factory(
            String jdbcDriver, Properties jdbcProperties, ClassLoader classLoader) {
        return JdbcConnection.patternBasedFactory(
                formatJdbcUrl(jdbcProperties), jdbcDriver, classLoader);
    }

    private static String formatJdbcUrl(Properties jdbcProperties) {
        Properties combinedProperties = new Properties();
        combinedProperties.putAll(DEFAULT_JDBC_PROPERTIES);
        if (jdbcProperties != null) {
            combinedProperties.putAll(jdbcProperties);
        }
        String urlPattern = DM_URL_PATTERN;
        StringBuilder jdbcUrlStringBuilder = new StringBuilder(urlPattern);
        combinedProperties.forEach(
                (key, value) -> {
                    jdbcUrlStringBuilder.append("&").append(key).append("=").append(value);
                });
        return jdbcUrlStringBuilder.toString();
    }

    /**
     * Get table list by database name pattern and table name pattern.
     *
     * @param dbPattern Database name pattern.
     * @param tbPattern Table name pattern.
     * @return Table list.
     * @throws SQLException If a database access error occurs.
     */
    public List<String> getTables(String dbPattern, String tbPattern) throws SQLException {
        List<String> result = new ArrayList<>();
        DatabaseMetaData metaData = connection().getMetaData();
        List<String> dbNames = getResultList(metaData.getSchemas(), "TABLE_SCHEM");
        dbNames = dbNames.stream()
                .filter(dbName -> Pattern.matches(dbPattern, dbName))
                .collect(Collectors.toList());
        for (String dbName : dbNames) {
            List<String> tableNames =
                    getResultList(
                            metaData.getTables(null, dbName, null, new String[]{"TABLE"}),
                            "TABLE_NAME");
            tableNames.stream()
                    .filter(tbName -> Pattern.matches(tbPattern, tbName))
                    .forEach(tbName -> result.add(dbName + "." + tbName));
        }
        return result;
    }

    private List<String> getResultList(ResultSet resultSet, String columnName) throws SQLException {
        List<String> result = new ArrayList<>();
        while (resultSet.next()) {
            result.add(resultSet.getString(columnName));
        }
        return result;
    }

}
