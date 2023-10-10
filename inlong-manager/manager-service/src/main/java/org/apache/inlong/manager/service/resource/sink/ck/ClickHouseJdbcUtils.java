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

package org.apache.inlong.manager.service.resource.sink.ck;

import org.apache.inlong.manager.common.util.UrlVerificationUtils;
import org.apache.inlong.manager.pojo.sink.ck.ClickHouseFieldInfo;
import org.apache.inlong.manager.pojo.sink.ck.ClickHouseTableInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.yandex.clickhouse.ClickHouseDatabaseMetadata;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Utils for ClickHouse JDBC.
 */
public class ClickHouseJdbcUtils {

    private static final String CLICKHOUSE_DRIVER_CLASS = "ru.yandex.clickhouse.ClickHouseDriver";
    private static final String COLUMN_LABEL = "TABLE_NAME";
    private static final String CLICKHOUSE_JDBC_PREFIX = "jdbc:clickhouse://";

    private static final Logger LOG = LoggerFactory.getLogger(ClickHouseJdbcUtils.class);

    /**
     * Get ClickHouse connection from ClickHouse URL and user.
     *
     * @param url      JDBC URL, such as jdbc:clickhouse://host:port/database
     * @param user     Username for JDBC URL
     * @param password User password
     * @return {@link Connection}
     * @throws Exception on get connection error
     */
    public static Connection getConnection(String url, String user, String password) throws Exception {
        // Non-empty validation
        UrlVerificationUtils.extractHostAndValidatePortFromJdbcUrl(url, CLICKHOUSE_JDBC_PREFIX);
        Connection conn = establishConnection(url, user, password);
        return conn;
    }

    /**
     * Establishes a ClickHouse JDBC connection using the provided URL, username, and password.
     *
     * @param url      The ClickHouse JDBC URL
     * @param user     The username
     * @param password The user's password
     * @return A {@link Connection} object representing the ClickHouse database connection
     * @throws Exception If an error occurs while obtaining the connection
     */
    private static Connection establishConnection(String url, String user, String password) throws Exception {
        Connection conn;
        try {
            Class.forName(CLICKHOUSE_DRIVER_CLASS);
            conn = DriverManager.getConnection(url, user, password);
        } catch (Exception e) {
            LOG.error("get ClickHouse connection error, please check ClickHouse JDBC URL, username or password", e);
            throw new Exception("get ClickHouse connection error, please check JDBC URL, username or password. "
                    + "other error msg: " + e.getMessage());
        }

        if (conn == null) {
            throw new Exception("get ClickHouse connection failed, please contact administrator");
        }
        LOG.info("get ClickHouse connection success, url={}", url);
        return conn;
    }

    /**
     * Execute One ClickHouse Sql command
     */
    public static void executeSql(String sql, String url, String user, String password) throws Exception {
        try (Connection conn = getConnection(url, user, password)) {
            Statement stmt = conn.createStatement();
            stmt.execute(sql);
            LOG.info("execute sql [{}] success for url: {}", sql, url);
        }
    }

    /**
     * Execute Batch ClickHouse Sql commands
     */
    public static void executeSqlBatch(List<String> sql, String url, String user, String password) throws Exception {
        try (Connection conn = getConnection(url, user, password)) {
            Statement stmt = conn.createStatement();
            for (String entry : sql) {
                stmt.execute(entry);
            }
            LOG.info("execute sql [{}] success for url: {}", sql, url);
        }
    }

    /**
     * Create ClickHouse database
     */
    public static void createDb(String url, String user, String password, String dbName) throws Exception {
        String createDbSql = ClickHouseSqlBuilder.buildCreateDbSql(dbName);
        executeSql(createDbSql, url, user, password);
    }

    /**
     * Create ClickHouse table
     */
    public static void createTable(String url, String user, String password,
            ClickHouseTableInfo tableInfo) throws Exception {
        String createTableSql = ClickHouseSqlBuilder.buildCreateTableSql(tableInfo);
        ClickHouseJdbcUtils.executeSql(createTableSql, url, user, password);
    }

    /**
     * Get ClickHouse tables from the ClickHouse metadata
     */
    public static List<String> getTables(String url, String user, String password, String dbname) throws Exception {
        List<String> tables = new ArrayList<>();
        try (Connection conn = getConnection(url, user, password)) {
            ClickHouseDatabaseMetadata metaData = (ClickHouseDatabaseMetadata) conn.getMetaData();
            LOG.info("dbname is {}", dbname);
            ResultSet rs = metaData.getTables(dbname, dbname, null, new String[]{"TABLE"});
            while (rs.next()) {
                String tableName = rs.getString(COLUMN_LABEL);
                tables.add(tableName);
            }
            rs.close();
        }
        return tables;
    }

    /**
     * Query ClickHouse field
     */
    public static List<ClickHouseFieldInfo> getFields(String url, String user, String password, String dbName,
            String tableName) throws Exception {

        String querySql = ClickHouseSqlBuilder.buildDescTableSql(dbName, tableName);
        try (Connection conn = getConnection(url, user, password);
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(querySql)) {
            List<ClickHouseFieldInfo> fieldList = new ArrayList<>();
            while (rs.next()) {
                ClickHouseFieldInfo fieldInfo = new ClickHouseFieldInfo();
                if (Objects.equals(rs.getString(1), "inlong_ttl_date_time")) {
                    continue;
                }
                fieldInfo.setFieldName(rs.getString(1));
                fieldInfo.setFieldType(rs.getString(2));
                fieldInfo.setDefaultType(rs.getString(3));
                fieldInfo.setDefaultExpr(rs.getString(4));
                fieldInfo.setFieldComment(rs.getString(5));
                fieldInfo.setCompressionCode(rs.getString(6));
                fieldInfo.setTtlExpr(rs.getString(7));
                fieldList.add(fieldInfo);
            }
            return fieldList;
        }
    }

    /**
     * Add columns for ClickHouse table
     */
    public static void addColumns(String url, String user, String password, String dbName, String tableName,
            List<ClickHouseFieldInfo> columnList) throws Exception {
        List<String> addColumnSql = ClickHouseSqlBuilder.buildAddColumnsSql(dbName, tableName, columnList);
        ClickHouseJdbcUtils.executeSqlBatch(addColumnSql, url, user, password);
    }

}
