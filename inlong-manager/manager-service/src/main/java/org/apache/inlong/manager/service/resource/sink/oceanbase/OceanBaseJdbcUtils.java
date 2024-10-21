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

package org.apache.inlong.manager.service.resource.sink.oceanbase;

import org.apache.inlong.manager.common.util.UrlVerificationUtils;
import org.apache.inlong.manager.pojo.sink.oceanbase.OceanBaseColumnInfo;
import org.apache.inlong.manager.pojo.sink.oceanbase.OceanBaseTableInfo;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Utils for OceanBase JDBC.
 */
public class OceanBaseJdbcUtils {

    private static final String OCEANBASE_JDBC_PREFIX = "jdbc:oceanbase://";
    private static final String OCEANBASE_DRIVER_CLASS = "com.oceanbase.jdbc.Driver";
    private static final Logger LOGGER = LoggerFactory.getLogger(OceanBaseJdbcUtils.class);

    /**
     * Get OceanBase connection from the url and user.
     *
     * @param url jdbc url, such as jdbc:oceanbase://host:port/database
     * @param user Username for JDBC URL
     * @param password User password
     * @return {@link Connection}
     * @throws Exception on get connection error
     */
    public static Connection getConnection(String url, String user, String password) throws Exception {
        if (StringUtils.isNotBlank(url) && StringUtils.isNotBlank(url) && StringUtils.isNotBlank(password)) {
            UrlVerificationUtils.extractHostAndValidatePortFromJdbcUrl(url, OCEANBASE_JDBC_PREFIX);
            return establishDatabaseConnection(url, user, password);
        }
        return null;
    }

    /**
     * Establishes a database connection using the provided URL, username, and password.
     *
     * @param url      The JDBC URL
     * @param user     The username
     * @param password The user's password
     * @return A {@link Connection} object representing the database connection
     * @throws Exception If an error occurs while obtaining the connection
     */
    private static Connection establishDatabaseConnection(String url, String user, String password) throws Exception {
        Connection conn;
        if (StringUtils.isBlank(url) || !url.startsWith(OCEANBASE_JDBC_PREFIX)) {
            throw new Exception("OceanusBase URL was invalid, it should start with jdbc:oceanbase");
        }
        try {
            Class.forName(OCEANBASE_DRIVER_CLASS);
            conn = DriverManager.getConnection(url, user, password);
        } catch (Exception e) {
            String errorMsg =
                    "Failed to get OceanBase connection, please check OceanBase JDBC URL, username, or password!";
            LOGGER.error(errorMsg, e);
            throw new Exception(errorMsg + " Other error message: " + e.getMessage());
        }
        if (conn == null) {
            throw new Exception("get OceanBase connection failed, please contact administrator");
        }
        LOGGER.info("get OceanBase connection success for url={}", url);
        return conn;
    }

    /**
     * Execute SQL command on OceanBase.
     *
     * @param conn JDBC {@link Connection}
     * @param sql SQL to be executed
     * @throws Exception on execute SQL error
     */
    public static void executeSql(final Connection conn, final String sql) throws Exception {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
            LOGGER.info("execute sql [{}] success", sql);
        }
    }

    /**
     * Execute batch query SQL on OceanBase.
     *
     * @param conn JDBC {@link Connection}
     * @param sqls SQL to be executed
     * @throws Exception on get execute SQL batch error
     */
    public static void executeSqlBatch(final Connection conn, final List<String> sqls) throws Exception {
        conn.setAutoCommit(false);
        try (Statement stmt = conn.createStatement()) {
            for (String entry : sqls) {
                stmt.execute(entry);
            }
            conn.commit();
            LOGGER.info("execute sql [{}] success", sqls);
        } finally {
            conn.setAutoCommit(true);
        }
    }

    /**
     * Create OceanBase database
     *
     * @param conn JDBC {@link Connection}
     * @param dbName database name
     * @throws Exception on create database error
     */
    public static void createDb(final Connection conn, final String dbName) throws Exception {
        if (!checkDbExist(conn, dbName)) {
            final String createDbSql = OceanBaseSqlBuilder.buildCreateDbSql(dbName);
            executeSql(conn, createDbSql);
            LOGGER.info("execute sql [{}] success", createDbSql);
        } else {
            LOGGER.info("The database [{}] are exists", dbName);
        }
    }

    /**
     * Check database from the OceanBase information_schema.
     *
     * @param conn JDBC {@link Connection}
     * @param dbName database name
     * @return true if table exist, otherwise false
     * @throws Exception on check database exist error
     */
    public static boolean checkDbExist(final Connection conn, final String dbName) throws Exception {
        final String checkDbSql = OceanBaseSqlBuilder.getCheckDatabase(dbName);
        try (Statement stmt = conn.createStatement();
                ResultSet resultSet = stmt.executeQuery(checkDbSql)) {
            if (Objects.nonNull(resultSet)) {
                if (resultSet.next()) {
                    LOGGER.info("check db exist for db={}, result=true", dbName);
                    return true;
                }
            }
        }
        LOGGER.info("check db exist for db={}, result=false", dbName);
        return false;
    }

    /**
     * Create OceanBase table by OceanBaseTableInfo
     *
     * @param conn JDBC {@link Connection}
     * @param tableInfo table info  {@link OceanBaseTableInfo}
     * @throws Exception on create table error
     */
    public static void createTable(final Connection conn, final OceanBaseTableInfo tableInfo) throws Exception {
        if (checkTablesExist(conn, tableInfo.getDbName(), tableInfo.getTableName())) {
            LOGGER.info("The table [{}] are exists", tableInfo.getTableName());
        } else {
            final String createTableSql = OceanBaseSqlBuilder.buildCreateTableSql(tableInfo);
            executeSql(conn, createTableSql);
            LOGGER.info("execute sql [{}] success", createTableSql);
        }
    }

    /**
     * Check tables from the OceanBase information_schema.
     *
     * @param conn JDBC {@link Connection}
     * @param dbName database name
     * @param tableName table name
     * @return true if table exist, otherwise false
     * @throws Exception on check table exist error
     */
    public static boolean checkTablesExist(final Connection conn, final String dbName, final String tableName)
            throws Exception {
        boolean result = false;
        final String checkTableSql = OceanBaseSqlBuilder.getCheckTable(dbName, tableName);
        try (Statement stmt = conn.createStatement();
                ResultSet resultSet = stmt.executeQuery(checkTableSql)) {
            if (Objects.nonNull(resultSet)) {
                if (resultSet.next()) {
                    result = true;
                }
            }
        }
        LOGGER.info("check table exist for db={} table={}, result={}", dbName, tableName, result);
        return result;
    }

    /**
     * Check whether the column exists in the OceanBase table.
     *
     * @param conn JDBC Connection  {@link Connection}
     * @param dbName database name
     * @param tableName table name
     * @param column table column name
     * @return true if column exist in the table, otherwise false
     * @throws Exception on check column exist error
     */
    public static boolean checkColumnExist(final Connection conn, final String dbName, final String tableName,
            final String column) throws Exception {
        boolean result = false;
        final String checkTableSql = OceanBaseSqlBuilder.getCheckColumn(dbName, tableName, column);
        try (Statement stmt = conn.createStatement();
                ResultSet resultSet = stmt.executeQuery(checkTableSql)) {
            if (Objects.nonNull(resultSet)) {
                if (resultSet.next()) {
                    result = true;
                }
            }
        }
        LOGGER.info("check column exist for db={} table={}, result={} column={}", dbName, tableName, result, column);
        return result;
    }

    /**
     * Query all OceanBase table columns by the given tableName.
     *
     * @param conn JDBC {@link Connection}
     * @param dbName database name
     * @param tableName table name
     * @return {@link List}
     * @throws Exception on get columns error
     */
    public static List<OceanBaseColumnInfo> getColumns(final Connection conn, final String dbName,
            final String tableName)
            throws Exception {
        final String querySql = OceanBaseSqlBuilder.buildDescTableSql(dbName, tableName);
        final List<OceanBaseColumnInfo> columnList = new ArrayList<>();

        try (Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(querySql)) {
            if (Objects.nonNull(rs)) {
                while (rs.next()) {
                    OceanBaseColumnInfo columnInfo = new OceanBaseColumnInfo(rs.getString(1),
                            rs.getString(2), rs.getString(3));
                    columnList.add(columnInfo);
                }
            }
        }
        return columnList;
    }

    /**
     * Add columns for OceanBase table.
     *
     * @param conn JDBC Connection  {@link Connection}
     * @param dbName database name
     * @param tableName table name
     * @param columns columns to be added
     * @throws Exception on add columns error
     */
    public static void addColumns(final Connection conn, final String dbName, final String tableName,
            final List<OceanBaseColumnInfo> columns) throws Exception {
        final List<OceanBaseColumnInfo> columnInfos = Lists.newArrayList();

        for (OceanBaseColumnInfo columnInfo : columns) {
            if (!checkColumnExist(conn, dbName, tableName, columnInfo.getName())) {
                columnInfos.add(columnInfo);
            }
        }
        final List<String> addColumnSql = OceanBaseSqlBuilder.buildAddColumnsSql(dbName, tableName, columnInfos);
        executeSqlBatch(conn, addColumnSql);
    }

}