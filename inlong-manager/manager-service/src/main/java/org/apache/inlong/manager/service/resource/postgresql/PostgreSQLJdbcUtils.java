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

package org.apache.inlong.manager.service.resource.postgresql;

import org.apache.commons.lang3.StringUtils;
import org.apache.inlong.manager.common.pojo.sink.postgresql.PostgreSQLColumnInfo;
import org.apache.inlong.manager.common.pojo.sink.postgresql.PostgreSQLTableInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * Utils for PostgreSQL JDBC.
 */
public class PostgreSQLJdbcUtils {

    private static final String POSTGRES_DRIVER_CLASS = "org.postgresql.Driver";
    private static final String SCHEMA_PATTERN = "public";
    private static final String TABLE_TYPE = "TABLE";
    private static final String COLUMN_LABEL_TABLE = "TABLE_NAME";
    private static final String COLUMN_LABEL_COUNT = "count";
    private static final String POSTGRES_JDBC_PREFIX = "jdbc:postgresql";

    private static final Logger LOG = LoggerFactory.getLogger(PostgreSQLJdbcUtils.class);

    /**
     * Get PostgreSQL connection from the url and user
     */
    public static Connection getConnection(String url, String user, String password)
            throws Exception {
        if (StringUtils.isBlank(url) || !url.startsWith(POSTGRES_JDBC_PREFIX)) {
            throw new Exception("PostgreSQL server URL was invalid, it should start with jdbc:postgresql");
        }
        Connection conn;
        try {
            Class.forName(POSTGRES_DRIVER_CLASS);
            conn = DriverManager.getConnection(url, user, password);
        } catch (Exception e) {
            String errorMsg = "get postgresql connection error, please check postgres jdbc url, username or password!";
            LOG.error(errorMsg, e);
            throw new Exception(errorMsg + " other error msg: " + e.getMessage());
        }
        if (conn == null) {
            throw new Exception("get postgresql connection failed, please contact administrator");
        }
        LOG.info("get postgresql connection success, url={}", url);
        return conn;
    }

    /**
     * Execute SQL command on PostgreSQL
     *
     * @return true if execute successfully
     */
    public static boolean executeSql(String sql, String url, String user, String password) throws Exception {
        try (Connection conn = getConnection(url, user, password)) {
            Statement stmt = conn.createStatement();
            LOG.info("execute sql [{}] success for url: {}", sql, url);
            return stmt.execute(sql);
        }
    }

    /**
     * Execute query SQL on PostgreSQL
     *
     * @return the query result set
     */
    public static ResultSet executeQuerySql(String sql, String url, String user, String password)
            throws Exception {
        try (Connection conn = getConnection(url, user, password)) {
            Statement stmt = conn.createStatement();
            LOG.info("execute sql [{}] success for url: {}", sql, url);
            return stmt.executeQuery(sql);
        }
    }

    /**
     * Execute batch SQL commands on PostgreSQL
     */
    public static void executeSqlBatch(List<String> sql, String url, String user, String password)
            throws Exception {
        try (Connection conn = getConnection(url, user, password)) {
            Statement stmt = conn.createStatement();
            for (String entry : sql) {
                stmt.execute(entry);
            }
            LOG.info("execute sql [{}] success for url: {}", sql, url);
        }
    }

    /**
     * Create PostgreSQL database
     */
    public static void createDb(String url, String user, String password, String dbName) throws Exception {
        String checkDbSql = PostgreSQLSqlBuilder.getCheckDatabase(dbName);
        ResultSet resultSet = executeQuerySql(checkDbSql, url, user, password);
        if (resultSet != null) {
            resultSet.next();
            if (resultSet.getInt(COLUMN_LABEL_COUNT) == 0) {
                String createDbSql = PostgreSQLSqlBuilder.buildCreateDbSql(dbName);
                executeSql(createDbSql, url, user, password);
            }
        }
    }

    /**
     * Create PostgreSQL table
     */
    public static void createTable(String url, String user, String password, PostgreSQLTableInfo tableInfo)
            throws Exception {
        String createTableSql = PostgreSQLSqlBuilder.buildCreateTableSql(tableInfo);
        PostgreSQLJdbcUtils.executeSql(createTableSql, url, user, password);
    }

    /**
     * Get PostgreSQL tables from the PostgreSQL metadata
     */
    public static boolean checkTablesExist(String url, String user, String password, String dbName, String tableName)
            throws Exception {
        boolean result = false;
        try (Connection conn = getConnection(url, user, password)) {
            DatabaseMetaData metaData = conn.getMetaData();
            ResultSet rs = metaData.getTables(conn.getCatalog(), SCHEMA_PATTERN, tableName, new String[]{TABLE_TYPE});
            if (rs != null) {
                rs.next();
                result = rs.getRow() > 0 && tableName.equals(rs.getString(COLUMN_LABEL_TABLE));
                LOG.info("check table exist for db={} table={}, result={}", dbName, tableName, result);
            }
        }
        return result;
    }

    /**
     * Query PostgreSQL columns
     */
    public static List<PostgreSQLColumnInfo> getColumns(String url, String user, String password, String tableName)
            throws Exception {
        String querySql = PostgreSQLSqlBuilder.buildDescTableSql(tableName);
        try (Connection conn = getConnection(url, user, password);
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(querySql)) {
            List<PostgreSQLColumnInfo> columnList = new ArrayList<>();

            while (rs.next()) {
                PostgreSQLColumnInfo columnInfo = new PostgreSQLColumnInfo();
                columnInfo.setName(rs.getString(1));
                columnInfo.setType(rs.getString(2));
                columnList.add(columnInfo);
            }
            return columnList;
        }
    }

    /**
     * Add columns for PostgreSQL table
     */
    public static void addColumns(String url, String user, String password, String tableName,
            List<PostgreSQLColumnInfo> columnList) throws Exception {
        List<String> addColumnSql = PostgreSQLSqlBuilder.buildAddColumnsSql(tableName, columnList);
        PostgreSQLJdbcUtils.executeSqlBatch(addColumnSql, url, user, password);
    }

}
