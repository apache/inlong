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

package org.apache.inlong.manager.service.resource.sqlserver;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.apache.inlong.manager.common.pojo.sink.sqlserver.SQLServerColumnInfo;
import org.apache.inlong.manager.common.pojo.sink.sqlserver.SQLServerTableInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * Utils for SqlServer JDBC.
 */
public class SQLServerJdbcUtils {

    private static final String SQLSERVER_JDBC_PREFIX = "jdbc:sqlserver";

    private static final String SQLSERVER_DRIVER_CLASS = "com.microsoft.sqlserver.jdbc.SQLServerDriver";

    private static final Logger LOG = LoggerFactory.getLogger(SQLServerJdbcUtils.class);

    /**
     * Get SqlServer connection from the url and user.
     *
     * @param url jdbc url, such as jdbc:sqlserver://host:port
     * @param user Username for JDBC URL
     * @param password User password
     * @return {@link Connection}
     * @throws Exception on get connection error
     */
    public static Connection getConnection(String url, String user, String password)
            throws Exception {
        if (StringUtils.isBlank(url) || !url.startsWith(SQLSERVER_JDBC_PREFIX)) {
            throw new Exception("SqlServer server URL was invalid, it should start with jdbc:sqlserver");
        }
        Connection conn;
        try {
            Class.forName(SQLSERVER_DRIVER_CLASS);
            conn = DriverManager.getConnection(url, user, password);
        } catch (Exception e) {
            String errorMsg = "get SqlServer connection error, please check SqlServer JDBC url, username or password!";
            LOG.error(errorMsg, e);
            throw new Exception(errorMsg + " other error msg: " + e.getMessage());
        }
        if (conn == null) {
            throw new Exception("get SqlServer connection failed, please contact administrator.");
        }
        LOG.info("get SqlServer connection success, url={}", url);
        return conn;
    }

    /**
     * Execute SQL command on SqlServer.
     *
     * @param conn JDBC Connection  {@link Connection}
     * @param sql SQL string to be executed
     * @throws Exception on execute SQL error
     */
    public static void executeSql(Connection conn, String sql) throws Exception {
        conn.setAutoCommit(true);
        Statement stmt = conn.createStatement();
        LOG.info("execute sql [{}] success !", sql);
        stmt.execute(sql);
        stmt.close();
    }

    /**
     * Execute query SQL on SqlServer.
     *
     * @param conn JDBC Connection  {@link Connection}
     * @param sql SQL string to be executed
     * @return {@link ResultSet}
     * @throws Exception on execute query SQL error
     */
    public static ResultSet executeQuerySql(Connection conn, String sql)
            throws Exception {
        Statement stmt = conn.createStatement();
        LOG.info("execute sql [{}] success !", sql);
        return stmt.executeQuery(sql);
    }

    /**
     * Execute batch query SQL on SqlServer.
     *
     * @param conn JDBC Connection  {@link Connection}
     * @param sqls SQL string to be executed
     * @throws Exception on get execute SQL batch error
     */
    public static void executeSqlBatch(Connection conn, List<String> sqls)
            throws Exception {
        conn.setAutoCommit(false);
        Statement stmt = conn.createStatement();
        for (String entry : sqls) {
            stmt.execute(entry);
        }
        conn.commit();
        stmt.close();
        conn.setAutoCommit(true);
        LOG.info("execute sql [{}] success! ", sqls);
    }

    /**
     * create SqlServer schema
     *
     * @param conn JDBC Connection  {@link Connection}
     * @param schemaName SqlServer schema name
     * @throws Exception on create schema error
     */
    public static void createSchema(Connection conn, String schemaName) throws Exception {
        if (!checkSchemaExist(conn, schemaName)) {
            String createSql = SQLServerSqlBuilder.buildCreateSchemaSql(schemaName);
            executeSql(conn, createSql);
            LOG.info("execute sql [{}] success! ", createSql);
        } else {
            LOG.info("The schemaName [{}] are exists !", schemaName);
        }
    }

    /**
     * Create SqlServer table by SqlServerTableInfo
     *
     * @param conn JDBC Connection  {@link Connection}
     * @param tableInfo SqlServer table info  {@link SQLServerTableInfo}
     * @throws Exception on create table error
     */
    public static void createTable(Connection conn, SQLServerTableInfo tableInfo)
            throws Exception {
        if (checkTablesExist(conn, tableInfo.getSchemaName(), tableInfo.getTableName())) {
            LOG.info("The table [{}] are exists !", tableInfo.getTableName());
        } else {
            List<String> createTableSqls = SQLServerSqlBuilder.buildCreateTableSql(tableInfo);
            executeSqlBatch(conn, createTableSqls);
            LOG.info("execute sql [{}] success! ", createTableSqls);
        }
    }

    /**
     * Check tables from the SqlServer information_schema.
     *
     * @param conn JDBC Connection  {@link Connection}
     * @param schemaName SqlServer schema name
     * @param tableName SqlServer table name
     * @return true if table exist, otherwise false
     * @throws Exception on check table exist error
     */
    public static boolean checkTablesExist(Connection conn, String schemaName, String tableName)
            throws Exception {
        boolean result = false;
        String checkTableSql = SQLServerSqlBuilder.getCheckTable(schemaName, tableName);
        ResultSet resultSet = executeQuerySql(conn, checkTableSql);
        if (null != resultSet && resultSet.next()) {
            int count = resultSet.getInt(1);
            if (count > 0) {
                result = true;
            }
        }
        LOG.info("check table exist for schema={} table={}, result={}", schemaName, tableName, result);
        resultSet.close();
        return result;
    }

    /**
     * Check schema from the SqlServer information_schema.schemata.
     *
     * @param conn JDBC Connection  {@link Connection}
     * @param schemaName SqlServer schema
     * @return true if schema exist, otherwise false
     * @throws Exception on check schema exist error
     */
    public static boolean checkSchemaExist(Connection conn, String schemaName) throws Exception {
        boolean result = false;
        String checkSchemaSql = SQLServerSqlBuilder.getCheckSchema(schemaName);
        ResultSet resultSet = executeQuerySql(conn, checkSchemaSql);
        if (null != resultSet && resultSet.next()) {
            int count = resultSet.getInt(1);
            if (count > 0) {
                result = true;
            }
        }
        LOG.info("check schema exist for schemaName={}, result={}", schemaName, result);
        resultSet.close();
        return result;
    }

    /**
     * Check whether the column exists in the table.
     *
     * @param conn JDBC Connection  {@link Connection}
     * @param schemaName SqlServer schema name
     * @param tableName SqlServer table name
     * @param column SqlServer table column name
     * @return true if column exist in the table, otherwise false
     * @throws Exception on check column exist error
     */
    public static boolean checkColumnExist(Connection conn, String schemaName, String tableName, String column)
            throws Exception {
        boolean result = false;
        String checkTableSql = SQLServerSqlBuilder.getCheckColumn(schemaName, tableName, column);
        ResultSet resultSet = executeQuerySql(conn, checkTableSql);
        if (null != resultSet && resultSet.next()) {
            int count = resultSet.getInt(1);
            if (count > 0) {
                result = true;
            }
        }
        LOG.info("check column exist for schema={} table={}, result={} column={}",
                schemaName, tableName, result, column);
        resultSet.close();
        return result;
    }

    /**
     * Query all columns of the tableName.
     *
     * @param conn JDBC Connection  {@link Connection}
     * @param schemaName SqlServer schema name
     * @param tableName SqlServer table name
     * @return {@link List}
     * @throws Exception on get columns error
     */
    public static List<SQLServerColumnInfo> getColumns(Connection conn, String schemaName, String tableName)
            throws Exception {
        String querySql = SQLServerSqlBuilder.buildDescTableSql(schemaName, tableName);
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(querySql);
        List<SQLServerColumnInfo> columnList = new ArrayList<>();
        while (rs.next()) {
            SQLServerColumnInfo columnInfo = new SQLServerColumnInfo();
            columnInfo.setName(rs.getString(1));
            columnInfo.setType(rs.getString(2));
            columnInfo.setComment(rs.getString(3));
            columnList.add(columnInfo);
        }
        rs.close();
        stmt.close();
        return columnList;
    }

    /**
     * Add columns for SqlServer table.
     *
     * @param conn JDBC Connection  {@link Connection}
     * @param schemaName SqlServer schema name
     * @param tableName SqlServer table name
     * @param columns SqlServer columns to be added
     * @throws Exception on add columns error
     */
    public static void addColumns(Connection conn, String schemaName, String tableName,
            List<SQLServerColumnInfo> columns) throws Exception {
        List<SQLServerColumnInfo> columnInfos = Lists.newArrayList();

        for (SQLServerColumnInfo columnInfo : columns) {
            if (!checkColumnExist(conn, schemaName, tableName, columnInfo.getName())) {
                columnInfos.add(columnInfo);
            }
        }
        executeSqlBatch(conn, SQLServerSqlBuilder.buildAddColumnsSql(schemaName, tableName, columnInfos));
    }
}
