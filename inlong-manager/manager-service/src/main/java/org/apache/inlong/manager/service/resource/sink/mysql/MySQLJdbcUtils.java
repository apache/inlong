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

package org.apache.inlong.manager.service.resource.sink.mysql;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.apache.inlong.manager.common.pojo.sink.mysql.MySQLColumnInfo;
import org.apache.inlong.manager.common.pojo.sink.mysql.MySQLTableInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * Utils for MySQL JDBC.
 */
public class MySQLJdbcUtils {

    private static final String MYSQL_JDBC_PREFIX = "jdbc:mysql";

    private static final String MYSQL_DRIVER_CLASS = "com.mysql.cj.jdbc.Driver";

    private static final Logger LOG = LoggerFactory.getLogger(MySQLJdbcUtils.class);

    /**
     * Get MySQL connection from the url and user.
     *
     * @param url jdbc url, such as jdbc:mysql://host:port/database
     * @param user Username for JDBC URL
     * @param password User password
     * @return {@link Connection}
     * @throws Exception on get connection error
     */
    public static Connection getConnection(String url, String user, String password)
            throws Exception {
        if (StringUtils.isBlank(url) || !url.startsWith(MYSQL_JDBC_PREFIX)) {
            throw new Exception("MySQL server URL was invalid, it should start with jdbc:mysql");
        }
        Connection conn;
        try {
            Class.forName(MYSQL_DRIVER_CLASS);
            conn = DriverManager.getConnection(url, user, password);
        } catch (Exception e) {
            String errorMsg = "get MySQL connection error, please check MySQL JDBC url, username or password!";
            LOG.error(errorMsg, e);
            throw new Exception(errorMsg + " other error msg: " + e.getMessage());
        }
        if (conn == null) {
            throw new Exception("get MySQL connection failed, please contact administrator.");
        }
        LOG.info("get MySQL connection success, url={}", url);
        return conn;
    }

    /**
     * Execute SQL command on MySQL.
     *
     * @param conn JDBC Connection  {@link Connection}
     * @param sql SQL string to be executed
     * @throws Exception on execute SQL error
     */
    public static void executeSql(Connection conn, String sql) throws Exception {
        Statement stmt = conn.createStatement();
        LOG.info("execute sql [{}] success !", sql);
        stmt.execute(sql);
        stmt.close();
    }

    /**
     * Execute query SQL on MySQL.
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
     * Execute batch query SQL on MySQL.
     *
     * @param conn JDBC Connection  {@link Connection}
     * @param sqls SQL string to be executed
     * @throws Exception on get execute SQL batch error
     */
    public static void executeSqlBatch(Connection conn, List<String> sqls)
            throws Exception {
        Statement stmt = conn.createStatement();
        for (String entry : sqls) {
            stmt.execute(entry);
        }
        stmt.close();
        LOG.info("execute sql [{}] success! ", sqls);
    }

    /**
     * Create MySQL database
     *
     * @param conn JDBC Connection  {@link Connection}
     * @param dbName database name
     * @throws Exception on create database error
     */
    public static void createDb(Connection conn, String dbName) throws Exception {
        String checkDbSql = MySQLSqlBuilder.getCheckDatabase(dbName);
        ResultSet resultSet = executeQuerySql(conn, checkDbSql);
        if (resultSet != null) {
            if (!resultSet.next()) {
                String createDbSql = MySQLSqlBuilder.buildCreateDbSql(dbName);
                executeSql(conn, createDbSql);
                LOG.info("execute sql [{}] success! ", createDbSql);
            } else {
                LOG.info("The database [{}] are exists !", dbName);
            }
            resultSet.close();
        }
    }

    /**
     * Create MySQL table by MySQLTableInfo
     *
     * @param conn JDBC Connection  {@link Connection}
     * @param tableInfo MySQL table info  {@link MySQLTableInfo}
     * @throws Exception on create table error
     */
    public static void createTable(Connection conn, MySQLTableInfo tableInfo)
            throws Exception {
        if (checkTablesExist(conn, tableInfo.getDbName(), tableInfo.getTableName())) {
            LOG.info("The table [{}] are exists !", tableInfo.getTableName());
        } else {
            String createTableSql = MySQLSqlBuilder.buildCreateTableSql(tableInfo);
            executeSql(conn, createTableSql);
            LOG.info("execute sql [{}] success! ", createTableSql);
        }
    }

    /**
     * Check tables from the MySQL information_schema.
     *
     * @param conn JDBC Connection  {@link Connection}
     * @param dbName MySQL database name
     * @param tableName MySQL table name
     * @return true if table exist, otherwise false
     * @throws Exception on check table exist error
     */
    public static boolean checkTablesExist(Connection conn, String dbName, String tableName)
            throws Exception {
        boolean result = false;
        String checkTableSql = MySQLSqlBuilder.getCheckTable(dbName, tableName);
        ResultSet resultSet = executeQuerySql(conn, checkTableSql);
        if (resultSet != null) {
            if (resultSet.next()) {
                LOG.info("check table exist for db={} table={}, result={}", dbName, tableName, result);
                return true;
            }
        }
        resultSet.close();
        return result;
    }

    /**
     * Check whether the column exists in the table.
     *
     * @param conn JDBC Connection  {@link Connection}
     * @param dbName MySQL database name
     * @param tableName MySQL table name
     * @param column MySQL table column name
     * @return true if column exist in the table, otherwise false
     * @throws Exception on check column exist error
     */
    public static boolean checkColumnExist(Connection conn, String dbName, String tableName, String column)
            throws Exception {
        boolean result = false;
        String checkTableSql = MySQLSqlBuilder.getCheckColumn(dbName, tableName, column);
        ResultSet resultSet = executeQuerySql(conn, checkTableSql);
        if (resultSet != null) {
            if (resultSet.next()) {
                LOG.info("check column exist for db={} table={}, result={} column={}",
                        dbName, tableName, result, column);
                return true;
            }
        }
        resultSet.close();
        return result;
    }

    /**
     * Query all columns of the tableName.
     *
     * @param conn JDBC Connection  {@link Connection}
     * @param dbName MySQL database name
     * @param tableName MySQL table name
     * @return {@link List}
     * @throws Exception on get columns error
     */
    public static List<MySQLColumnInfo> getColumns(Connection conn, String dbName, String tableName)
            throws Exception {
        String querySql = MySQLSqlBuilder.buildDescTableSql(dbName, tableName);
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(querySql);
        List<MySQLColumnInfo> columnList = new ArrayList<>();
        while (rs.next()) {
            MySQLColumnInfo columnInfo = new MySQLColumnInfo();
            columnInfo.setName(rs.getString(1));
            columnInfo.setType(rs.getString(2));
            columnInfo.setComment(rs.getString(3));
            columnList.add(columnInfo);
        }
        rs.close();
        return columnList;
    }

    /**
     * Add columns for MySQL table.
     *
     * @param conn JDBC Connection  {@link Connection}
     * @param dbName MySQL database name
     * @param tableName MySQL table name
     * @param columns MySQL columns to be added
     * @throws Exception on add columns error
     */
    public static void addColumns(Connection conn, String dbName, String tableName, List<MySQLColumnInfo> columns)
            throws Exception {
        List<MySQLColumnInfo> columnInfos = Lists.newArrayList();

        for (MySQLColumnInfo columnInfo : columns) {
            if (!checkColumnExist(conn, dbName, tableName, columnInfo.getName())) {
                columnInfos.add(columnInfo);
            }
        }
        List<String> addColumnSql = MySQLSqlBuilder.buildAddColumnsSql(dbName, tableName, columnInfos);
        executeSqlBatch(conn, addColumnSql);
    }
}
