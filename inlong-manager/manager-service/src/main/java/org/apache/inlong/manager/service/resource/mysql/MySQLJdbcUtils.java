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

package org.apache.inlong.manager.service.resource.mysql;

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

public class MySQLJdbcUtils {

    private static final String MYSQL_JDBC_PREFIX = "jdbc:mysql";
    private static final String MYSQL_DRIVER_CLASS = "com.mysql.cj.jdbc.Driver";

    private static final Logger LOG = LoggerFactory.getLogger(MySQLJdbcUtils.class);

    /**
     * Get MySQL connection from the url and user
     * @param url
     * @param user
     * @param password
     * @return
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
            String errorMsg = "get mysql connection error, please check mysql jdbc url, username or password!";
            LOG.error(errorMsg, e);
            throw new Exception(errorMsg + " other error msg: " + e.getMessage());
        }
        if (conn == null) {
            throw new Exception("get mysql connection failed, please contact administrator");
        }
        LOG.info("get mysql connection success, url={}", url);
        return conn;
    }

    /**
     * Execute SQL command on MySQL
     * @param conn
     * @param sql
     * @return
     */
    public static boolean executeSql(Connection conn, String sql) throws Exception {
        Statement stmt = conn.createStatement();
        LOG.info("execute sql [{}] success !", sql);
        return stmt.execute(sql);
    }

    /**
     * Execute query SQL on MySQL
     * @param conn
     * @param sql
     * @return
     */
    public static ResultSet executeQuerySql(Connection conn, String sql)
            throws Exception {
        Statement stmt = conn.createStatement();
        LOG.info("execute sql [{}] success !", sql);
        return stmt.executeQuery(sql);
    }

    /**
     * Execute batch query SQL on MySQL
     * @param conn
     * @param sqls
     * @throws Exception
     */
    public static void executeSqlBatch(Connection conn, List<String> sqls)
            throws Exception {
        Statement stmt = conn.createStatement();
        for (String entry : sqls) {
            stmt.execute(entry);
        }
        LOG.info("execute sql [{}] success! ", sqls);

    }

    /**
     * Create MySQL database
     * @param conn
     * @param dbName
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
        }
    }

    /**
     * Create MySQL table
     * @param conn
     * @param tableInfo
     * @throws Exception
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
     * check tables from the MySQL information_schema
     * @param conn
     * @param dbName
     * @param tableName
     * @return
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
        return result;
    }

    /**
     * check column exist
     * @param conn
     * @param dbName
     * @param tableName
     * @param column
     * @return
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
        return result;
    }

    /**
     * Query MySQL columns
     * @param conn
     * @param tableName
     * @return
     */
    public static List<MySQLColumnInfo> getColumns(Connection conn, String dbName, String tableName)
            throws Exception {
        String querySql = MySQLSqlBuilder.buildDescTableSql(dbName, tableName);
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(querySql);
        List<MySQLColumnInfo> columnList = new ArrayList<>();
        while (rs.next()) {
            MySQLColumnInfo columnInfo = new MySQLColumnInfo();
            columnInfo.setColName(rs.getString(1));
            columnInfo.setDataType(rs.getString(2));
            columnInfo.setComment(rs.getString(3));
            columnList.add(columnInfo);
        }
        return columnList;
    }

    /**
     * Add columns for MySQL table
     * @param tableName
     * @throws Exception
     */
    public static void addColumns(Connection conn, String dbName, String tableName, List<MySQLColumnInfo> columns)
            throws Exception {
        List<MySQLColumnInfo> columnInfos = Lists.newArrayList();

        for (MySQLColumnInfo columnInfo : columns) {
            if (!checkColumnExist(conn, dbName, tableName, columnInfo.getColName())) {
                columnInfos.add(columnInfo);
            }
        }
        List<String> addColumnSql = MySQLSqlBuilder.buildAddColumnsSql(dbName, tableName, columnInfos);
        executeSqlBatch(conn, addColumnSql);
    }
}
