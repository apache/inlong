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

package org.apache.inlong.manager.service.resource.sink.oracle;

import org.apache.commons.lang3.StringUtils;
import org.apache.inlong.manager.common.pojo.sink.oracle.OracleColumnInfo;
import org.apache.inlong.manager.common.pojo.sink.oracle.OracleTableInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * Utils for Oracle JDBC.
 */
public class OracleJdbcUtils {

    private static final String ORACLE_JDBC_PREFIX = "jdbc:oracle";

    private static final String ORACLE_DRIVER_CLASS = "oracle.jdbc.driver.OracleDriver";

    private static final Logger LOG = LoggerFactory.getLogger(OracleJdbcUtils.class);

    /**
     * Get Oracle connection from the url and user.
     *
     * @param url jdbc url,such as jdbc:oracle:thin@host:port:sid or jdbc:oracle:thin@host:port/service_name
     * @param user Username for JDBC URL
     * @param password User password
     * @return {@link Connection}
     * @throws Exception on get connection error
     */
    public static Connection getConnection(String url, String user, String password)
            throws Exception {
        if (StringUtils.isBlank(url) || !url.startsWith(ORACLE_JDBC_PREFIX)) {
            throw new Exception("Oracle server URL was invalid, it should start with jdbc:oracle");
        }
        Connection conn;
        try {
            Class.forName(ORACLE_DRIVER_CLASS);
            conn = DriverManager.getConnection(url, user, password);
        } catch (Exception e) {
            String errorMsg = "get Oracle connection error, please check Oracle JDBC url, username or password!";
            LOG.error(errorMsg, e);
            throw new Exception(errorMsg + " other error msg: " + e.getMessage());
        }
        if (conn == null) {
            throw new Exception("get Oracle connection failed, please contact administrator.");
        }
        LOG.info("get Oracle connection success, url={}", url);
        return conn;
    }

    /**
     * Execute SQL command on Oracle.
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
     * Execute query SQL on Oracle.
     *
     * @param conn JDBC Connection  {@link Connection}
     * @param sql SQL string to be executed
     * @return {@link ResultSet}
     * @throws Exception on execute query SQL error
     */
    public static ResultSet executeQuerySql(Connection conn, String sql)
            throws Exception {
        Statement stmt = conn.createStatement();
        ResultSet resultSet = stmt.executeQuery(sql);
        LOG.info("execute sql [{}] success !", sql);
        return resultSet;
    }

    /**
     * Execute batch query SQL on Oracle.
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
     * Create Oracle table by OracleTableInfo
     *
     * @param conn JDBC Connection  {@link Connection}
     * @param tableInfo Oracle table info  {@link OracleTableInfo}
     * @throws Exception on create table error
     */
    public static void createTable(Connection conn, OracleTableInfo tableInfo)
            throws Exception {
        if (checkTablesExist(conn, tableInfo.getUserName(), tableInfo.getTableName())) {
            LOG.info("The table [{}] are exists !", tableInfo.getTableName());
        } else {
            List<String> createTableSqls = OracleSqlBuilder.buildCreateTableSql(tableInfo);
            executeSqlBatch(conn, createTableSqls);
            LOG.info("execute sql [{}] success! ", createTableSqls);
        }
    }

    /**
     * Check tables from the Oracle information_schema.
     *
     * @param conn JDBC Connection  {@link Connection}
     * @param userName Oracle database name
     * @param tableName Oracle table name
     * @return true if table exist, otherwise false
     * @throws Exception on check table exist error
     */
    public static boolean checkTablesExist(Connection conn, String userName, String tableName)
            throws Exception {
        boolean result = false;
        String checkTableSql = OracleSqlBuilder.getCheckTable(userName, tableName);
        ResultSet resultSet = executeQuerySql(conn, checkTableSql);
        if (null != resultSet && resultSet.next()) {
            int size = resultSet.getInt(1);
            if (size > 0) {
                LOG.info("check table exist for username={} table={}, result={}", userName, tableName, result);
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
     * @param tableName Oracle table name
     * @param column Oracle table column name
     * @return true if column exist in the table, otherwise false
     * @throws Exception on check column exist error
     */
    public static boolean checkColumnExist(Connection conn, String tableName, String column)
            throws Exception {
        boolean result = false;
        String checkColumnSql = OracleSqlBuilder.getCheckColumn(tableName, column);
        ResultSet resultSet = executeQuerySql(conn, checkColumnSql);
        if (resultSet != null && resultSet.next()) {
            int count = resultSet.getInt(1);
            if (count > 0) {
                result = true;
            }
        }
        resultSet.close();
        LOG.info("check column exist for table={}, column={}, result={} ", tableName, column, result);
        return result;
    }

    /**
     * Query all columns of the tableName.
     *
     * @param conn JDBC Connection  {@link Connection}
     * @param tableName Oracle table name
     * @return {@link List}
     * @throws Exception on get columns error
     */
    public static List<OracleColumnInfo> getColumns(Connection conn, String tableName)
            throws Exception {
        String querySql = OracleSqlBuilder.buildDescTableSql(tableName);
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(querySql);
        List<OracleColumnInfo> columnList = new ArrayList<>();
        while (rs.next()) {
            OracleColumnInfo columnInfo = new OracleColumnInfo();
            columnInfo.setName(rs.getString(1));
            columnInfo.setType(rs.getString(2));
            columnInfo.setComment(rs.getString(3));
            columnList.add(columnInfo);
        }
        rs.close();
        return columnList;
    }

    /**
     * Add columns for Oracle table.
     *
     * @param conn JDBC Connection  {@link Connection}
     * @param tableName Oracle table name
     * @param columns Oracle columns to be added
     * @throws Exception on add columns error
     */
    public static void addColumns(Connection conn, String tableName, List<OracleColumnInfo> columns)
            throws Exception {
        List<OracleColumnInfo> columnInfos = new ArrayList<>();

        for (OracleColumnInfo columnInfo : columns) {
            if (!checkColumnExist(conn, tableName, columnInfo.getName())) {
                columnInfos.add(columnInfo);
            }
        }
        List<String> addColumnSql = OracleSqlBuilder.buildAddColumnsSql(tableName, columnInfos);
        executeSqlBatch(conn, addColumnSql);
    }
}
