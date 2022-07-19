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

import org.apache.commons.lang3.StringUtils;
import org.apache.inlong.manager.common.pojo.sink.mysql.MySQLColumnInfo;
import org.apache.inlong.manager.common.pojo.sink.mysql.MySQLTableInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Builder the SQL string for MySQL
 */
public class MySQLSqlBuilder {

    private static final Logger LOGGER = LoggerFactory.getLogger(MySQLSqlBuilder.class);

    /**
     * build SQL to check whether the database exists.
     *
     * @param dbName mysql database name
     * @return the check database sql string
     */
    public static String getCheckDatabase(String dbName) {
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT schema_name ")
                .append(" FROM information_schema.schemata ")
                .append("WHERE schema_name = '")
                .append(dbName)
                .append("';");
        LOGGER.info("check database sql: {}", sqlBuilder);
        return sqlBuilder.toString();
    }

    /**
     * build SQL to check whether the table exists.
     *
     * @param dbName    mysql database name
     * @param tableName mysql table name
     * @return the check table sql string
     */
    public static String getCheckTable(String dbName, String tableName) {
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("select table_schema,table_name ")
                .append(" from information_schema.tables where table_schema = '")
                .append(dbName)
                .append("' and table_name = '")
                .append(tableName)
                .append("' ;");
        LOGGER.info("check table sql: {}", sqlBuilder);
        return sqlBuilder.toString();
    }

    /**
     * build SQL to check whether the column exists.
     *
     * @param dbName     mysql database name
     * @param tableName  mysql table name
     * @param columnName mysql column name
     * @return the check column sql string
     */
    public static String getCheckColumn(String dbName, String tableName, String columnName) {
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT COLUMN_NAME,COLUMN_TYPE,COLUMN_COMMENT ")
                .append(" from  information_schema.COLUMNS where table_schema='")
                .append(dbName)
                .append("' and table_name = '")
                .append(tableName)
                .append("' and column_name = '")
                .append(columnName)
                .append("';");
        LOGGER.info("check table sql: {}", sqlBuilder);
        return sqlBuilder.toString();
    }

    /**
     * Build create database SQL
     *
     * @param dbName mysql database name
     * @return the create database sql string
     */
    public static String buildCreateDbSql(String dbName) {
        String sql = "CREATE DATABASE " + dbName;
        LOGGER.info("create db sql: {}", sql);
        return sql;
    }

    /**
     * Build create table SQL by MySQLTableInfo
     *
     * @param table mysql table info {@link MySQLTableInfo}
     * @return the create table sql String
     */
    public static String buildCreateTableSql(MySQLTableInfo table) {
        StringBuilder sql = new StringBuilder();
        // Support _ beginning with underscore
        sql.append("CREATE TABLE ").append(table.getDbName())
                .append(".")
                .append(table.getTableName());

        // Construct columns and partition columns
        sql.append(buildCreateColumnsSql(table));

        if (StringUtils.isEmpty(table.getEngine())) {
            sql.append(" ENGINE=InnoDB ");
        } else {
            sql.append(" ENGINE=")
                    .append(table.getEngine())
                    .append(" ");
        }
        if (!StringUtils.isEmpty(table.getCharset())) {
            sql.append(" DEFAULT CHARSET=")
                    .append(table.getCharset())
                    .append(" ");
        }

        LOGGER.info("create table sql: {}", sql);
        return sql.toString();
    }

    /**
     * Build add columns SQL
     *
     * @param tableName  mysql table name
     * @param columnList mysql column list {@link List}
     * @return add column sql string list
     */
    public static List<String> buildAddColumnsSql(String dbName, String tableName,
                                                  List<MySQLColumnInfo> columnList) {
        List<String> columnInfoList = getColumnsInfo(columnList);
        List<String> resultList = new ArrayList<>();
        StringBuilder sqlBuilder = new StringBuilder();
        for (String columnInfo : columnInfoList) {
            sqlBuilder.append("ALTER TABLE ")
                    .append(dbName)
                    .append(".")
                    .append(tableName)
                    .append(" ADD COLUMN ")
                    .append(columnInfo)
                    .append(";");
            resultList.add(sqlBuilder.toString());
            sqlBuilder.delete(0, sqlBuilder.length());
        }

        LOGGER.info("add columns sql={}", resultList);
        return resultList;
    }

    /**
     * Build create column SQL
     *
     * @param table mysql table info {@link MySQLTableInfo}
     * @return create column sub sql string
     */
    private static String buildCreateColumnsSql(MySQLTableInfo table) {
        StringBuilder sql = new StringBuilder();
        sql.append(" (");
        List<String> columnList = getColumnsInfo(table.getColumns());
        sql.append(StringUtils.join(columnList, ","));
        if (!StringUtils.isEmpty(table.getPrimaryKey())) {
            sql.append(", PRIMARY KEY (")
                    .append(table.getPrimaryKey())
                    .append(")");
        }
        sql.append(") ");
        LOGGER.info("create columns sql={}", sql);
        return sql.toString();
    }

    /**
     * Build column info by MySQLColumnInfo list
     *
     * @param columns mysql column info {@link MySQLColumnInfo} list
     * @return the sql list
     */
    private static List<String> getColumnsInfo(List<MySQLColumnInfo> columns) {
        List<String> columnList = new ArrayList<>();
        for (MySQLColumnInfo columnInfo : columns) {
            // Construct columns and partition columns
            StringBuilder columnBuilder = new StringBuilder();
            columnBuilder.append("`")
                    .append(columnInfo.getColName())
                    .append("`")
                    .append(" ")
                    .append(columnInfo.getDataType());
            if (!StringUtils.isEmpty(columnInfo.getComment())) {
                columnBuilder.append(" COMMENT '")
                        .append(columnInfo.getComment())
                        .append("'");
            }
            columnBuilder.append(" ");
            columnList.add(columnBuilder.toString());
        }
        return columnList;
    }

    /**
     * Build query table SQL
     *
     * @param dbName    mysql database name
     * @param tableName mysql table name
     * @return desc table sql string
     */
    public static String buildDescTableSql(String dbName, String tableName) {
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT COLUMN_NAME,COLUMN_TYPE,COLUMN_COMMENT ")
                .append(" from  information_schema.COLUMNS where table_schema='")
                .append(dbName)
                .append("' and table_name = '")
                .append(tableName)
                .append("';");
        LOGGER.info("desc table sql={}", sql);
        return sql.toString();
    }

}
