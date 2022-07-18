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

import com.google.common.base.Strings;
import org.apache.commons.lang3.StringUtils;
import org.apache.inlong.manager.common.pojo.sink.mysql.MySQLColumnInfo;
import org.apache.inlong.manager.common.pojo.sink.mysql.MySQLTableInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class MySQLSqlBuilder {

    private static final Logger LOGGER = LoggerFactory.getLogger(MySQLSqlBuilder.class);

    /**
     * Build check database exists SQL
     *
     * @param dbName
     * @return
     */
    public static String getCheckDatabase(String dbName) {
        String sql = "SELECT schema_name FROM information_schema.schemata WHERE schema_name = '" + dbName + "'";
        LOGGER.info("check database sql: {}", sql);
        return sql;
    }

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
     * @param dbName
     * @return
     */
    public static String buildCreateDbSql(String dbName) {
        String sql = "CREATE DATABASE " + dbName;
        LOGGER.info("create db sql: {}", sql);
        return sql;
    }

    /**
     * Build create table SQL
     *
     * @param table
     * @return
     */
    public static String buildCreateTableSql(MySQLTableInfo table) {
        StringBuilder sql = new StringBuilder();
        // Support _ beginning with underscore
        String dbTableName = table.getTableName();
        sql.append("CREATE TABLE ").append(table.getDbName())
                .append(".")
                .append(table.getTableName());

        // Construct columns and partition columns
        sql.append(buildCreateColumnsSql(table));

        if (Strings.isNullOrEmpty(table.getEngine())) {
            sql.append(" ENGINE=InnoDB ");
        } else {
            sql.append(" ENGINE=");
            sql.append(table.getEngine());
            sql.append(" ");
        }
        if (!Strings.isNullOrEmpty(table.getCharset())) {
            sql.append(" DEFAULT CHARSET=");
            sql.append(table.getCharset());
            sql.append(" ");
        }

        LOGGER.info("create table sql: {}", sql);
        return sql.toString();
    }

    /**
     * Build add column SQL
     *
     * @param tableName
     * @param columnList
     * @return
     */
    public static List<String> buildAddColumnsSql(String dbName, String tableName,
                                                  List<MySQLColumnInfo> columnList) {
        List<String> columnInfoList = getColumnsInfo(columnList);
        List<String> resultList = new ArrayList<>();
        for (String columnInfo : columnInfoList) {
            StringBuilder sqlBuilder = new StringBuilder();
            sqlBuilder.append("ALTER TABLE ");
            sqlBuilder.append(dbName);
            sqlBuilder.append(".");
            sqlBuilder.append(tableName);
            sqlBuilder.append(" ADD COLUMN ");
            sqlBuilder.append(columnInfo);
            resultList.add(sqlBuilder.toString());
        }

        LOGGER.info("add columns sql={}", resultList);
        return resultList;
    }

    /**
     * Build create column SQL
     *
     * @param table
     * @return
     */
    private static String buildCreateColumnsSql(MySQLTableInfo table) {
        StringBuilder sql = new StringBuilder();
        sql.append(" (");
        List<String> columnList = getColumnsInfo(table.getColumns());
        sql.append(StringUtils.join(columnList, ","));
        if (!Strings.isNullOrEmpty(table.getPrimaryKey())) {
            sql.append(", PRIMARY KEY (");
            sql.append(table.getPrimaryKey());
            sql.append(")");
        }
        sql.append(") ");
        LOGGER.info("create columns sql={}", sql.toString());
        return sql.toString();
    }

    /**
     * Build column info
     */
    private static List<String> getColumnsInfo(List<MySQLColumnInfo> columns) {
        List<String> columnList = new ArrayList<>();
        for (MySQLColumnInfo columnInfo : columns) {
            // Construct columns and partition columns
            StringBuilder columnBuilder = new StringBuilder();
            columnBuilder.append(columnInfo.getColName())
                    .append(" ")
                    .append(columnInfo.getDataType());
            if (!Strings.isNullOrEmpty(columnInfo.getComment())) {
                columnBuilder.append(" COMMENT '")
                        .append(columnInfo.getComment())
                        .append("'");
            }
            columnBuilder.append(" ;");
            columnList.add(columnBuilder.toString());
        }
        return columnList;
    }

    /**
     * Build query table SQL
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
