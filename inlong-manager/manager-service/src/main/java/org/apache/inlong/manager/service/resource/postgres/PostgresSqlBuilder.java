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

package org.apache.inlong.manager.service.resource.postgres;

import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.inlong.manager.common.pojo.sink.postgres.PostgresColumnInfo;
import org.apache.inlong.manager.common.pojo.sink.postgres.PostgresTableInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Builder for Postgres SQL string
 */
public class PostgresSqlBuilder {
    private static final Logger LOGGER = LoggerFactory.getLogger(PostgresSqlBuilder.class);

    private static final int FIRST_COLUMN_INDEX = 0;

    public static String getCheckDatabase(String dbName) {
        String sql = "select count(*) from pg_catalog.pg_database where datname = '" + dbName + "'";
        return sql;
    }

    /**
     * Build create database SQL
     */
    public static String buildCreateDbSql(String dbName) {
        String sql = "CREATE DATABASE " + dbName;
        LOGGER.info("create db sql: {}", sql);
        return sql;
    }

    /**
     * Build create table SQL
     */
    public static String buildCreateTableSql(PostgresTableInfo table) {
        StringBuilder sql = new StringBuilder();
        // Support _ beginning with underscore
        String dbTableName = table.getTableName();
        sql.append("CREATE TABLE ").append(dbTableName);

        // Construct columns and partition columns
        sql.append(buildCreateColumnsSql(table.getColumns()));

        LOGGER.info("create table sql: {}", sql);
        return sql.toString();
    }

    /**
     * Build add column SQL
     */
    public static List<String> buildAddColumnsSql(String tableName,
            List<PostgresColumnInfo> columnList) {
        String dbTableName = tableName;
        List<String> columnInfoList = getColumnsInfo(columnList);
        List<String> resultList = new ArrayList<>();
        for (String columnInfo : columnInfoList) {
            StringBuilder sql = new StringBuilder().append("ALTER TABLE ")
                    .append(dbTableName).append(" ADD COLUMN ").append(columnInfo);
            resultList.add(sql.toString());
        }
        return resultList;
    }

    /**
     * Build create column SQL
     */
    private static String buildCreateColumnsSql(List<PostgresColumnInfo> columns) {
        List<String> columnList = getColumnsInfo(columns);
        StringBuilder result = new StringBuilder().append(" (")
                .append(StringUtils.join(columnList, ",")).append(") ");
        return result.toString();
    }

    /**
     * Build column info
     */
    private static List<String> getColumnsInfo(List<PostgresColumnInfo> columns) {
        List<String> columnList = new ArrayList<>();
        for (PostgresColumnInfo columnInfo : columns) {
            // Construct columns and partition columns
            StringBuilder columnStr = new StringBuilder().append(columnInfo.getName())
                    .append(" ").append(columnInfo.getType());
            columnList.add(columnStr.toString());
        }
        return columnList;
    }

    /**
     * Build query table SQL
     */
    public static String buildDescTableSql(String tableName) {
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT a.attname as filedName, format_type(a.atttypid,a.atttypmod) as "
                + "filedType FROM pg_class as c,pg_attribute as a WHERE a.attrelid "
                + "= c.oid and a.attnum > 0 and c.relname = '").append(tableName).append("';");
        LOGGER.info("desc table sql={}", sql);
        return sql.toString();
    }
}
