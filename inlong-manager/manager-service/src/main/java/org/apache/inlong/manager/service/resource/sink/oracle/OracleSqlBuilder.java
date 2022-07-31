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
import org.apache.inlong.manager.pojo.sink.oracle.OracleColumnInfo;
import org.apache.inlong.manager.pojo.sink.oracle.OracleTableInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class OracleSqlBuilder {

    private static final Logger LOGGER = LoggerFactory.getLogger(OracleSqlBuilder.class);

    /**
     * Build SQL to check whether the table exists.
     *
     * @param userName Oracle database name
     * @param tableName Oracle table name
     * @return the check table SQL string
     */
    public static String getCheckTable(String userName, String tableName) {
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT COUNT(*) FROM ALL_TABLES WHERE OWNER = UPPER('")
                .append(userName)
                .append("') ")
                .append("AND TABLE_NAME = '")
                .append(tableName)
                .append("' ");
        LOGGER.info("check table sql: {}", sqlBuilder);
        return sqlBuilder.toString();
    }

    /**
     * Build SQL to check whether the column exists.
     *
     * @param tableName Oracle table name
     * @param columnName Oracle column name
     * @return the check column SQL string
     */
    public static String getCheckColumn(String tableName, String columnName) {
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT count(1) ")
                .append(" from  USER_TAB_COLUMNS where TABLE_NAME= '")
                .append(tableName)
                .append("' and COLUMN_NAME = '")
                .append(columnName)
                .append("' ");
        LOGGER.info("check table sql: {}", sqlBuilder);
        return sqlBuilder.toString();
    }

    /**
     * Build create table SQL by OracleTableInfo.
     *
     * @param table Oracle table info {@link OracleTableInfo}
     * @return the create table SQL String
     */
    public static List<String> buildCreateTableSql(OracleTableInfo table) {
        List<String> sqls = new ArrayList<>();
        StringBuilder createSql = new StringBuilder();
        // Support _ beginning with underscore
        createSql.append("CREATE TABLE ").append(table.getUserName())
                .append(".\"")
                .append(table.getTableName())
                .append("\"");
        // Construct columns and partition columns
        createSql.append(buildCreateColumnsSql(table));
        sqls.add(createSql.toString());
        sqls.addAll(getColumnsComment(table.getTableName(), table.getColumns()));
        LOGGER.info("create table sql: {}", sqls);
        return sqls;
    }

    /**
     * Build add columns SQL.
     *
     * @param tableName Oracle table name
     * @param columnList Oracle column list {@link List}
     * @return add column SQL string list
     */
    public static List<String> buildAddColumnsSql(String tableName, List<OracleColumnInfo> columnList) {
        List<String> resultList = new ArrayList<>();

        for (OracleColumnInfo columnInfo : columnList) {
            StringBuilder sqlBuilder = new StringBuilder();
            sqlBuilder.append("ALTER TABLE \"")
                    .append(tableName)
                    .append("\" ADD \"")
                    .append(columnInfo.getName())
                    .append("\" ")
                    .append(columnInfo.getType())
                    .append(" ");
            resultList.add(sqlBuilder.toString());
        }
        resultList.addAll(getColumnsComment(tableName, columnList));
        LOGGER.info("add columns sql={}", resultList);
        return resultList;
    }

    /**
     * Build create column SQL.
     *
     * @param table Oracle table info {@link OracleColumnInfo}
     * @return create column SQL string
     */
    private static String buildCreateColumnsSql(OracleTableInfo table) {
        StringBuilder sql = new StringBuilder();
        sql.append(" (");
        List<String> columnList = getColumnsInfo(table.getColumns());
        sql.append(StringUtils.join(columnList, ","));
        sql.append(") ");
        LOGGER.info("create columns sql={}", sql);
        return sql.toString();
    }

    /**
     * Build column info by OracleColumnInfo list.
     *
     * @param columns Oracle column info {@link OracleColumnInfo} list
     * @return the SQL list
     */
    private static List<String> getColumnsInfo(List<OracleColumnInfo> columns) {
        List<String> columnList = new ArrayList<>();
        for (OracleColumnInfo columnInfo : columns) {
            // Construct columns and partition columns
            StringBuilder columnBuilder = new StringBuilder();
            columnBuilder.append("\"")
                    .append(columnInfo.getName())
                    .append("\" ")
                    .append(columnInfo.getType());
            columnList.add(columnBuilder.toString());
        }
        return columnList;
    }

    private static List<String> getColumnsComment(String tableName, List<OracleColumnInfo> columns) {
        List<String> commentList = new ArrayList<>();
        for (OracleColumnInfo columnInfo : columns) {
            if (StringUtils.isNoneBlank(columnInfo.getComment())) {
                StringBuilder commSql = new StringBuilder();
                commSql.append("COMMENT ON COLUMN \"")
                        .append(tableName)
                        .append("\".\"")
                        .append(columnInfo.getName())
                        .append("\" IS '")
                        .append(columnInfo.getComment())
                        .append("' ");
                commentList.add(commSql.toString());
            }
        }
        return commentList;
    }

    /**
     * Build query table SQL.
     *
     * @param tableName Oracle table name
     * @return desc table SQL string
     */
    public static String buildDescTableSql(String tableName) {
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT A.COLUMN_NAME,A.DATA_TYPE,B.COMMENTS ")
                .append(" FROM USER_TAB_COLUMNS A LEFT JOIN USER_COL_COMMENTS B ")
                .append("  ON A.TABLE_NAME=B.TABLE_NAME AND A.COLUMN_NAME=B.COLUMN_NAME ")
                .append("WHERE  A.TABLE_NAME = '")
                .append(tableName)
                .append("'  ORDER  BY A.COLUMN_ID ");
        LOGGER.info("desc table sql={}", sql);
        return sql.toString();
    }

}
