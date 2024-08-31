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

package org.apache.inlong.sort.jdbc.dialect.jdbc;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.connector.jdbc.converter.JdbcRowConverter;
import org.apache.flink.connector.jdbc.dialect.AbstractDialect;
import org.apache.flink.connector.jdbc.internal.converter.MySQLRowConverter;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static java.lang.String.format;

/**
 * JDBC dialect for SQL.
 */
public class JdbcDialect extends AbstractDialect {

    public static final Logger LOG = LoggerFactory.getLogger(JdbcDialect.class);

    private static final int MAX_TIMESTAMP_PRECISION = 8;
    private static final int MIN_TIMESTAMP_PRECISION = 0;

    private static final int MAX_DECIMAL_PRECISION = 128;
    private static final int MIN_DECIMAL_PRECISION = 32;
    private static final String POINT = ".";

    @Override
    public String dialectName() {
        return "mysql";
    }

    @Override
    public JdbcRowConverter getRowConverter(RowType rowType) {
        return new MySQLRowConverter(rowType);
    }

    @Override
    public String getLimitClause(long limit) {
        return "LIMIT " + limit;
    }

    @Override
    public Optional<String> defaultDriverName() {
        return Optional.of("com.mysql.cj.jdbc.Driver");
    }

    @Override
    public String quoteIdentifier(String identifier) {
        return "`" + identifier + "`";
    }

    @Override
    public Optional<String> getUpsertStatement(String tableName, String[] fieldNames, String[] uniqueKeyFields) {
        return Optional.empty();
    }

    @Override
    public Optional<Range> decimalPrecisionRange() {
        return Optional.of(Range.of(MIN_DECIMAL_PRECISION, MAX_DECIMAL_PRECISION));
    }

    /**
     * Defines the supported types for the dialect.
     *
     * @return a set of logical type roots.
     */
    @Override
    public Set<LogicalTypeRoot> supportedTypes() {
        return new HashSet<LogicalTypeRoot>() {

            {
                add(LogicalTypeRoot.CHAR);
                add(LogicalTypeRoot.VARCHAR);
                add(LogicalTypeRoot.BOOLEAN);
                add(LogicalTypeRoot.DECIMAL);
                add(LogicalTypeRoot.TINYINT);
                add(LogicalTypeRoot.SMALLINT);
                add(LogicalTypeRoot.INTEGER);
                add(LogicalTypeRoot.BIGINT);
                add(LogicalTypeRoot.FLOAT);
                add(LogicalTypeRoot.DOUBLE);
                add(LogicalTypeRoot.DATE);
                add(LogicalTypeRoot.TIME_WITHOUT_TIME_ZONE);
                add(LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE);
            }
        };

    }

    @Override
    public Optional<Range> timestampPrecisionRange() {
        return Optional.of(Range.of(MIN_TIMESTAMP_PRECISION, MAX_TIMESTAMP_PRECISION));
    }

    /**
     * Get update one row statement by condition fields
     */
    @Override
    public String getUpdateStatement(
            String tableName, String[] fieldNames, String[] conditionFields) {
        List<String> conditionFieldList = Arrays.asList(conditionFields);
        String setClause =
                Arrays.stream(fieldNames)
                        .filter(fieldName -> !conditionFieldList.contains(fieldName))
                        .map(f -> format("%s = :%s", quoteIdentifier(f), f))
                        .collect(Collectors.joining(", "));

        String conditionClause =
                Arrays.stream(conditionFields)
                        .map(f -> format("%s = :%s", quoteIdentifier(f), f))
                        .collect(Collectors.joining(" AND "));
        Pair<String, String> databaseAndTableName = getDatabaseAndTableName(tableName);
        return "ALTER TABLE "
                + quoteIdentifier(databaseAndTableName.getLeft())
                + POINT
                + quoteIdentifier(databaseAndTableName.getRight())
                + " UPDATE "
                + setClause
                + " WHERE "
                + conditionClause;
    }

    /**
     * mysql throw exception "Table default.test_user doesn't exist". But jdbc-url have database name.
     * So we specify database when exec query. This method parse tableName to database and table.
     * @param tableName include database.table
     * @return pair left is database, right is table
     */
    private Pair<String, String> getDatabaseAndTableName(String tableName) {
        String databaseName = "default";
        if (tableName.contains(POINT)) {
            String[] tableNameArray = tableName.split("\\.");
            databaseName = tableNameArray[0];
            tableName = tableNameArray[1];
        } else {
            LOG.warn("TableName doesn't include database name, so using default as database name");
        }
        return Pair.of(databaseName, tableName);
    }

    /**
     * Get delete one row statement by condition fields
     */
    @Override
    public String getDeleteStatement(String tableName, String[] conditionFields) {
        String conditionClause =
                Arrays.stream(conditionFields)
                        .map(f -> format("%s = :%s", quoteIdentifier(f), f))
                        .collect(Collectors.joining(" AND "));
        Pair<String, String> databaseAndTableName = getDatabaseAndTableName(tableName);
        return "ALTER TABLE "
                + quoteIdentifier(databaseAndTableName.getLeft())
                + POINT
                + quoteIdentifier(databaseAndTableName.getRight())
                + " DELETE WHERE " + conditionClause;
    }

    @Override
    public String getInsertIntoStatement(String tableName, String[] fieldNames) {
        String columns =
                Arrays.stream(fieldNames)
                        .map(this::quoteIdentifier)
                        .collect(Collectors.joining(", "));
        String placeholders =
                Arrays.stream(fieldNames).map(f -> ":" + f).collect(Collectors.joining(", "));
        Pair<String, String> databaseAndTableName = getDatabaseAndTableName(tableName);
        return "INSERT INTO "
                + quoteIdentifier(databaseAndTableName.getLeft())
                + POINT
                + quoteIdentifier(databaseAndTableName.getRight())
                + "("
                + columns
                + ")"
                + " VALUES ("
                + placeholders
                + ")";
    }

    @Override
    public String getSelectFromStatement(
            String tableName, String[] selectFields, String[] conditionFields) {
        String selectExpressions =
                Arrays.stream(selectFields)
                        .map(this::quoteIdentifier)
                        .collect(Collectors.joining(", "));
        String fieldExpressions =
                Arrays.stream(conditionFields)
                        .map(f -> format("%s = :%s", quoteIdentifier(f), f))
                        .collect(Collectors.joining(" AND "));
        Pair<String, String> databaseAndTableName = getDatabaseAndTableName(tableName);
        return "SELECT "
                + selectExpressions
                + " FROM "
                + quoteIdentifier(databaseAndTableName.getLeft())
                + POINT
                + quoteIdentifier(databaseAndTableName.getRight())
                + (conditionFields.length > 0 ? " WHERE " + fieldExpressions : "");
    }

    @Override
    public String getRowExistsStatement(String tableName, String[] conditionFields) {
        String fieldExpressions =
                Arrays.stream(conditionFields)
                        .map(f -> format("%s = :%s", quoteIdentifier(f), f))
                        .collect(Collectors.joining(" AND "));
        Pair<String, String> pair = getDatabaseAndTableName(tableName);
        return "SELECT 1 FROM "
                + quoteIdentifier(pair.getLeft())
                + POINT
                + quoteIdentifier(pair.getRight())
                + " WHERE " + fieldExpressions;
    }

}
