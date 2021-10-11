/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.inlong.sort.flink.clickhouse;

import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Collectors;

public class ClickHouseStatementFactory {

    public static String getInsertIntoStatement(String tableName, String[] fieldNames) {
        String columns = Arrays.stream(fieldNames).map(ClickHouseStatementFactory::quoteIdentifier)
                .collect(Collectors.joining(", "));
        String placeholders = Arrays.stream(fieldNames).map(f -> "?").collect(Collectors.joining(", "));
        return "INSERT INTO " + quoteIdentifier(tableName) + "(" + columns + ") VALUES (" + placeholders + ")";
    }

    public static String getUpdateStatement(
            String tableName,
            String[] fieldNames,
            String[] conditionFields,
            Optional<String> clusterName) {
        String setClause = Arrays.stream(fieldNames).map(f -> quoteIdentifier(f) + "=?")
                .collect(Collectors.joining(", "));
        String conditionClause = Arrays.stream(conditionFields).map(f -> quoteIdentifier(f) + "=?")
                .collect(Collectors.joining(" AND "));
        String onClusterClause = "";
        if (clusterName.isPresent()) {
            onClusterClause = " ON CLUSTER " + quoteIdentifier(clusterName.get());
        }
        return "ALTER TABLE " + quoteIdentifier(tableName) + onClusterClause
                + " UPDATE " + setClause + " WHERE " + conditionClause;
    }

    public static String getDeleteStatement(
            String tableName,
            String[] conditionFields,
            Optional<String> clusterName) {
        String conditionClause = Arrays.stream(conditionFields).map(f -> quoteIdentifier(f) + "=?")
                .collect(Collectors.joining(" AND "));
        String onClusterClause = "";
        if (clusterName.isPresent()) {
            onClusterClause = " ON CLUSTER " + quoteIdentifier(clusterName.get());
        }
        return "ALTER TABLE " + quoteIdentifier(tableName) + onClusterClause + " DELETE WHERE " + conditionClause;
    }

    public static String getRowExistsStatement(String tableName, String[] conditionFields) {
        String fieldExpressions = Arrays.<String>stream(conditionFields).map(f -> quoteIdentifier(f) + "=?")
                .collect(Collectors.joining(" AND "));
        return "SELECT 1 FROM " + quoteIdentifier(tableName) + " WHERE " + fieldExpressions;
    }

    public static String quoteIdentifier(String identifier) {
        return "`" + identifier + "`";
    }
}
