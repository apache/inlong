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

package org.apache.inlong.sort.util;

import org.apache.inlong.sort.protocol.ddl.expressions.AlterColumn;
import org.apache.inlong.sort.protocol.ddl.expressions.Column;
import org.apache.inlong.sort.protocol.ddl.operations.AlterOperation;
import org.apache.inlong.sort.protocol.ddl.operations.Operation;
import org.apache.inlong.sort.protocol.enums.SchemaChangePolicy;
import org.apache.inlong.sort.protocol.enums.SchemaChangeType;

import com.google.common.base.Preconditions;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.StringJoiner;

/**
 * Schema-change Utils
 */
public final class SchemaChangeUtils {

    private final static String DELIMITER = "&";
    private final static String KEY_VALUE_DELIMITER = "=";

    private SchemaChangeUtils() {
    }

    /**
     * deserialize the policies to a Map[{@link SchemaChangeType}, {@link SchemaChangePolicy}]
     *
     * @param policies The policies format by 'key1=value1&key2=value2...'
     * @return A policy Map[{@link SchemaChangeType}, {@link SchemaChangePolicy}]
     */
    public static Map<SchemaChangeType, SchemaChangePolicy> deserialize(String policies) {
        Preconditions.checkNotNull(policies, "policies is null");
        Map<SchemaChangeType, SchemaChangePolicy> policyMap = new HashMap<>();
        for (String kv : policies.split(DELIMITER)) {
            int index = kv.indexOf(KEY_VALUE_DELIMITER);
            if (index < 1 || index == kv.length() - 1) {
                throw new IllegalArgumentException(
                        "The format of policies must be like 'key1=value1&key2=value2...'");
            }
            String typeCode = kv.substring(0, index);
            String policyCode = kv.substring(index + 1);
            SchemaChangeType type;
            SchemaChangePolicy policy;
            try {
                type = SchemaChangeType.getInstance(Integer.parseInt(typeCode));
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(
                        String.format("Unsupported type of schema-change: %s for InLong", typeCode));
            }
            try {
                policy = SchemaChangePolicy.getInstance(Integer.parseInt(policyCode));
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(
                        String.format("Unsupported policy of schema-change: %s for InLong", policyCode));
            }
            policyMap.put(type, policy);
        }
        return policyMap;
    }

    /**
     * Serialize the policy Map[{@link SchemaChangeType}, {@link SchemaChangePolicy}] to a string
     *
     * @param policyMap The policy Map[{@link SchemaChangeType}, {@link SchemaChangePolicy}]
     * @return A string format by 'key1=value1&key2=value2...'
     */
    public static String serialize(Map<SchemaChangeType, SchemaChangePolicy> policyMap) {
        Preconditions.checkNotNull(policyMap, "policyMap is null");
        StringJoiner joiner = new StringJoiner(DELIMITER);
        for (Entry<SchemaChangeType, SchemaChangePolicy> kv : policyMap.entrySet()) {
            joiner.add(kv.getKey().getCode() + KEY_VALUE_DELIMITER + kv.getValue().getCode());
        }
        return joiner.toString();
    }

    /**
     * Extract the schema change types from {@link Operation}
     *
     * @param operation The operation
     * @return Set of {@link SchemaChangeType}
     */
    public static Set<SchemaChangeType> extractSchemaChangeTypes(Operation operation) {
        Set<SchemaChangeType> types = new HashSet<>();
        switch (operation.getOperationType()) {
            case ALTER:
                AlterOperation alterOperation = (AlterOperation) operation;
                Preconditions.checkState(alterOperation.getAlterColumns() != null
                        && !alterOperation.getAlterColumns().isEmpty(), "alter columns is empty");
                for (AlterColumn alterColumn : alterOperation.getAlterColumns()) {
                    extractSchemaChangeType(alterColumn, types);
                }
                break;
            case CREATE:
                types.add(SchemaChangeType.CREATE_TABLE);
                break;
            case TRUNCATE:
                types.add(SchemaChangeType.TRUNCATE_TABLE);
                break;
            case RENAME:
                types.add(SchemaChangeType.RENAME_TABLE);
                break;
            case DROP:
                types.add(SchemaChangeType.DROP_TABLE);
            default:
        }
        return types;
    }

    /**
     * Extract the schema change type from {@link Operation}
     *
     * @param operation The operation
     * @return A type of {@link SchemaChangeType}
     */
    public static SchemaChangeType extractSchemaChangeType(Operation operation) {
        SchemaChangeType type = null;
        switch (operation.getOperationType()) {
            case ALTER:
                return SchemaChangeType.ALTER;
            case CREATE:
                type = SchemaChangeType.CREATE_TABLE;
                break;
            case TRUNCATE:
                type = SchemaChangeType.TRUNCATE_TABLE;
                break;
            case RENAME:
                type = SchemaChangeType.RENAME_TABLE;
                break;
            case DROP:
                type = SchemaChangeType.DROP_TABLE;
            default:
        }
        return type;
    }

    /**
     * Extract the schema change types from {@link AlterColumn}
     *
     * @param alterColumn The alterColumn
     * @return Set of {@link SchemaChangeType}
     */
    public static Set<SchemaChangeType> extractSchemaChangeType(AlterColumn alterColumn) {
        return extractSchemaChangeType(alterColumn, new HashSet<>());
    }

    /**
     * Extract the schema change types from {@link AlterColumn}
     *
     * @param alterColumn The alterColumn
     * @param types The types
     * @return Set of {@link SchemaChangeType}
     */
    public static Set<SchemaChangeType> extractSchemaChangeType(AlterColumn alterColumn, Set<SchemaChangeType> types) {
        if (types == null) {
            types = new HashSet<>();
        }
        switch (alterColumn.getAlterType()) {
            case ADD_COLUMN:
                types.add(SchemaChangeType.ADD_COLUMN);
                break;
            case DROP_COLUMN:
                types.add(SchemaChangeType.DROP_COLUMN);
                break;
            case RENAME_COLUMN:
                types.add(SchemaChangeType.RENAME_COLUMN);
                break;
            case CHANGE_COLUMN:
                parseTypeOfChangeColumn(alterColumn, types);
                break;
            default:
        }
        return types;
    }

    /**
     * Parse the schema change type from {@link AlterColumn}
     * It is used in the scenario of modifying the column, there is a modified column ddl to
     * implement multiple column change scenarios, such as modifying the column name and column type at the same time,
     * we need to parse the specific type.
     *
     * @param alterColumn The AlterColumn
     */
    private static void parseTypeOfChangeColumn(AlterColumn alterColumn, Set<SchemaChangeType> types) {
        Preconditions.checkNotNull(alterColumn.getNewColumn(), "The new column is null");
        Column newColumn = alterColumn.getNewColumn();
        Column oldColumn = alterColumn.getOldColumn();
        Preconditions.checkState(isNotEmpty(newColumn), "The new column name is blank");
        if (isNotEmpty(oldColumn) && !oldColumn.getName().equals(newColumn.getName())) {
            types.add(SchemaChangeType.RENAME_COLUMN);
        } else {
            types.add(SchemaChangeType.CHANGE_COLUMN_TYPE);
        }
    }

    /**
     * Check if column is empty
     *
     * @param column The column {@link Column}
     * @return true if not empty else false
     */
    public static boolean isNotEmpty(Column column) {
        if (column == null || column.getName() == null) {
            return false;
        }
        return !column.getName().trim().isEmpty();
    }
}
