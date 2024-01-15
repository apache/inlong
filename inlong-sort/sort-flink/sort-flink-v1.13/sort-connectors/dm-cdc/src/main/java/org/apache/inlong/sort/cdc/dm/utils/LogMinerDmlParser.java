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

package org.apache.inlong.sort.cdc.dm.utils;

import io.debezium.DebeziumException;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *  copied from Debezium LogMiner DMLParser and modified locally to parse incoming sql.
 *  the original oracle redo log has many slight differences (like how quotes and white spaces are used) from dm,
 *  so this part is very error-prone and modified heavily.
 */
@Slf4j
public class LogMinerDmlParser {

    private static final String NULL_SENTINEL = "${DBZ_NULL}";
    private static final String NULL = "NULL";
    private static final String INSERT_INTO = "INSERT INTO ";
    private static final String UPDATE = "UPDATE ";
    private static final String DELETE_FROM = "DELETE FROM ";
    private static final String AND = "AND ";
    private static final String OR = "OR ";
    private static final String SET = " SET ";
    private static final String WHERE = " WHERE ";
    private static final String VALUES = " VALUES";
    private static final String IS_NULL = "IS NULL";
    // Use by Oracle for specific data types that cannot be represented in SQL
    private static final String UNSUPPORTED = "Unsupported";
    private static final String UNSUPPORTED_TYPE = "Unsupported Type";

    private static final int INSERT_INTO_LENGTH = INSERT_INTO.length();
    private static final int UPDATE_LENGTH = UPDATE.length();
    private static final int DELETE_FROM_LENGTH = DELETE_FROM.length();
    private static final int VALUES_LENGTH = VALUES.length();
    private static final int SET_LENGTH = SET.length();
    private static final int WHERE_LENGTH = WHERE.length();

    private final List<String> columns;

    public LogMinerDmlParser(List<String> columns) {
        log.info("parse columns: " + columns);
        this.columns = columns;
    }

    public List<String> getColumns() {
        return this.columns;
    }

    public List<Map<String, Object>> parse(String sql, String operation) {
        log.debug("sql parser incoming sql is: " + sql);
        try {
            switch (operation) {
                case "INSERT":
                    return parseInsert(sql, columns);
                case "DELETE":
                    return parseDelete(sql, columns);
                case "UPDATE":
                    return parseUpdate(sql, columns);
                default:
                    throw new RuntimeException("operation type is not supported " + operation);
            }
        } catch (Throwable e) {
            log.error("dm sql parse field error ", e);
        }
        return null;
    }

    /**
     * Parse an {@code INSERT} SQL statement.
     */
    public List<Map<String, Object>> parseInsert(String sql, List<String> columns) {
        List<Map<String, Object>> result = new ArrayList<>();
        result.add(new HashMap<>());
        try {
            Map<String, Object> after = new HashMap<>();
            result.add(after);
            // advance beyond "insert into "
            int index = INSERT_INTO_LENGTH;
            // parse table
            index = parseTableName(sql, index);
            // capture column names
            String[] columnNames = new String[columns.size()];
            index = parseColumnListClause(sql, index, columnNames);
            // capture values
            Object[] newValues = new Object[columns.size()];
            parseColumnValuesClause(sql, index, columnNames, newValues, Arrays.asList(columnNames));

            for (int i = 0; i < columns.size(); i++) {
                after.put(columnNames[i], newValues[i]);
            }

            return result;
        } catch (Exception e) {
            throw new RuntimeException("Failed to parse insert DML: '" + sql + "'", e);
        }
    }

    /**
     * Parse an {@code UPDATE} SQL statement.
     */
    public List<Map<String, Object>> parseUpdate(String sql, List<String> columns) {
        List<Map<String, Object>> result = new ArrayList<>();
        try {
            Map<String, Object> before = new HashMap<>();
            Map<String, Object> after = new HashMap<>();
            result.add(before);
            result.add(after);
            // advance beyond "update "
            int index = UPDATE_LENGTH;

            // parse table
            index = parseTableName(sql, index);

            // parse set
            Object[] newValues = new Object[columns.size()];
            index = parseSetClause(sql, index, newValues, columns);

            // parse where
            Object[] oldValues = new Object[columns.size()];
            parseWhereClause(sql, index, oldValues, columns);

            // For each after state field that is either a NULL_SENTINEL (explicitly wants NULL) or
            // that wasn't specified and therefore remained null, correctly adapt the after state
            // accordingly, leaving any field's after value alone if it isn't null or a sentinel.
            for (int i = 0; i < oldValues.length; ++i) {
                if (newValues[i] == NULL_SENTINEL) {
                    // field is explicitly set to NULL, clear the sentinel and continue
                    newValues[i] = null;
                } else if (newValues[i] == null) {
                    // field wasn't specified in set-clause, copy before state to after state
                    newValues[i] = oldValues[i];
                }

                String columnName = columns.get(i);
                before.put(columnName, oldValues[i]);
                after.put(columnName, newValues[i]);
            }

            return result;
        } catch (Exception e) {
            throw new RuntimeException("Failed to parse update DML: '" + sql + "'", e);
        }
    }

    /**
     * Parses a SQL {@code DELETE} statement.
     */
    public List<Map<String, Object>> parseDelete(String sql, List<String> columns) {
        List<Map<String, Object>> result = new ArrayList<>();
        Map<String, Object> before = new HashMap<>();
        result.add(before);
        result.add(new HashMap<>());
        try {
            // advance beyond "delete from "
            int index = DELETE_FROM_LENGTH;

            // parse table
            index = parseTableName(sql, index);

            // parse where
            Object[] oldValues = new Object[columns.size()];
            parseWhereClause(sql, index, oldValues, columns);

            for (int i = 0; i < columns.size(); i++) {
                before.put(columns.get(i), oldValues[i]);
            }
            return result;
        } catch (Exception e) {
            throw new RuntimeException("Failed to parse delete DML: '" + sql + "'", e);
        }
    }

    /**
     * Parses a table-name in the SQL clause
     *
     * @param sql the sql statement
     * @param index the index into the sql statement to begin parsing
     * @return the index into the sql string where the table name ended
     */
    private static int parseTableName(String sql, int index) {
        boolean inQuote = false;

        for (; index < sql.length(); ++index) {
            char c = sql.charAt(index);
            if (c == '"') {
                if (inQuote) {
                    inQuote = false;
                    continue;
                }
                inQuote = true;
            } else if ((c == ' ' || c == '(') && !inQuote) {
                break;
            }
        }

        return index;
    }

    /**
     * Parse an {@code INSERT} statement's column-list clause.
     *
     * @param sql the sql statement
     * @param start the index into the sql statement to begin parsing
     * @param columnNames the list that will be populated with the column names
     * @return the index into the sql string where the column-list clause ended
     */
    private static int parseColumnListClause(String sql, int start, String[] columnNames) {
        int index = start;
        boolean inQuote = false;
        int columnIndex = 0;
        for (; index < sql.length(); ++index) {
            char c = sql.charAt(index);
            if (c == '(' && !inQuote) {
                start = index + 1;
            } else if (c == ')' && !inQuote) {
                index++;
                break;
            } else if (c == '\"') {
                if (inQuote) {
                    inQuote = false;
                    // remove the double quotes
                    columnNames[columnIndex++] = sql.substring(start + 1, index).replace("\"", "");
                    start = index + 2;
                    continue;
                }
                inQuote = true;
            }
        }
        return index;
    }

    /**
     * Parse an {@code INSERT} statement's column-values clause.
     *
     * @param sql the sql statement
     * @param start the index into the sql statement to begin parsing
     * @param columnNames the column names array, already indexed based on relational table column order
     * @param values the values array that will be populated with column values
     * @return the index into the sql string where the column-values clause ended
     */
    private int parseColumnValuesClause(String sql, int start, String[] columnNames, Object[] values,
            List<String> columns) {
        int index = start;
        int nested = 0;
        boolean inQuote = false;
        boolean inValues = false;

        int value = sql.indexOf(VALUES, index);
        if (value == -1) {
            throw new DebeziumException(
                    "parse value clause failed to parse DML: " + sql + start + sql.indexOf(VALUES, index));
        } else if (value != index) {
            // find table alias
            index = value;
        }

        index += VALUES_LENGTH;

        int columnIndex = 0;
        for (; index < sql.length(); ++index) {
            char c = sql.charAt(index);
            if (c == '(' && !inQuote && !inValues) {
                inValues = true;
                start = index + 1;
            } else if (c == '(' && !inQuote) {
                nested++;
            } else if (c == '\"') {
                if (inQuote) {
                    inQuote = false;
                    continue;
                }
                inQuote = true;
            } else if (!inQuote && (c == ',' || c == ')')) {
                if (c == ')' && nested != 0) {
                    nested--;
                    continue;
                }
                if (c == ',' && nested != 0) {
                    continue;
                }

                if (sql.charAt(start) == '\"' && sql.charAt(index - 1) == '\"') {
                    log.info("quoted strings");
                    // value is single-quoted at the start/end, substring without the quotes.
                    int position = getColumnIndexByName(columnNames[columnIndex], columns);
                    if (position == 0) {
                        values[position] = sql.substring(start + 1, index - 1);
                    } else if (position > 0) {
                        values[position] = sql.substring(start + 2, index - 1);
                    }
                } else {
                    // use value as-is
                    String s = sql.substring(start, index);
                    if (!s.equals(UNSUPPORTED_TYPE) && !s.equals(NULL)) {
                        int position = getColumnIndexByName(columnNames[columnIndex], columns);
                        // log.info("print debug index: " + position + columnNames[columnIndex]);
                        // remove the leading whitespace, except for the first one
                        if (position == 0) {
                            values[position] = s;
                        } else {
                            String strValue = s.substring(1);
                            values[position] = strValue;
                        }
                    }
                }

                columnIndex++;
                start = index + 1;
            }
        }

        return index;
    }

    private int getColumnIndexByName(String columnName, List<String> columns) {
        int index = columns.indexOf(columnName);
        if (index == -1) {
            throw new RuntimeException(("failed to get column index by name:" + columnName + ", " + columns));
        }
        return index;
    }

    /**
     * Parse an {@code UPDATE} statement's {@code SET} clause.
     *
     * @param sql the sql statement
     * @param start the index into the sql statement to begin parsing
     * @param newValues the new values array to be populated
     * @return the index into the sql string where the set-clause ended
     */
    private int parseSetClause(String sql, int start, Object[] newValues, List<String> columns) {
        boolean inDoubleQuote = false;
        boolean inSingleQuote = false;
        boolean inColumnName = true;
        boolean inColumnValue = false;
        boolean inSpecial = false;
        int nested = 0;

        // verify entering set-clause
        int set = sql.indexOf(SET, start);
        if (set == -1) {
            throw new DebeziumException(
                    "parse set clause failed to parse DML: " + sql + start + sql.indexOf(SET, start));
        } else if (set != start) {
            // find table alias
            start = set;
        }
        start += SET_LENGTH;

        int index = start;
        String currentColumnName = null;
        for (; index < sql.length(); ++index) {
            char c = sql.charAt(index);
            char lookAhead = (index + 1 < sql.length()) ? sql.charAt(index + 1) : 0;
            if (c == '"' && inColumnName) {
                // Set clause column names are double-quoted
                if (inDoubleQuote) {
                    inDoubleQuote = false;
                    currentColumnName = sql.substring(start + 1, index);
                    start = index + 1;
                    inColumnName = false;
                    continue;
                }
                inDoubleQuote = true;
                start = index;
            } else if (c == '=' && !inColumnName && !inColumnValue) {
                inColumnValue = true;
                // Oracle SQL generated is always ' = ', skipping following space
                index += 1;
                start = index + 1;
            } else if (nested == 0 & c == ' ' && lookAhead == '|') {
                // Possible concatenation, nothing to do yet
            } else if (nested == 0 & c == '|' && lookAhead == '|') {
                // Concatenation
                for (int i = index + 2; i < sql.length(); ++i) {
                    if (sql.charAt(i) != ' ') {
                        // found next non-whitespace character
                        index = i - 1;
                        break;
                    }
                }
            } else if (c == '\'' && inColumnValue) {
                // Skip over double single quote
                if (inSingleQuote && lookAhead == '\'') {
                    index += 1;
                    continue;
                }
                // Set clause single-quoted column value
                if (inSingleQuote) {
                    inSingleQuote = false;
                    if (nested == 0) {
                        int position = getColumnIndexByName(currentColumnName, columns);
                        newValues[position] = sql.substring(start + 1, index);
                        start = index + 1;
                        inColumnValue = false;
                        inColumnName = false;
                    }
                    continue;
                }
                if (!inSpecial) {
                    start = index;
                }
                inSingleQuote = true;
            } else if (c == ',' && !inColumnValue && !inColumnName) {
                // Set clause uses ', ' skip following space
                inColumnName = true;
                index += 1;
                start = index;
            } else if (inColumnValue && !inSingleQuote) {
                if (!inSpecial) {
                    start = index;
                    inSpecial = true;
                }
                // characters as a part of the value
                if (c == '(') {
                    nested++;
                } else if (c == ')' && nested > 0) {
                    nested--;
                } else if ((c == ',' || c == ' ' || c == ';') && nested == 0) {
                    String value = sql.substring(start, index);
                    if (value.equals(NULL) || value.equals(UNSUPPORTED_TYPE)) {
                        if (value.equals(NULL)) {
                            // In order to identify when a field is not present in the set-clause or when
                            // a field is explicitly set to null, the NULL_SENTINEL value is used to then
                            // indicate that the field is explicitly being cleared to NULL.
                            // This sentinel value will be cleared later when we reconcile before/after
                            // state in parseUpdate()
                            int position = getColumnIndexByName(currentColumnName, columns);
                            newValues[position] = NULL_SENTINEL;
                        }
                        start = index + 1;
                        inColumnValue = false;
                        inSpecial = false;
                        inColumnName = true;
                        continue;
                    } else if (value.equals(UNSUPPORTED)) {
                        continue;
                    }
                    int position = getColumnIndexByName(currentColumnName, columns);
                    newValues[position] = value;
                    start = index + 1;
                    inColumnValue = false;
                    inSpecial = false;
                    inColumnName = true;
                }
            } else if (!inDoubleQuote && !inSingleQuote) {
                if (c == 'W' && lookAhead == 'H' && sql.indexOf(WHERE, index - 1) == index - 1) {
                    index -= 1;
                    break;
                }
            }
        }

        return index;
    }

    /**
     * Parses a {@code WHERE} clause populates the provided column names and values arrays.
     *
     * @param sql the sql statement
     * @param start the index into the sql statement to begin parsing
     * @param values the column values to be parsed from the where clause
     * @return the index into the sql string to continue parsing
     */
    private int parseWhereClause(String sql, int start, Object[] values, List<String> columns) {
        int nested = 0;
        boolean inColumnName = true;
        boolean inColumnValue = false;
        boolean inDoubleQuote = false;
        boolean inSingleQuote = false;
        boolean inSpecial = false;

        // DBZ-3235
        // LogMiner can generate SQL without a WHERE condition under some circumstances and if it does
        // we shouldn't immediately fail DML parsing.
        if (start >= sql.length()) {
            return start;
        }

        // verify entering where-clause
        int where = sql.indexOf(WHERE, start);
        if (where == -1) {
            throw new DebeziumException("Failed to parse DML: " + sql);
        } else if (where != start) {
            // find table alias
            start = where;
        }
        start += WHERE_LENGTH;

        int index = start;
        String currentColumnName = null;
        for (; index < sql.length(); ++index) {
            char c = sql.charAt(index);
            char lookAhead = (index + 1 < sql.length()) ? sql.charAt(index + 1) : 0;
            if (c == '"' && inColumnName) {
                // Where clause column names are double-quoted
                if (inDoubleQuote) {
                    inDoubleQuote = false;
                    currentColumnName = sql.substring(start + 1, index);
                    start = index + 1;
                    inColumnName = false;
                    continue;
                }
                inDoubleQuote = true;
                start = index;
            } else if (c == '=' && !inColumnName && !inColumnValue) {
                inColumnValue = true;
                // Oracle SQL generated is always ' = ', skipping following space
                index += 1;
                start = index + 1;
            } else if (c == 'I' && !inColumnName && !inColumnValue) {
                if (sql.indexOf(IS_NULL, index) == index) {
                    index += 6;
                    start = index;
                    continue;
                }
            } else if (c == '\'' && inColumnValue) {
                // Skip over double single quote
                if (inSingleQuote && lookAhead == '\'') {
                    index += 1;
                    continue;
                }
                // Where clause single-quoted column value
                if (inSingleQuote) {
                    inSingleQuote = false;
                    if (nested == 0) {
                        int position = getColumnIndexByName(currentColumnName, columns);
                        values[position] = sql.substring(start + 1, index);
                        start = index + 1;
                        inColumnValue = false;
                        inColumnName = false;
                    }
                    continue;
                }
                if (!inSpecial) {
                    start = index;
                }
                inSingleQuote = true;
            } else if (inColumnValue && !inSingleQuote) {
                if (!inSpecial) {
                    start = index;
                    inSpecial = true;
                }
                if (c == '(') {
                    nested++;
                } else if (c == ')' && nested > 0) {
                    nested--;
                } else if (nested == 0 & c == ' ' && lookAhead == '|') {
                    // Possible concatenation, nothing to do yet
                } else if (nested == 0 & c == '|' && lookAhead == '|') {
                    // Concatenation
                    for (int i = index + 2; i < sql.length(); ++i) {
                        if (sql.charAt(i) != ' ') {
                            // found next non-whitespace character
                            index = i - 1;
                            break;
                        }
                    }
                } else if ((c == ';' || c == ' ') && nested == 0) {
                    String value = sql.substring(start, index);
                    if (value.equals(NULL) || value.equals(UNSUPPORTED_TYPE)) {
                        start = index + 1;
                        inColumnValue = false;
                        inSpecial = false;
                        inColumnName = true;
                        continue;
                    } else if (value.equals(UNSUPPORTED)) {
                        continue;
                    }
                    int position = getColumnIndexByName(currentColumnName, columns);
                    values[position] = value;
                    start = index + 1;
                    inColumnValue = false;
                    inSpecial = false;
                    inColumnName = true;
                }
            } else if (!inColumnValue && !inColumnName) {
                if (c == 'A' && lookAhead == 'N' && sql.indexOf(AND, index) == index) {
                    index += 3;
                    start = index;
                    inColumnName = true;
                } else if (c == 'O' && lookAhead == 'R' && sql.indexOf(OR, index) == index) {
                    index += 2;
                    start = index;
                    inColumnName = true;
                }
            }
        }

        return index;
    }
}
