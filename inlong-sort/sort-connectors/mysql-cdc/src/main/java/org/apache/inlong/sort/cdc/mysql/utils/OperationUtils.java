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

package org.apache.inlong.sort.cdc.mysql.utils;

import static org.apache.inlong.sort.cdc.mysql.utils.MetaDataUtils.getSqlType;
import static org.apache.inlong.sort.ddl.Utils.ColumnUtils.parseColumnWithPosition;
import static org.apache.inlong.sort.ddl.Utils.ColumnUtils.parseColumns;
import static org.apache.inlong.sort.ddl.Utils.ColumnUtils.parseComment;
import static org.apache.inlong.sort.ddl.Utils.ColumnUtils.removeContinuousBackQuotes;

import io.debezium.relational.history.TableChanges.TableChange;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.alter.Alter;
import net.sf.jsqlparser.statement.alter.RenameTableStatement;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.drop.Drop;
import net.sf.jsqlparser.statement.truncate.Truncate;
import org.apache.inlong.sort.cdc.base.debezium.table.RowDataDebeziumDeserializeSchema;
import org.apache.inlong.sort.ddl.enums.AlterType;
import org.apache.inlong.sort.ddl.enums.IndexType;
import org.apache.inlong.sort.ddl.expressions.AlterColumn;
import org.apache.inlong.sort.ddl.indexes.Index;
import org.apache.inlong.sort.ddl.operations.AlterOperation;
import org.apache.inlong.sort.ddl.operations.CreateTableOperation;
import org.apache.inlong.sort.ddl.operations.DropTableOperation;
import org.apache.inlong.sort.ddl.operations.Operation;
import org.apache.inlong.sort.ddl.operations.RenameTableOperation;
import org.apache.inlong.sort.ddl.operations.TruncateTableOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Utils for generate operation from statement from sqlParser.
 */
public class OperationUtils {

    private static final Logger LOG = LoggerFactory.getLogger(RowDataDebeziumDeserializeSchema.class);
    public static final String PRIMARY_KEY = "PRIMARY KEY";
    public static final String NORMAL_INDEX = "NORMAL_INDEX";
    public static final String FIRST = "FIRST";

    /**
     * generate operation from sql and table schema.
     * @param sql sql from binlog
     * @param tableSchema table schema
     * @return Operation
     */
    public static Operation generateOperation(String sql, TableChange tableSchema) {
        try {
            // now sqlParser don't support FIRST position
            // remove it first and add it later
            boolean endsWithFirst = sql.endsWith(FIRST);
            if (endsWithFirst) {
                sql = removeFirstFlag(sql);
            }
            Statement statement = CCJSqlParserUtil.parse(sql);
            if (statement instanceof Alter) {
                return parseAlterOperation(
                    (Alter) statement, tableSchema, endsWithFirst);
            } else if (statement instanceof CreateTable) {
                return parseCreateTableOperation(
                    (CreateTable) statement, tableSchema);
            } else if (statement instanceof Drop) {
                return new DropTableOperation();
            } else if (statement instanceof Truncate) {
                return new TruncateTableOperation();
            } else if (statement instanceof RenameTableStatement) {
                return new RenameTableOperation();
            }
            else {
                LOG.warn("doesn't support sql {}, statement {}", sql, statement);
            }
        } catch (Exception e) {
            LOG.error("parse ddl error: {}， set ddl to null", sql, e);
        }
        return null;
    }

    /**
     * parse alter operation from Alter from sqlParser.
     * @param statement alter statement
     * @param tableSchema table schema
     * @param isFirst whether the column is first
     * @return AlterOperation
     */
    private static AlterOperation parseAlterOperation(Alter statement,
        TableChange tableSchema, boolean isFirst) {

        Map<String, Integer> sqlType = getSqlType(tableSchema);
        List<AlterColumn> alterColumns = new ArrayList<>();

        statement.getAlterExpressions().forEach(alterExpression -> {
            alterColumns.add(new AlterColumn(AlterType.ADD_COLUMN,
                parseColumnWithPosition(isFirst, sqlType,
                    alterExpression.getColDataTypeList().get(0)), null));
        });

        return new AlterOperation(alterColumns);
    }


    /**
     * parse create table operation from CreateTable from sqlParser.
     * @param statement create table statement
     * @param tableSchema table schema
     * @return CreateTableOperation
     */
    private static CreateTableOperation parseCreateTableOperation(
        CreateTable statement, TableChange tableSchema) {

        Map<String, Integer> sqlType = getSqlType(tableSchema);
        CreateTableOperation createTableOperation = new CreateTableOperation();
        List<ColumnDefinition> columnDefinitions = statement.getColumnDefinitions();

        if (statement.getLikeTable() != null) {
            createTableOperation.setLikeTable(parseLikeTable(statement));
            return createTableOperation;
        }

        createTableOperation.setColumns(parseColumns(sqlType, columnDefinitions));
        createTableOperation.setIndexes(parseIndexes(statement));
        createTableOperation.setComment(parseComment(statement.getTableOptionsStrings()));
        return createTableOperation;
    }

    /**
     * parse indexes from statement
     * only support primary key and normal index.
     * @param statement create table statement
     * @return list of indexes
     */
    private static List<Index> parseIndexes(CreateTable statement) {

        if (statement.getIndexes() == null) {
            return new ArrayList<>();
        }
        List<Index> indexList = new ArrayList<>();

        for (net.sf.jsqlparser.statement.create.table.Index perIndex : statement.getIndexes()) {
            Index index = new Index();
            switch (perIndex.getType()) {
                case PRIMARY_KEY:
                    index.setIndexType(IndexType.PRIMARY_KEY);
                    break;
                case NORMAL_INDEX:
                    index.setIndexType(IndexType.NORMAL_INDEX);
                    break;
                default:
                    LOG.error("unsupported index type {}", perIndex.getType());
                    break;
            }
            List<String> columns = new ArrayList<>();
            perIndex.getColumnsNames().forEach(columnName ->
                columns.add(removeContinuousBackQuotes(columnName)));
            index.setIndexName(removeContinuousBackQuotes(perIndex.getName()));
            index.setIndexColumns(columns);
            indexList.add(index);
        }

        return indexList;

    }

    /**
     * remove the first flag from sql.
     * @param sql sql from binlog
     * @return sql without first flag
     */
    private static String removeFirstFlag(String sql) {
        return sql.substring(0, sql.lastIndexOf(FIRST));
    }

    /**
     * get like table from statement.
     * @param statement create table statement
     * @return like table name
     */
    private static String parseLikeTable(CreateTable statement) {
        if (statement.getLikeTable() != null) {
            return statement.getLikeTable().getName();
        }
        return "";
    }

}
