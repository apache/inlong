package org.apache.inlong.sort.cdc.mysql.utils;

import static org.apache.inlong.sort.cdc.mysql.utils.MetaDataUtils.getSqlType;
import static org.apache.inlong.sort.ddl.Utils.ColumnUtils.getComment;
import static org.apache.inlong.sort.ddl.Utils.ColumnUtils.getDefaultValue;
import static org.apache.inlong.sort.ddl.Utils.ColumnUtils.getNullable;
import static org.apache.inlong.sort.ddl.Utils.ColumnUtils.getPosition;

import io.debezium.relational.history.TableChanges.TableChange;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.alter.Alter;
import net.sf.jsqlparser.statement.alter.AlterExpression;
import net.sf.jsqlparser.statement.alter.AlterExpression.ColumnDataType;
import net.sf.jsqlparser.statement.create.table.ColDataType;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import org.apache.inlong.sort.cdc.base.debezium.table.RowDataDebeziumDeserializeSchema;
import org.apache.inlong.sort.ddl.Column;
import org.apache.inlong.sort.ddl.Position;
import org.apache.inlong.sort.ddl.enums.AlterType;
import org.apache.inlong.sort.ddl.enums.IndexType;
import org.apache.inlong.sort.ddl.enums.PositionType;
import org.apache.inlong.sort.ddl.expressions.AlterColumn;
import org.apache.inlong.sort.ddl.indexes.Index;
import org.apache.inlong.sort.ddl.operations.AlterOperation;
import org.apache.inlong.sort.ddl.operations.CreateTableOperation;
import org.apache.inlong.sort.ddl.operations.Operation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class OperationUtils {

    public static final String FIRST = "FIRST";
    private static final Logger LOG = LoggerFactory.getLogger(RowDataDebeziumDeserializeSchema.class);

    public static Operation generateOperation(String sql, TableChange tableSchema) {
        try {

            boolean endsWithFirst = sql.endsWith(FIRST);
            if (endsWithFirst) {
                sql = sql.substring(0, sql.lastIndexOf(FIRST));
            }

            Statement statement = CCJSqlParserUtil.parse(sql);
            if (statement instanceof Alter) {
                return getAlterOperation(
                    (Alter) statement, tableSchema, endsWithFirst);
            } else if (statement instanceof CreateTable) {
                return getCreateTableOperation(
                    (CreateTable) statement, tableSchema);
            } else {
                LOG.warn("doesn't support sql {}, statement {}", sql, statement);
            }

        } catch (Exception e) {
            LOG.error("parse ddl error: {}， set ddl to null", sql, e);
        }
        return null;
    }


    private static AlterOperation getAlterOperation(Alter statement, TableChange tableSchema, boolean isFirst) {
        Map<String, Integer> sqlType = getSqlType(tableSchema);

        List<AlterColumn> alterColumns = new ArrayList<>();

        for (AlterExpression alterExpression : statement.getAlterExpressions()) {
            List<String> definitions = new ArrayList<>();
            ColumnDataType columnDataType = alterExpression.getColDataTypeList().get(0);
            ColDataType colDataType = columnDataType.getColDataType();
            if (colDataType.getArgumentsStringList() != null) {
                definitions.addAll(colDataType.getArgumentsStringList());
            }
            Column column;
            List<String> columnSpecs = columnDataType.getColumnSpecs();
            if (isFirst) {
                column = new Column(columnDataType.getColumnName(), definitions,
                    sqlType.get(columnDataType.getColumnName()),
                    new Position(PositionType.FIRST, null), getNullable(columnSpecs), getDefaultValue(
                    columnSpecs),
                    getComment(columnSpecs));
            } else {
                column = new Column(columnDataType.getColumnName(), definitions,
                    sqlType.get(columnDataType.getColumnName()),
                    getPosition(columnSpecs), getNullable(columnSpecs), getDefaultValue(
                    columnSpecs),
                    getComment(columnSpecs));
            }

            alterColumns.add(new AlterColumn(AlterType.ADD_COLUMN, column, null));
        }

        return new AlterOperation(alterColumns);
    }


    private static CreateTableOperation getCreateTableOperation(
        CreateTable statement, TableChange tableSchema) {
        Map<String, Integer> sqlType = getSqlType(tableSchema);

        CreateTableOperation createTableOperation = new CreateTableOperation();

        if (statement.getLikeTable() != null) {
            createTableOperation.setLikeTable(statement.getLikeTable().getName());
        }

        List<ColumnDefinition> columnDefinitions = statement.getColumnDefinitions();
        List<Column> columns = new ArrayList<>();
        for (ColumnDefinition columnDefinition : columnDefinitions) {

            List<String> definitions = new ArrayList<>();
            ColDataType colDataType = columnDefinition.getColDataType();
            if (colDataType.getArgumentsStringList() != null) {
                definitions.addAll(colDataType.getArgumentsStringList());
            }

            Column column = new Column(columnDefinition.getColumnName(), definitions,
                sqlType.get(columnDefinition.getColumnName()),
                null, getNullable(definitions), getDefaultValue(definitions), getComment(definitions));
            columns.add(column);
        }

        createTableOperation.setColumns(columns);

        if (statement.getIndexes() != null) {
            createTableOperation.setIndexes(statement.getIndexes().stream().map(index -> {
                Index index1 = new Index();
                index1.setIndexName(index.getName());
                if (Objects.equals(index.getType(), "PRIMARY KEY")) {
                    index1.setIndexType(IndexType.PRIMARY_KEY);
                } else if (Objects.equals(index.getType(), "NORMAL_INDEX")) {
                    index1.setIndexType(IndexType.NORMAL_INDEX);
                }
                index1.setIndexColumns(index.getColumnsNames());
                return index1;
            }).collect(Collectors.toList()));
        }
        return createTableOperation;
    }

}
