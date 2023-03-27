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

package org.apache.inlong.sort.cdc.mysql.table;

import static org.apache.inlong.sort.base.Constants.DDL_FIELD_NAME;
import static org.apache.inlong.sort.ddl.Utils.ColumnUtils.getComment;
import static org.apache.inlong.sort.ddl.Utils.ColumnUtils.getDefaultValue;
import static org.apache.inlong.sort.ddl.Utils.ColumnUtils.getNullable;
import static org.apache.inlong.sort.ddl.Utils.ColumnUtils.getPosition;

import io.debezium.connector.AbstractSourceInfo;
import io.debezium.data.Envelope;
import io.debezium.data.Envelope.FieldName;
import io.debezium.relational.Table;
import io.debezium.relational.TableSchema;
import io.debezium.relational.history.TableChanges;
import io.debezium.relational.history.TableChanges.TableChange;
import java.util.LinkedHashMap;
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
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.DataType;
import org.apache.inlong.sort.cdc.base.debezium.table.MetadataConverter;
import org.apache.inlong.sort.cdc.base.util.RecordUtils;
import org.apache.inlong.sort.ddl.Column;
import org.apache.inlong.sort.ddl.Position;
import org.apache.inlong.sort.ddl.enums.AlterType;
import org.apache.inlong.sort.ddl.enums.IndexType;
import org.apache.inlong.sort.ddl.enums.PositionType;
import org.apache.inlong.sort.ddl.expressions.AlterColumn;
import org.apache.inlong.sort.ddl.indexes.Index;
import org.apache.inlong.sort.ddl.operations.AlterOperation;
import org.apache.inlong.sort.ddl.operations.CreateTableOperation;
import org.apache.inlong.sort.formats.json.canal.CanalJson;
import org.apache.inlong.sort.formats.json.debezium.DebeziumJson;
import org.apache.inlong.sort.formats.json.debezium.DebeziumJson.Source;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Defines the supported metadata columns for {@link MySqlTableSource}.
 */
public enum MySqlReadableMetadata {


    /**
     * Name of the table that contain the row.
     */
    TABLE_NAME(
            "table_name",
            DataTypes.STRING().notNull(),
            new MetadataConverter() {

                private static final long serialVersionUID = 1L;

                @Override
                public Object read(SourceRecord record) {
                    return StringData.fromString(getMetaData(record, AbstractSourceInfo.TABLE_NAME_KEY));
                }
            }),

    /**
     * Name of the database that contain the row.
     */
    DATABASE_NAME(
            "database_name",
            DataTypes.STRING().notNull(),
            new MetadataConverter() {

                private static final long serialVersionUID = 1L;

                @Override
                public Object read(SourceRecord record) {
                    return StringData.fromString(getMetaData(record, AbstractSourceInfo.DATABASE_NAME_KEY));
                }
            }),

    /**
     * It indicates the time that the change was made in the database. If the record is read from
     * snapshot of the table instead of the binlog, the value is always 0.
     */
    OP_TS(
            "op_ts",
            DataTypes.TIMESTAMP_LTZ(3).notNull(),
            new MetadataConverter() {

                private static final long serialVersionUID = 1L;

                @Override
                public Object read(SourceRecord record) {
                    Struct messageStruct = (Struct) record.value();
                    Struct sourceStruct = messageStruct.getStruct(FieldName.SOURCE);
                    return TimestampData.fromEpochMillis(
                            (Long) sourceStruct.get(AbstractSourceInfo.TIMESTAMP_KEY));
                }
            }),

    DATA_DEFAULT(
            "meta.data",
            DataTypes.STRING(),
            new MetadataConverter() {

                private static final long serialVersionUID = 1L;

                @Override
                public Object read(SourceRecord record) {
                    return null;
                }

                @Override
                public Object read(SourceRecord record,
                        @Nullable TableChanges.TableChange tableSchema, RowData rowData) {
                    // construct canal json
                    return getCanalData(record, (GenericRowData) rowData, tableSchema);
                }
            }),

    DATA(
            "meta.data_canal",
            DataTypes.STRING(),
            new MetadataConverter() {

                private static final long serialVersionUID = 1L;

                @Override
                public Object read(SourceRecord record) {
                    return null;
                }

                @Override
                public Object read(SourceRecord record,
                        @Nullable TableChanges.TableChange tableSchema, RowData rowData) {
                    // construct canal json
                    return getCanalData(record, (GenericRowData) rowData, tableSchema);
                }
            }),

    DATA_DEBEZIUM(
            "meta.data_debezium",
            DataTypes.STRING(),
            new MetadataConverter() {

                private static final long serialVersionUID = 1L;

                @Override
                public Object read(SourceRecord record) {
                    return null;
                }

                @Override
                public Object read(SourceRecord record,
                        @Nullable TableChanges.TableChange tableSchema, RowData rowData) {
                    // construct debezium json
                    Struct messageStruct = (Struct) record.value();
                    Struct sourceStruct = messageStruct.getStruct(FieldName.SOURCE);
                    GenericRowData data = (GenericRowData) rowData;
                    Map<String, Object> field = (Map<String, Object>) data.getField(0);

                    Source source = Source.builder().db(getMetaData(record, AbstractSourceInfo.DATABASE_NAME_KEY))
                            .table(getMetaData(record, AbstractSourceInfo.TABLE_NAME_KEY))
                            .name(sourceStruct.getString(AbstractSourceInfo.SERVER_NAME_KEY))
                            .sqlType(getSqlType(tableSchema))
                            .pkNames(getPkNames(tableSchema))
                            .mysqlType(getMysqlType(tableSchema))
                            .build();
                    DebeziumJson debeziumJson = DebeziumJson.builder().after(field).source(source)
                            .tsMs(sourceStruct.getInt64(AbstractSourceInfo.TIMESTAMP_KEY)).op(getDebeziumOpType(data))
                            .tableChange(tableSchema).build();

                    try {
                        return StringData.fromString(OBJECT_MAPPER.writeValueAsString(debeziumJson));
                    } catch (Exception e) {
                        throw new IllegalStateException("exception occurs when get meta data", e);
                    }
                }
            }),

    /**
     * Name of the table that contain the row. .
     */
    META_TABLE_NAME(
            "meta.table_name",
            DataTypes.STRING().notNull(),
            new MetadataConverter() {

                private static final long serialVersionUID = 1L;

                @Override
                public Object read(SourceRecord record) {
                    return StringData.fromString(getMetaData(record, AbstractSourceInfo.TABLE_NAME_KEY));
                }
            }),

    /**
     * Name of the database that contain the row.
     */
    META_DATABASE_NAME(
            "meta.database_name",
            DataTypes.STRING().notNull(),
            new MetadataConverter() {

                private static final long serialVersionUID = 1L;

                @Override
                public Object read(SourceRecord record) {
                    return StringData.fromString(getMetaData(record, AbstractSourceInfo.DATABASE_NAME_KEY));
                }
            }),

    /**
     * It indicates the time that the change was made in the database. If the record is read from
     * snapshot of the table instead of the binlog, the value is always 0.
     */
    META_OP_TS(
            "meta.op_ts",
            DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3).notNull(),
            new MetadataConverter() {

                private static final long serialVersionUID = 1L;

                @Override
                public Object read(SourceRecord record) {
                    Struct messageStruct = (Struct) record.value();
                    Struct sourceStruct = messageStruct.getStruct(FieldName.SOURCE);
                    return TimestampData.fromEpochMillis(
                            (Long) sourceStruct.get(AbstractSourceInfo.TIMESTAMP_KEY));
                }
            }),

    /**
     * Operation type, INSERT/UPDATE/DELETE.
     */
    OP_TYPE(
            "meta.op_type",
            DataTypes.STRING().notNull(),
            new MetadataConverter() {

                private static final long serialVersionUID = 1L;

                @Override
                public Object read(SourceRecord record) {
                    return StringData.fromString(getOpType(record));
                }
            }),

    /**
     * Not important, a simple increment counter.
     */
    BATCH_ID(
            "meta.batch_id",
            DataTypes.BIGINT().nullable(),
            new MetadataConverter() {

                private static final long serialVersionUID = 1L;

                private long id = 0;

                @Override
                public Object read(SourceRecord record) {
                    return id++;
                }
            }),

    /**
     * Source does not emit ddl data.
     */
    IS_DDL(
            "meta.is_ddl",
            DataTypes.BOOLEAN().notNull(),
            new MetadataConverter() {

                private static final long serialVersionUID = 1L;

                @Override
                public Object read(SourceRecord record) {
                    return false;
                }
            }),

    /**
     * The update-before data for UPDATE record.
     */
    OLD(
            "meta.update_before",
            DataTypes.ARRAY(
                    DataTypes.MAP(
                            DataTypes.STRING().nullable(),
                            DataTypes.STRING().nullable())
                            .nullable())
                    .nullable(),
            new MetadataConverter() {

                private static final long serialVersionUID = 1L;

                @Override
                public Object read(SourceRecord record) {
                    final Envelope.Operation op = Envelope.operationFor(record);
                    if (op != Envelope.Operation.UPDATE) {
                        return null;
                    }
                    return record;
                }
            }),

    MYSQL_TYPE(
            "meta.mysql_type",
            DataTypes.MAP(DataTypes.STRING().nullable(), DataTypes.STRING().nullable()).nullable(),
            new MetadataConverter() {

                private static final long serialVersionUID = 1L;

                @Override
                public Object read(SourceRecord record) {
                    return null;
                }

                @Override
                public Object read(
                        SourceRecord record, @Nullable TableChanges.TableChange tableSchema) {
                    if (tableSchema == null) {
                        return null;
                    }
                    Map<StringData, StringData> mysqlType = new HashMap<>();
                    final Table table = tableSchema.getTable();
                    table.columns()
                            .forEach(
                                    column -> {
                                        mysqlType.put(
                                                StringData.fromString(column.name()),
                                                StringData.fromString(
                                                        String.format(
                                                                "%s(%d)",
                                                                column.typeName(),
                                                                column.length())));
                                    });

                    return new GenericMapData(mysqlType);
                }
            }),

    PK_NAMES(
            "meta.pk_names",
            DataTypes.ARRAY(DataTypes.STRING().nullable()).nullable(),
            new MetadataConverter() {

                private static final long serialVersionUID = 1L;

                @Override
                public Object read(SourceRecord record) {
                    return null;
                }

                @Override
                public Object read(
                        SourceRecord record, @Nullable TableChanges.TableChange tableSchema) {
                    if (tableSchema == null) {
                        return null;
                    }
                    return new GenericArrayData(
                            tableSchema.getTable().primaryKeyColumnNames().stream()
                                    .map(StringData::fromString)
                                    .toArray());
                }
            }),

    SQL(
            "meta.sql",
            DataTypes.STRING().nullable(),
            new MetadataConverter() {

                private static final long serialVersionUID = 1L;

                @Override
                public Object read(SourceRecord record) {
                    return StringData.fromString("");
                }
            }),

    SQL_TYPE(
            "meta.sql_type",
            DataTypes.MAP(DataTypes.STRING().nullable(), DataTypes.INT().nullable()).nullable(),
            new MetadataConverter() {

                private static final long serialVersionUID = 1L;

                @Override
                public Object read(SourceRecord record) {
                    return null;
                }

                @Override
                public Object read(
                        SourceRecord record, @Nullable TableChanges.TableChange tableSchema) {
                    if (tableSchema == null) {
                        return null;
                    }
                    Map<StringData, Integer> mysqlType = new HashMap<>();
                    final Table table = tableSchema.getTable();
                    table.columns()
                            .forEach(
                                    column -> {
                                        mysqlType.put(
                                                StringData.fromString(column.name()),
                                                column.jdbcType());
                                    });

                    return new GenericMapData(mysqlType);
                }
            }),

    TS(
            "meta.ts",
            DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3).notNull(),
            new MetadataConverter() {

                private static final long serialVersionUID = 1L;

                @Override
                public Object read(SourceRecord record) {
                    Struct messageStruct = (Struct) record.value();
                    return TimestampData.fromEpochMillis(
                            (Long) messageStruct.get(FieldName.TIMESTAMP));
                }
            });

    private static StringData getCanalData(SourceRecord record, GenericRowData rowData,
            TableChange tableSchema) {
        Struct messageStruct = (Struct) record.value();
        Struct sourceStruct = messageStruct.getStruct(FieldName.SOURCE);
        // tableName
        String tableName = getMetaData(record, AbstractSourceInfo.TABLE_NAME_KEY);
        // databaseName
        String databaseName = getMetaData(record, AbstractSourceInfo.DATABASE_NAME_KEY);
        // opTs
        long opTs = (Long) sourceStruct.get(AbstractSourceInfo.TIMESTAMP_KEY);
        // actual data
        GenericRowData data = rowData;
        Map<String, Object> field = (Map<String, Object>) data.getField(0);
        List<Map<String, Object>> dataList = new ArrayList<>();

        CanalJson canalJson = CanalJson.builder()
                .database(databaseName)
                .es(opTs).pkNames(getPkNames(tableSchema))
                .mysqlType(getMysqlType(tableSchema)).table(tableName)
                .type(getCanalOpType(rowData)).sqlType(getSqlType(tableSchema)).build();

        try {
            if (RecordUtils.isDdlRecord(messageStruct)) {
                String sql = (String) field.get(DDL_FIELD_NAME);
                canalJson.setSql(sql);
                injectStatement(sql, canalJson, tableSchema);
                canalJson.setDdl(true);
                canalJson.setData(dataList);
            } else {
                canalJson.setDdl(false);
                canalJson.setTs((Long) messageStruct.get(FieldName.TIMESTAMP));
                dataList.add(field);
                canalJson.setData(dataList);
            }
            return StringData.fromString(OBJECT_MAPPER.writeValueAsString(canalJson));
            //return StringData.fromString("NULL");
        } catch (Exception e) {
            throw new IllegalStateException("exception occurs when get meta data", e);
        }

    }

    private static void injectStatement(String sql, CanalJson canalJson, TableChange tableSchema) {
        try {

            boolean isFirst = sql.endsWith("FIRST");
            if (sql.endsWith("FIRST") ) {
                sql = sql.substring(0, sql.lastIndexOf("FIRST"));
            }
            Statement statement = CCJSqlParserUtil.parse(sql);
            if (statement instanceof Alter) {
                canalJson.setOperation(getAlterOperation(
                    (Alter) statement, tableSchema, isFirst));
            } if (statement instanceof CreateTable) {
                canalJson.setOperation(getCreateTableOperation(
                    (CreateTable) statement, tableSchema));
            }

        } catch (Exception e) {
            LOG.error("parse ddl error: {}， set ddl to null", sql, e);
        }
    }

    private static AlterOperation getAlterOperation(Alter statement, TableChange tableSchema, boolean isFirst) {
        Alter alter = statement;
        Map<String, Integer> sqlType = getSqlType(tableSchema);

        List<AlterColumn> alterColumns = new ArrayList<>();

        for (AlterExpression alterExpression : alter.getAlterExpressions()) {
            List<String> definitions = new ArrayList<>();
            ColumnDataType columnDataType = alterExpression.getColDataTypeList().get(0);
            ColDataType colDataType = columnDataType.getColDataType();
            if (colDataType.getArgumentsStringList() != null) {
                definitions.addAll(colDataType.getArgumentsStringList());
            }
            Column column;
            List<String> columnSpecs = columnDataType.getColumnSpecs();
            if (isFirst) {
                column = new Column(columnDataType.getColumnName(), definitions, sqlType.get(columnDataType.getColumnName()),
                    new Position(PositionType.FIRST, null), getNullable(columnSpecs), getDefaultValue(
                    columnSpecs), getComment(columnSpecs));
            } else {
                column = new Column(columnDataType.getColumnName(), definitions, sqlType.get(columnDataType.getColumnName()),
                    getPosition(columnSpecs), getNullable(columnSpecs), getDefaultValue(
                    columnSpecs), getComment(columnSpecs));
            }

            alterColumns.add(new AlterColumn(AlterType.ADD_COLUMN, column, null));
        }

        return new AlterOperation(alterColumns);
    }

    private static CreateTableOperation getCreateTableOperation(CreateTable statement, TableChange tableSchema) {
        CreateTable createTable = statement;
        Map<String, Integer> sqlType = getSqlType(tableSchema);

        CreateTableOperation createTableOperation = new CreateTableOperation();

        if (createTable.getLikeTable() != null) {
            createTableOperation.setLikeTable(createTable.getLikeTable().getName());
        }

        List<ColumnDefinition> columnDefinitions = createTable.getColumnDefinitions();
        List<Column> columns = new ArrayList<>();
        for (ColumnDefinition columnDefinition : columnDefinitions) {

            List<String> definitions = new ArrayList<>();
            ColDataType colDataType = columnDefinition.getColDataType();
            if (colDataType.getArgumentsStringList() != null) {
                definitions.addAll(colDataType.getArgumentsStringList());
            }

            Column column = new Column(columnDefinition.getColumnName(), definitions, sqlType.get(columnDefinition.getColumnName()),
                null, getNullable(definitions), getDefaultValue(definitions), getComment(definitions));
            columns.add(column);
        }

        createTableOperation.setColumns(columns);

        if (createTable.getIndexes() != null) {
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

    private final String key;
    private final DataType dataType;
    private final MetadataConverter converter;
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final Logger LOG = LoggerFactory.getLogger(MySqlReadableMetadata.class);

    MySqlReadableMetadata(String key, DataType dataType, MetadataConverter converter) {
        this.key = key;
        this.dataType = dataType;
        this.converter = converter;
    }

    private static String getOpType(SourceRecord record) {
        String opType;
        final Envelope.Operation op = Envelope.operationFor(record);
        if (op == Envelope.Operation.CREATE || op == Envelope.Operation.READ) {
            opType = "INSERT";
        } else if (op == Envelope.Operation.DELETE) {
            opType = "DELETE";
        } else {
            opType = "UPDATE";
        }
        return opType;
    }

    private static String getCanalOpType(GenericRowData record) {
        String opType;
        switch (record.getRowKind()) {
            case DELETE:
            case UPDATE_BEFORE:
                opType = "DELETE";
                break;
            case INSERT:
            case UPDATE_AFTER:
                opType = "INSERT";
                break;
            default:
                throw new IllegalStateException("the record only have states in DELETE, "
                    + "UPDATE_BEFORE, INSERT and UPDATE_AFTER");
        }
        return opType;
    }

    private static String getDebeziumOpType(GenericRowData record) {
        String opType;
        switch (record.getRowKind()) {
            case DELETE:
            case UPDATE_BEFORE:
                opType = "d";
                break;
            case INSERT:
            case UPDATE_AFTER:
                opType = "c";
                break;
            default:
                throw new IllegalStateException("the record only have states in DELETE, "
                        + "UPDATE_BEFORE, INSERT and UPDATE_AFTER");
        }
        return opType;
    }

    private static List<String> getPkNames(@Nullable TableChanges.TableChange tableSchema) {
        if (tableSchema == null) {
            return null;
        }
        return tableSchema.getTable().primaryKeyColumnNames();
    }

    public static Map<String, String> getMysqlType(@Nullable TableChanges.TableChange tableSchema) {
        if (tableSchema == null) {
            return null;
        }
        Map<String, String> mysqlType = new LinkedHashMap<>();
        final Table table = tableSchema.getTable();
        table.columns()
                .forEach(
                        column -> {
                            mysqlType.put(
                                    column.name(),
                                    String.format(
                                            "%s(%d)",
                                            column.typeName(),
                                            column.length()));
                        });
        return mysqlType;
    }

    /**
     * get sql type from table schema, represents the jdbc data type
     *
     * @param tableSchema table schema
     */
    public static Map<String, Integer> getSqlType(@Nullable TableChanges.TableChange tableSchema) {
        if (tableSchema == null) {
            return null;
        }
        Map<String, Integer> sqlType = new LinkedHashMap<>();
        final Table table = tableSchema.getTable();
        table.columns().forEach(
                column -> sqlType.put(column.name(), column.jdbcType()));
        return sqlType;
    }

    public static String getMetaData(SourceRecord record, String tableNameKey) {
        Struct messageStruct = (Struct) record.value();
        Struct sourceStruct = messageStruct.getStruct(FieldName.SOURCE);
        return sourceStruct.getString(tableNameKey);
    }

    public String getKey() {
        return key;
    }

    public DataType getDataType() {
        return dataType;
    }

    public MetadataConverter getConverter() {
        return converter;
    }
}
