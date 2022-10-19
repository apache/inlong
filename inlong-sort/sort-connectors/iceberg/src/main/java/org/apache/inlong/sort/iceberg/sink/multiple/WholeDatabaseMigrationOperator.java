/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements. See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License. You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package org.apache.inlong.sort.iceberg.sink.multiple;

import org.apache.flink.formats.json.JsonToRowDataConverters;
import org.apache.flink.formats.json.JsonToRowDataConverters.JsonToRowDataConverter;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.types.RowKind;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.inlong.sort.base.format.AbstractDynamicSchemaFormat;
import org.apache.inlong.sort.base.format.DynamicSchemaFormatFactory;
import org.apache.inlong.sort.base.format.JsonDynamicSchemaFormat;
import org.apache.inlong.sort.base.sink.MultipleSinkOption;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class WholeDatabaseMigrationOperator extends AbstractStreamOperator<RecordWithSchema>
        implements OneInputStreamOperator<RowData, RecordWithSchema> {

    private final ObjectMapper objectMapper = new ObjectMapper();

    // todo:这里是根据SqlType转换成Flink的Type再转换成Iceberg的Type，所以最终解析出来的Schema可能和表中的Schema在精度上有误差
    private static final Map<Integer, Type> SQL_TYPE_2_ICEBERG_TYPE_MAPPING =
            ImmutableMap.<Integer, Type>builder()
                    .put(java.sql.Types.CHAR, Types.StringType.get())
                    .put(java.sql.Types.VARCHAR, Types.StringType.get())
                    .put(java.sql.Types.SMALLINT, Types.IntegerType.get())
                    .put(java.sql.Types.INTEGER, Types.IntegerType.get())
                    .put(java.sql.Types.BIGINT, Types.LongType.get())
                    .put(java.sql.Types.REAL, Types.FloatType.get())
                    .put(java.sql.Types.DOUBLE, Types.DoubleType.get())
                    .put(java.sql.Types.FLOAT, Types.FloatType.get())
                    .put(java.sql.Types.DECIMAL,
                            Types.DecimalType.of(DecimalType.DEFAULT_PRECISION, DecimalType.DEFAULT_SCALE))
                    .put(java.sql.Types.NUMERIC,
                            Types.DecimalType.of(DecimalType.DEFAULT_PRECISION, DecimalType.DEFAULT_SCALE))
                    .put(java.sql.Types.BIT, Types.IntegerType.get())
                    .put(java.sql.Types.TIME, Types.TimeType.get())
                    .put(java.sql.Types.TIMESTAMP_WITH_TIMEZONE, Types.TimestampType.withoutZone())
                    .put(java.sql.Types.TIMESTAMP, Types.TimestampType.withZone())
                    .put(java.sql.Types.BINARY, Types.FixedType.ofLength(BinaryType.DEFAULT_LENGTH))
                    .put(java.sql.Types.VARBINARY, Types.BinaryType.get())
                    .put(java.sql.Types.BLOB, Types.BinaryType.get())
                    .put(java.sql.Types.DATE, Types.DateType.get())
                    .put(java.sql.Types.BOOLEAN, Types.BooleanType.get())
                    .put(java.sql.Types.OTHER, Types.StringType.get())
                    .build();
    private static final String OP_INSERT = "INSERT";
    private static final String OP_UPDATE = "UPDATE";
    private static final String OP_DELETE = "DELETE";
    private static final String OP_CREATE = "CREATE";

    private final CatalogLoader catalogLoader;
    private final JsonToRowDataConverters rowDataConverters;
    private final MultipleSinkOption multipleSinkOption;

    private transient Catalog catalog;
    private transient SupportsNamespaces asNamespaceCatalog;
    private transient AbstractDynamicSchemaFormat<JsonNode> dynamicSchemaFormat;

    // record cache, wait schema to consume record
    private transient Map<TableIdentifier, Queue<RecordWithSchema>> recordQueues;

    // schema cache
    private transient Map<TableIdentifier, Schema> schemaCache;

    public WholeDatabaseMigrationOperator(CatalogLoader catalogLoader,
            JsonToRowDataConverters rowDataConverters,
            MultipleSinkOption multipleSinkOption) {
        this.catalogLoader = catalogLoader;
        this.rowDataConverters = rowDataConverters;
        this.multipleSinkOption = multipleSinkOption;
    }

    @Override
    public void open() throws Exception {
        super.open();
        this.catalog = catalogLoader.loadCatalog();
        this.asNamespaceCatalog =
                catalog instanceof SupportsNamespaces ? (SupportsNamespaces) catalog : null;
        this.recordQueues = new HashMap<>();
        this.schemaCache = new HashMap<>();
        this.dynamicSchemaFormat = DynamicSchemaFormatFactory.getFormat(multipleSinkOption.getFormat());
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (catalog instanceof Closeable) {
            ((Closeable) catalog).close();
        }
    }

    @Override
    public void processElement(StreamRecord<RowData> element) throws Exception {
        String wholeData = element.getValue().getString(0).toString();

        // 将sql语句切换成DML和DDL语句，对于DML转发给下游，对于DDL用插件的方式提交
        JsonNode jsonNode = objectMapper.readTree(wholeData);
        boolean isDDL = jsonNode.get("ddl").asBoolean();
        if (isDDL) {
            execDDL(jsonNode);
        } else {
            execDML(jsonNode);
        }
    }

    private void execDDL(JsonNode jsonNode) {
    }

    private void execDML(JsonNode jsonNode) throws IOException {
        RecordWithSchema record = parseRecord(jsonNode);
        Schema schema = schemaCache.get(record.getTableId());
        Schema dataSchema = record.getSchema();
        recordQueues.compute(record.getTableId(), (k, v) -> {
            if (v == null) {
                v = new LinkedList<>();
            }
            v.add(record);
            return v;
        });

        if (schema == null) {
            handleTableCreateEventFromOperator(record.getTableId(), dataSchema);
        } else {
            handleSchemaInfoEvent(record.getTableId(), schema);
        }
    }


    // ================================ 所有的与coordinator交互的request和response方法 ============================
    private void handleSchemaInfoEvent(TableIdentifier tableId, Schema schema) {
        schemaCache.put(tableId, schema);
        Schema currentSchema = schemaCache.get(tableId);
        Queue<RecordWithSchema> queue = recordQueues.get(tableId);
        while (queue != null && !queue.isEmpty()) {
            Schema dataSchema = queue.peek().getSchema();
            // if compatible, this means that the current schema is the latest schema
            // if not, prove the need to update the current schema
            if (isCompatible(currentSchema, dataSchema)) {
                // todo:后续这里的解析替换成canalJson解析或者DebeuizmJson解析
                RecordWithSchema recordWithSchema = queue.poll();
                recordWithSchema.refreshFieldId(currentSchema);
                List<RecordWithSchema> records = recordWithSchema.refreshRowData((JsonNode dataStr, Schema schema1) -> {
                    JsonToRowDataConverter rowDataConverter = rowDataConverters.createConverter(FlinkSchemaUtil.convert(schema1));  // todo:这个地方每次都新建conveter挺耗性能的
                    return (RowData) rowDataConverter.convert(dataStr);
                });
                for (RecordWithSchema record : records) {
                    output.collect(new StreamRecord<>(record));
                }
            } else {
                handldAlterSchemaEventFromOperator(tableId, currentSchema, dataSchema);
            }
        }
    }

    // ================================ 所有的coordinator处理的方法 ==============================================
    private void handleTableCreateEventFromOperator(TableIdentifier tableId, Schema schema) {
        if (!catalog.tableExists(tableId)) {
            if (asNamespaceCatalog != null && !asNamespaceCatalog.namespaceExists(tableId.namespace())) {
                try {
                    asNamespaceCatalog.createNamespace(tableId.namespace());
                    LOG.info("Auto create Database({}) in Catalog({}).", tableId.namespace(), catalog.name());
                } catch (AlreadyExistsException e) {
                    LOG.warn("Database({}) already exist in Catalog({})!", tableId.namespace(), catalog.name());
                }
            }

            ImmutableMap.Builder<String, String> properties = ImmutableMap.builder();
            properties.put("format-version", "2"); // todo:后续考虑默认参数给哪些，并且将这个默认参数暴露在表参数上
            properties.put("write.upsert.enabled", "true");

            try {
                catalog.createTable(tableId, schema, PartitionSpec.unpartitioned(), properties.build());
                LOG.info("Auto create Table({}) in Database({}) in Catalog({})!",
                        tableId.name(), tableId.namespace(), catalog.name());
            } catch (AlreadyExistsException e) {
                LOG.warn("Table({}) already exist in Database({}) in Catalog({})!",
                        tableId.name(), tableId.namespace(), catalog.name());
            }
        }

        handleSchemaInfoEvent(tableId, catalog.loadTable(tableId).schema());
    }

    private void handldAlterSchemaEventFromOperator(TableIdentifier tableId, Schema oldSchema, Schema newSchema) {
        Table table = catalog.loadTable(tableId);

        // The transactionality of changes is guaranteed by comparing the old schema with the current schema of the
        // table.
        // Judging whether changes can be made by schema comparison (currently only column additions are supported),
        // for scenarios that cannot be changed, it is always considered that there is a problem with the data.
        Transaction transaction = table.newTransaction();
        if (table.schema().sameSchema(oldSchema)) {
            List<TableChange> tableChanges = TableChange.diffSchema(oldSchema, newSchema);
            TableChange.applySchemaChanges(transaction.updateSchema(), tableChanges);
            LOG.info("Schema evolution in table({}) for table change: {}", tableId, tableChanges);
        }
        transaction.commitTransaction();
        handleSchemaInfoEvent(tableId, table.schema());
    }

    // =============================== 工具方法 =================================================================
    // The way to judge compatibility is whether all the field names in the old schema exist in the new schema
    private boolean isCompatible(Schema newSchema, Schema oldSchema) {
        for (NestedField field : oldSchema.columns()) {
            if (newSchema.findField(field.name()) == null) {
                return false;
            }
        }
        return true;
    }

    // 从数据中解析schema信息并转换成为flink内置的schema,对不同的格式（canal-json、ogg）以插件接口的方式提供这个转换方式
    private RecordWithSchema parseRecord(JsonNode data) throws IOException {
        String databaseStr = dynamicSchemaFormat.parse(data, multipleSinkOption.getDatabasePattern());
        String tableStr = dynamicSchemaFormat.parse(data, multipleSinkOption.getTablePattern());
        String op = data.get("type").asText();
        JsonNode schemaStr = data.get("sqlType");
        JsonNode dataStr = data.get("data");
        List<String> pkListStr = dynamicSchemaFormat.extractPrimaryKeyNames(data);

        // parse schema, primary key
        List<Integer> pks = new ArrayList<>();
        List<NestedField> fields = new ArrayList<>();
        int index = 0;
        Iterator<Entry<String, JsonNode>> schemaFields = schemaStr.fields();
        while (schemaFields.hasNext()) {
            Entry<String, JsonNode> entry = schemaFields.next();
            String name = entry.getKey();
            Type type = sqlType2IcebergType(entry.getValue().asInt());

            boolean isOptional = true;
            if (pkListStr.contains(name)) {
                pks.add(index);
                isOptional = false;
            }

            fields.add(NestedField.of(index++, isOptional, name, type));
        }
        Schema schema = new Schema(fields);
        RecordWithSchema record = new RecordWithSchema(
                dataStr, parseRowKind(op), schema, TableIdentifier.of(databaseStr, tableStr), pks);
        return record;
    }

    private RowKind parseRowKind(String op) {
        if (OP_INSERT.equals(op)) {
            return RowKind.INSERT;
        } else if (OP_UPDATE.equals(op)) {
            return RowKind.UPDATE_AFTER;
        } else if (OP_DELETE.equals(op)) {
            return RowKind.DELETE;
        } else {
            throw new IllegalArgumentException("Unsupported op_type: " + op);
        }
    }

    private Type sqlType2IcebergType(int jdbcType) {
        if (SQL_TYPE_2_ICEBERG_TYPE_MAPPING.containsKey(jdbcType)) {
            return SQL_TYPE_2_ICEBERG_TYPE_MAPPING.get(jdbcType);
        } else {
            throw new IllegalArgumentException("Unsupported jdbcType: " + jdbcType);
        }
    }
}
