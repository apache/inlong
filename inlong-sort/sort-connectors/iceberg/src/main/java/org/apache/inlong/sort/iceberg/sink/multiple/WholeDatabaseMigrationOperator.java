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

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
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
import org.apache.iceberg.types.Types.NestedField;
import org.apache.inlong.sort.base.format.AbstractDynamicSchemaFormat;
import org.apache.inlong.sort.base.format.DynamicSchemaFormatFactory;
import org.apache.inlong.sort.base.sink.MultipleSinkOption;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

public class WholeDatabaseMigrationOperator extends AbstractStreamOperator<RecordWithSchema>
        implements OneInputStreamOperator<RowData, RecordWithSchema> {

    private final ObjectMapper objectMapper = new ObjectMapper();

    private final CatalogLoader catalogLoader;
    private final MultipleSinkOption multipleSinkOption;

    private transient Catalog catalog;
    private transient SupportsNamespaces asNamespaceCatalog;
    private transient AbstractDynamicSchemaFormat<JsonNode> dynamicSchemaFormat;

    // record cache, wait schema to consume record
    private transient Map<TableIdentifier, Queue<RecordWithSchema>> recordQueues;

    // schema cache
    private transient Map<TableIdentifier, Schema> schemaCache;

    public WholeDatabaseMigrationOperator(CatalogLoader catalogLoader,
            MultipleSinkOption multipleSinkOption) {
        this.catalogLoader = catalogLoader;
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

        JsonNode jsonNode = objectMapper.readTree(wholeData);
        boolean isDDL = dynamicSchemaFormat.extractDDLFlag(jsonNode);
        if (isDDL) {
            execDDL(jsonNode);
        } else {
            execDML(jsonNode);
        }
    }

    private void execDDL(JsonNode jsonNode) {
        // todo:parse ddl sql
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
                RecordWithSchema recordWithSchema = queue.poll();
                output.collect(new StreamRecord<>(
                        recordWithSchema
                                .refreshFieldId(currentSchema)
                                .refreshRowData((jsonNode, schema1) ->
                                     dynamicSchemaFormat.extractRowData(jsonNode, FlinkSchemaUtil.convert(schema1))
                                )));
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
            // 设置了这个属性自动建表后hive才能查询到
            properties.put("engine.hive.enabled", "true");

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
        List<String> pkListStr = dynamicSchemaFormat.extractPrimaryKeyNames(data);
        RowType schema = dynamicSchemaFormat.extractSchema(data, pkListStr);

        RecordWithSchema record = new RecordWithSchema(
                data,
                FlinkSchemaUtil.convert(FlinkSchemaUtil.toSchema(schema)),
                TableIdentifier.of(databaseStr, tableStr),
                pkListStr);
        return record;
    }
}
