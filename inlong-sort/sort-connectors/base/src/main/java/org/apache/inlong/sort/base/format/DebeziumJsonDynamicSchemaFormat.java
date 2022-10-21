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

package org.apache.inlong.sort.base.format;

import org.apache.flink.formats.json.JsonToRowDataConverters.JsonToRowDataConverter;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Debezium json dynamic format
 */
public class DebeziumJsonDynamicSchemaFormat extends JsonDynamicSchemaFormat {

    private static final String IDENTIFIER = "debezium-json";
    private static final String DDL_FLAG = "ddl";
    private static final String SCHEMA = "sqlType";
    private static final String AFTER = "after";
    private static final String BEFORE = "before";
    private static final String SOURCE = "source";
    private static final String PK_NAMES = "pkNames";
    private static final String OP_TYPE = "op";
    private static final String PAYLOAD = "payload";
    /**
     * Snapshot read
     */
    private static final String OP_READ = "r";
    /**
     * Insert
     */
    private static final String OP_CREATE = "c";
    /**
     * Update
     */
    private static final String OP_UPDATE = "u";
    /**
     * Delete
     */
    private static final String OP_DELETE = "d";


    private static final DebeziumJsonDynamicSchemaFormat FORMAT = new DebeziumJsonDynamicSchemaFormat();

    private DebeziumJsonDynamicSchemaFormat() {

    }

    @SuppressWarnings("rawtypes")
    public static AbstractDynamicSchemaFormat getInstance() {
        return FORMAT;
    }

    @Override
    public JsonNode getPhysicalData(JsonNode root) {
        JsonNode payload = root.get(PAYLOAD);
        if (payload == null) {
            JsonNode physicalData = root.get(AFTER);
            if (physicalData == null) {
                physicalData = root.get(BEFORE);
            }
            return physicalData;
        }
        return getPhysicalData(payload);
    }

    @Override
    public List<String> extractPrimaryKeyNames(JsonNode data) {
        List<String> pkNames = new ArrayList<>();
        JsonNode payload = data.get(PAYLOAD);
        if (payload == null) {
            JsonNode sourceNode = data.get(SOURCE);
            if (sourceNode == null) {
                return pkNames;
            }
            JsonNode pkNamesNode = sourceNode.get(PK_NAMES);
            if (pkNamesNode != null && pkNamesNode.isArray()) {
                for (int i = 0; i < pkNamesNode.size(); i++) {
                    pkNames.add(pkNamesNode.get(i).asText());
                }
            }
            return pkNames;
        }
        return extractPrimaryKeyNames(payload);
    }

    @Override
    public String parse(JsonNode rootNode, String pattern) throws IOException {
        JsonNode payload = rootNode.get(PAYLOAD);
        if (payload == null) {
            return super.parse(rootNode, pattern);
        }
        return super.parse(payload, pattern);
    }

    @Override
    public boolean extractDDLFlag(JsonNode data) {
        JsonNode payload = data.get(PAYLOAD);
        if (payload == null) {
            return data.has(DDL_FLAG) && data.get(DDL_FLAG).asBoolean(false);
        }
        return extractDDLFlag(payload);
    }

    @Override
    public RowType extractSchema(JsonNode data, List<String> pkNames) {
        JsonNode schema = data.get(SCHEMA);
        return extractSchemaNode(schema, pkNames);
    }

    @Override
    public List<RowData> extractRowData(JsonNode data, RowType rowType) {
        JsonNode payload = data.get(PAYLOAD);
        if (payload == null) {
            JsonNode opNode = data.get(OP_TYPE);
            JsonNode dataBeforeNode = data.get(BEFORE);
            JsonNode dataAfterNode = data.get(AFTER);
            if (opNode == null || (dataBeforeNode == null && dataAfterNode == null)) {
                throw new IllegalArgumentException(
                        String.format("Error opNode: %s, or dataBeforeNode: %s, dataAfterNode: %s",
                                opNode, dataBeforeNode, dataAfterNode));
            }
            List<RowData> rowDataList = new ArrayList<>();
            JsonToRowDataConverter rowDataConverter = rowDataConverters.createConverter(rowType);

            String op = data.get(OP_TYPE).asText();
            if (OP_CREATE.equals(op) || OP_READ.equals(op)) {
                RowData rowData = (RowData) rowDataConverter.convert(dataAfterNode);
                rowData.setRowKind(RowKind.INSERT);
                rowDataList.add(rowData);
            } else if (OP_UPDATE.equals(op)) {
                RowData rowData = (RowData) rowDataConverter.convert(dataBeforeNode);
                rowData.setRowKind(RowKind.UPDATE_BEFORE);
                rowDataList.add(rowData);
                rowData = (RowData) rowDataConverter.convert(dataAfterNode);
                rowData.setRowKind(RowKind.UPDATE_AFTER);
                rowDataList.add(rowData);
            } else if (OP_DELETE.equals(op)) {
                RowData rowData = (RowData) rowDataConverter.convert(dataBeforeNode);
                rowData.setRowKind(RowKind.DELETE);
                rowDataList.add(rowData);
            } else {
                throw new IllegalArgumentException("Unsupported op_type: " + op);
            }
            return rowDataList;
        }
        return extractRowData(payload, rowType);
    }

    /**
     * Get the identifier of this dynamic schema format
     *
     * @return The identifier of this dynamic schema format
     */
    @Override
    public String identifier() {
        return IDENTIFIER;
    }
}
