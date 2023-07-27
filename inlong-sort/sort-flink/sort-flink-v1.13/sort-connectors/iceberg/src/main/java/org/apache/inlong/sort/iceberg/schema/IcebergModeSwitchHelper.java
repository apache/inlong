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

package org.apache.inlong.sort.iceberg.schema;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.RowType.RowField;

import java.util.List;

import static org.apache.flink.table.api.DataTypes.FIELD;
import static org.apache.flink.table.api.DataTypes.ROW;

/**
 * this class helps to manipulate iceberg table schema
 * when SWITCH_APPEND_UPSERT_ENABLE equals to true
 */
public class IcebergModeSwitchHelper {

    private final RowType tableSchemaRowType;
    private final RowDataConverter rowDataConverter;
    private final int incrementalFieldIndex;
    public static final String META_INCREMENTAL = "incremental_inlong";
    public static final int DEFAULT_META_INDEX = -1;

    public IcebergModeSwitchHelper(RowType tableSchemaRowType, int incrementalFieldIndex) {
        this.tableSchemaRowType = tableSchemaRowType;
        this.rowDataConverter = new RowDataConverter(tableSchemaRowType.getChildren());
        this.incrementalFieldIndex = incrementalFieldIndex;
    }

    /**
     * remove incremental field from rowData
     * @param rowData input row data
     * @return row data without incremental field
     */
    public RowData removeIncrementalField(RowData rowData) {
        if (incrementalFieldIndex == DEFAULT_META_INDEX) {
            return rowData;
        }
        GenericRowData newRowData = new GenericRowData(tableSchemaRowType.getFieldCount() - 1);
        for (int i = 0, j = 0; i < tableSchemaRowType.getFieldCount(); i++) {
            if (i != incrementalFieldIndex) {
                newRowData.setField(j++, rowDataConverter.get(rowData, i));
            }
        }
        return newRowData;
    }

    /**
     * remove incremental field from table schema
     * @param requestedSchema input table schema
     * @return table schema without incremental field
     */
    public static DataType filterOutMetaField(TableSchema requestedSchema) {
        DataTypes.Field[] fields = requestedSchema.getTableColumns().stream()
                .filter(column -> !META_INCREMENTAL.equals(column.getName()))
                .map(column -> FIELD(column.getName(), column.getType()))
                .toArray(DataTypes.Field[]::new);
        return ROW(fields).notNull();
    }

    /**
     * get incremental field index
     * @param tableSchema input table schema
     * @return incremental field index
     */
    public static int getMetaFieldIndex(TableSchema tableSchema) {
        RowType rowType = (RowType) tableSchema.toRowDataType().getLogicalType();
        List<RowField> fields = rowType.getFields();
        int metaFieldIndex = DEFAULT_META_INDEX;
        for (int i = 0; i < fields.size(); i++) {
            RowType.RowField rowField = fields.get(i);
            if (META_INCREMENTAL.equals(rowField.getName())) {
                metaFieldIndex = i;
                break;
            }
        }
        return metaFieldIndex;
    }

}
