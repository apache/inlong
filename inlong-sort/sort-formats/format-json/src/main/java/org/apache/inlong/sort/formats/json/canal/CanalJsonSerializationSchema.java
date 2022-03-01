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

package org.apache.inlong.sort.formats.json.canal;

import static org.apache.flink.table.types.utils.TypeConversions.fromLogicalToDataType;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.formats.json.JsonOptions;
import org.apache.flink.formats.json.JsonRowDataSerializationSchema;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.DataTypes.Field;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.util.DataFormatConverters;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.DataTypeUtils;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.inlong.sort.formats.json.MysqlBinLogData;
import org.apache.inlong.sort.formats.json.canal.CanalJsonDecodingFormat.ReadableMetadata;

/**
 * Copied from apache flink project with a litter change.
 *
 * Serialization schema that serializes an object of Flink Table/SQL internal data structure {@link
 * RowData} into a Canal JSON bytes.
 *
 * @see <a href="https://github.com/alibaba/canal">Alibaba Canal</a>
 */
public class CanalJsonSerializationSchema implements SerializationSchema<RowData> {

    private static final long serialVersionUID = 1L;

    private static final StringData OP_INSERT = StringData.fromString("INSERT");
    private static final StringData OP_DELETE = StringData.fromString("DELETE");

    private transient GenericRowData reuse;

    /** The serializer to serialize Canal JSON data. */
    private final JsonRowDataSerializationSchema jsonSerializer;

    private final DataFormatConverters.RowConverter consumedRowConverter;

    private final DataFormatConverters.RowConverter physicalRowConverter;

    private final Map<Integer, ReadableMetadata> fieldIndexToMetadata;

    public CanalJsonSerializationSchema(
            RowType physicalRowType,
            Map<Integer, ReadableMetadata> fieldIndexToMetadata,
            DataFormatConverters.RowConverter consumedRowConverter,
            DataFormatConverters.RowConverter physicalRowConverter,
            TimestampFormat timestampFormat,
            JsonOptions.MapNullKeyMode mapNullKeyMode,
            String mapNullKeyLiteral,
            boolean encodeDecimalAsPlainNumber) {
        jsonSerializer =
                new JsonRowDataSerializationSchema(
                        createJsonRowType(fromLogicalToDataType(physicalRowType), fieldIndexToMetadata.values()),
                        timestampFormat,
                        mapNullKeyMode,
                        mapNullKeyLiteral,
                        encodeDecimalAsPlainNumber);

        this.fieldIndexToMetadata = fieldIndexToMetadata;
        this.consumedRowConverter = consumedRowConverter;
        this.physicalRowConverter = physicalRowConverter;
    }

    @Override
    public void open(InitializationContext context) {
        reuse = new GenericRowData(2 + fieldIndexToMetadata.size());
    }

    @Override
    public byte[] serialize(RowData row) {
        try {
            MysqlBinLogData mysqlBinLogData = getMysqlBinLongData(row);

            ArrayData arrayData = new GenericArrayData(new RowData[] {mysqlBinLogData.getPhysicalData()});
            reuse.setField(0, arrayData);
            reuse.setField(1, rowKind2String(row.getRowKind()));

            // Set metadata
            Map<String, Object> metadataMap = mysqlBinLogData.getMetadataMap();
            int index = 2;
            for (ReadableMetadata readableMetadata : fieldIndexToMetadata.values()) {
                reuse.setField(index, metadataMap.get(readableMetadata.key));
                index++;
            }

            return jsonSerializer.serialize(reuse);
        } catch (Throwable t) {
            throw new RuntimeException("Could not serialize row '" + row + "'.", t);
        }
    }

    private StringData rowKind2String(RowKind rowKind) {
        switch (rowKind) {
            case INSERT:
            case UPDATE_AFTER:
                return OP_INSERT;
            case UPDATE_BEFORE:
            case DELETE:
                return OP_DELETE;
            default:
                throw new UnsupportedOperationException(
                        "Unsupported operation '" + rowKind + "' for row kind.");
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CanalJsonSerializationSchema that = (CanalJsonSerializationSchema) o;
        return Objects.equals(jsonSerializer, that.jsonSerializer);
    }

    @Override
    public int hashCode() {
        return Objects.hash(jsonSerializer);
    }

    private static RowType createJsonRowType(
            DataType dataSchema,
            Collection<ReadableMetadata> metadataSet) {
        DataType root =  DataTypes.ROW(
                DataTypes.FIELD("data", DataTypes.ARRAY(dataSchema)),
                DataTypes.FIELD("type", DataTypes.STRING())
        );

        final List<Field> metadataFields =
                metadataSet.stream()
                        .map(m -> m.requiredJsonField)
                        .distinct()
                        .collect(Collectors.toList());

        return (RowType) DataTypeUtils.appendRowFields(root, metadataFields).getLogicalType();
    }

    private MysqlBinLogData getMysqlBinLongData(RowData consumedRowData) {
        Row consumedRow = consumedRowConverter.toExternal(consumedRowData);
        int consumedRowArity = consumedRow.getArity();
        Set<Integer> metadataIndices = fieldIndexToMetadata.keySet();

        Row physicalRow = new Row(consumedRowArity - metadataIndices.size());
        Map<String, Object> metadataMap = new HashMap<>();
        int physicalRowDataIndex = 0;
        for (int i = 0; i < consumedRowArity; i++) {
            if (!metadataIndices.contains(i)) {
                physicalRow.setField(physicalRowDataIndex, consumedRow.getField(i));
                physicalRowDataIndex++;
            } else {
                metadataMap.put(
                        fieldIndexToMetadata.get(i).key,
                        fieldIndexToMetadata.get(i).converter.convert(consumedRow.getField(i)));
            }
        }

        return new MysqlBinLogData(physicalRowConverter.toInternal(physicalRow), metadataMap);
    }
}
