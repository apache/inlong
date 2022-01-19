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

package org.apache.inlong.sort.flink.transformation;

import com.google.common.annotations.VisibleForTesting;
import java.util.HashMap;
import java.util.Map;
import org.apache.flink.api.java.typeutils.runtime.RowSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.types.Row;
import org.apache.inlong.sort.flink.Record;
import org.apache.inlong.sort.flink.SerializedRecord;
import org.apache.inlong.sort.meta.MetaManager.DataFlowInfoListener;
import org.apache.inlong.sort.protocol.DataFlowInfo;
import org.apache.inlong.sort.util.CommonUtils;

/**
 * TODO, introduce a multi-tenancy row serializer/deserializer to avoid transformation.
 */
public class RecordTransformer implements DataFlowInfoListener {

    private DataOutputSerializer dataOutputSerializer;

    private final DataInputDeserializer dataInputDeserializer = new DataInputDeserializer();

    private final Map<Long, RowSerializer> rowSerializers = new HashMap<>();

    private final int serializationInitialBufferSize;

    public RecordTransformer(int serializationInitialBufferSize) {
        this.serializationInitialBufferSize = serializationInitialBufferSize;
    }

    @Override
    public void addDataFlow(DataFlowInfo dataFlowInfo) throws Exception {
        updateDataFlow(dataFlowInfo);
    }

    public void updateDataFlow(DataFlowInfo dataFlowInfo) {
        // currently, transformation is bases on sink fields
        rowSerializers.put(dataFlowInfo.getId(), CommonUtils.generateRowSerializer(
                CommonUtils.generateRowFormatInfo(dataFlowInfo.getSinkInfo().getFields())));
    }

    public void removeDataFlow(DataFlowInfo dataFlowInfo) {
        rowSerializers.remove(dataFlowInfo.getId());
    }

    /**
     * Serialize Record.
     * @param record record to be serialized
     * @return serialized record
     */
    public SerializedRecord toSerializedRecord(Record record) throws Exception {
        if (dataOutputSerializer == null) {
            dataOutputSerializer = new DataOutputSerializer(serializationInitialBufferSize);
        }
        final long dataFlowId = record.getDataflowId();
        RowSerializer rowSerializer = getRowSerializer(dataFlowId);
        Record newRecord = matchRecordAndSerializerField(record, rowSerializer);
        final SerializedRecord serializedRecord;
        try {
            rowSerializer.serialize(newRecord.getRow(), dataOutputSerializer);
            serializedRecord = new SerializedRecord(dataFlowId,
                    record.getTimestampMillis(),
                    dataOutputSerializer.getCopyOfBuffer());
        } catch (Exception e) {
            throw new Exception("Schema not match for data flow: " + dataFlowId);
        } finally {
            dataOutputSerializer.clear();
        }
        return serializedRecord;
    }

    public Record toRecord(SerializedRecord serializedRecord) throws Exception {
        final long dataFlowId = serializedRecord.getDataFlowId();
        dataInputDeserializer.setBuffer(serializedRecord.getData());
        RowSerializer rowSerializer = getRowSerializer(dataFlowId);
        final Row row;
        try {
            row = rowSerializer.deserialize(dataInputDeserializer);
            assert dataInputDeserializer.available() == 0;
        } catch (Exception | AssertionError e) {
            throw new Exception("Schema not match for data flow: " + dataFlowId);
        } finally {
            dataInputDeserializer.releaseArrays();
        }
        return new Record(dataFlowId, serializedRecord.getTimestampMillis(), row);
    }

    private RowSerializer getRowSerializer(long dataFlowId) throws Exception {
        final RowSerializer rowSerializer = rowSerializers.get(dataFlowId);
        if (rowSerializer == null) {
            throw new Exception("No schema found for data flow:" + dataFlowId);
        }
        return rowSerializer;
    }

    @VisibleForTesting
    Record matchRecordAndSerializerField(Record oldRecord, RowSerializer rowSerializer) {
        if (rowSerializer.getArity() == oldRecord.getRow().getArity()) {
            // do nothing
            return oldRecord;
        }
        Row newRow = new Row(rowSerializer.getArity());
        Row oldRow = oldRecord.getRow();
        for (int i = 0; i < rowSerializer.getArity(); i++) {
            if (i < oldRow.getArity()) {
                newRow.setField(i, oldRow.getField(i));
            } else {
                newRow.setField(i, null);
            }
        }
        return new Record(oldRecord.getDataflowId(), oldRecord.getTimestampMillis(), newRow);
    }

    @VisibleForTesting
    Map<Long, RowSerializer> getRowSerializers() {
        return rowSerializers;
    }

    @VisibleForTesting
    DataOutputSerializer getDataOutputSerializer() {
        return dataOutputSerializer;
    }

    @VisibleForTesting
    DataInputDeserializer getDataInputDeserializer() {
        return dataInputDeserializer;
    }
}
