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

import java.util.HashMap;
import java.util.Map;
import org.apache.flink.types.Row;
import org.apache.inlong.sort.flink.Record;
import org.apache.inlong.sort.meta.MetaManager.DataFlowInfoListener;
import org.apache.inlong.sort.protocol.DataFlowInfo;
import org.apache.inlong.sort.protocol.FieldInfo;
import org.apache.inlong.sort.protocol.sink.SinkInfo;
import org.apache.inlong.sort.protocol.source.SourceInfo;

import static org.apache.inlong.sort.configuration.Constants.DATA_TIME_FIELD;

/**
 * TODO, replace it with operator when it supports complex transformation.
 */
public class FieldMappingTransformer implements DataFlowInfoListener {

    /**
     * Skips time and attribute fields of source record.
     *
     * TODO, are all formats should be skipped by 2?
     */
    public static final int SOURCE_FIELD_SKIP_STEP = 2;

    private final Map<Long, FieldsIndexer> fieldsIndexerMap = new HashMap<>();

    @Override
    public void addDataFlow(DataFlowInfo dataFlowInfo) throws Exception {
        updateDataFlow(dataFlowInfo);
    }

    public void updateDataFlow(DataFlowInfo dataFlowInfo) {
        fieldsIndexerMap.put(dataFlowInfo.getId(), new FieldsIndexer(dataFlowInfo));
    }

    public void removeDataFlow(DataFlowInfo dataFlowInfo) {
        fieldsIndexerMap.remove(dataFlowInfo.getId());
    }

    public Record transform(Record sourceRecord) throws Exception {
        final FieldsIndexer fieldsIndexer = fieldsIndexerMap.get(sourceRecord.getDataflowId());
        if (fieldsIndexer == null) {
            throw new Exception("No schema found for data flow:" + sourceRecord.getDataflowId());
        }
        final Row sourceRow = sourceRecord.getRow();
        final Row sinkRow = new Row(fieldsIndexer.indices.length);
        for (int i = 0; i < fieldsIndexer.indices.length; i++) {
            final int fieldIndex = fieldsIndexer.indices[i];
            if (fieldIndex < 0 || fieldIndex > sourceRow.getArity()) {
                sinkRow.setField(i, null);
            } else {
                sinkRow.setField(i, sourceRow.getField(fieldIndex));
            }
        }
        return new Record(sourceRecord.getDataflowId(), sinkRow);
    }

    /**
     * It used to map sink field index to source field index.
     */
    private static class FieldsIndexer {

        /**
         * The index of array is sink field index, the value of array is source field index.
         */
        private final int[] indices;

        public FieldsIndexer(DataFlowInfo dataFlowInfo) {
            final SourceInfo sourceInfo = dataFlowInfo.getSourceInfo();
            final Map<String, Integer> sourceFieldIndices = new HashMap<>();
            for (int i = 0; i < sourceInfo.getFields().length; i++) {
                final FieldInfo fieldInfo = sourceInfo.getFields()[i];
                // use name to map field from source and sink
                // skip time and attributes fields
                sourceFieldIndices.put(fieldInfo.getName(), i + SOURCE_FIELD_SKIP_STEP);
            }
            final SinkInfo sinkInfo = dataFlowInfo.getSinkInfo();
            indices = new int[sinkInfo.getFields().length];
            for (int i = 0; i < sinkInfo.getFields().length; i++) {
                final FieldInfo fieldInfo = sinkInfo.getFields()[i];
                final Integer fieldIndex = sourceFieldIndices.get(fieldInfo.getName());
                if (fieldIndex != null) {
                    indices[i] = fieldIndex;
                } else if (DATA_TIME_FIELD.equals(fieldInfo.getName())) {
                    // built-in data time field
                    indices[i] = 0;
                } else {
                    indices[i] = -1;
                }
            }
        }
    }

}
