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

package org.apache.inlong.sort.iceberg.sink;

import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.RowData.FieldGetter;
import org.apache.flink.table.data.util.RowDataUtil;
import org.apache.flink.table.runtime.operators.TableStreamOperator;
import org.apache.flink.table.runtime.util.StreamRecordCollector;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types.NestedField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * This Operator has two functional above:
 * 1. Aggregating calculation in advance, reduce downstream computational workload
 * 2. Clustering data according to partition, reduce memory pressure caused by opening multiple writers downstream
 */
public class IcebergMiniBatchGroupOperator extends TableStreamOperator<RowData>
        implements
            OneInputStreamOperator<RowData, RowData>,
            BoundedOneInput {

    private static final long serialVersionUID = 9042068324817807379L;

    private static final Logger LOG = LoggerFactory.getLogger(IcebergMiniBatchGroupOperator.class);

    private transient StreamRecordCollector<RowData> collector;
    private transient Map<List<Object>, RowData> inputBuffer;

    private final FieldGetter[] fieldsGetter;
    private final int[] equalityFieldIndex; // the position ordered of equality field in row schema
    private final int[] partitionIndex; // the position ordered of partition field in delete schema

    /**
     * Initialize field index.
     *
     * @param fieldsGetter function to get object from {@link RowData}
     * @param partitionSchema partition spec schema
     * @param deleteSchema equality fields schema
     * @param rowSchema row data schema
     */
    public IcebergMiniBatchGroupOperator(
            FieldGetter[] fieldsGetter,
            Schema partitionSchema,
            Schema deleteSchema,
            Schema rowSchema) {
        this.fieldsGetter = fieldsGetter;
        // note: here because `NestedField` does not override equals function, so can not indexOf by `NestedField`
        this.equalityFieldIndex = deleteSchema.columns().stream()
                .map(field -> rowSchema.columns()
                        .stream()
                        .map(NestedField::fieldId)
                        .collect(Collectors.toList())
                        .indexOf(field.fieldId()))
                .sorted()
                .mapToInt(Integer::valueOf)
                .toArray();
        this.partitionIndex = partitionSchema.columns().stream()
                .map(field -> deleteSchema.columns()
                        .stream()
                        .map(NestedField::fieldId)
                        .collect(Collectors.toList())
                        .indexOf(field.fieldId()))
                .sorted()
                .mapToInt(Integer::valueOf)
                .toArray();
        // do some check, check whether index is legal. can not be null and unique, and number in fields range.
        // indexOf不行，因为它是基于equals的，nestedField没有覆盖equals方法，所以indexIOf会找不到
    }

    @Override
    public void open() throws Exception {
        super.open();
        LOG.info("Opening IcebergMiniBatchGroupOperator");

        this.collector = new StreamRecordCollector<>(output);
        this.inputBuffer = new HashMap<>();
    }

    @Override
    public void processElement(StreamRecord<RowData> element) throws Exception {
        RowData row = element.getValue();
        List<Object> equalityFields = Arrays.stream(equalityFieldIndex)
                .boxed()
                .map(index -> fieldsGetter[index].getFieldOrNull(row))
                .collect(Collectors.toList());
        if (RowDataUtil.isAccumulateMsg(row)) {
            inputBuffer.put(equalityFields, row);
        } else {
            inputBuffer.remove(equalityFields);
        }
    }

    @Override
    public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
        super.prepareSnapshotPreBarrier(checkpointId);
        flush();
    }

    @Override
    public void close() throws Exception {
        super.close();
        flush();
    }

    @Override
    public void endInput() throws Exception {
        flush();
    }

    private void flush() throws Exception {
        LOG.info("Flushing IcebergMiniBatchGroupOperator.");
        if (!inputBuffer.isEmpty()) {
            // Emit the rows group by partition
            Map<List<Object>, List<RowData>> map0 = inputBuffer.entrySet()
                    .stream()
                    .<Map<List<Object>, List<RowData>>>reduce(
                            new HashMap<>(),
                            (map, record) -> {
                                List<Object> partition = Arrays.stream(partitionIndex)
                                        .mapToObj(index -> record.getKey().get(index))
                                        .collect(Collectors.toList());
                                map.compute(partition, (List<Object> par, List<RowData> oldList) -> {
                                    if (oldList == null) {
                                        List<RowData> list = new ArrayList<>();
                                        list.add(record.getValue());
                                        return list;
                                    }

                                    oldList.add(record.getValue());
                                    return oldList;
                                });
                                return map;
                            },
                            (map1, map2) -> {
                                for (List<Object> key : map2.keySet()) {
                                    if (!map1.containsKey(key)) {
                                        map1.put(key, map2.get(key));
                                    } else {
                                        map1.get(key).addAll(map2.get(key));
                                    }
                                }
                                return map1;
                            });
            map0.values()
                    .forEach(
                            list -> list.forEach(record -> collector.collect(record)));
        }
        inputBuffer.clear();
    }
}
