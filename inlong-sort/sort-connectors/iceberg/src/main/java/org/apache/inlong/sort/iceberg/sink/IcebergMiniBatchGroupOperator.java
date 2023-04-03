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
import org.apache.flink.table.runtime.operators.TableStreamOperator;
import org.apache.flink.table.runtime.util.StreamRecordCollector;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.flink.RowDataWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class IcebergMiniBatchGroupOperator extends TableStreamOperator<RowData>
        implements
            OneInputStreamOperator<RowData, RowData>,
            BoundedOneInput {

    private static final long serialVersionUID = 9042068324817807379L;

    private static final Logger LOG = LoggerFactory.getLogger(IcebergMiniBatchGroupOperator.class);

    private transient RowDataWrapper rowDataWrapper;
    private transient StreamRecordCollector<RowData> collector;
    private transient Map<String, List<RowData>> inputBuffer;

    private final Schema schema;
    private final PartitionKey partitionKey;
    private final RowType flinkSchema;

    public IcebergMiniBatchGroupOperator(PartitionSpec spec, Schema schema, RowType flinkSchema) {
        this.schema = schema;
        this.partitionKey = new PartitionKey(spec, schema);
        this.flinkSchema = flinkSchema;
    }

    @Override
    public void open() throws Exception {
        super.open();
        LOG.info("Opening IcebergMiniBatchGroupOperator");

        this.rowDataWrapper = new RowDataWrapper(flinkSchema, schema.asStruct());;
        this.collector = new StreamRecordCollector<>(output);
        this.inputBuffer = new HashMap<>();
    }

    @Override
    public void processElement(StreamRecord<RowData> element) throws Exception {
        RowData input = element.getValue();
        partitionKey.partition(rowDataWrapper.wrap(input));

        inputBuffer.compute(partitionKey.toPath(), (key, value) -> {
            if (value == null) {
                List<RowData> list = new ArrayList<>();
                list.add(input);
                return list;
            }

            value.add(input);
            return value;
        });
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
        LOG.info("Closing StreamSortOperator");

        // BoundedOneInput can not coexistence with checkpoint, so we emit output in close.
        if (!inputBuffer.isEmpty()) {
            // Emit the rows group by partition
            inputBuffer.entrySet().forEach(
                    (Map.Entry<String, List<RowData>> entry) -> {
                        for (RowData row : entry.getValue()) {
                            collector.collect(row);
                        }
                    });
        }
        inputBuffer.clear();
    }
}
