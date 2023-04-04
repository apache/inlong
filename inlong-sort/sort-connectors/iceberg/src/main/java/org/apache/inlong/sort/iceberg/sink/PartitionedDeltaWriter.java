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

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.io.FileAppenderFactory;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.Tasks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Copy from iceberg-flink:iceberg-flink-1.13:0.13.2
 */
class PartitionedDeltaWriter extends BaseDeltaTaskWriter {

    private static final Logger LOG = LoggerFactory.getLogger(PartitionedDeltaWriter.class);

    private final PartitionKey partitionKey;

    private final Map<PartitionKey, RowDataDeltaWriter> writers;

    PartitionedDeltaWriter(PartitionSpec spec,
            FileFormat format,
            FileAppenderFactory<RowData> appenderFactory,
            OutputFileFactory fileFactory,
            FileIO io,
            long targetFileSize,
            Schema schema,
            RowType flinkSchema,
            List<Integer> equalityFieldIds,
            boolean upsert,
            boolean isLRU) {
        super(spec, format, appenderFactory, fileFactory, io, targetFileSize, schema, flinkSchema, equalityFieldIds,
                upsert);
        this.partitionKey = new PartitionKey(spec, schema);
        this.writers = isLRU ? new LinkedHashMap<PartitionKey, RowDataDeltaWriter>() {

            @Override
            protected boolean removeEldestEntry(Entry<PartitionKey, RowDataDeltaWriter> eldest) {
                if (size() > 1) {
                    try {
                        LOG.info("Eliminated writer for partition {}", eldest.getKey().toPath());
                        eldest.getValue().close();
                    } catch (IOException e) {
                        PartitionedDeltaWriter.this.setFailure(e); // todo:这里把异常隐藏了，能不能提前检测并且暴露
                    }
                }
                return size() > 1;
            }
        } : Maps.newHashMap();
    }

    @Override
    RowDataDeltaWriter route(RowData row) {
        partitionKey.partition(wrapper().wrap(row));

        RowDataDeltaWriter writer = writers.get(partitionKey);
        if (writer == null) {
            // NOTICE: we need to copy a new partition key here, in case of messing up the keys in writers.
            PartitionKey copiedKey = partitionKey.copy();
            writer = new RowDataDeltaWriter(copiedKey);
            writers.put(copiedKey, writer);
        }

        return writer;
    }

    @Override
    public void close() {
        try {
            Tasks.foreach(writers.values())
                    .throwFailureWhenFinished()
                    .noRetry()
                    .run(RowDataDeltaWriter::close, IOException.class);

            writers.clear();
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to close equality delta writer", e);
        }
    }
}
