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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public class GroupedPartitionedDeltaWriter extends BaseDeltaTaskWriter {

    private static final Logger LOG = LoggerFactory.getLogger(GroupedPartitionedDeltaWriter.class);

    private final PartitionKey partitionKey;

    private String latestPartitionPath;

    private RowDataDeltaWriter latestWriter;

    GroupedPartitionedDeltaWriter(PartitionSpec spec,
            FileFormat format,
            FileAppenderFactory<RowData> appenderFactory,
            OutputFileFactory fileFactory,
            FileIO io,
            long targetFileSize,
            Schema schema,
            RowType flinkSchema,
            List<Integer> equalityFieldIds,
            boolean upsert) {
        super(spec, format, appenderFactory, fileFactory, io, targetFileSize, schema, flinkSchema, equalityFieldIds,
                upsert);
        this.partitionKey = new PartitionKey(spec, schema);
    }

    @Override
    public RowDataDeltaWriter route(RowData row) {
        partitionKey.partition(wrapper().wrap(row));
        if (latestPartitionPath != null && partitionKey.toPath().equals(latestPartitionPath)) {
            return latestWriter;
        }

        closeCurrentWriter();
        latestPartitionPath = partitionKey.toPath();
        latestWriter = new RowDataDeltaWriter(partitionKey.copy());
        return latestWriter;
    }

    @Override
    public void close() {
        closeCurrentWriter();
        latestWriter = null;
        latestPartitionPath = null;
    }

    private void closeCurrentWriter() {
        if (latestWriter != null) {
            try {
                latestWriter.close();
            } catch (IOException e) {
                LOG.error("Exception occur when closing file {}.", latestPartitionPath);
                throw new RuntimeException(e);
            }
        }
    }
}
