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

import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.io.BaseTaskWriter;
import org.apache.iceberg.io.FileAppenderFactory;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFileFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public abstract class GroupedPartitionedFanoutWriter<T> extends BaseTaskWriter<T> {

    private static final Logger LOG = LoggerFactory.getLogger(GroupedPartitionedFanoutWriter.class);

    private String latestPartitionPath;

    private RollingFileWriter latestWriter;

    protected GroupedPartitionedFanoutWriter(PartitionSpec spec, FileFormat format,
            FileAppenderFactory<T> appenderFactory,
            OutputFileFactory fileFactory, FileIO io, long targetFileSize) {
        super(spec, format, appenderFactory, fileFactory, io, targetFileSize);
    }

    /**
     * Create a PartitionKey from the values in row.
     * <p>
     * Any PartitionKey returned by this method can be reused by the implementation.
     *
     * @param row a data row
     */
    protected abstract PartitionKey partition(T row);

    @Override
    public void write(T row) throws IOException {
        PartitionKey partitionKey = partition(row);
        if (latestPartitionPath == null || !partitionKey.toPath().equals(latestPartitionPath)) {
            // NOTICE: we need to copy a new partition key here, in case of messing up the keys in writers.
            closeCurrentWriter();
            latestWriter = new RollingFileWriter(partitionKey.copy());
            latestPartitionPath = partitionKey.toPath();
        }

        latestWriter.write(row);
    }

    @Override
    public void close() throws IOException {
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
