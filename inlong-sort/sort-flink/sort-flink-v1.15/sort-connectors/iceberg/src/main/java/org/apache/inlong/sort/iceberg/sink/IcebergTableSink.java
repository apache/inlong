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

import org.apache.inlong.sort.iceberg.IcebergReadableMetadata;
import org.apache.inlong.sort.iceberg.utils.SinkMetadataUtils;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.constraints.UniqueConstraint;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.ProviderContext;
import org.apache.flink.table.connector.sink.DataStreamSinkProvider;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.abilities.SupportsOverwrite;
import org.apache.flink.table.connector.sink.abilities.SupportsPartitioning;
import org.apache.flink.table.connector.sink.abilities.SupportsWritingMetadata;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Preconditions;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Copy from iceberg-flink:iceberg-flink-1.15:1.3.1
 */
public class IcebergTableSink
        implements
            DynamicTableSink,
            SupportsPartitioning,
            SupportsOverwrite,
            SupportsWritingMetadata {

    private final TableLoader tableLoader;
    private final TableSchema tableSchema;
    private final ReadableConfig readableConfig;
    private final Map<String, String> writeProps;
    protected DataType consumedDataType;
    protected List<String> metadataKeys;

    private boolean overwrite = false;

    private IcebergTableSink(IcebergTableSink toCopy) {
        this.tableLoader = toCopy.tableLoader;
        this.tableSchema = toCopy.tableSchema;
        this.overwrite = toCopy.overwrite;
        this.readableConfig = toCopy.readableConfig;
        this.writeProps = toCopy.writeProps;
        this.metadataKeys = toCopy.metadataKeys;
        this.consumedDataType = toCopy.consumedDataType;
    }

    public IcebergTableSink(
            TableLoader tableLoader,
            TableSchema tableSchema,
            ReadableConfig readableConfig,
            Map<String, String> writeProps) {
        this.tableLoader = tableLoader;
        this.tableSchema = tableSchema;
        this.readableConfig = readableConfig;
        this.writeProps = writeProps;
        this.metadataKeys = ImmutableList.of();
        this.consumedDataType = tableSchema.toPhysicalRowDataType();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        Preconditions.checkState(
                !overwrite || context.isBounded(),
                "Unbounded data stream doesn't support overwrite operation.");

        List<String> equalityColumns =
                tableSchema.getPrimaryKey().map(UniqueConstraint::getColumns).orElseGet(ImmutableList::of);
        SinkMetadataUtils sinkMetadataUtils = new SinkMetadataUtils(metadataKeys, consumedDataType);
        return new DataStreamSinkProvider() {

            @Override
            public DataStreamSink<?> consumeDataStream(
                    ProviderContext providerContext, DataStream<RowData> dataStream) {
                return FlinkSink.forRowData(dataStream)
                        .tableLoader(tableLoader)
                        .tableSchema(tableSchema)
                        .equalityFieldColumns(equalityColumns)
                        .overwrite(overwrite)
                        .setAll(writeProps)
                        .flinkConf(readableConfig)
                        .SinkMetadataUtils(sinkMetadataUtils)
                        .append();
            }
        };
    }

    @Override
    public void applyStaticPartition(Map<String, String> partition) {
        // The flink's PartitionFanoutWriter will handle the static partition write policy
        // automatically.
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        ChangelogMode.Builder builder = ChangelogMode.newBuilder();
        for (RowKind kind : requestedMode.getContainedKinds()) {
            builder.addContainedKind(kind);
        }
        return builder.build();
    }

    @Override
    public DynamicTableSink copy() {
        return new IcebergTableSink(this);
    }

    @Override
    public String asSummaryString() {
        return "Iceberg table sink";
    }

    @Override
    public void applyOverwrite(boolean newOverwrite) {
        this.overwrite = newOverwrite;
    }

    @Override
    public Map<String, DataType> listWritableMetadata() {
        return Stream.of(IcebergReadableMetadata.values())
                .collect(
                        Collectors.toMap(
                                IcebergReadableMetadata::getKey, IcebergReadableMetadata::getDataType));
    }

    @Override
    public void applyWritableMetadata(List<String> metadataKeys, DataType consumedDataType) {
        this.metadataKeys = metadataKeys;
        this.consumedDataType = consumedDataType;
    }

}
