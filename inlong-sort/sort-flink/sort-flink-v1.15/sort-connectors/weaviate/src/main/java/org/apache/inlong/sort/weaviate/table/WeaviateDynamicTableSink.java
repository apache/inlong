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

package org.apache.inlong.sort.weaviate.table;

import org.apache.inlong.sort.base.metric.MetricOption;
import org.apache.inlong.sort.weaviate.sink.WeaviateSinkFunction;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;

/**
 * Weaviate dynamic table sink for writing data to Weaviate vector database.
 * This class implements DynamicTableSink to provide table sink functionality
 * for Flink SQL.
 */
public class WeaviateDynamicTableSink implements DynamicTableSink {

    private final ResolvedSchema resolvedSchema;
    private final ReadableConfig config;
    private final MetricOption metricOption;

    public WeaviateDynamicTableSink(
            ResolvedSchema resolvedSchema,
            ReadableConfig config,
            MetricOption metricOption) {
        this.resolvedSchema = resolvedSchema;
        this.config = config;
        this.metricOption = metricOption;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        // Check if upsert is enabled
        boolean upsertEnabled = config.get(WeaviateOptions.UPSERT_ENABLED);

        if (upsertEnabled) {
            // Support INSERT, UPDATE, DELETE operations for upsert mode
            return ChangelogMode.newBuilder()
                    .addContainedKind(RowKind.INSERT)
                    .addContainedKind(RowKind.UPDATE_AFTER)
                    .addContainedKind(RowKind.DELETE)
                    .build();
        } else {
            // Only support INSERT operations for append mode
            return ChangelogMode.insertOnly();
        }
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        DataType physicalDataType = resolvedSchema.toPhysicalRowDataType();

        WeaviateSinkFunction sinkFunction = new WeaviateSinkFunction(
                config,
                physicalDataType,
                metricOption);

        return SinkFunctionProvider.of(sinkFunction);
    }

    @Override
    public DynamicTableSink copy() {
        return new WeaviateDynamicTableSink(resolvedSchema, config, metricOption);
    }

    @Override
    public String asSummaryString() {
        return String.format("WeaviateSink(url=%s, className=%s, batchSize=%d)",
                config.get(WeaviateOptions.URL),
                config.get(WeaviateOptions.CLASS_NAME),
                config.get(WeaviateOptions.BATCH_SIZE));
    }
}