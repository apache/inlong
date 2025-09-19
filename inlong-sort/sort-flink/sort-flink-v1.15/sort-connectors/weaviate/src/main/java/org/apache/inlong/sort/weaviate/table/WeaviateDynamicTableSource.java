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
import org.apache.inlong.sort.weaviate.source.WeaviateSourceFunction;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.types.DataType;

/**
 * Weaviate dynamic table source for reading data from Weaviate vector database.
 * This class implements DynamicTableSource and ScanTableSource to provide
 * table source functionality for Flink SQL.
 */
public class WeaviateDynamicTableSource implements DynamicTableSource, ScanTableSource {

    private final ResolvedSchema resolvedSchema;
    private final ReadableConfig config;
    private final MetricOption metricOption;

    public WeaviateDynamicTableSource(
            ResolvedSchema resolvedSchema,
            ReadableConfig config,
            MetricOption metricOption) {
        this.resolvedSchema = resolvedSchema;
        this.config = config;
        this.metricOption = metricOption;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        // Weaviate source only supports INSERT mode for reading data
        return ChangelogMode.insertOnly();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext scanContext) {
        DataType physicalDataType = resolvedSchema.toPhysicalRowDataType();

        WeaviateSourceFunction sourceFunction = new WeaviateSourceFunction(
                config,
                physicalDataType,
                metricOption);

        return SourceFunctionProvider.of(sourceFunction, false);
    }

    @Override
    public DynamicTableSource copy() {
        return new WeaviateDynamicTableSource(resolvedSchema, config, metricOption);
    }

    @Override
    public String asSummaryString() {
        return String.format("WeaviateSource(url=%s, className=%s)",
                config.get(WeaviateOptions.URL),
                config.get(WeaviateOptions.CLASS_NAME));
    }
}