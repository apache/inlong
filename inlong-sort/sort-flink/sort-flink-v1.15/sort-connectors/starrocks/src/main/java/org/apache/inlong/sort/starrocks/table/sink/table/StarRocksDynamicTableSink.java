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

package org.apache.inlong.sort.starrocks.table.sink.table;

import com.starrocks.connector.flink.table.sink.StarRocksDynamicSinkFunctionBase;
import com.starrocks.connector.flink.table.sink.StarRocksSinkOptions;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.data.RowData;

/**
 * This class is used to create a DynamicTableSink for Starrocks.
 */
public class StarRocksDynamicTableSink implements DynamicTableSink {

    private transient TableSchema flinkSchema;
    private StarRocksSinkOptions sinkOptions;
    private String inlongMetric;
    private String auditHostAndPorts;
    private String auditKeys;
    private boolean ignoreJsonParseError;

    public StarRocksDynamicTableSink(StarRocksSinkOptions sinkOptions, TableSchema schema,
            String inlongMetric, String auditHostAndPorts, String auditKeys, boolean ignoreJsonParseError) {
        this.flinkSchema = schema;
        this.sinkOptions = sinkOptions;
        this.inlongMetric = inlongMetric;
        this.auditHostAndPorts = auditHostAndPorts;
        this.auditKeys = auditKeys;
        this.ignoreJsonParseError = ignoreJsonParseError;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        return requestedMode;
    }

    @Override
    @SuppressWarnings("unchecked")
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {

        final TypeInformation<RowData> rowDataTypeInfo = context.createTypeInformation(flinkSchema.toRowDataType());
        StarRocksDynamicSinkFunctionBase<RowData> starrocksSinkFunction = SinkFunctionFactory.createSinkFunction(
                sinkOptions,
                flinkSchema,
                new StarRocksTableRowTransformer(rowDataTypeInfo, ignoreJsonParseError),
                inlongMetric,
                auditHostAndPorts,
                auditKeys);
        return SinkFunctionProvider.of(starrocksSinkFunction, sinkOptions.getSinkParallelism());

    }

    @Override
    public DynamicTableSink copy() {
        return new StarRocksDynamicTableSink(sinkOptions, flinkSchema, inlongMetric,
                auditHostAndPorts, auditKeys, ignoreJsonParseError);
    }

    @Override
    public String asSummaryString() {
        return "starrocks_sink";
    }

}