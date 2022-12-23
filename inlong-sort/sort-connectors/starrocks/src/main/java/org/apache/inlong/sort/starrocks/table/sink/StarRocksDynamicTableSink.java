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

package org.apache.inlong.sort.starrocks.table.sink;

import com.starrocks.connector.flink.row.sink.StarRocksTableRowTransformer;
import com.starrocks.connector.flink.table.sink.StarRocksSinkOptions;
import javax.annotation.Nullable;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.inlong.sort.base.dirty.DirtyOptions;
import org.apache.inlong.sort.base.dirty.sink.DirtySink;
import org.apache.inlong.sort.base.sink.SchemaUpdateExceptionPolicy;

public class StarRocksDynamicTableSink implements DynamicTableSink {

    private transient TableSchema flinkSchema;
    private StarRocksSinkOptions sinkOptions;
    private final boolean multipleSink;
    private final String sinkMultipleFormat;
    private final String databasePattern;
    private final String tablePattern;
    private final String inlongMetric;
    private final String auditHostAndPorts;
    private final SchemaUpdateExceptionPolicy schemaUpdatePolicy;
    private final DirtyOptions dirtyOptions;
    private @Nullable final DirtySink<Object> dirtySink;

    public StarRocksDynamicTableSink(StarRocksSinkOptions sinkOptions,
            TableSchema schema,
            boolean multipleSink,
            String sinkMultipleFormat,
            String databasePattern,
            String tablePattern,
            String inlongMetric,
            String auditHostAndPorts,
            SchemaUpdateExceptionPolicy schemaUpdatePolicy,
            DirtyOptions dirtyOptions,
            @Nullable DirtySink<Object> dirtySink) {
        this.flinkSchema = schema;
        this.sinkOptions = sinkOptions;
        this.multipleSink = multipleSink;
        this.sinkMultipleFormat = sinkMultipleFormat;
        this.databasePattern = databasePattern;
        this.tablePattern = tablePattern;
        this.inlongMetric = inlongMetric;
        this.auditHostAndPorts = auditHostAndPorts;
        this.schemaUpdatePolicy = schemaUpdatePolicy;
        this.dirtyOptions = dirtyOptions;
        this.dirtySink = dirtySink;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        return requestedMode;
    }

    @Override
    @SuppressWarnings("unchecked")
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        final TypeInformation<RowData> rowDataTypeInfo = context.createTypeInformation(flinkSchema.toRowDataType());
        StarRocksDynamicSinkFunction<RowData> starrocksSinkFunction = new StarRocksDynamicSinkFunction<>(sinkOptions,
                flinkSchema,
                new StarRocksTableRowTransformer(rowDataTypeInfo),
                multipleSink,
                sinkMultipleFormat,
                databasePattern,
                tablePattern,
                inlongMetric,
                auditHostAndPorts,
                schemaUpdatePolicy,
                dirtyOptions,
                dirtySink);
        return SinkFunctionProvider.of(starrocksSinkFunction, sinkOptions.getSinkParallelism());
    }

    @Override
    public DynamicTableSink copy() {
        return new StarRocksDynamicTableSink(sinkOptions,
                flinkSchema,
                multipleSink,
                sinkMultipleFormat,
                databasePattern,
                tablePattern,
                inlongMetric,
                auditHostAndPorts,
                schemaUpdatePolicy,
                dirtyOptions,
                dirtySink);
    }

    @Override
    public String asSummaryString() {
        return "starrocks_sink";
    }
}
