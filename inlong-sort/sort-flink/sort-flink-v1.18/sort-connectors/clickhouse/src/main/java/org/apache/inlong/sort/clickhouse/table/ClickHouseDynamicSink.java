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

package org.apache.inlong.sort.clickhouse.table;

import org.apache.inlong.sort.clickhouse.source.ClickHouseSinkFunction;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

public class ClickHouseDynamicSink implements DynamicTableSink {

    private final ResolvedSchema schema;
    private final ReadableConfig options;

    public ClickHouseDynamicSink(ResolvedSchema schema, ReadableConfig options) {
        this.schema = schema;
        this.options = options;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        return ChangelogMode.newBuilder()
                .addContainedKind(RowKind.INSERT)
                .addContainedKind(RowKind.UPDATE_AFTER)
                .build();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        String jdbcUrl = "jdbc:clickhouse://" + options.get(ClickHouseConnectorOptions.HOSTS)
                + "/" + options.get(ClickHouseConnectorOptions.DATABASE);
        String username = options.get(ClickHouseConnectorOptions.USERNAME);
        String password = options.get(ClickHouseConnectorOptions.PASSWORD);
        String tableName = options.get(ClickHouseConnectorOptions.TABLE_NAME);
        RowType rowType = (RowType) schema.toPhysicalRowDataType().getLogicalType();

        ClickHouseSinkFunction sinkFunction = new ClickHouseSinkFunction(
                jdbcUrl,
                username,
                password,
                tableName,
                rowType);

        return SinkFunctionProvider.of(sinkFunction);
    }

    @Override
    public DynamicTableSink copy() {
        return new ClickHouseDynamicSink(schema, options);
    }

    @Override
    public String asSummaryString() {
        return "ClickHouse Dynamic Table Sink";
    }
}
