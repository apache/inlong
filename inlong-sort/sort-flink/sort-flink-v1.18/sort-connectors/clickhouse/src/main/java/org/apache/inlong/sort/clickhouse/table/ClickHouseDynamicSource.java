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

import org.apache.inlong.sort.clickhouse.source.ClickHouseOptions;
import org.apache.inlong.sort.clickhouse.source.ClickHouseRowDataInputFormat;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.InputFormatProvider;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.types.logical.RowType;

public class ClickHouseDynamicSource implements ScanTableSource {

    private final ResolvedSchema resolvedSchema;
    private final ReadableConfig config;

    public ClickHouseDynamicSource(ResolvedSchema resolvedSchema, ReadableConfig config) {
        this.resolvedSchema = resolvedSchema;
        this.config = config;
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        RowType rowType = (RowType) resolvedSchema.toPhysicalRowDataType().getLogicalType();

        Configuration configuration = new Configuration();
        configuration.set(ClickHouseOptions.URL, "jdbc:clickhouse://localhost:8123/default");
        configuration.set(ClickHouseOptions.USERNAME, "default");
        configuration.set(ClickHouseOptions.PASSWORD, "");
        configuration.set(ClickHouseOptions.QUERY, "SELECT * FROM my_table");

        ClickHouseRowDataInputFormat inputFormat =
                new ClickHouseRowDataInputFormat(configuration, rowType);

        return InputFormatProvider.of(inputFormat);
    }

    @Override
    public ScanTableSource copy() {
        return new ClickHouseDynamicSource(resolvedSchema, config);
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public String asSummaryString() {
        return "ClickHouse Dynamic Table Source";
    }
}
