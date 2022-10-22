/*
 * Copyright 2021 TiDB Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.tidb.bigdata.flink.connector.table;

import io.tidb.bigdata.flink.connector.source.TiDBSourceBuilder;
import java.util.Arrays;
import java.util.List;
import org.apache.flink.connector.jdbc.internal.options.JdbcLookupOptions;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceProvider;
import org.apache.flink.table.connector.source.abilities.SupportsFilterPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsLimitPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.expressions.ResolvedExpression;

public class TiDBDynamicTableSource implements ScanTableSource, LookupTableSource,
        SupportsProjectionPushDown, SupportsFilterPushDown, SupportsLimitPushDown {

    private final ResolvedCatalogTable table;
    private final ChangelogMode changelogMode;
    private final LookupTableSourceHelper lookupTableSourceHelper;
    private FilterPushDownHelper filterPushDownHelper;
    private int[] projectedFields;
    private Integer limit;
    private final boolean appendMode;
    private final boolean initMode;

    public TiDBDynamicTableSource(ResolvedCatalogTable table,
            ChangelogMode changelogMode,
            JdbcLookupOptions lookupOptions,
            boolean appendMode,
            boolean initMode) {
        this(table, changelogMode, new LookupTableSourceHelper(lookupOptions), appendMode, initMode);
    }

    private TiDBDynamicTableSource(ResolvedCatalogTable table,
            ChangelogMode changelogMode, LookupTableSourceHelper lookupTableSourceHelper,
            boolean appendMode, boolean initMode) {
        this.table = table;
        this.changelogMode = changelogMode;
        this.lookupTableSourceHelper = lookupTableSourceHelper;
        this.filterPushDownHelper = new FilterPushDownHelper(table);
        this.appendMode = appendMode;
        this.initMode = initMode;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return changelogMode;
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext scanContext) {
        /* Disable metadata as it doesn't work with projection push down at this time */
        return SourceProvider.of(
                new TiDBSourceBuilder(table, scanContext::createTypeInformation, null, projectedFields,
                        filterPushDownHelper.getTiDBExpressions(), limit, appendMode, initMode).build());
    }

    @Override
    public DynamicTableSource copy() {
        TiDBDynamicTableSource otherSource =
                new TiDBDynamicTableSource(table, changelogMode, lookupTableSourceHelper, appendMode, initMode);
        otherSource.projectedFields = this.projectedFields;
        otherSource.filterPushDownHelper = this.filterPushDownHelper;
        return otherSource;
    }

    @Override
    public String asSummaryString() {
        return "";
    }

    @Override
    public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {
        return lookupTableSourceHelper.getLookupRuntimeProvider(table, context);
    }

    @Override
    public boolean supportsNestedProjection() {
        return false;
    }

    @Override
    public void applyProjection(int[][] projectedFields) {
        this.projectedFields = Arrays.stream(projectedFields).mapToInt(f -> f[0]).toArray();
    }

    @Override
    public Result applyFilters(List<ResolvedExpression> filters) {
        return filterPushDownHelper.applyFilters(filters);
    }

    @Override
    public void applyLimit(long limit) {
        this.limit = limit > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) limit;
    }
}