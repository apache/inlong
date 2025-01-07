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

package org.apache.inlong.sort.iceberg.source;

import org.apache.inlong.sort.base.Constants;
import org.apache.inlong.sort.iceberg.IcebergReadableMetadata;
import org.apache.inlong.sort.iceberg.IcebergReadableMetadata.MetadataConverter;

import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.ProviderContext;
import org.apache.flink.table.connector.source.DataStreamScanProvider;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsFilterPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsLimitPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsReadingMetadata;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.types.DataType;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.flink.FlinkConfigOptions;
import org.apache.iceberg.flink.FlinkFilters;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.source.FlinkSource;
import org.apache.iceberg.flink.source.assigner.SplitAssignerType;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Flink Iceberg table source.
 * Copy from iceberg-flink:iceberg-flink-1.15:1.3.1
 */
@Internal
public class IcebergTableSource
        implements
            ScanTableSource,
            SupportsProjectionPushDown,
            SupportsFilterPushDown,
            SupportsLimitPushDown,
            SupportsReadingMetadata {

    private int[] projectedFields;
    private Long limit;
    private List<Expression> filters;
    protected DataType producedDataType;
    protected List<String> metadataKeys;

    private final TableLoader loader;
    private final TableSchema schema;
    private final Map<String, String> properties;
    private final boolean isLimitPushDown;
    private final ReadableConfig readableConfig;
    private final boolean enableLogReport;

    private IcebergTableSource(IcebergTableSource toCopy) {
        this.loader = toCopy.loader;
        this.schema = toCopy.schema;
        this.properties = toCopy.properties;
        this.projectedFields = toCopy.projectedFields;
        this.isLimitPushDown = toCopy.isLimitPushDown;
        this.limit = toCopy.limit;
        this.filters = toCopy.filters;
        this.readableConfig = toCopy.readableConfig;
        this.producedDataType = toCopy.producedDataType;
        this.metadataKeys = toCopy.metadataKeys;
        this.enableLogReport = toCopy.enableLogReport;
    }

    public IcebergTableSource(
            TableLoader loader,
            TableSchema schema,
            Map<String, String> properties,
            ReadableConfig readableConfig) {
        this(loader, schema, properties, null, false, null, ImmutableList.of(), readableConfig);
    }

    private IcebergTableSource(
            TableLoader loader,
            TableSchema schema,
            Map<String, String> properties,
            int[] projectedFields,
            boolean isLimitPushDown,
            Long limit,
            List<Expression> filters,
            ReadableConfig readableConfig) {
        this.loader = loader;
        this.schema = schema;
        this.properties = properties;
        this.projectedFields = projectedFields;
        this.isLimitPushDown = isLimitPushDown;
        this.limit = limit;
        this.filters = filters;
        this.readableConfig = readableConfig;
        this.producedDataType = schema.toPhysicalRowDataType();
        this.metadataKeys = new ArrayList<>();
        this.enableLogReport = readableConfig.get(Constants.ENABLE_LOG_REPORT);
    }

    @Override
    public void applyProjection(int[][] projectFields) {
        this.projectedFields = new int[projectFields.length];
        for (int i = 0; i < projectFields.length; i++) {
            Preconditions.checkArgument(
                    projectFields[i].length == 1, "Don't support nested projection in iceberg source now.");
            this.projectedFields[i] = projectFields[i][0];
        }
    }

    private DataStreamSource<RowData> createFLIP27Stream(
            StreamExecutionEnvironment env,
            TypeInformation<RowData> typeInfo) {
        SplitAssignerType assignerType =
                readableConfig.get(FlinkConfigOptions.TABLE_EXEC_SPLIT_ASSIGNER_TYPE);
        MetadataConverter[] converters = getMetadataConverters();
        IcebergSource<RowData> source =
                IcebergSource.forRowData()
                        .tableLoader(loader)
                        .assignerFactory(assignerType.factory())
                        .properties(properties)
                        .project(getProjectedSchema())
                        .limit(limit)
                        .filters(filters)
                        .flinkConfig(readableConfig)
                        .metadataConverters(converters)
                        .build();
        DataStreamSource<RowData> stream =
                env.fromSource(
                        source,
                        WatermarkStrategy.noWatermarks(),
                        source.name(),
                        typeInfo);
        return stream;
    }

    private TableSchema getProjectedSchema() {
        if (projectedFields == null) {
            return schema;
        } else {
            String[] fullNames = schema.getFieldNames();
            DataType[] fullTypes = schema.getFieldDataTypes();
            return TableSchema.builder()
                    .fields(
                            Arrays.stream(projectedFields).mapToObj(i -> fullNames[i]).toArray(String[]::new),
                            Arrays.stream(projectedFields).mapToObj(i -> fullTypes[i]).toArray(DataType[]::new))
                    .build();
        }
    }

    @Override
    public void applyLimit(long newLimit) {
        this.limit = newLimit;
    }

    @Override
    public Result applyFilters(List<ResolvedExpression> flinkFilters) {
        List<ResolvedExpression> acceptedFilters = Lists.newArrayList();
        List<Expression> expressions = Lists.newArrayList();

        for (ResolvedExpression resolvedExpression : flinkFilters) {
            Optional<Expression> icebergExpression = FlinkFilters.convert(resolvedExpression);
            if (icebergExpression.isPresent()) {
                expressions.add(icebergExpression.get());
                acceptedFilters.add(resolvedExpression);
            }
        }

        this.filters = expressions;
        return Result.of(acceptedFilters, flinkFilters);
    }

    @Override
    public boolean supportsNestedProjection() {
        // TODO: support nested projection
        return false;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {

        final TypeInformation<RowData> typeInfo =
                runtimeProviderContext.createTypeInformation(producedDataType);
        return new DataStreamScanProvider() {

            @Override
            public DataStream<RowData> produceDataStream(
                    ProviderContext providerContext, StreamExecutionEnvironment execEnv) {
                return createFLIP27Stream(execEnv, typeInfo);
            }

            @Override
            public boolean isBounded() {
                return FlinkSource.isBounded(properties);
            }
        };
    }

    @Override
    public DynamicTableSource copy() {
        return new IcebergTableSource(this);
    }

    @Override
    public String asSummaryString() {
        return "Iceberg table source";
    }

    @Override
    public Map<String, DataType> listReadableMetadata() {
        return Stream.of(IcebergReadableMetadata.values())
                .collect(
                        Collectors.toMap(
                                IcebergReadableMetadata::getKey, IcebergReadableMetadata::getDataType));

    }

    @Override
    public void applyReadableMetadata(List<String> metadataKeys, DataType producedDataType) {
        this.metadataKeys = metadataKeys;
        this.producedDataType = producedDataType;
    }

    private MetadataConverter[] getMetadataConverters() {
        if (CollectionUtils.isEmpty(metadataKeys)) {
            return new MetadataConverter[0];
        }
        return metadataKeys.stream()
                .map(
                        key -> Stream.of(IcebergReadableMetadata.values())
                                .filter(m -> m.getKey().equals(key))
                                .findFirst()
                                .orElseThrow(IllegalStateException::new))
                .map(IcebergReadableMetadata::getConverter)
                .toArray(MetadataConverter[]::new);
    }
}
