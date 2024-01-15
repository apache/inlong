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

package org.apache.inlong.sort.cdc.dm.table;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.connector.source.abilities.SupportsReadingMetadata;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.inlong.sort.cdc.dm.DMSource;
import org.apache.inlong.sort.cdc.dm.source.RowDataDMDeserializationSchema;

import java.time.Duration;
import java.time.ZoneId;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** A {@link DynamicTableSource} implementation for DM. */
public class DMTableSource implements ScanTableSource, SupportsReadingMetadata {

    private final ResolvedSchema physicalSchema;

    private final StartupMode startupMode;
    private final String username;
    private final String password;
    private final String databaseName;
    private final String schemaName;
    private final String tableName;
    private final String tableList;
    private final Duration connectTimeout;
    private final String serverTimeZone;
    private final String hostname;
    private final Integer port;
    private final Properties jdbcProperties;
    private final Long startupTimestamp;
    private final String inlongMetrics;
    private final String inlongAudit;

    // --------------------------------------------------------------------------------------------
    // Mutable attributes
    // --------------------------------------------------------------------------------------------

    /** Data type that describes the final output of the source. */
    protected DataType producedDataType;

    /** Metadata that is appended at the end of a physical source row. */
    protected List<String> metadataKeys;

    public DMTableSource(
            ResolvedSchema physicalSchema,
            StartupMode startupMode,
            String username,
            String password,
            String databaseName,
            String schemaName,
            String tableName,
            String tableList,
            String serverTimeZone,
            Duration connectTimeout,
            String hostname,
            Integer port,
            Properties jdbcProperties,
            Long startupTimestamp,
            String inlongMetrics,
            String inlongAudit) {
        this.physicalSchema = physicalSchema;
        this.startupMode = checkNotNull(startupMode);
        this.username = checkNotNull(username);
        this.password = checkNotNull(password);
        this.databaseName = databaseName;
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.tableList = tableList;
        this.serverTimeZone = serverTimeZone;
        this.connectTimeout = connectTimeout;
        this.hostname = hostname;
        this.port = port;
        this.jdbcProperties = jdbcProperties;
        this.startupTimestamp = startupTimestamp;
        this.producedDataType = physicalSchema.toPhysicalRowDataType();
        this.metadataKeys = Collections.emptyList();
        this.inlongMetrics = inlongMetrics;
        this.inlongAudit = inlongAudit;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.all();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext context) {
        RowType physicalDataType =
                (RowType) physicalSchema.toPhysicalRowDataType().getLogicalType();
        DMMetadataConverter[] metadataConverters = getMetadataConverters();
        TypeInformation<RowData> resultTypeInfo = context.createTypeInformation(producedDataType);

        RowDataDMDeserializationSchema deserializer =
                RowDataDMDeserializationSchema.newBuilder()
                        .setPhysicalRowType(physicalDataType)
                        .setMetadataConverters(metadataConverters)
                        .setResultTypeInfo(resultTypeInfo)
                        .setServerTimeZone(ZoneId.of(serverTimeZone))
                        .build();

        DMSource.Builder<RowData> builder =
                DMSource.<RowData>builder()
                        .startupMode(startupMode)
                        .username(username)
                        .password(password)
                        .databaseName(databaseName)
                        .schemaName(schemaName)
                        .tableName(tableName)
                        .tableList(tableList)
                        .serverTimeZone(serverTimeZone)
                        .connectTimeout(connectTimeout)
                        .hostname(hostname)
                        .port(port)
                        .jdbcProperties(jdbcProperties)
                        .startupTimestamp(startupTimestamp)
                        .deserializer(deserializer)
                        .inlongMetric(inlongMetrics)
                        .auditHostAndPorts(inlongAudit);
        return SourceFunctionProvider.of(builder.build(), false);
    }

    protected DMMetadataConverter[] getMetadataConverters() {
        if (metadataKeys.isEmpty()) {
            return new DMMetadataConverter[0];
        }
        return metadataKeys.stream()
                .map(
                        key -> Stream.of(DMReadableMetadata.values())
                                .filter(m -> m.getKey().equals(key))
                                .findFirst()
                                .orElseThrow(IllegalStateException::new))
                .map(DMReadableMetadata::getConverter)
                .toArray(DMMetadataConverter[]::new);
    }

    @Override
    public Map<String, DataType> listReadableMetadata() {
        return Stream.of(DMReadableMetadata.values())
                .collect(
                        Collectors.toMap(
                                DMReadableMetadata::getKey,
                                DMReadableMetadata::getDataType));
    }

    @Override
    public void applyReadableMetadata(List<String> metadataKeys, DataType producedDataType) {
        this.metadataKeys = metadataKeys;
        this.producedDataType = producedDataType;
    }

    @Override
    public DynamicTableSource copy() {
        DMTableSource source =
                new DMTableSource(
                        physicalSchema,
                        startupMode,
                        username,
                        password,
                        databaseName,
                        schemaName,
                        tableName,
                        tableList,
                        serverTimeZone,
                        connectTimeout,
                        hostname,
                        port,
                        jdbcProperties,
                        startupTimestamp,
                        inlongMetrics,
                        inlongAudit);
        source.metadataKeys = metadataKeys;
        source.producedDataType = producedDataType;
        return source;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DMTableSource that = (DMTableSource) o;
        return Objects.equals(this.physicalSchema, that.physicalSchema)
                && Objects.equals(this.startupMode, that.startupMode)
                && Objects.equals(this.username, that.username)
                && Objects.equals(this.password, that.password)
                && Objects.equals(this.databaseName, that.databaseName)
                && Objects.equals(this.schemaName, that.schemaName)
                && Objects.equals(this.tableName, that.tableName)
                && Objects.equals(this.tableList, that.tableList)
                && Objects.equals(this.serverTimeZone, that.serverTimeZone)
                && Objects.equals(this.connectTimeout, that.connectTimeout)
                && Objects.equals(this.hostname, that.hostname)
                && Objects.equals(this.port, that.port)
                && Objects.equals(this.jdbcProperties, that.jdbcProperties)
                && Objects.equals(this.startupTimestamp, that.startupTimestamp)
                && Objects.equals(this.producedDataType, that.producedDataType)
                && Objects.equals(this.metadataKeys, that.metadataKeys)
                && Objects.equals(this.inlongMetrics, that.inlongMetrics)
                && Objects.equals(this.inlongAudit, that.inlongAudit);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                physicalSchema,
                startupMode,
                username,
                password,
                databaseName,
                tableName,
                tableList,
                serverTimeZone,
                connectTimeout,
                hostname,
                port,
                jdbcProperties,
                startupTimestamp,
                producedDataType,
                metadataKeys,
                inlongMetrics,
                inlongAudit);
    }

    @Override
    public String asSummaryString() {
        return "DM-CDC";
    }
}
