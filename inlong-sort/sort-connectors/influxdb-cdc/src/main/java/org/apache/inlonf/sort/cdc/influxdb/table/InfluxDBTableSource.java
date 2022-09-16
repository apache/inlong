/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.inlonf.sort.cdc.influxdb.table;

import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.connector.source.abilities.SupportsReadingMetadata;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;
import org.apache.inlonf.sort.cdc.influxdb.DebeziumSourceFunction;
import org.apache.inlonf.sort.cdc.influxdb.InfluxDBSource;

public class InfluxDBTableSource implements ScanTableSource, SupportsReadingMetadata {

    private final ResolvedSchema physicalSchema;

    private Integer port = 8000;

    private Integer linesPerRequest = 1000;

    private Integer ingestQueueCapacity = 1000;

    private Long enqueueTimeout = 5L;

    private String serverURL = "http://127.0.0.1:8086";

    private String username = "root";

    private String password = "root";

    private Integer connectTimeout = 10;

    private Integer writeTimeout = 10;

    private Integer readTimeout = 10;

    private Boolean retryOnConnectionFailure = true;

    /** Data type that describes the final output of the source. */
    protected DataType producedDataType;
    /** Metadata that is appended at the end of a physical source row. */
    protected List<String> metadataKeys;
    private final String inlongMetric;
    private final String inlongAudit;

    public InfluxDBTableSource(ResolvedSchema physicalSchema, int port, int linesPerRequest,
                               int ingestQueueCapacity, long enqueueTimeout,String serverURL,
                               String username, String password, int connectTimeout, int writeTimeout,
                               int readTimeout, boolean retryOnConnectionFailure,
                               String inlongMetric, String inlongAudit) {
        this.physicalSchema = physicalSchema;
        this.port = port;
        this.linesPerRequest = linesPerRequest;
        this.ingestQueueCapacity = ingestQueueCapacity;
        this.enqueueTimeout = enqueueTimeout;
        this.serverURL = serverURL;
        this.username = username;
        this.password = password;
        this.connectTimeout = connectTimeout;
        this.writeTimeout = writeTimeout;
        this.readTimeout = readTimeout;
        this.retryOnConnectionFailure = retryOnConnectionFailure;
        this.producedDataType =  physicalSchema.toPhysicalRowDataType();
        this.metadataKeys = Collections.emptyList();
        this.inlongMetric = inlongMetric;
        this.inlongAudit = inlongAudit;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.newBuilder()
                .addContainedKind(RowKind.INSERT)
                .addContainedKind(RowKind.UPDATE_AFTER)
                .addContainedKind(RowKind.DELETE)
                .build();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        //1. Data deserializer
        // zcy
        DebeziumDeserializationSchema<RowData> deserializer = null;
        InfluxDBSource.Builder<RowData> builder =
                InfluxDBSource.<RowData>builder()
                        .port(port)
                        .enqueueTimeout(enqueueTimeout)
                        .ingestQueueCapacity(ingestQueueCapacity)
                        .linesPerRequest(linesPerRequest)
                        .serverURL(serverURL)
                        .username(username)
                        .password(password)
                        .connectTimeout(connectTimeout)
                        .writeTimeout(writeTimeout)
                        .readTimeout(readTimeout)
                        .retryOnConnectionFailure(retryOnConnectionFailure)
                        .deserializer(deserializer);

        Optional.ofNullable(inlongMetric).ifPresent(builder::inlongMetric);
        Optional.ofNullable(inlongAudit).ifPresent(builder::inlongAudit);
        DebeziumSourceFunction<RowData> sourceFunction = builder.build();

        return SourceFunctionProvider.of(sourceFunction, false);
    }

    @Override
    public DynamicTableSource copy() {
        return null;
    }

    @Override
    public String asSummaryString() {
        return "InfluxB-CDC";
    }

    @Override
    public Map<String, DataType> listReadableMetadata() {
        return null;
    }

    @Override
    public void applyReadableMetadata(List<String> metadataKeys, DataType producedDataType) {
        this.metadataKeys = metadataKeys;
        this.producedDataType = producedDataType;
    }
}
