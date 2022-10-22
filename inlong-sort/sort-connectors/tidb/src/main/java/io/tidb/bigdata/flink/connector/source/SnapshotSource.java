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

package io.tidb.bigdata.flink.connector.source;

import io.tidb.bigdata.flink.connector.source.enumerator.TiDBSourceSplitEnumState;
import io.tidb.bigdata.flink.connector.source.enumerator.TiDBSourceSplitEnumStateSerializer;
import io.tidb.bigdata.flink.connector.source.enumerator.TiDBSourceSplitEnumerator;
import io.tidb.bigdata.flink.connector.source.reader.TiDBSourceReader;
import io.tidb.bigdata.flink.connector.source.reader.TiDBSourceSplitReader;
import io.tidb.bigdata.flink.connector.source.split.TiDBSourceSplit;
import io.tidb.bigdata.flink.connector.source.split.TiDBSourceSplitSerializer;
import io.tidb.bigdata.tidb.ClientConfig;
import io.tidb.bigdata.tidb.ClientSession;
import io.tidb.bigdata.tidb.ColumnHandleInternal;
import java.sql.Timestamp;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.table.data.RowData;
import org.tikv.common.expression.Expression;
import org.tikv.common.meta.TiTimestamp;

public class SnapshotSource implements Source<RowData, TiDBSourceSplit, TiDBSourceSplitEnumState>,
        ResultTypeQueryable<RowData> {

    private final String databaseName;
    private final String tableName;
    private final Map<String, String> properties;
    private final TiDBSchemaAdapter schema;
    private final Expression expression;
    private final Integer limit;
    private final TiTimestamp timestamp;

    public SnapshotSource(String databaseName, String tableName,
            Map<String, String> properties, TiDBSchemaAdapter schema,
            Expression expression, Integer limit) {
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.properties = properties;
        this.schema = schema;
        this.expression = expression;
        this.limit = limit;
        this.timestamp = getOptionalVersion().orElseGet(() -> getOptionalTimestamp().orElse(null));
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    private static Configuration toConfiguration(Map<String, String> properties) {
        Configuration config = new Configuration();
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            config.setString(entry.getKey(), entry.getValue());
        }
        return config;
    }

    @Override
    public SourceReader<RowData, TiDBSourceSplit> createReader(SourceReaderContext context)
            throws Exception {

        ClientSession session = null;
        try {
            final Map<String, String> properties = this.properties;
            session = ClientSession.create(new ClientConfig(properties));
            final List<ColumnHandleInternal> columns =
                    session.getTableColumns(databaseName, tableName, schema.getPhysicalFieldNames())
                            .orElseThrow(() -> new NullPointerException("Could not get columns for TiDB table:"
                                    + databaseName + "." + tableName));
            final ClientSession s = session;
            schema.open();

            return new TiDBSourceReader(
                    () -> new TiDBSourceSplitReader(s, columns, schema, expression, limit, timestamp),
                    toConfiguration(properties), context);
        } catch (Exception ex) {
            if (session != null) {
                session.close();
            }
            throw ex;
        }
    }

    @Override
    public SplitEnumerator<TiDBSourceSplit, TiDBSourceSplitEnumState> createEnumerator(
            SplitEnumeratorContext<TiDBSourceSplit> context) {
        return new TiDBSourceSplitEnumerator(this.properties, context);
    }

    @Override
    public SplitEnumerator<TiDBSourceSplit, TiDBSourceSplitEnumState> restoreEnumerator(
            SplitEnumeratorContext<TiDBSourceSplit> context,
            TiDBSourceSplitEnumState state) {
        return new TiDBSourceSplitEnumerator(this.properties, context, state.assignedSplits());
    }

    @Override
    public SimpleVersionedSerializer<TiDBSourceSplit> getSplitSerializer() {
        return new TiDBSourceSplitSerializer();
    }

    @Override
    public SimpleVersionedSerializer<TiDBSourceSplitEnumState> getEnumeratorCheckpointSerializer() {
        return new TiDBSourceSplitEnumStateSerializer();
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return schema.getProducedType();
    }

    private Optional<TiTimestamp> getOptionalTimestamp() {
        return Optional
                .ofNullable(properties.get(ClientConfig.SNAPSHOT_TIMESTAMP))
                .filter(StringUtils::isNoneEmpty)
                .map(s -> new TiTimestamp(Timestamp.from(ZonedDateTime.parse(s).toInstant()).getTime(), 0));
    }

    private Optional<TiTimestamp> getOptionalVersion() {
        return Optional
                .ofNullable(properties.get(ClientConfig.SNAPSHOT_VERSION))
                .filter(StringUtils::isNoneEmpty)
                .map(Long::parseUnsignedLong)
                .map(tso -> new TiTimestamp(tso >> 18, tso & 0x3FFFF));
    }
}