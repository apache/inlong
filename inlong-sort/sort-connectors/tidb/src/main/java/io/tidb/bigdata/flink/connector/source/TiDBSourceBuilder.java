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

import static io.tidb.bigdata.flink.connector.source.TiDBOptions.DATABASE_NAME;
import static io.tidb.bigdata.flink.connector.source.TiDBOptions.STREAMING_CODEC;
import static io.tidb.bigdata.flink.connector.source.TiDBOptions.STREAMING_CODEC_CRAFT;
import static io.tidb.bigdata.flink.connector.source.TiDBOptions.STREAMING_CODEC_JSON;
import static io.tidb.bigdata.flink.connector.source.TiDBOptions.STREAMING_SOURCE;
import static io.tidb.bigdata.flink.connector.source.TiDBOptions.STREAMING_SOURCE_KAFKA;
import static io.tidb.bigdata.flink.connector.source.TiDBOptions.TABLE_NAME;
import static io.tidb.bigdata.flink.connector.source.TiDBOptions.VALID_STREAMING_CODECS;
import static io.tidb.bigdata.flink.connector.source.TiDBOptions.VALID_STREAMING_SOURCES;
import static io.tidb.bigdata.flink.format.cdc.CDCOptions.IGNORE_PARSE_ERRORS;

import io.tidb.bigdata.flink.connector.source.enumerator.TiDBSourceSplitEnumerator;
import io.tidb.bigdata.tidb.ClientConfig;
import java.io.Serializable;
import java.sql.Timestamp;
import java.time.ZonedDateTime;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.connector.base.source.hybrid.HybridSource;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Preconditions;
import org.tikv.common.expression.Expression;
import org.tikv.common.meta.TiTimestamp;

public class TiDBSourceBuilder implements Serializable {

    private String databaseName;
    private String tableName;
    private String streamingSource;
    private String streamingCodec;
    private Map<String, String> properties;
    private boolean ignoreParseErrors;
    private final TiDBSchemaAdapter schema;
    private final Expression expression;
    private final Integer limit;
    private final boolean appendMode;
    private final boolean initMode;

    public TiDBSourceBuilder(ResolvedCatalogTable table,
            Function<DataType, TypeInformation<RowData>> typeInfoFactory,
            TiDBMetadata[] metadata,
            int[] projectedFields,
            Expression expression,
            Integer limit,
            boolean appendMode,
            boolean initMode) {
        this.schema = new TiDBSchemaAdapter(table, typeInfoFactory, metadata, projectedFields);
        setProperties(table.getOptions());
        this.expression = expression;
        this.limit = limit;
        this.appendMode = appendMode;
        this.initMode = initMode;
    }

    private static String validateRequired(String key, String value) {
        Preconditions.checkNotNull(value, "'%s' is not set", key);
        Preconditions.checkArgument(!value.trim().isEmpty(),
                "'%s' is not set", key);
        return value;
    }

    private static String validateProperty(String key, String value, Set<String> validOptions) {
        if (!validOptions.contains(value)) {
            throw new IllegalArgumentException("Invalid value '" + value + "' for '" + key + "'");
        }
        return value;
    }

    private String getRequiredProperty(String key) {
        return validateRequired(key, properties.get(key));
    }

    private Optional<String> getOptionalProperty(String key) {
        return Optional.ofNullable(properties.get(key));
    }

    private TiDBSourceBuilder setProperties(Map<String, String> properties) {
        this.properties = properties;
        this.databaseName = getRequiredProperty(DATABASE_NAME.key());
        this.tableName = getRequiredProperty(TABLE_NAME.key());
        this.streamingSource = getOptionalProperty(STREAMING_SOURCE.key())
                .map(v -> validateProperty(STREAMING_SOURCE.key(), v, VALID_STREAMING_SOURCES))
                .orElse(null);
        this.streamingCodec = getOptionalProperty(STREAMING_CODEC.key())
                .map(v -> validateProperty(STREAMING_CODEC.key(), v, VALID_STREAMING_CODECS))
                .orElse(STREAMING_CODEC_CRAFT);
        this.ignoreParseErrors = getOptionalProperty(IGNORE_PARSE_ERRORS.key())
                .map(Boolean::parseBoolean).orElse(false);
        return this;
    }

    private CDCSourceBuilder createCDCBuilder(TiTimestamp timestamp) {
        if (streamingSource.equals(STREAMING_SOURCE_KAFKA)) {
            return CDCSourceBuilder
                    .kafka(databaseName, tableName, timestamp, schema, appendMode)
                    .<KafkaCDCSourceBuilder>ignoreParseErrors(ignoreParseErrors)
                    .setProperties(properties);
        } else {
            throw new IllegalArgumentException(
                    "Only kafka is supported as streaming source at this time");
        }
    }

    public Source<RowData, ?, ?> build() {
        if (initMode) {
            final SnapshotSource source = new SnapshotSource(databaseName, tableName, properties, schema,
                    expression, limit);
            if (streamingSource == null) {
                return source;
            }

            if (limit != null) {
                throw new IllegalStateException("Limit push down is not supported for streaming source");
            }
            if (new ClientConfig(properties).isFilterPushDown()) {
                throw new IllegalStateException("Filter push down is not supported for streaming source");
            }

            HybridSource.HybridSourceBuilder<RowData, TiDBSourceSplitEnumerator> builder =
                    HybridSource.builder(source);

            builder.addSource(
                    (switchContext) -> {
                        TiTimestamp ts;
                        if (switchContext.getPreviousEnumerator() == null) {
                            // restore from checkpoint
                            ts = new TiTimestamp(0, 0);
                        } else {
                            ts = switchContext.getPreviousEnumerator().getTimestamp();
                        }
                        return getCdcSource(ts);
                    },
                    Boundedness.CONTINUOUS_UNBOUNDED);
            return builder.build();
        }
        // only cdc source
        Optional<TiTimestamp> tiTimestamp = Optional.ofNullable(properties.get(ClientConfig.SNAPSHOT_TIMESTAMP))
                .filter(StringUtils::isNoneEmpty)
                .map(s -> new TiTimestamp(Timestamp.from(ZonedDateTime.parse(s).toInstant()).getTime(), 0));
        return getCdcSource(tiTimestamp.orElse(new TiTimestamp(0, 0)));

    }

    private Source<RowData, ?, ?> getCdcSource(TiTimestamp ts) {
        final CDCSourceBuilder cdcBuilder = createCDCBuilder(ts);
        switch (streamingCodec) {
            case STREAMING_CODEC_CRAFT:
                return cdcBuilder.craft();
            case STREAMING_CODEC_JSON:
                return cdcBuilder.json();
            default:
                throw new IllegalArgumentException("Invalid streaming codec: '"
                        + streamingCodec + "'");
        }
    }

}
