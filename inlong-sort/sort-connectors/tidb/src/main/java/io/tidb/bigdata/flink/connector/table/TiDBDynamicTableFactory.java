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

import static io.tidb.bigdata.flink.connector.source.TiDBOptions.DATABASE_NAME;
import static io.tidb.bigdata.flink.connector.source.TiDBOptions.STREAMING_SOURCE;
import static io.tidb.bigdata.flink.connector.source.TiDBOptions.WRITE_MODE;

import com.google.common.collect.ImmutableSet;
import io.tidb.bigdata.flink.connector.source.TiDBOptions;
import io.tidb.bigdata.tidb.ClientConfig;
import io.tidb.bigdata.tidb.ClientSession;
import io.tidb.bigdata.tidb.TiDBWriteMode;
import java.time.Duration;
import java.util.Set;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.internal.options.JdbcDmlOptions;
import org.apache.flink.connector.jdbc.internal.options.JdbcLookupOptions;
import org.apache.flink.connector.jdbc.internal.options.JdbcOptions;
import org.apache.flink.connector.jdbc.table.JdbcDynamicTableSink;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;

public class TiDBDynamicTableFactory implements DynamicTableSourceFactory, DynamicTableSinkFactory {

    public static final String IDENTIFIER = "tidb";

    /**
     * see ${@link org.apache.flink.connector.jdbc.table.JdbcDynamicTableFactory}
     */
    public static final ConfigOption<Integer> SINK_BUFFER_FLUSH_MAX_ROWS = ConfigOptions
            .key("sink.buffer-flush.max-rows")
            .intType()
            .defaultValue(100)
            .withDescription(
                    "the flush max size (includes all append, upsert and delete records), over this number"
                            + " of records, will flush data. The default value is 100.");
    private static final ConfigOption<Duration> SINK_BUFFER_FLUSH_INTERVAL = ConfigOptions
            .key("sink.buffer-flush.interval")
            .durationType()
            .defaultValue(Duration.ofSeconds(1))
            .withDescription(
                    "the flush interval mills, over this time, asynchronous threads will flush data. The "
                            + "default value is 1s.");
    public static final ConfigOption<Integer> SINK_MAX_RETRIES = ConfigOptions
            .key("sink.max-retries")
            .intType()
            .defaultValue(3)
            .withDescription("the max retry times if writing records to database failed.");
    public static final ConfigOption<Long> LOOKUP_CACHE_MAX_ROWS = ConfigOptions
            .key("lookup.cache.max-rows")
            .longType()
            .defaultValue(-1L)
            .withDescription("the max number of rows of lookup cache, over this value, "
                    + "the oldest rows will be eliminated. \"cache.max-rows\" and \"cache.ttl\" options "
                    + "must all be specified if any of them is specified. "
                    + "Cache is not enabled as default.");
    public static final ConfigOption<Duration> LOOKUP_CACHE_TTL = ConfigOptions
            .key("lookup.cache.ttl")
            .durationType()
            .defaultValue(Duration.ofSeconds(10))
            .withDescription("the cache time to live.");
    public static final ConfigOption<Integer> LOOKUP_MAX_RETRIES = ConfigOptions
            .key("lookup.max-retries")
            .intType()
            .defaultValue(3)
            .withDescription("the max retry times if lookup database failed.");
    public static final ConfigOption<Boolean> APPEND_MODE =
            ConfigOptions.key("append-mode")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Whether works as append source.");
    public static final ConfigOption<Boolean> INIT_MODE =
            ConfigOptions.key("init-mode")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("Whether to synchronize inventory data.");

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return TiDBOptions.requiredOptions();
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        ReadableConfig config = helper.getOptions();
        final boolean appendMode = config.get(APPEND_MODE);
        final boolean initMode = config.get(INIT_MODE);

        boolean streamingPresent = config.getOptional(STREAMING_SOURCE).isPresent();
        ChangelogMode changelogMode = ChangelogMode.insertOnly();
        if (streamingPresent) {
            changelogMode = ChangelogMode.all();
            if (appendMode) {
                changelogMode = ChangelogMode.insertOnly();
            }
        }
        return new TiDBDynamicTableSource(context.getCatalogTable(),
                changelogMode,
                new JdbcLookupOptions(
                        config.get(LOOKUP_CACHE_MAX_ROWS),
                        config.get(LOOKUP_CACHE_TTL).toMillis(),
                        config.get(LOOKUP_MAX_RETRIES)),
                appendMode,
                initMode);
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return TiDBOptions.withMoreOptionalOptions(
                SINK_BUFFER_FLUSH_INTERVAL,
                SINK_BUFFER_FLUSH_MAX_ROWS,
                SINK_MAX_RETRIES,
                APPEND_MODE,
                INIT_MODE);
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil
                .createTableFactoryHelper(this, context);
        ReadableConfig config = helper.getOptions();
        TableSchema schema = context.getCatalogTable().getSchema();
        String databaseName = config.get(DATABASE_NAME);
        // jdbc options
        JdbcOptions jdbcOptions = JdbcUtils.getJdbcOptions(context.getCatalogTable().toProperties());
        // execution options
        JdbcExecutionOptions jdbcExecutionOptions = JdbcExecutionOptions.builder()
                .withBatchSize(config.get(SINK_BUFFER_FLUSH_MAX_ROWS))
                .withBatchIntervalMs(config.get(SINK_BUFFER_FLUSH_INTERVAL).toMillis())
                .withMaxRetries(config.get(SINK_MAX_RETRIES))
                .build();
        // dml options
        JdbcDmlOptions jdbcDmlOptions = JdbcDmlOptions.builder()
                .withTableName(jdbcOptions.getTableName())
                .withDialect(jdbcOptions.getDialect())
                .withFieldNames(schema.getFieldNames())
                .withKeyFields(getKeyFields(context, config, databaseName, jdbcOptions.getTableName()))
                .build();

        return new JdbcDynamicTableSink(jdbcOptions, jdbcExecutionOptions, jdbcDmlOptions, schema);
    }

    private String[] getKeyFields(Context context, ReadableConfig config, String databaseName,
            String tableName) {
        // check write mode
        TiDBWriteMode writeMode = TiDBWriteMode.fromString(config.get(WRITE_MODE));
        String[] keyFields = null;
        if (writeMode == TiDBWriteMode.UPSERT) {
            try (ClientSession clientSession = ClientSession.create(
                    new ClientConfig(context.getCatalogTable().toProperties()))) {
                Set<String> set = ImmutableSet.<String>builder()
                        .addAll(clientSession.getUniqueKeyColumns(databaseName, tableName))
                        .addAll(clientSession.getPrimaryKeyColumns(databaseName, tableName))
                        .build();
                keyFields = set.size() == 0 ? null : set.toArray(new String[0]);
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        }
        return keyFields;
    }
}