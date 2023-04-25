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

package org.apache.inlong.sort.cdc.postgres.table;

import com.ververica.cdc.connectors.base.options.StartupOptions;
import com.ververica.cdc.debezium.table.DebeziumChangelogMode;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;

import java.time.Duration;
import java.util.HashSet;
import java.util.Set;

import static com.ververica.cdc.debezium.table.DebeziumOptions.DEBEZIUM_OPTIONS_PREFIX;
import static com.ververica.cdc.debezium.table.DebeziumOptions.getDebeziumProperties;
import static com.ververica.cdc.debezium.utils.ResolvedSchemaUtils.getPhysicalSchema;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkState;
import static org.apache.inlong.sort.base.Constants.INLONG_AUDIT;
import static org.apache.inlong.sort.base.Constants.INLONG_METRIC;
import static org.apache.inlong.sort.base.Constants.SOURCE_MULTIPLE_ENABLE;

import static org.apache.inlong.sort.cdc.base.util.ObjectUtils.doubleCompare;
import static org.apache.inlong.sort.cdc.postgres.source.options.PostgresSourceOptions.CHANGELOG_MODE;
import static org.apache.inlong.sort.cdc.postgres.source.options.PostgresSourceOptions.CHUNK_META_GROUP_SIZE;
import static org.apache.inlong.sort.cdc.postgres.source.options.PostgresSourceOptions.CONNECTION_POOL_SIZE;
import static org.apache.inlong.sort.cdc.postgres.source.options.PostgresSourceOptions.CONNECT_MAX_RETRIES;
import static org.apache.inlong.sort.cdc.postgres.source.options.PostgresSourceOptions.CONNECT_TIMEOUT;
import static org.apache.inlong.sort.cdc.postgres.source.options.PostgresSourceOptions.HEARTBEAT_INTERVAL;
import static org.apache.inlong.sort.cdc.postgres.source.options.PostgresSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_CHUNK_KEY_COLUMN;
import static org.apache.inlong.sort.cdc.postgres.source.options.PostgresSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE;
import static org.apache.inlong.sort.cdc.postgres.source.options.PostgresSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_ENABLED;
import static org.apache.inlong.sort.cdc.postgres.source.options.PostgresSourceOptions.SCAN_SNAPSHOT_FETCH_SIZE;
import static org.apache.inlong.sort.cdc.postgres.source.options.PostgresSourceOptions.SCAN_STARTUP_MODE;
import static org.apache.inlong.sort.cdc.postgres.source.options.PostgresSourceOptions.SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND;
import static org.apache.inlong.sort.cdc.postgres.source.options.PostgresSourceOptions.SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND;

/**
 * Factory for creating configured instance of
 * {@link org.apache.inlong.sort.cdc.postgres.table.PostgreSQLTableSource}.
 */
public class PostgreSQLTableFactory implements DynamicTableSourceFactory {

    private static final String IDENTIFIER = "postgres-cdc-inlong";

    private static final ConfigOption<String> HOSTNAME =
            ConfigOptions.key("hostname")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("IP address or hostname of the PostgreSQL database server.");

    private static final ConfigOption<Integer> PORT =
            ConfigOptions.key("port")
                    .intType()
                    .defaultValue(5432)
                    .withDescription("Integer port number of the PostgreSQL database server.");

    private static final ConfigOption<String> USERNAME =
            ConfigOptions.key("username")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Name of the PostgreSQL database to use when connecting to the PostgreSQL database server"
                                    + ".");

    private static final ConfigOption<String> PASSWORD =
            ConfigOptions.key("password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Password to use when connecting to the PostgreSQL database server.");

    private static final ConfigOption<String> DATABASE_NAME =
            ConfigOptions.key("database-name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Database name of the PostgreSQL server to monitor.");

    private static final ConfigOption<String> SCHEMA_NAME =
            ConfigOptions.key("schema-name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Schema name of the PostgreSQL database to monitor.");

    private static final ConfigOption<String> TABLE_NAME =
            ConfigOptions.key("table-name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Table name of the PostgreSQL database to monitor.");

    private static final ConfigOption<String> DECODING_PLUGIN_NAME =
            ConfigOptions.key("decoding.plugin.name")
                    .stringType()
                    .defaultValue("decoderbufs")
                    .withDescription(
                            "The name of the Postgres logical decoding plug-in installed on the server.\n"
                                    + "Supported values are decoderbufs, wal2json, wal2json_rds, wal2json_streaming,\n"
                                    + "wal2json_rds_streaming and pgoutput.");

    private static final ConfigOption<String> SLOT_NAME =
            ConfigOptions.key("slot.name")
                    .stringType()
                    .defaultValue("flink")
                    .withDescription(
                            "The name of the PostgreSQL logical decoding slot that was created for streaming changes "
                                    + "from a particular plug-in for a particular database/schema. The server uses "
                                    + "this slot "
                                    + "to stream events to the connector that you are configuring. Default is "
                                    + "\"flink\".");

    /**
     * The session time zone in database server.
     */
    public static final ConfigOption<String> SERVER_TIME_ZONE =
            ConfigOptions.key("server-time-zone")
                    .stringType()
                    .defaultValue("UTC")
                    .withDescription("The session time zone in database server.");

    /**
     * row kinds to be filtered
     */
    public static final ConfigOption<String> ROW_KINDS_FILTERED =
            ConfigOptions.key("row-kinds-filtered")
                    .stringType()
                    .defaultValue("+I&-U&+U&-D")
                    .withDescription("row kinds to be filtered,"
                            + " here filtered means keep the data of certain row kind"
                            + "the format follows rowKind1&rowKind2, supported row kinds are "
                            + "\"+I\" represents INSERT.\n"
                            + "\"-U\" represents UPDATE_BEFORE.\n"
                            + "\"+U\" represents UPDATE_AFTER.\n"
                            + "\"-D\" represents DELETE.");

    /**
     * Whether works as append source.
     */
    public static final ConfigOption<Boolean> APPEND_MODE =
            ConfigOptions.key("append-mode")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Whether works as append source.");

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);
        helper.validateExcept(DEBEZIUM_OPTIONS_PREFIX);

        final ReadableConfig config = helper.getOptions();
        String hostname = config.get(HOSTNAME);
        String username = config.get(USERNAME);
        String password = config.get(PASSWORD);
        String databaseName = config.get(DATABASE_NAME);
        String schemaName = config.get(SCHEMA_NAME);
        String tableName = config.get(TABLE_NAME);
        int port = config.get(PORT);
        String pluginName = config.get(DECODING_PLUGIN_NAME);
        String slotName = config.get(SLOT_NAME);
        String serverTimeZone = config.get(SERVER_TIME_ZONE);
        // ResolvedSchema physicalSchema = context.getCatalogTable().getResolvedSchema();
        final boolean sourceMultipleEnable = config.get(SOURCE_MULTIPLE_ENABLE);
        String inlongMetric = config.getOptional(INLONG_METRIC).orElse(null);
        String inlongAudit = config.get(INLONG_AUDIT);
        final boolean appendSource = config.get(APPEND_MODE);
        final String rowKindFiltered = config.get(ROW_KINDS_FILTERED).isEmpty() ? ROW_KINDS_FILTERED.defaultValue()
                : config.get(ROW_KINDS_FILTERED);

        DebeziumChangelogMode changelogMode = config.get(CHANGELOG_MODE);
        ResolvedSchema physicalSchema =
                getPhysicalSchema(context.getCatalogTable().getResolvedSchema());
        if (changelogMode == DebeziumChangelogMode.UPSERT) {
            checkArgument(
                    physicalSchema.getPrimaryKey().isPresent(),
                    "Primary key must be present when upsert mode is selected.");
        }
        boolean enableParallelRead = config.get(SCAN_INCREMENTAL_SNAPSHOT_ENABLED);
        StartupOptions startupOptions = getStartupOptions(config);
        int splitSize = config.get(SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE);
        int splitMetaGroupSize = config.get(CHUNK_META_GROUP_SIZE);
        int fetchSize = config.get(SCAN_SNAPSHOT_FETCH_SIZE);
        Duration connectTimeout = config.get(CONNECT_TIMEOUT);
        int connectMaxRetries = config.get(CONNECT_MAX_RETRIES);
        int connectionPoolSize = config.get(CONNECTION_POOL_SIZE);
        double distributionFactorUpper = config.get(SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND);
        double distributionFactorLower = config.get(SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND);
        Duration heartbeatInterval = config.get(HEARTBEAT_INTERVAL);
        String chunkKeyColumn =
                config.getOptional(SCAN_INCREMENTAL_SNAPSHOT_CHUNK_KEY_COLUMN).orElse(null);

        if (enableParallelRead) {
            validateIntegerOption(SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE, splitSize, 1);
            validateIntegerOption(SCAN_SNAPSHOT_FETCH_SIZE, fetchSize, 1);
            validateIntegerOption(CHUNK_META_GROUP_SIZE, splitMetaGroupSize, 1);
            validateIntegerOption(CONNECTION_POOL_SIZE, connectionPoolSize, 1);
            validateIntegerOption(CONNECT_MAX_RETRIES, connectMaxRetries, 0);
            validateDistributionFactorUpper(distributionFactorUpper);
            validateDistributionFactorLower(distributionFactorLower);
        }

        return new PostgreSQLTableSource(
                physicalSchema,
                port,
                hostname,
                databaseName,
                schemaName,
                tableName,
                username,
                password,
                pluginName,
                slotName,
                serverTimeZone,
                getDebeziumProperties(context.getCatalogTable().getOptions()),
                appendSource,
                rowKindFiltered,
                sourceMultipleEnable,
                inlongMetric,
                inlongAudit,
                enableParallelRead,
                splitSize,
                splitMetaGroupSize,
                fetchSize,
                connectTimeout,
                connectMaxRetries,
                connectionPoolSize,
                distributionFactorUpper,
                distributionFactorLower,
                heartbeatInterval,
                startupOptions,
                chunkKeyColumn);
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(HOSTNAME);
        options.add(USERNAME);
        options.add(PASSWORD);
        options.add(DATABASE_NAME);
        options.add(SCHEMA_NAME);
        options.add(TABLE_NAME);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(PORT);
        options.add(DECODING_PLUGIN_NAME);
        options.add(SLOT_NAME);
        options.add(SERVER_TIME_ZONE);
        options.add(SOURCE_MULTIPLE_ENABLE);
        options.add(INLONG_METRIC);
        options.add(INLONG_AUDIT);
        options.add(APPEND_MODE);
        options.add(ROW_KINDS_FILTERED);
        options.add(CHANGELOG_MODE);
        options.add(SCAN_STARTUP_MODE);
        options.add(SCAN_INCREMENTAL_SNAPSHOT_ENABLED);
        options.add(SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE);
        options.add(SCAN_INCREMENTAL_SNAPSHOT_CHUNK_KEY_COLUMN);
        options.add(SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND);
        options.add(SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND);
        options.add(CHUNK_META_GROUP_SIZE);
        options.add(SCAN_SNAPSHOT_FETCH_SIZE);
        options.add(CONNECT_TIMEOUT);
        options.add(CONNECT_MAX_RETRIES);
        options.add(CONNECTION_POOL_SIZE);
        options.add(HEARTBEAT_INTERVAL);
        return options;
    }

    private static final String SCAN_STARTUP_MODE_VALUE_INITIAL = "initial";
    private static final String SCAN_STARTUP_MODE_VALUE_LATEST = "latest-offset";
    private static StartupOptions getStartupOptions(ReadableConfig config) {
        String modeString = config.get(SCAN_STARTUP_MODE);

        switch (modeString.toLowerCase()) {
            case SCAN_STARTUP_MODE_VALUE_INITIAL:
                return StartupOptions.initial();

            case SCAN_STARTUP_MODE_VALUE_LATEST:
                return StartupOptions.latest();

            default:
                throw new ValidationException(
                        String.format(
                                "Invalid value for option '%s'. Supported values are [%s, %s], but was: %s",
                                SCAN_STARTUP_MODE.key(),
                                SCAN_STARTUP_MODE_VALUE_INITIAL,
                                SCAN_STARTUP_MODE_VALUE_LATEST,
                                modeString));
        }
    }

    /** Checks the value of given integer option is valid. */
    private void validateIntegerOption(
            ConfigOption<Integer> option, int optionValue, int exclusiveMin) {
        checkState(
                optionValue > exclusiveMin,
                String.format(
                        "The value of option '%s' must larger than %d, but is %d",
                        option.key(), exclusiveMin, optionValue));
    }

    /** Checks the value of given evenly distribution factor upper bound is valid. */
    private void validateDistributionFactorUpper(double distributionFactorUpper) {
        checkState(
                doubleCompare(distributionFactorUpper, 1.0d) >= 0,
                String.format(
                        "The value of option '%s' must larger than or equals %s, but is %s",
                        SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND.key(),
                        1.0d,
                        distributionFactorUpper));
    }

    /** Checks the value of given evenly distribution factor lower bound is valid. */
    private void validateDistributionFactorLower(double distributionFactorLower) {
        checkState(
                doubleCompare(distributionFactorLower, 0.0d) >= 0
                        && doubleCompare(distributionFactorLower, 1.0d) <= 0,
                String.format(
                        "The value of option '%s' must between %s and %s inclusively, but is %s",
                        SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND.key(),
                        0.0d,
                        1.0d,
                        distributionFactorLower));
    }
}
