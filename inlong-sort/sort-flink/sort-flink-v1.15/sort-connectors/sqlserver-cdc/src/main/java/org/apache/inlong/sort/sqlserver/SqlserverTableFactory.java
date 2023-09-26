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

package org.apache.inlong.sort.sqlserver;

import com.ververica.cdc.connectors.base.options.StartupOptions;
import com.ververica.cdc.connectors.sqlserver.table.SqlServerTableSource;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;

import java.time.Duration;
import java.time.ZoneId;
import java.util.HashSet;
import java.util.Set;

import static com.ververica.cdc.debezium.table.DebeziumOptions.DEBEZIUM_OPTIONS_PREFIX;
import static com.ververica.cdc.debezium.table.DebeziumOptions.getDebeziumProperties;
import static com.ververica.cdc.debezium.utils.ResolvedSchemaUtils.getPhysicalSchema;
import static org.apache.inlong.sort.base.Constants.*;

public class SqlserverTableFactory implements DynamicTableSourceFactory {

    private static final String IDENTIFIER = "sqlserver-cdc-inlong";

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
        options.add(TABLE_NAME);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(PORT);
        options.add(SERVER_TIME_ZONE);
        options.add(SCAN_STARTUP_MODE);
        options.add(INLONG_METRIC);
        options.add(INLONG_AUDIT);
        options.add(AUDIT_KEYS);
        options.add(ENABLE_PARALLEL_READ);
        options.add(SPLIT_SIZE);
        options.add(SPLIT_META_GROUP_SIZE);
        options.add(FETCH_SIZE);
        options.add(CONNECT_TIMEOUT);
        options.add(CONNECT_POOL_SIZE);
        options.add(CONNECT_MAX_RETRIES);
        options.add(DISTRIBUTION_FACTOR_UPPER);
        options.add(DISTRIBUTION_FACTOR_LOWER);
        options.add(CHUNK_KEY_COLUMN);
        options.add(CLOSE_ID_READERS);
        return options;
    }

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
        String tableName = config.get(TABLE_NAME);
        String inlongMetric = config.getOptional(INLONG_METRIC).orElse(null);
        String auditHostAndPorts = config.get(INLONG_AUDIT);
        Boolean enableParallelRead = config.get(ENABLE_PARALLEL_READ);
        Integer splitSize = config.get(SPLIT_SIZE);
        Integer splitMetaGroupSize = config.get(SPLIT_META_GROUP_SIZE);
        Integer fetchSize = config.get(FETCH_SIZE);
        Duration connectTimeout = Duration.parse(config.get(CONNECT_TIMEOUT));
        Integer connectMaxRetries = config.get(CONNECT_MAX_RETRIES);
        Integer connectionPoolSize = config.get(CONNECT_POOL_SIZE);
        Double distributionFactorUpper = config.get(DISTRIBUTION_FACTOR_UPPER);
        Double distributionFactorLower = config.get(DISTRIBUTION_FACTOR_LOWER);
        String chunkKeyColumn = config.get(CHUNK_KEY_COLUMN);
        Boolean closeIdleReaders = config.get(CLOSE_ID_READERS);
        ZoneId serverTimeZone = ZoneId.of(config.get(SERVER_TIME_ZONE));
        int port = config.get(PORT);
        ResolvedSchema physicalSchema =
                getPhysicalSchema(context.getCatalogTable().getResolvedSchema());

        return new SqlServerTableSource(
                physicalSchema,
                port,
                hostname,
                databaseName,
                tableName,
                serverTimeZone,
                username,
                password,
                getDebeziumProperties(context.getCatalogTable().getOptions()),
                getStartupOptions(config),
                enableParallelRead,
                splitSize,
                splitMetaGroupSize,
                fetchSize,
                connectTimeout,
                connectMaxRetries,
                connectionPoolSize,
                distributionFactorUpper,
                distributionFactorLower,
                chunkKeyColumn,
                closeIdleReaders);
    }

    private static final ConfigOption<String> HOSTNAME =
            ConfigOptions.key("hostname")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("IP address or hostname of the SqlServer database server.");

    private static final ConfigOption<Integer> PORT =
            ConfigOptions.key("port")
                    .intType()
                    .defaultValue(1433)
                    .withDescription("Integer port number of the SqlServer database server.");

    private static final ConfigOption<Boolean> ENABLE_PARALLEL_READ =
            ConfigOptions.key("enable-parallel-read")
                    .booleanType()
                    .noDefaultValue()
                    .withDescription("Whether to enable parallel read function.");

    private static final ConfigOption<Integer> SPLIT_SIZE =
            ConfigOptions.key("split-size")
                    .intType()
                    .noDefaultValue()
                    .withDescription("The split size.");

    private static final ConfigOption<Integer> SPLIT_META_GROUP_SIZE =
            ConfigOptions.key("split-meta-group-size")
                    .intType()
                    .noDefaultValue()
                    .withDescription("The split meta group size.");

    private static final ConfigOption<Integer> FETCH_SIZE =
            ConfigOptions.key("fetch-size")
                    .intType()
                    .noDefaultValue()
                    .withDescription("The fetch size.");

    private static final ConfigOption<String> CONNECT_TIMEOUT =
            ConfigOptions.key("connect-timeout")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The connect timeout of connection.");

    private static final ConfigOption<Integer> CONNECT_MAX_RETRIES =
            ConfigOptions.key("connect-max-retries")
                    .intType()
                    .noDefaultValue()
                    .withDescription("The max retries of the connection.");

    private static final ConfigOption<Integer> CONNECT_POOL_SIZE =
            ConfigOptions.key("connect-pool-size")
                    .intType()
                    .noDefaultValue()
                    .withDescription("The pool size of connection");

    private static final ConfigOption<Double> DISTRIBUTION_FACTOR_UPPER =
            ConfigOptions.key("distribution-factor-upper")
                    .doubleType()
                    .noDefaultValue()
                    .withDescription("The distribution factor upper.");

    private static final ConfigOption<Double> DISTRIBUTION_FACTOR_LOWER =
            ConfigOptions.key("distribution-factor-lower")
                    .doubleType()
                    .noDefaultValue()
                    .withDescription("The distribution factor lower.");

    private static final ConfigOption<String> CHUNK_KEY_COLUMN =
            ConfigOptions.key("chunk-key-column")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The chunk key column.");

    private static final ConfigOption<Boolean> CLOSE_ID_READERS =
            ConfigOptions.key("close-id-readers")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("The close id readers.");

    private static final ConfigOption<String> USERNAME =
            ConfigOptions.key("username")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Name of the SqlServer database to use when connecting to the SqlServer database server.");

    private static final ConfigOption<String> PASSWORD =
            ConfigOptions.key("password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Password to use when connecting to the SqlServer database server.");

    private static final ConfigOption<String> DATABASE_NAME =
            ConfigOptions.key("database-name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Database name of the SqlServer server to monitor.");

    private static final ConfigOption<String> TABLE_NAME =
            ConfigOptions.key("table-name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Table name of the SqlServer database to monitor.");

    public static final ConfigOption<String> SERVER_TIME_ZONE =
            ConfigOptions.key("server-time-zone")
                    .stringType()
                    .defaultValue("UTC")
                    .withDescription("The session time zone in database server.");

    public static final ConfigOption<String> SCAN_STARTUP_MODE =
            ConfigOptions.key("scan.startup.mode")
                    .stringType()
                    .defaultValue("initial")
                    .withDescription(
                            "Optional startup mode for SqlServer CDC consumer, valid enumerations are "
                                    + "\"initial\", \"initial-only\", \"latest-offset\"");

    private static final String SCAN_STARTUP_MODE_VALUE_INITIAL = "initial";
    private static final String SCAN_STARTUP_MODE_VALUE_INITIAL_ONLY = "initial-only";
    private static final String SCAN_STARTUP_MODE_VALUE_LATEST = "latest-offset";

    private static final String SCAN_STARTUP_MODE_VALUE_EARLIEST = "earliest-offset";

    private static StartupOptions getStartupOptions(ReadableConfig config) {
        String modeString = config.get(SCAN_STARTUP_MODE);

        switch (modeString.toLowerCase()) {
            case SCAN_STARTUP_MODE_VALUE_INITIAL:
                return StartupOptions.initial();

            case SCAN_STARTUP_MODE_VALUE_EARLIEST:
                return StartupOptions.earliest();

            case SCAN_STARTUP_MODE_VALUE_LATEST:
                return StartupOptions.latest();

            default:
                throw new ValidationException(
                        String.format(
                                "Invalid value for option '%s'. Supported values are [%s, %s, %s], but was: %s",
                                SCAN_STARTUP_MODE.key(),
                                SCAN_STARTUP_MODE_VALUE_INITIAL,
                                SCAN_STARTUP_MODE_VALUE_INITIAL_ONLY,
                                SCAN_STARTUP_MODE_VALUE_LATEST,
                                modeString));
        }
    }
}
