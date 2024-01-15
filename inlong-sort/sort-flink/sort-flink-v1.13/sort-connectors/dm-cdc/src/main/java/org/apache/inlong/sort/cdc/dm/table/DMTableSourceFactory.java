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

import com.ververica.cdc.debezium.utils.JdbcUrlUtils;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.inlong.sort.cdc.dm.utils.OptionUtils;

import java.time.Duration;
import java.util.HashSet;
import java.util.Set;

import static org.apache.inlong.sort.base.Constants.AUDIT_KEYS;
import static org.apache.inlong.sort.base.Constants.INLONG_AUDIT;
import static org.apache.inlong.sort.base.Constants.INLONG_METRIC;
import static org.apache.inlong.sort.base.Constants.SOURCE_MULTIPLE_ENABLE;

/** Factory for creating configured instance of {@link DMTableSource}. */
public class DMTableSourceFactory implements DynamicTableSourceFactory {

    private static final String IDENTIFIER = "dm-cdc";

    public static final ConfigOption<String> SCAN_STARTUP_MODE =
            ConfigOptions.key("scan.startup.mode")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Optional startup mode for DM CDC consumer, valid enumerations are "
                                    + "\"initial\", \"latest-offset\" or \"timestamp\"");

    public static final ConfigOption<String> USERNAME =
            ConfigOptions.key("username")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Username to be used when connecting to DM.");

    public static final ConfigOption<String> PASSWORD =
            ConfigOptions.key("password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Password to be used when connecting to DM.");

    public static final ConfigOption<String> DATABASE_NAME =
            ConfigOptions.key("database-name")
                    .stringType()
                    .defaultValue("SYSDBA")
                    .withDescription(
                            "Database name of DM to monitor, should be regular expression. Only can be used with 'initial' mode.");

    public static final ConfigOption<String> SCHEMA_NAME =
            ConfigOptions.key("schema-name")
                    .stringType()
                    .defaultValue("SYSDBA")
                    .withDescription(
                            "Schema name of DM to monitor, should be regular expression. Only can be used with 'initial' mode.");

    public static final ConfigOption<String> TABLE_NAME =
            ConfigOptions.key("table-name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Table name of DM to monitor, should be regular expression. Only can be used with 'initial' mode.");

    public static final ConfigOption<String> TABLE_LIST =
            ConfigOptions.key("table-list")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "List of full names of tables, separated by commas, e.g. \"db1.table1, db2.table2\".");

    public static final ConfigOption<String> SERVER_TIME_ZONE =
            ConfigOptions.key("server-time-zone")
                    .stringType()
                    .defaultValue("+00:00")
                    .withDescription("The session time zone in database server.");

    public static final ConfigOption<Duration> CONNECT_TIMEOUT =
            ConfigOptions.key("connect.timeout")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(30))
                    .withDescription(
                            "The maximum time that the connector should wait after trying to connect to the database server or log proxy server before timing out.");

    public static final ConfigOption<String> HOSTNAME =
            ConfigOptions.key("hostname")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "IP address or hostname of the DM database server or DM proxy server.");

    public static final ConfigOption<Integer> PORT =
            ConfigOptions.key("port")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "Integer port number of DM database server or DM proxy server.");

    public static final ConfigOption<String> JDBC_DRIVER =
            ConfigOptions.key("jdbc.driver")
                    .stringType()
                    .defaultValue("dm.jdbc.driver.DmDriver")
                    .withDescription("JDBC driver class name, use 'com.mysql.jdbc.Driver' by default.");

    public static final ConfigOption<Long> SCAN_STARTUP_TIMESTAMP =
            ConfigOptions.key("scan.startup.timestamp")
                    .longType()
                    .noDefaultValue()
                    .withDescription(
                            "Optional timestamp in seconds used in case of \"timestamp\" startup mode.");

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);
        helper.validateExcept(JdbcUrlUtils.PROPERTIES_PREFIX);

        ResolvedSchema physicalSchema = context.getCatalogTable().getResolvedSchema();

        ReadableConfig config = helper.getOptions();
        StartupMode startupMode = StartupMode.getStartupMode(config.get(SCAN_STARTUP_MODE));

        String username = config.get(USERNAME);
        String password = config.get(PASSWORD);
        String databaseName = config.get(DATABASE_NAME);
        String schemaName = config.get(SCHEMA_NAME);
        String tableName = config.get(TABLE_NAME);
        String tableList = config.get(TABLE_LIST);

        String serverTimeZone = config.get(SERVER_TIME_ZONE);
        Duration connectTimeout = config.get(CONNECT_TIMEOUT);

        String hostname = config.get(HOSTNAME);
        Integer port = config.get(PORT);

        Long startupTimestamp = config.get(SCAN_STARTUP_TIMESTAMP);

        String inlongMetric = config.getOptional(INLONG_METRIC).orElse(null);
        String auditHostAndPorts = config.get(INLONG_AUDIT);

        OptionUtils.printOptions(IDENTIFIER, ((Configuration) config).toMap());

        return new DMTableSource(
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
                JdbcUrlUtils.getJdbcProperties(context.getCatalogTable().getOptions()),
                startupTimestamp,
                inlongMetric,
                auditHostAndPorts);
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(SCAN_STARTUP_MODE);
        options.add(USERNAME);
        options.add(PASSWORD);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(SCAN_STARTUP_TIMESTAMP);
        options.add(DATABASE_NAME);
        options.add(SCHEMA_NAME);
        options.add(TABLE_NAME);
        options.add(TABLE_LIST);
        options.add(HOSTNAME);
        options.add(PORT);
        options.add(JDBC_DRIVER);
        options.add(CONNECT_TIMEOUT);
        options.add(SERVER_TIME_ZONE);
        options.add(INLONG_METRIC);
        options.add(INLONG_AUDIT);
        options.add(AUDIT_KEYS);
        options.add(SOURCE_MULTIPLE_ENABLE);
        return options;
    }
}
