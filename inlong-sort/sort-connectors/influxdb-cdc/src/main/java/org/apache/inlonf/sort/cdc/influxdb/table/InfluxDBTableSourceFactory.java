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

import static org.apache.inlong.sort.base.Constants.INLONG_AUDIT;
import static org.apache.inlong.sort.base.Constants.INLONG_METRIC;

import java.util.HashSet;
import java.util.Set;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.inlong.sort.base.util.ValidateMetricOptionUtils;

public class InfluxDBTableSourceFactory implements DynamicTableSourceFactory {

    private static final String IDENTIFIER = "influx-cdc-inlong";

    public static final ConfigOption<Long> ENQUEUE_WAIT_TIME =
            ConfigOptions.key("source.influxDB.timeout.enqueue")
                    .longType()
                    .defaultValue(5L)
                    .withDescription(
                            "The time out in seconds for enqueuing an HTTP request to the queue.");

    public static final ConfigOption<Integer> INGEST_QUEUE_CAPACITY =
            ConfigOptions.key("source.influxDB.queue_capacity.ingest")
                    .intType()
                    .defaultValue(1000)
                    .withDescription(
                            "Size of queue that buffers HTTP requests data points before fetching.");

    public static final ConfigOption<Integer> MAXIMUM_LINES_PER_REQUEST =
            ConfigOptions.key("source.influxDB.limit.lines_per_request")
                    .intType()
                    .defaultValue(1000)
                    .withDescription(
                            "The maximum number of lines that should be parsed per HTTP request.");

    public static final ConfigOption<Integer> PORT =
            ConfigOptions.key("source.influxDB.port")
                    .intType()
                    .defaultValue(8000)
                    .withDescription(
                            "TCP port on which the split reader's HTTP server is running on.");

    public static final ConfigOption<String> SERVER_URL =
        ConfigOptions.key("source.influxDB.serverURL")
            .stringType()
            .defaultValue("http://127.0.0.1:8086")
            .withDescription(
                "The url to connect to.");

    public static final ConfigOption<String> USERNAME =
        ConfigOptions.key("source.influxDB.username")
            .stringType()
            .defaultValue("root")
            .withDescription(
                "The username which is used to authorize against the influxDB instance.");

    public static final ConfigOption<String> PASSWORD =
        ConfigOptions.key("source.influxDB.password")
            .stringType()
            .defaultValue("root")
            .withDescription(
                "The password for the username which is used to authorize against the influxDB instance.");

    public static final ConfigOption<Integer> CONNECT_TIMEOUT =
        ConfigOptions.key("source.influxDB.connectTimeout")
            .intType()
            .defaultValue(10)
            .withDescription(
                "Default connect timeout for new connections.");

    public static final ConfigOption<Integer> WRITE_TIMEOUT =
        ConfigOptions.key("source.influxDB.writeTimeout")
            .intType()
            .defaultValue(10)
            .withDescription(
                "Default write timeout for new connections.");

    public static final ConfigOption<Integer> READ_TIMEOUT =
        ConfigOptions.key("source.influxDB.readTimeout")
            .intType()
            .defaultValue(10)
            .withDescription(
                "Default read timeout for new connections.");

    public static final ConfigOption<Boolean> RETRY_ON_CONNECTION_FAILURE =
        ConfigOptions.key("source.influxDB.retryOnConnectionFailure")
            .booleanType()
            .defaultValue(true)
            .withDescription(
                "Configure this client to retry or not when a connectivity problem is encountered.");

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);
        helper.validate();
        final ReadableConfig config = helper.getOptions();

        final int port = config.get(PORT);

        final int linesPerRequest = config.get(MAXIMUM_LINES_PER_REQUEST);

        final int ingestQueueCapacity = config.get(INGEST_QUEUE_CAPACITY);

        final long enqueueTimeout =  config.get(ENQUEUE_WAIT_TIME);

        final String serverURL = config.get(SERVER_URL);

        final String username = config.get(USERNAME);

        final String password = config.get(PASSWORD);

        final Integer connectTimeout = config.get(CONNECT_TIMEOUT);

        final Integer writeTimeout = config.get(WRITE_TIMEOUT);

        final Integer readTimeout = config.get(READ_TIMEOUT);

        final Boolean retryOnConnectionFailure = config.get(RETRY_ON_CONNECTION_FAILURE);

        final String inlongMetric = config.get(INLONG_METRIC);

        final String inlongAudit = config.get(INLONG_AUDIT);

        ValidateMetricOptionUtils.validateInlongMetricIfSetInlongAudit(inlongMetric, inlongAudit);

        ResolvedSchema physicalSchema = context.getCatalogTable().getResolvedSchema();

        return new InfluxDBTableSource(physicalSchema, port, linesPerRequest, ingestQueueCapacity,
            enqueueTimeout, serverURL, username, password, connectTimeout, writeTimeout,
            readTimeout, retryOnConnectionFailure, inlongMetric, inlongAudit);
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(PORT);
        options.add(MAXIMUM_LINES_PER_REQUEST);
        options.add(INGEST_QUEUE_CAPACITY);
        options.add(ENQUEUE_WAIT_TIME);
        options.add(SERVER_URL);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(USERNAME);
        options.add(PASSWORD);
        options.add(CONNECT_TIMEOUT);
        options.add(WRITE_TIMEOUT);
        options.add(READ_TIMEOUT);
        options.add(RETRY_ON_CONNECTION_FAILURE);
        options.add(INLONG_METRIC);
        options.add(INLONG_AUDIT);;
        return options;
    }
}
