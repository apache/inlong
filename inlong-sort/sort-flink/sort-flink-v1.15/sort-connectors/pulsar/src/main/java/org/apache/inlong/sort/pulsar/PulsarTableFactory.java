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

package org.apache.inlong.sort.pulsar;

import org.apache.inlong.sort.protocol.node.ExtractNode;
import org.apache.inlong.sort.pulsar.table.PulsarTableDeserializationSchemaFactory;
import org.apache.inlong.sort.pulsar.table.PulsarTableSource;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.pulsar.common.config.PulsarOptions;
import org.apache.flink.connector.pulsar.sink.PulsarSinkOptions;
import org.apache.flink.connector.pulsar.source.PulsarSourceOptions;
import org.apache.flink.connector.pulsar.source.enumerator.cursor.StartCursor;
import org.apache.flink.connector.pulsar.source.enumerator.cursor.StopCursor;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;
import org.apache.pulsar.client.api.SubscriptionType;

import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_ADMIN_URL;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_SERVICE_URL;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_SUBSCRIPTION_NAME;
import static org.apache.flink.table.factories.FactoryUtil.SINK_PARALLELISM;
import static org.apache.inlong.sort.base.Constants.*;
import static org.apache.inlong.sort.pulsar.PulsarTableOptionUtils.createKeyFormatProjection;
import static org.apache.inlong.sort.pulsar.PulsarTableOptionUtils.createValueFormatProjection;
import static org.apache.inlong.sort.pulsar.PulsarTableOptionUtils.getKeyDecodingFormat;
import static org.apache.inlong.sort.pulsar.PulsarTableOptionUtils.getPulsarProperties;
import static org.apache.inlong.sort.pulsar.PulsarTableOptionUtils.getStartCursor;
import static org.apache.inlong.sort.pulsar.PulsarTableOptionUtils.getStopCursor;
import static org.apache.inlong.sort.pulsar.PulsarTableOptionUtils.getSubscriptionType;
import static org.apache.inlong.sort.pulsar.PulsarTableOptionUtils.getTopicListFromOptions;
import static org.apache.inlong.sort.pulsar.PulsarTableOptionUtils.getValueDecodingFormat;
import static org.apache.inlong.sort.pulsar.PulsarTableOptions.ADMIN_URL;
import static org.apache.inlong.sort.pulsar.PulsarTableOptions.EXPLICIT;
import static org.apache.inlong.sort.pulsar.PulsarTableOptions.KEY_FIELDS;
import static org.apache.inlong.sort.pulsar.PulsarTableOptions.KEY_FORMAT;
import static org.apache.inlong.sort.pulsar.PulsarTableOptions.SERVICE_URL;
import static org.apache.inlong.sort.pulsar.PulsarTableOptions.SINK_CUSTOM_TOPIC_ROUTER;
import static org.apache.inlong.sort.pulsar.PulsarTableOptions.SINK_MESSAGE_DELAY_INTERVAL;
import static org.apache.inlong.sort.pulsar.PulsarTableOptions.SINK_TOPIC_ROUTING_MODE;
import static org.apache.inlong.sort.pulsar.PulsarTableOptions.SOURCE_START_FROM_MESSAGE_ID;
import static org.apache.inlong.sort.pulsar.PulsarTableOptions.SOURCE_START_FROM_PUBLISH_TIME;
import static org.apache.inlong.sort.pulsar.PulsarTableOptions.SOURCE_STOP_AFTER_MESSAGE_ID;
import static org.apache.inlong.sort.pulsar.PulsarTableOptions.SOURCE_STOP_AT_MESSAGE_ID;
import static org.apache.inlong.sort.pulsar.PulsarTableOptions.SOURCE_STOP_AT_PUBLISH_TIME;
import static org.apache.inlong.sort.pulsar.PulsarTableOptions.SOURCE_SUBSCRIPTION_NAME;
import static org.apache.inlong.sort.pulsar.PulsarTableOptions.SOURCE_SUBSCRIPTION_TYPE;
import static org.apache.inlong.sort.pulsar.PulsarTableOptions.STARTUP_MODE;
import static org.apache.inlong.sort.pulsar.PulsarTableOptions.TOPIC;
import static org.apache.inlong.sort.pulsar.PulsarTableOptions.VALUE_FORMAT;
import static org.apache.inlong.sort.pulsar.PulsarTableValidationUtils.validatePrimaryKeyConstraints;
import static org.apache.inlong.sort.pulsar.PulsarTableValidationUtils.validateTableSourceOptions;
import static org.apache.pulsar.shade.org.apache.commons.lang3.RandomStringUtils.randomAlphabetic;

/**
 * Pulsar table source factory.
 * Modified from {@link org.apache.flink.connector.pulsar.table.PulsarTableFactory}.
 */
public class PulsarTableFactory implements DynamicTableSourceFactory {

    public static final String IDENTIFIER = "pulsar-inlong";

    public static final String DEFAULT_SUBSCRIPTION_NAME_PREFIX = "flink-sql-connector-pulsar-";

    public static final boolean UPSERT_DISABLED = false;

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        // Format options should be retrieved before validation.
        final DecodingFormat<DeserializationSchema<RowData>> keyDecodingFormat =
                getKeyDecodingFormat(helper);
        final DecodingFormat<DeserializationSchema<RowData>> valueDecodingFormat =
                getValueDecodingFormat(helper);
        ReadableConfig tableOptions = helper.getOptions();

        // Validate configs are not conflict; each options is consumed; no unwanted configs
        // PulsarOptions, PulsarSourceOptions and PulsarSinkOptions is not part of the validation.
        helper.validateExcept(
                PulsarOptions.CLIENT_CONFIG_PREFIX,
                PulsarOptions.ADMIN_CONFIG_PREFIX,
                PulsarSourceOptions.SOURCE_CONFIG_PREFIX,
                PulsarSourceOptions.CONSUMER_CONFIG_PREFIX,
                PulsarSinkOptions.PRODUCER_CONFIG_PREFIX,
                PulsarSinkOptions.SINK_CONFIG_PREFIX,
                ExtractNode.INLONG_MSG);

        validatePrimaryKeyConstraints(
                context.getObjectIdentifier(), context.getPrimaryKeyIndexes(), helper);

        validateTableSourceOptions(tableOptions);

        // Retrieve configs
        final List<String> topics = getTopicListFromOptions(tableOptions);
        final StartCursor startCursor = getStartCursor(tableOptions);
        final StopCursor stopCursor = getStopCursor(tableOptions);
        final SubscriptionType subscriptionType = getSubscriptionType(tableOptions);
        final boolean enableLogReport = context.getConfiguration().get(ENABLE_LOG_REPORT);
        // Forward source configs
        final Properties properties = getPulsarProperties(tableOptions);
        properties.setProperty(PULSAR_ADMIN_URL.key(), tableOptions.get(ADMIN_URL));
        properties.setProperty(PULSAR_SERVICE_URL.key(), tableOptions.get(SERVICE_URL));
        // Set random subscriptionName if not provided
        properties.setProperty(
                PULSAR_SUBSCRIPTION_NAME.key(),
                tableOptions
                        .getOptional(SOURCE_SUBSCRIPTION_NAME)
                        .orElse(DEFAULT_SUBSCRIPTION_NAME_PREFIX + randomAlphabetic(5)));
        // Retrieve physical fields (not including computed or metadata fields),
        // and projections and create a schema factory based on such information.
        final DataType physicalDataType = context.getPhysicalRowDataType();

        final int[] valueProjection = createValueFormatProjection(tableOptions, physicalDataType);
        final int[] keyProjection = createKeyFormatProjection(tableOptions, physicalDataType);

        String inlongMetric = tableOptions.getOptional(INLONG_METRIC).orElse(null);
        String auditHostAndPorts = tableOptions.get(INLONG_AUDIT);
        String auditKeys = tableOptions.get(AUDIT_KEYS);

        final PulsarTableDeserializationSchemaFactory deserializationSchemaFactory =
                new PulsarTableDeserializationSchemaFactory(
                        physicalDataType,
                        keyDecodingFormat,
                        keyProjection,
                        valueDecodingFormat,
                        valueProjection,
                        UPSERT_DISABLED,
                        inlongMetric,
                        auditHostAndPorts,
                        auditKeys);

        // Set default values for configuration not exposed to user.
        final DecodingFormat<DeserializationSchema<RowData>> decodingFormatForMetadataPushdown =
                valueDecodingFormat;
        final ChangelogMode changelogMode = decodingFormatForMetadataPushdown.getChangelogMode();
        return new PulsarTableSource(
                deserializationSchemaFactory,
                decodingFormatForMetadataPushdown,
                changelogMode,
                topics,
                properties,
                startCursor,
                stopCursor,
                subscriptionType,
                enableLogReport);
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return Stream.of(TOPIC, ADMIN_URL, SERVICE_URL).collect(Collectors.toSet());
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return Stream.of(
                FactoryUtil.FORMAT,
                VALUE_FORMAT,
                SOURCE_SUBSCRIPTION_NAME,
                SOURCE_SUBSCRIPTION_TYPE,
                STARTUP_MODE,
                SOURCE_START_FROM_MESSAGE_ID,
                SOURCE_START_FROM_PUBLISH_TIME,
                SOURCE_STOP_AT_MESSAGE_ID,
                SOURCE_STOP_AFTER_MESSAGE_ID,
                SOURCE_STOP_AT_PUBLISH_TIME,
                SINK_CUSTOM_TOPIC_ROUTER,
                SINK_TOPIC_ROUTING_MODE,
                SINK_MESSAGE_DELAY_INTERVAL,
                SINK_PARALLELISM,
                KEY_FORMAT,
                KEY_FIELDS,
                EXPLICIT,
                AUDIT_KEYS,
                INLONG_METRIC,
                INLONG_AUDIT).collect(Collectors.toSet());
    }

    /** Format and Delivery guarantee related options are not forward options. */
    @Override
    public Set<ConfigOption<?>> forwardOptions() {
        return Stream.of(
                TOPIC,
                ADMIN_URL,
                SERVICE_URL,
                SOURCE_SUBSCRIPTION_TYPE,
                SOURCE_SUBSCRIPTION_NAME,
                STARTUP_MODE,
                SOURCE_START_FROM_PUBLISH_TIME,
                SOURCE_STOP_AT_MESSAGE_ID,
                SOURCE_STOP_AFTER_MESSAGE_ID,
                SOURCE_STOP_AT_PUBLISH_TIME,
                SINK_CUSTOM_TOPIC_ROUTER,
                SINK_TOPIC_ROUTING_MODE,
                SINK_MESSAGE_DELAY_INTERVAL)
                .collect(Collectors.toSet());
    }
}