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

package org.apache.inlong.sort.kafka.table;

import org.apache.inlong.sort.base.Constants;
import org.apache.inlong.sort.kafka.KafkaOptions;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.source.KafkaSourceOptions;
import org.apache.flink.streaming.connectors.kafka.config.StartupMode;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions;
import org.apache.flink.streaming.connectors.kafka.table.KafkaDynamicSink;
import org.apache.flink.streaming.connectors.kafka.table.KafkaDynamicSource;
import org.apache.flink.streaming.connectors.kafka.table.SinkBufferFlushMode;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.format.Format;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.FactoryUtil.TableFactoryHelper;
import org.apache.flink.table.factories.SerializationFormatFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Pattern;

import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions.DELIVERY_GUARANTEE;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions.KEY_FIELDS_PREFIX;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions.KEY_FORMAT;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions.SCAN_TOPIC_PARTITION_DISCOVERY;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions.SINK_PARALLELISM;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions.TOPIC;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions.TRANSACTIONAL_ID_PREFIX;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions.VALUE_FORMAT;
import static org.apache.inlong.sort.kafka.table.KafkaConnectorOptionsUtil.PROPERTIES_PREFIX;
import static org.apache.inlong.sort.kafka.table.KafkaConnectorOptionsUtil.StartupOptions;
import static org.apache.inlong.sort.kafka.table.KafkaConnectorOptionsUtil.autoCompleteSchemaRegistrySubject;
import static org.apache.inlong.sort.kafka.table.KafkaConnectorOptionsUtil.createKeyFormatProjection;
import static org.apache.inlong.sort.kafka.table.KafkaConnectorOptionsUtil.createValueFormatProjection;
import static org.apache.inlong.sort.kafka.table.KafkaConnectorOptionsUtil.getFlinkKafkaPartitioner;
import static org.apache.inlong.sort.kafka.table.KafkaConnectorOptionsUtil.getSourceTopicPattern;
import static org.apache.inlong.sort.kafka.table.KafkaConnectorOptionsUtil.getSourceTopics;
import static org.apache.inlong.sort.kafka.table.KafkaConnectorOptionsUtil.getStartupOptions;
import static org.apache.inlong.sort.kafka.table.KafkaConnectorOptionsUtil.validateTableSinkOptions;
import static org.apache.inlong.sort.kafka.table.KafkaConnectorOptionsUtil.validateTableSourceOptions;

@Internal
public class KafkaDynamicTableFactory implements DynamicTableSourceFactory, DynamicTableSinkFactory {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaDynamicTableFactory.class);
    public static final String IDENTIFIER = "kafka-inlong";

    private static final ConfigOption<String> SINK_MULTIPLE_PARTITION_PATTERN =
            ConfigOptions.key("sink.multiple.partition-pattern")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "option 'sink.multiple.partition-pattern' used either when the partitioner is raw-hash, or when passing in designated partition field names for custom field partitions");

    private static final ConfigOption<String> SINK_FIXED_IDENTIFIER =
            ConfigOptions.key("sink.fixed.identifier")
                    .stringType()
                    .defaultValue("-1");

    private static final ConfigOption<String> SINK_SEMANTIC =
            ConfigOptions.key("sink.semantic")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Optional semantic when committing.");

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(KafkaConnectorOptions.PROPS_BOOTSTRAP_SERVERS);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(FactoryUtil.FORMAT);
        options.add(KEY_FORMAT);
        options.add(KafkaConnectorOptions.KEY_FIELDS);
        options.add(KEY_FIELDS_PREFIX);
        options.add(VALUE_FORMAT);
        options.add(KafkaConnectorOptions.VALUE_FIELDS_INCLUDE);
        options.add(KafkaConnectorOptions.TOPIC);
        options.add(KafkaConnectorOptions.TOPIC_PATTERN);
        options.add(KafkaConnectorOptions.PROPS_GROUP_ID);
        options.add(KafkaConnectorOptions.SCAN_STARTUP_MODE);
        options.add(KafkaConnectorOptions.SCAN_STARTUP_SPECIFIC_OFFSETS);
        options.add(SCAN_TOPIC_PARTITION_DISCOVERY);
        options.add(KafkaConnectorOptions.SCAN_STARTUP_TIMESTAMP_MILLIS);
        options.add(KafkaConnectorOptions.SINK_PARTITIONER);
        options.add(FactoryUtil.SINK_PARALLELISM);
        options.add(Constants.INLONG_METRIC);
        options.add(Constants.INLONG_AUDIT);
        options.add(Constants.AUDIT_KEYS);
        options.add(Constants.SINK_MULTIPLE_FORMAT);
        options.add(Constants.PATTERN_PARTITION_MAP);
        options.add(Constants.DATASOURCE_PARTITION_MAP);
        options.add(Constants.SINK_SCHEMA_CHANGE_ENABLE);
        options.add(Constants.SINK_SCHEMA_CHANGE_POLICIES);
        options.add(KafkaOptions.KAFKA_IGNORE_ALL_CHANGELOG);
        options.add(SINK_MULTIPLE_PARTITION_PATTERN);
        options.add(SINK_FIXED_IDENTIFIER);
        options.add(SINK_SEMANTIC);
        return options;
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        final TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);

        final Optional<DecodingFormat<DeserializationSchema<RowData>>> keyDecodingFormat =
                getKeyDecodingFormat(helper);

        final DecodingFormat<DeserializationSchema<RowData>> valueDecodingFormat =
                getValueDecodingFormat(helper);

        helper.validateExcept(PROPERTIES_PREFIX);

        final ReadableConfig tableOptions = helper.getOptions();

        validateTableSourceOptions(tableOptions);

        validatePKConstraints(
                context.getObjectIdentifier(),
                context.getPrimaryKeyIndexes(),
                context.getCatalogTable().getOptions(),
                valueDecodingFormat);

        final StartupOptions startupOptions = getStartupOptions(tableOptions);

        final Properties properties = getKafkaProperties(context.getCatalogTable().getOptions());

        // add topic-partition discovery
        final Optional<Long> partitionDiscoveryInterval =
                tableOptions.getOptional(SCAN_TOPIC_PARTITION_DISCOVERY).map(Duration::toMillis);
        properties.setProperty(
                KafkaSourceOptions.PARTITION_DISCOVERY_INTERVAL_MS.key(),
                partitionDiscoveryInterval.orElse(-1L).toString());

        final DataType physicalDataType = context.getPhysicalRowDataType();

        final int[] keyProjection = createKeyFormatProjection(tableOptions, physicalDataType);

        final int[] valueProjection = createValueFormatProjection(tableOptions, physicalDataType);

        final String keyPrefix = tableOptions.getOptional(KEY_FIELDS_PREFIX).orElse(null);

        return createKafkaTableSource(
                physicalDataType,
                keyDecodingFormat.orElse(null),
                valueDecodingFormat,
                keyProjection,
                valueProjection,
                keyPrefix,
                getSourceTopics(tableOptions),
                getSourceTopicPattern(tableOptions),
                properties,
                startupOptions.startupMode,
                startupOptions.specificOffsets,
                startupOptions.startupTimestampMillis,
                context.getObjectIdentifier().asSummaryString());
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        final TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(
                        this, autoCompleteSchemaRegistrySubject(context));

        final Optional<EncodingFormat<SerializationSchema<RowData>>> keyEncodingFormat =
                getKeyEncodingFormat(helper);

        final EncodingFormat<SerializationSchema<RowData>> valueEncodingFormat =
                getValueEncodingFormat(helper);

        helper.validateExcept(KafkaConnectorOptionsUtil.PROPERTIES_PREFIX);

        final ReadableConfig tableOptions = helper.getOptions();

        final DeliveryGuarantee deliveryGuarantee = validateDeprecatedSemantic(tableOptions);
        validateTableSinkOptions(tableOptions);

        KafkaConnectorOptionsUtil.validateDeliveryGuarantee(tableOptions);

        validatePKConstraints(
                context.getObjectIdentifier(),
                context.getPrimaryKeyIndexes(),
                context.getCatalogTable().getOptions(),
                valueEncodingFormat);

        final DataType physicalDataType = context.getPhysicalRowDataType();

        final int[] keyProjection = createKeyFormatProjection(tableOptions, physicalDataType);

        final int[] valueProjection = createValueFormatProjection(tableOptions, physicalDataType);

        final String keyPrefix = tableOptions.getOptional(KEY_FIELDS_PREFIX).orElse(null);

        final Integer parallelism = tableOptions.getOptional(SINK_PARALLELISM).orElse(null);

        return createKafkaTableSink(
                physicalDataType,
                keyEncodingFormat.orElse(null),
                valueEncodingFormat,
                keyProjection,
                valueProjection,
                keyPrefix,
                tableOptions.get(TOPIC).get(0),
                getKafkaProperties(context.getCatalogTable().getOptions()),
                getFlinkKafkaPartitioner(tableOptions, context.getClassLoader()).orElse(null),
                deliveryGuarantee,
                parallelism,
                tableOptions.get(TRANSACTIONAL_ID_PREFIX));
    }

    private static Optional<DecodingFormat<DeserializationSchema<RowData>>> getKeyDecodingFormat(
            TableFactoryHelper helper) {
        final Optional<DecodingFormat<DeserializationSchema<RowData>>> keyDecodingFormat =
                helper.discoverOptionalDecodingFormat(
                        DeserializationFormatFactory.class, KEY_FORMAT);
        keyDecodingFormat.ifPresent(
                format -> {
                    if (!format.getChangelogMode().containsOnly(RowKind.INSERT)) {
                        throw new ValidationException(
                                String.format(
                                        "A key format should only deal with INSERT-only records. "
                                                + "But %s has a changelog mode of %s.",
                                        helper.getOptions().get(KEY_FORMAT),
                                        format.getChangelogMode()));
                    }
                });
        return keyDecodingFormat;
    }

    private static Optional<EncodingFormat<SerializationSchema<RowData>>> getKeyEncodingFormat(
            TableFactoryHelper helper) {
        final Optional<EncodingFormat<SerializationSchema<RowData>>> keyEncodingFormat =
                helper.discoverOptionalEncodingFormat(SerializationFormatFactory.class, KEY_FORMAT);
        keyEncodingFormat.ifPresent(
                format -> {
                    if (!format.getChangelogMode().containsOnly(RowKind.INSERT)) {
                        throw new ValidationException(
                                String.format(
                                        "A key format should only deal with INSERT-only records. "
                                                + "But %s has a changelog mode of %s.",
                                        helper.getOptions().get(KEY_FORMAT),
                                        format.getChangelogMode()));
                    }
                });
        return keyEncodingFormat;
    }

    private static DecodingFormat<DeserializationSchema<RowData>> getValueDecodingFormat(
            TableFactoryHelper helper) {
        return helper.discoverOptionalDecodingFormat(
                DeserializationFormatFactory.class, FactoryUtil.FORMAT)
                .orElseGet(
                        () -> helper.discoverDecodingFormat(
                                DeserializationFormatFactory.class, VALUE_FORMAT));
    }

    private static EncodingFormat<SerializationSchema<RowData>> getValueEncodingFormat(
            TableFactoryHelper helper) {
        return helper.discoverOptionalEncodingFormat(
                SerializationFormatFactory.class, FactoryUtil.FORMAT)
                .orElseGet(
                        () -> helper.discoverEncodingFormat(
                                SerializationFormatFactory.class, VALUE_FORMAT));
    }

    private static void validatePKConstraints(
            ObjectIdentifier tableName,
            int[] primaryKeyIndexes,
            Map<String, String> options,
            Format format) {
        if (primaryKeyIndexes.length > 0
                && format.getChangelogMode().containsOnly(RowKind.INSERT)) {
            Configuration configuration = Configuration.fromMap(options);
            String formatName =
                    configuration
                            .getOptional(FactoryUtil.FORMAT)
                            .orElse(configuration.get(VALUE_FORMAT));
            throw new ValidationException(
                    String.format(
                            "The Kafka table '%s' with '%s' format doesn't support defining PRIMARY KEY constraint"
                                    + " on the table, because it can't guarantee the semantic of primary key.",
                            tableName.asSummaryString(), formatName));
        }
    }

    public static Properties getKafkaProperties(Map<String, String> tableOptions) {
        final Properties kafkaProperties = new Properties();

        if (hasKafkaClientProperties(tableOptions)) {
            tableOptions.keySet().stream()
                    .filter(key -> key.startsWith(PROPERTIES_PREFIX))
                    .forEach(
                            key -> {
                                final String value = tableOptions.get(key);
                                final String subKey = key.substring((PROPERTIES_PREFIX).length());
                                kafkaProperties.put(subKey, value);
                            });
        }
        return kafkaProperties;
    }

    /**
     * Decides if the table options contains Kafka client properties that start with prefix
     * 'properties'.
     */
    private static boolean hasKafkaClientProperties(Map<String, String> tableOptions) {
        return tableOptions.keySet().stream().anyMatch(k -> k.startsWith(PROPERTIES_PREFIX));
    }

    private static DeliveryGuarantee validateDeprecatedSemantic(ReadableConfig tableOptions) {
        if (tableOptions.getOptional(SINK_SEMANTIC).isPresent()) {
            LOG.warn(
                    "{} is deprecated and will be removed. Please use {} instead.",
                    SINK_SEMANTIC.key(),
                    DELIVERY_GUARANTEE.key());
            return DeliveryGuarantee.valueOf(
                    tableOptions.get(SINK_SEMANTIC).toUpperCase().replace("-", "_"));
        }
        return tableOptions.get(DELIVERY_GUARANTEE);
    }

    protected KafkaDynamicSource createKafkaTableSource(
            DataType physicalDataType,
            @Nullable DecodingFormat<DeserializationSchema<RowData>> keyDecodingFormat,
            DecodingFormat<DeserializationSchema<RowData>> valueDecodingFormat,
            int[] keyProjection,
            int[] valueProjection,
            @Nullable String keyPrefix,
            @Nullable List<String> topics,
            @Nullable Pattern topicPattern,
            Properties properties,
            StartupMode startupMode,
            Map<KafkaTopicPartition, Long> specificStartupOffsets,
            long startupTimestampMillis,
            String tableIdentifier) {
        return new KafkaDynamicSource(
                physicalDataType,
                keyDecodingFormat,
                valueDecodingFormat,
                keyProjection,
                valueProjection,
                keyPrefix,
                topics,
                topicPattern,
                properties,
                startupMode,
                specificStartupOffsets,
                startupTimestampMillis,
                false,
                tableIdentifier);
    }

    protected KafkaDynamicSink createKafkaTableSink(
            DataType physicalDataType,
            @Nullable EncodingFormat<SerializationSchema<RowData>> keyEncodingFormat,
            EncodingFormat<SerializationSchema<RowData>> valueEncodingFormat,
            int[] keyProjection,
            int[] valueProjection,
            @Nullable String keyPrefix,
            String topic,
            Properties properties,
            FlinkKafkaPartitioner<RowData> partitioner,
            DeliveryGuarantee deliveryGuarantee,
            Integer parallelism,
            @Nullable String transactionalIdPrefix) {
        return new KafkaDynamicSink(
                physicalDataType,
                physicalDataType,
                keyEncodingFormat,
                valueEncodingFormat,
                keyProjection,
                valueProjection,
                keyPrefix,
                topic,
                properties,
                partitioner,
                deliveryGuarantee,
                false,
                SinkBufferFlushMode.DISABLED,
                parallelism,
                transactionalIdPrefix);
    }

}
