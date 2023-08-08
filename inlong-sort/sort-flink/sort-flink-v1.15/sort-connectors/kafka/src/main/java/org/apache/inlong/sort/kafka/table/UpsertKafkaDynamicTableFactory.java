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

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.streaming.connectors.kafka.config.StartupMode;
import org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions;
import org.apache.flink.streaming.connectors.kafka.table.KafkaDynamicSink;
import org.apache.flink.streaming.connectors.kafka.table.KafkaDynamicSource;
import org.apache.flink.streaming.connectors.kafka.table.SinkBufferFlushMode;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
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
import org.apache.flink.table.factories.SerializationFormatFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Preconditions;

import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.stream.IntStream;

import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions.KEY_FIELDS;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions.KEY_FIELDS_PREFIX;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions.KEY_FORMAT;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions.PROPS_BOOTSTRAP_SERVERS;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions.SINK_BUFFER_FLUSH_INTERVAL;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions.SINK_BUFFER_FLUSH_MAX_ROWS;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions.SINK_PARALLELISM;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions.SINK_PARTITIONER;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions.TOPIC;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions.TRANSACTIONAL_ID_PREFIX;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions.VALUE_FIELDS_INCLUDE;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions.VALUE_FORMAT;
import static org.apache.inlong.sort.base.Constants.INLONG_METRIC;
import static org.apache.inlong.sort.kafka.KafkaOptions.KAFKA_IGNORE_ALL_CHANGELOG;
import static org.apache.inlong.sort.kafka.table.KafkaConnectorOptionsUtil.PROPERTIES_PREFIX;
import static org.apache.inlong.sort.kafka.table.KafkaConnectorOptionsUtil.autoCompleteSchemaRegistrySubject;
import static org.apache.inlong.sort.kafka.table.KafkaConnectorOptionsUtil.getKafkaProperties;
import static org.apache.inlong.sort.kafka.table.KafkaConnectorOptionsUtil.getSourceTopicPattern;
import static org.apache.inlong.sort.kafka.table.KafkaConnectorOptionsUtil.getSourceTopics;

public class UpsertKafkaDynamicTableFactory implements DynamicTableSourceFactory, DynamicTableSinkFactory {

    public static final String IDENTIFIER = "upsert-kafka-inlong";

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

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(PROPS_BOOTSTRAP_SERVERS);
        options.add(TOPIC);
        options.add(KEY_FORMAT);
        options.add(VALUE_FORMAT);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(KEY_FIELDS_PREFIX);
        options.add(VALUE_FIELDS_INCLUDE);
        options.add(SINK_PARALLELISM);
        options.add(SINK_BUFFER_FLUSH_INTERVAL);
        options.add(SINK_BUFFER_FLUSH_MAX_ROWS);
        options.add(KAFKA_IGNORE_ALL_CHANGELOG);
        options.add(INLONG_METRIC);
        options.add(SINK_PARTITIONER);
        options.add(SINK_MULTIPLE_PARTITION_PATTERN);
        options.add(SINK_FIXED_IDENTIFIER);
        return options;
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);

        ReadableConfig tableOptions = helper.getOptions();
        DecodingFormat<DeserializationSchema<RowData>> keyDecodingFormat =
                helper.discoverDecodingFormat(DeserializationFormatFactory.class, KEY_FORMAT);
        DecodingFormat<DeserializationSchema<RowData>> valueDecodingFormat =
                helper.discoverDecodingFormat(DeserializationFormatFactory.class, VALUE_FORMAT);

        // Validate the option data type.
        helper.validateExcept(PROPERTIES_PREFIX);
        validateSource(
                tableOptions,
                keyDecodingFormat,
                valueDecodingFormat,
                context.getPrimaryKeyIndexes());

        Tuple2<int[], int[]> keyValueProjections =
                createKeyValueProjections(context.getCatalogTable());
        String keyPrefix = tableOptions.getOptional(KEY_FIELDS_PREFIX).orElse(null);
        Properties properties = getKafkaProperties(context.getCatalogTable().getOptions());
        // always use earliest to keep data integrity
        StartupMode earliest = StartupMode.EARLIEST;

        return new KafkaDynamicSource(
                context.getPhysicalRowDataType(),
                keyDecodingFormat,
                new DecodingFormatWrapper(valueDecodingFormat),
                keyValueProjections.f0,
                keyValueProjections.f1,
                keyPrefix,
                getSourceTopics(tableOptions),
                getSourceTopicPattern(tableOptions),
                properties,
                earliest,
                Collections.emptyMap(),
                0,
                true,
                context.getObjectIdentifier().asSummaryString());
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(
                        this, autoCompleteSchemaRegistrySubject(context));

        final ReadableConfig tableOptions = helper.getOptions();

        EncodingFormat<SerializationSchema<RowData>> keyEncodingFormat =
                helper.discoverEncodingFormat(SerializationFormatFactory.class, KEY_FORMAT);
        EncodingFormat<SerializationSchema<RowData>> valueEncodingFormat =
                helper.discoverEncodingFormat(SerializationFormatFactory.class, VALUE_FORMAT);

        // Validate the option data type.
        helper.validateExcept(PROPERTIES_PREFIX);
        validateSink(
                tableOptions,
                keyEncodingFormat,
                valueEncodingFormat,
                context.getPrimaryKeyIndexes());

        Tuple2<int[], int[]> keyValueProjections =
                createKeyValueProjections(context.getCatalogTable());
        final String keyPrefix = tableOptions.getOptional(KEY_FIELDS_PREFIX).orElse(null);
        final Properties properties = getKafkaProperties(context.getCatalogTable().getOptions());

        Integer parallelism = tableOptions.get(SINK_PARALLELISM);

        int batchSize = tableOptions.get(SINK_BUFFER_FLUSH_MAX_ROWS);
        Duration batchInterval = tableOptions.get(SINK_BUFFER_FLUSH_INTERVAL);
        SinkBufferFlushMode flushMode =
                new SinkBufferFlushMode(batchSize, batchInterval.toMillis());

        // use {@link org.apache.kafka.clients.producer.internals.DefaultPartitioner}.
        // it will use hash partition if key is set else in round-robin behaviour.
        return new KafkaDynamicSink(
                context.getPhysicalRowDataType(),
                context.getPhysicalRowDataType(),
                keyEncodingFormat,
                new EncodingFormatWrapper(valueEncodingFormat),
                keyValueProjections.f0,
                keyValueProjections.f1,
                keyPrefix,
                tableOptions.get(TOPIC).get(0),
                properties,
                null,
                DeliveryGuarantee.AT_LEAST_ONCE,
                true,
                flushMode,
                parallelism,
                tableOptions.get(TRANSACTIONAL_ID_PREFIX));
    }

    private static void validateSource(
            ReadableConfig tableOptions,
            Format keyFormat,
            Format valueFormat,
            int[] primaryKeyIndexes) {
        validateTopic(tableOptions);
        validateFormat(keyFormat, valueFormat, tableOptions);
        validatePKConstraints(primaryKeyIndexes);
    }

    private static void validateSink(
            ReadableConfig tableOptions,
            Format keyFormat,
            Format valueFormat,
            int[] primaryKeyIndexes) {
        validateTopic(tableOptions);
        validateFormat(keyFormat, valueFormat, tableOptions);
        validatePKConstraints(primaryKeyIndexes);
        validateSinkBufferFlush(tableOptions);
    }

    private static void validateTopic(ReadableConfig tableOptions) {
        List<String> topic = tableOptions.get(TOPIC);
        if (topic.size() > 1) {
            throw new ValidationException(
                    "The 'upsert-kafka' connector doesn't support topic list now. "
                            + "Please use single topic as the value of the parameter 'topic'.");
        }
    }

    private static void validateFormat(
            Format keyFormat, Format valueFormat, ReadableConfig tableOptions) {
        if (!keyFormat.getChangelogMode().containsOnly(RowKind.INSERT)) {
            String identifier = tableOptions.get(KEY_FORMAT);
            throw new ValidationException(
                    String.format(
                            "'upsert-kafka' connector doesn't support '%s' as key format, "
                                    + "because '%s' is not in insert-only mode.",
                            identifier, identifier));
        }
        if (!valueFormat.getChangelogMode().containsOnly(RowKind.INSERT)) {
            String identifier = tableOptions.get(VALUE_FORMAT);
            throw new ValidationException(
                    String.format(
                            "'upsert-kafka' connector doesn't support '%s' as value format, "
                                    + "because '%s' is not in insert-only mode.",
                            identifier, identifier));
        }
    }

    private static void validatePKConstraints(int[] schema) {
        if (schema.length == 0) {
            throw new ValidationException(
                    "'upsert-kafka' tables require to define a PRIMARY KEY constraint. "
                            + "The PRIMARY KEY specifies which columns should be read from or write to the Kafka message key. "
                            + "The PRIMARY KEY also defines records in the 'upsert-kafka' table should update or delete on which keys.");
        }
    }

    private static void validateSinkBufferFlush(ReadableConfig tableOptions) {
        int flushMaxRows = tableOptions.get(SINK_BUFFER_FLUSH_MAX_ROWS);
        long flushIntervalMs = tableOptions.get(SINK_BUFFER_FLUSH_INTERVAL).toMillis();
        if (flushMaxRows > 0 && flushIntervalMs > 0) {
            // flush is enabled
            return;
        }
        if (flushMaxRows <= 0 && flushIntervalMs <= 0) {
            // flush is disabled
            return;
        }
        // one of them is set which is not allowed
        throw new ValidationException(
                String.format(
                        "'%s' and '%s' must be set to be greater than zero together to enable sink buffer flushing.",
                        SINK_BUFFER_FLUSH_MAX_ROWS.key(), SINK_BUFFER_FLUSH_INTERVAL.key()));
    }

    private Tuple2<int[], int[]> createKeyValueProjections(ResolvedCatalogTable catalogTable) {
        ResolvedSchema schema = catalogTable.getResolvedSchema();
        // primary key should validated earlier
        List<String> keyFields = schema.getPrimaryKey().get().getColumns();
        DataType physicalDataType = schema.toPhysicalRowDataType();

        Configuration tableOptions = Configuration.fromMap(catalogTable.getOptions());
        // upsert-kafka will set key.fields to primary key fields by default
        tableOptions.set(KEY_FIELDS, keyFields);

        int[] keyProjection = createKeyFormatProjection(tableOptions, physicalDataType);
        int[] valueProjection = createValueFormatProjection(tableOptions, physicalDataType);

        return Tuple2.of(keyProjection, valueProjection);
    }

    /**
     * Creates an array of indices that determine which physical fields of the table schema to
     * include in the key format and the order that those fields have in the key format.
     *
     * <p>See {@link KafkaConnectorOptions#KEY_FORMAT}, {@link KafkaConnectorOptions#KEY_FIELDS},
     * and {@link KafkaConnectorOptions#KEY_FIELDS_PREFIX} for more information.
     */
    public static int[] createKeyFormatProjection(
            ReadableConfig options, DataType physicalDataType) {
        final LogicalType physicalType = physicalDataType.getLogicalType();
        Preconditions.checkArgument(
                physicalType.is(LogicalTypeRoot.ROW), "Row data type expected.");
        final Optional<String> optionalKeyFormat = options.getOptional(KEY_FORMAT);
        final Optional<List<String>> optionalKeyFields = options.getOptional(KEY_FIELDS);

        if (!optionalKeyFormat.isPresent() && optionalKeyFields.isPresent()) {
            throw new ValidationException(
                    String.format(
                            "The option '%s' can only be declared if a key format is defined using '%s'.",
                            KEY_FIELDS.key(), KEY_FORMAT.key()));
        } else if (optionalKeyFormat.isPresent()
                && (!optionalKeyFields.isPresent() || optionalKeyFields.get().size() == 0)) {
            throw new ValidationException(
                    String.format(
                            "A key format '%s' requires the declaration of one or more of key fields using '%s'.",
                            KEY_FORMAT.key(), KEY_FIELDS.key()));
        }

        if (!optionalKeyFormat.isPresent()) {
            return new int[0];
        }

        final String keyPrefix = options.getOptional(KEY_FIELDS_PREFIX).orElse("");

        final List<String> keyFields = optionalKeyFields.get();
        final List<String> physicalFields = LogicalTypeChecks.getFieldNames(physicalType);
        return keyFields.stream()
                .mapToInt(
                        keyField -> {
                            final int pos = physicalFields.indexOf(keyField);
                            // check that field name exists
                            if (pos < 0) {
                                throw new ValidationException(
                                        String.format(
                                                "Could not find the field '%s' in the table schema for usage in the key format. "
                                                        + "A key field must be a regular, physical column. "
                                                        + "The following columns can be selected in the '%s' option:\n"
                                                        + "%s",
                                                keyField, KEY_FIELDS.key(), physicalFields));
                            }
                            // check that field name is prefixed correctly
                            if (!keyField.startsWith(keyPrefix)) {
                                throw new ValidationException(
                                        String.format(
                                                "All fields in '%s' must be prefixed with '%s' when option '%s' "
                                                        + "is set but field '%s' is not prefixed.",
                                                KEY_FIELDS.key(),
                                                keyPrefix,
                                                KEY_FIELDS_PREFIX.key(),
                                                keyField));
                            }
                            return pos;
                        })
                .toArray();
    }

    /**
     * Creates an array of indices that determine which physical fields of the table schema to
     * include in the value format.
     *
     * <p>See {@link KafkaConnectorOptions#VALUE_FORMAT}, {@link
     * KafkaConnectorOptions#VALUE_FIELDS_INCLUDE}, and {@link
     * KafkaConnectorOptions#KEY_FIELDS_PREFIX} for more information.
     */
    public static int[] createValueFormatProjection(
            ReadableConfig options, DataType physicalDataType) {
        final LogicalType physicalType = physicalDataType.getLogicalType();
        Preconditions.checkArgument(
                physicalType.is(LogicalTypeRoot.ROW), "Row data type expected.");
        final int physicalFieldCount = LogicalTypeChecks.getFieldCount(physicalType);
        final IntStream physicalFields = IntStream.range(0, physicalFieldCount);

        final String keyPrefix = options.getOptional(KEY_FIELDS_PREFIX).orElse("");

        final KafkaConnectorOptions.ValueFieldsStrategy strategy = options.get(VALUE_FIELDS_INCLUDE);
        if (strategy == KafkaConnectorOptions.ValueFieldsStrategy.ALL) {
            if (keyPrefix.length() > 0) {
                throw new ValidationException(
                        String.format(
                                "A key prefix is not allowed when option '%s' is set to '%s'. "
                                        + "Set it to '%s' instead to avoid field overlaps.",
                                VALUE_FIELDS_INCLUDE.key(),
                                KafkaConnectorOptions.ValueFieldsStrategy.ALL,
                                KafkaConnectorOptions.ValueFieldsStrategy.EXCEPT_KEY));
            }
            return physicalFields.toArray();
        } else if (strategy == KafkaConnectorOptions.ValueFieldsStrategy.EXCEPT_KEY) {
            final int[] keyProjection = createKeyFormatProjection(options, physicalDataType);
            return physicalFields
                    .filter(pos -> IntStream.of(keyProjection).noneMatch(k -> k == pos))
                    .toArray();
        }
        throw new TableException("Unknown value fields strategy:" + strategy);
    }

    /**
     * It is used to wrap the decoding format and expose the desired changelog mode. It's only works
     * for insert-only format.
     */
    protected static class DecodingFormatWrapper
            implements
                DecodingFormat<DeserializationSchema<RowData>> {

        private final DecodingFormat<DeserializationSchema<RowData>> innerDecodingFormat;

        private static final ChangelogMode SOURCE_CHANGELOG_MODE =
                ChangelogMode.newBuilder()
                        .addContainedKind(RowKind.UPDATE_AFTER)
                        .addContainedKind(RowKind.DELETE)
                        .build();

        public DecodingFormatWrapper(
                DecodingFormat<DeserializationSchema<RowData>> innerDecodingFormat) {
            this.innerDecodingFormat = innerDecodingFormat;
        }

        @Override
        public DeserializationSchema<RowData> createRuntimeDecoder(
                DynamicTableSource.Context context, DataType producedDataType) {
            return innerDecodingFormat.createRuntimeDecoder(context, producedDataType);
        }

        @Override
        public ChangelogMode getChangelogMode() {
            return SOURCE_CHANGELOG_MODE;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this) {
                return true;
            }

            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }

            DecodingFormatWrapper that = (DecodingFormatWrapper) obj;
            return Objects.equals(innerDecodingFormat, that.innerDecodingFormat);
        }

        @Override
        public int hashCode() {
            return Objects.hash(innerDecodingFormat);
        }
    }

    /**
     * It is used to wrap the encoding format and expose the desired changelog mode. It's only works
     * for insert-only format.
     */
    protected static class EncodingFormatWrapper
            implements
                EncodingFormat<SerializationSchema<RowData>> {

        private final EncodingFormat<SerializationSchema<RowData>> innerEncodingFormat;

        public static final ChangelogMode SINK_CHANGELOG_MODE =
                ChangelogMode.newBuilder()
                        .addContainedKind(RowKind.INSERT)
                        .addContainedKind(RowKind.UPDATE_AFTER)
                        .addContainedKind(RowKind.DELETE)
                        .build();

        public EncodingFormatWrapper(
                EncodingFormat<SerializationSchema<RowData>> innerEncodingFormat) {
            this.innerEncodingFormat = innerEncodingFormat;
        }

        @Override
        public SerializationSchema<RowData> createRuntimeEncoder(
                DynamicTableSink.Context context, DataType consumedDataType) {
            return innerEncodingFormat.createRuntimeEncoder(context, consumedDataType);
        }

        @Override
        public ChangelogMode getChangelogMode() {
            return SINK_CHANGELOG_MODE;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this) {
                return true;
            }

            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }

            EncodingFormatWrapper that = (EncodingFormatWrapper) obj;
            return Objects.equals(innerEncodingFormat, that.innerEncodingFormat);
        }

        @Override
        public int hashCode() {
            return Objects.hash(innerEncodingFormat);
        }
    }
}
