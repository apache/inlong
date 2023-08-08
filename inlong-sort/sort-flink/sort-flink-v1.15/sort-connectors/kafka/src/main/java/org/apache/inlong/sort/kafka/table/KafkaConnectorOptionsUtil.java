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

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.connectors.kafka.config.StartupMode;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;
import org.apache.flink.util.Preconditions;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.IntStream;

import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions.KEY_FIELDS;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions.KEY_FIELDS_PREFIX;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions.KEY_FORMAT;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions.SCAN_STARTUP_MODE;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions.SCAN_STARTUP_SPECIFIC_OFFSETS;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions.SCAN_STARTUP_TIMESTAMP_MILLIS;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions.TOPIC;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions.TOPIC_PATTERN;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions.VALUE_FIELDS_INCLUDE;

public class KafkaConnectorOptionsUtil {

    public static final String PROPERTIES_PREFIX = "properties.";

    // Other keywords.
    private static final String PARTITION = "partition";
    private static final String OFFSET = "offset";

    public static void validateTableSourceOptions(ReadableConfig tableOptions) {
        validateSourceTopic(tableOptions);
        validateScanStartupMode(tableOptions);
    }

    public static void validateSourceTopic(ReadableConfig tableOptions) {
        Optional<List<String>> topic = tableOptions.getOptional(TOPIC);
        Optional<String> pattern = tableOptions.getOptional(TOPIC_PATTERN);

        if (topic.isPresent() && pattern.isPresent()) {
            throw new ValidationException(
                    "Option 'topic' and 'topic-pattern' shouldn't be set together.");
        }

        if (!topic.isPresent() && !pattern.isPresent()) {
            throw new ValidationException("Either 'topic' or 'topic-pattern' must be set.");
        }
    }

    private static void validateScanStartupMode(ReadableConfig tableOptions) {
        tableOptions
                .getOptional(SCAN_STARTUP_MODE)
                .ifPresent(
                        mode -> {
                            switch (mode) {
                                case TIMESTAMP:
                                    if (!tableOptions
                                            .getOptional(SCAN_STARTUP_TIMESTAMP_MILLIS)
                                            .isPresent()) {
                                        throw new ValidationException(
                                                String.format(
                                                        "'%s' is required in '%s' startup mode"
                                                                + " but missing.",
                                                        SCAN_STARTUP_TIMESTAMP_MILLIS.key(),
                                                        KafkaConnectorOptions.ScanStartupMode.TIMESTAMP));
                                    }

                                    break;
                                case SPECIFIC_OFFSETS:
                                    if (!tableOptions
                                            .getOptional(SCAN_STARTUP_SPECIFIC_OFFSETS)
                                            .isPresent()) {
                                        throw new ValidationException(
                                                String.format(
                                                        "'%s' is required in '%s' startup mode"
                                                                + " but missing.",
                                                        SCAN_STARTUP_SPECIFIC_OFFSETS.key(),
                                                        KafkaConnectorOptions.ScanStartupMode.SPECIFIC_OFFSETS));
                                    }
                                    if (!isSingleTopic(tableOptions)) {
                                        throw new ValidationException(
                                                "Currently Kafka source only supports specific offset for single topic.");
                                    }
                                    String specificOffsets =
                                            tableOptions.get(SCAN_STARTUP_SPECIFIC_OFFSETS);
                                    parseSpecificOffsets(
                                            specificOffsets, SCAN_STARTUP_SPECIFIC_OFFSETS.key());

                                    break;
                            }
                        });
    }

    private static boolean isSingleTopic(ReadableConfig tableOptions) {
        // Option 'topic-pattern' is regarded as multi-topics.
        return tableOptions.getOptional(TOPIC).map(t -> t.size() == 1).orElse(false);
    }

    public static Map<Integer, Long> parseSpecificOffsets(
            String specificOffsetsStr, String optionKey) {
        final Map<Integer, Long> offsetMap = new HashMap<>();
        final String[] pairs = specificOffsetsStr.split(";");
        final String validationExceptionMessage =
                String.format(
                        "Invalid properties '%s' should follow the format "
                                + "'partition:0,offset:42;partition:1,offset:300', but is '%s'.",
                        optionKey, specificOffsetsStr);

        if (pairs.length == 0) {
            throw new ValidationException(validationExceptionMessage);
        }

        for (String pair : pairs) {
            if (null == pair || pair.length() == 0 || !pair.contains(",")) {
                throw new ValidationException(validationExceptionMessage);
            }

            final String[] kv = pair.split(",");
            if (kv.length != 2
                    || !kv[0].startsWith(PARTITION + ':')
                    || !kv[1].startsWith(OFFSET + ':')) {
                throw new ValidationException(validationExceptionMessage);
            }

            String partitionValue = kv[0].substring(kv[0].indexOf(":") + 1);
            String offsetValue = kv[1].substring(kv[1].indexOf(":") + 1);
            try {
                final Integer partition = Integer.valueOf(partitionValue);
                final Long offset = Long.valueOf(offsetValue);
                offsetMap.put(partition, offset);
            } catch (NumberFormatException e) {
                throw new ValidationException(validationExceptionMessage, e);
            }
        }
        return offsetMap;
    }

    public static StartupOptions getStartupOptions(ReadableConfig tableOptions) {
        final Map<KafkaTopicPartition, Long> specificOffsets = new HashMap<>();
        final StartupMode startupMode =
                tableOptions
                        .getOptional(SCAN_STARTUP_MODE)
                        .map(KafkaConnectorOptionsUtil::fromOption)
                        .orElse(StartupMode.GROUP_OFFSETS);
        if (startupMode == StartupMode.SPECIFIC_OFFSETS) {
            // It will be refactored after support specific offset for multiple topics in
            // FLINK-18602. We have already checked tableOptions.get(TOPIC) contains one topic in
            // validateScanStartupMode().
            buildSpecificOffsets(tableOptions, tableOptions.get(TOPIC).get(0), specificOffsets);
        }

        final StartupOptions options = new StartupOptions();
        options.startupMode = startupMode;
        options.specificOffsets = specificOffsets;
        if (startupMode == StartupMode.TIMESTAMP) {
            options.startupTimestampMillis = tableOptions.get(SCAN_STARTUP_TIMESTAMP_MILLIS);
        }
        return options;
    }

    private static void buildSpecificOffsets(
            ReadableConfig tableOptions,
            String topic,
            Map<KafkaTopicPartition, Long> specificOffsets) {
        String specificOffsetsStrOpt = tableOptions.get(SCAN_STARTUP_SPECIFIC_OFFSETS);
        final Map<Integer, Long> offsetMap =
                parseSpecificOffsets(specificOffsetsStrOpt, SCAN_STARTUP_SPECIFIC_OFFSETS.key());
        offsetMap.forEach(
                (partition, offset) -> {
                    final KafkaTopicPartition topicPartition =
                            new KafkaTopicPartition(topic, partition);
                    specificOffsets.put(topicPartition, offset);
                });
    }

    /**
     * Returns the {@link StartupMode} of Kafka Consumer by passed-in table-specific {@link
     * KafkaConnectorOptions.ScanStartupMode}.
     */
    private static StartupMode fromOption(KafkaConnectorOptions.ScanStartupMode scanStartupMode) {
        switch (scanStartupMode) {
            case EARLIEST_OFFSET:
                return StartupMode.EARLIEST;
            case LATEST_OFFSET:
                return StartupMode.LATEST;
            case GROUP_OFFSETS:
                return StartupMode.GROUP_OFFSETS;
            case SPECIFIC_OFFSETS:
                return StartupMode.SPECIFIC_OFFSETS;
            case TIMESTAMP:
                return StartupMode.TIMESTAMP;

            default:
                throw new TableException(
                        "Unsupported startup mode. Validator should have checked that.");
        }
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

    // --------------------------------------------------------------------------------------------
    // Utilities
    // --------------------------------------------------------------------------------------------

    public static List<String> getSourceTopics(ReadableConfig tableOptions) {
        return tableOptions.getOptional(TOPIC).orElse(null);
    }

    public static Pattern getSourceTopicPattern(ReadableConfig tableOptions) {
        return tableOptions.getOptional(TOPIC_PATTERN).map(Pattern::compile).orElse(null);
    }

    // --------------------------------------------------------------------------------------------
    // Inner classes
    // --------------------------------------------------------------------------------------------

    /** Kafka startup options. * */
    public static class StartupOptions {

        public StartupMode startupMode;
        public Map<KafkaTopicPartition, Long> specificOffsets;
        public long startupTimestampMillis;
    }

    private KafkaConnectorOptionsUtil() {
    }
}
