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

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.types.RowKind;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.shade.com.google.common.collect.Sets;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.apache.inlong.sort.pulsar.PulsarTableOptionUtils.getValueDecodingFormat;
import static org.apache.inlong.sort.pulsar.PulsarTableOptions.KEY_FIELDS;
import static org.apache.inlong.sort.pulsar.PulsarTableOptions.KEY_FORMAT;
import static org.apache.inlong.sort.pulsar.PulsarTableOptions.SOURCE_START_FROM_PUBLISH_TIME;
import static org.apache.inlong.sort.pulsar.PulsarTableOptions.SOURCE_STOP_AFTER_MESSAGE_ID;
import static org.apache.inlong.sort.pulsar.PulsarTableOptions.SOURCE_STOP_AT_MESSAGE_ID;
import static org.apache.inlong.sort.pulsar.PulsarTableOptions.SOURCE_STOP_AT_PUBLISH_TIME;
import static org.apache.inlong.sort.pulsar.PulsarTableOptions.SOURCE_SUBSCRIPTION_TYPE;
import static org.apache.inlong.sort.pulsar.PulsarTableOptions.STARTUP_MODE;
import static org.apache.inlong.sort.pulsar.PulsarTableOptions.TOPIC;
import static org.apache.pulsar.common.naming.TopicName.isValid;

/** Util class for source and sink validation rules.
 * Modified from {@link org.apache.flink.connector.pulsar.table.PulsarTableValidationUtils}.
*/
public class PulsarTableValidationUtils {

    private PulsarTableValidationUtils() {
    }

    public static void validatePrimaryKeyConstraints(
            ObjectIdentifier tableName,
            int[] primaryKeyIndexes,
            FactoryUtil.TableFactoryHelper helper) {
        final DecodingFormat<DeserializationSchema<RowData>> format =
                getValueDecodingFormat(helper);
        if (primaryKeyIndexes.length > 0
                && format.getChangelogMode().containsOnly(RowKind.INSERT)) {
            throw new ValidationException(
                    String.format(
                            "The Pulsar table '%s' with '%s' format doesn't support defining PRIMARY KEY constraint"
                                    + " on the table, because it can't guarantee the semantic of primary key.",
                            tableName.asSummaryString(), format));
        }
    }

    public static void validateTableSourceOptions(ReadableConfig tableOptions) {
        validateTopicsConfigs(tableOptions);
        validateStartCursorConfigs(tableOptions);
        validateStopCursorConfigs(tableOptions);
        validateSubscriptionTypeConfigs(tableOptions);
        validateKeyFormatConfigs(tableOptions);
    }

    protected static void validateTopicsConfigs(ReadableConfig tableOptions) {
        if (tableOptions.get(TOPIC).isEmpty()) {
            throw new ValidationException("The topics list should not be empty.");
        }

        for (String topic : tableOptions.get(TOPIC)) {
            if (!isValid(topic)) {
                throw new ValidationException(
                        String.format("The topics name %s is not a valid topic name.", topic));
            }
        }
    }

    protected static void validateStartCursorConfigs(ReadableConfig tableOptions) {
        if (tableOptions.getOptional(STARTUP_MODE).isPresent()
                && tableOptions.getOptional(SOURCE_START_FROM_PUBLISH_TIME).isPresent()) {
            throw new ValidationException(
                    String.format(
                            "Only one of %s and %s can be specified. Detected both of them",
                            STARTUP_MODE, SOURCE_START_FROM_PUBLISH_TIME));
        }
    }

    protected static void validateStopCursorConfigs(ReadableConfig tableOptions) {
        Set<ConfigOption<?>> conflictConfigOptions =
                Sets.newHashSet(
                        SOURCE_STOP_AT_MESSAGE_ID,
                        SOURCE_STOP_AFTER_MESSAGE_ID,
                        SOURCE_STOP_AT_PUBLISH_TIME);

        long configsNums =
                conflictConfigOptions.stream()
                        .map(tableOptions::getOptional)
                        .filter(Optional::isPresent)
                        .count();

        if (configsNums > 1) {
            throw new ValidationException(
                    String.format(
                            "Only one of %s, %s and %s can be specified. Detected more than 1 of them",
                            SOURCE_STOP_AT_MESSAGE_ID,
                            SOURCE_STOP_AFTER_MESSAGE_ID,
                            SOURCE_STOP_AT_PUBLISH_TIME));
        }
    }

    protected static void validateSubscriptionTypeConfigs(ReadableConfig tableOptions) {
        SubscriptionType subscriptionType = tableOptions.get(SOURCE_SUBSCRIPTION_TYPE);

        if (subscriptionType == SubscriptionType.Failover) {
            throw new ValidationException(
                    String.format(
                            "%s SubscriptionType is not supported. ", SubscriptionType.Failover));
        }
    }

    protected static void validateKeyFormatConfigs(ReadableConfig tableOptions) {
        final Optional<String> optionalKeyFormat = tableOptions.getOptional(KEY_FORMAT);
        final Optional<List<String>> optionalKeyFields = tableOptions.getOptional(KEY_FIELDS);
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
    }
}
