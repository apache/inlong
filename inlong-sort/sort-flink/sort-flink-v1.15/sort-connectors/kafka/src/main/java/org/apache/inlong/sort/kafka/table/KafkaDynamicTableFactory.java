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

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.inlong.sort.base.Constants;
import org.apache.inlong.sort.kafka.KafkaOptions;

import java.util.HashSet;
import java.util.Set;


@Internal
public class KafkaDynamicTableFactory implements DynamicTableSourceFactory, DynamicTableSinkFactory {

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
        options.add(KafkaConnectorOptions.KEY_FORMAT);
        options.add(KafkaConnectorOptions.KEY_FIELDS);
        options.add(KafkaConnectorOptions.KEY_FIELDS_PREFIX);
        options.add(KafkaConnectorOptions.VALUE_FORMAT);
        options.add(KafkaConnectorOptions.VALUE_FIELDS_INCLUDE);
        options.add(KafkaConnectorOptions.TOPIC);
        options.add(KafkaConnectorOptions.TOPIC_PATTERN);
        options.add(KafkaConnectorOptions.PROPS_GROUP_ID);
        options.add(KafkaConnectorOptions.SCAN_STARTUP_MODE);
        options.add(KafkaConnectorOptions.SCAN_STARTUP_SPECIFIC_OFFSETS);
        options.add(KafkaConnectorOptions.SCAN_TOPIC_PARTITION_DISCOVERY);
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
        return null;
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        return null;
    }

}
