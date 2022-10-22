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

package io.tidb.bigdata.flink.connector.source;

import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.PROPS_BOOTSTRAP_SERVERS;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.PROPS_GROUP_ID;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.TOPIC;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.SCAN_STARTUP_MODE;

import io.tidb.bigdata.flink.format.cdc.CDCDeserializationSchemaBuilder;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Pattern;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
import org.apache.flink.connector.kafka.source.KafkaSourceOptions;
import org.apache.flink.connector.kafka.source.enumerator.KafkaSourceEnumState;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.connector.kafka.source.split.KafkaPartitionSplit;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.table.data.RowData;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;

/**
 *  KafkaCDCSourceBuilder
 *
 * @see org.apache.flink.connector.kafka.source.KafkaSourceBuilder
 */
public class KafkaCDCSourceBuilder
        extends CDCSourceBuilder<KafkaPartitionSplit, KafkaSourceEnumState> {

    private final KafkaSourceBuilder<RowData> builder = KafkaSource.builder();

    private static final String OPTION_PREFIX = "tidb.streaming.kafka.";

    private static final int OPTION_PREFIX_LENGTH = OPTION_PREFIX.length();

    public KafkaCDCSourceBuilder(final CDCDeserializationSchemaBuilder builder) {
        super(builder);
    }

    @Override
    public Type type() {
        return Type.KAFKA;
    }

    @Override
    protected CDCSource<KafkaPartitionSplit, KafkaSourceEnumState>
    doBuild(DeserializationSchema<RowData> schema) {
        setDeserializer(schema);
        return new CDCSource<>(builder.build());
    }

    @Override
    protected CDCSource<KafkaPartitionSplit, KafkaSourceEnumState>
    doBuild(KafkaDeserializationSchema<RowData> schema) {
        setDeserializer(schema);
        return new CDCSource<>(builder.build());
    }

    public KafkaCDCSourceBuilder setBootstrapServers(String bootstrapServers) {
        return setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    }

    public KafkaCDCSourceBuilder setGroupId(String groupId) {
        return setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    }

    public KafkaCDCSourceBuilder setTopics(List<String> topics) {
        builder.setTopics(topics);
        return this;
    }

    public KafkaCDCSourceBuilder setTopics(String... topics) {
        return setTopics(Arrays.asList(topics));
    }

    public KafkaCDCSourceBuilder setTopicPattern(Pattern topicPattern) {
        builder.setTopicPattern(topicPattern);
        return this;
    }

    public KafkaCDCSourceBuilder setPartitions(Set<TopicPartition> partitions) {
        builder.setPartitions(partitions);
        return this;
    }

    public KafkaCDCSourceBuilder setStartingOffsets(
            OffsetsInitializer startingOffsetsInitializer) {
        builder.setStartingOffsets(startingOffsetsInitializer);
        return this;
    }

    public KafkaCDCSourceBuilder setDeserializer(
            DeserializationSchema<RowData> deserializationSchema) {
        builder.setValueOnlyDeserializer(deserializationSchema);
        return this;
    }

    public KafkaCDCSourceBuilder setDeserializer(
            KafkaDeserializationSchema<RowData> deserializationSchema) {
        builder.setDeserializer(KafkaRecordDeserializationSchema.of(deserializationSchema));
        return this;
    }

    public KafkaCDCSourceBuilder setClientIdPrefix(String prefix) {
        return setProperty(KafkaSourceOptions.CLIENT_ID_PREFIX.key(), prefix);
    }

    public KafkaCDCSourceBuilder setProperty(String key, String value) {
        builder.setProperty(key, value);
        return this;
    }

    private void handleOptions(String key, String value) {
        if (key.startsWith(OPTION_PREFIX)) {
            key = key.substring(OPTION_PREFIX_LENGTH);
        }

        if (PROPS_BOOTSTRAP_SERVERS.key().equals(key)) {
            setBootstrapServers(value);
        } else if (PROPS_GROUP_ID.key().equals(key)) {
            setGroupId(value);
        } else if (TOPIC.key().equals(key)) {
            setTopics(value);
        } else if (SCAN_STARTUP_MODE.key().equals(key)) {
            if (OffsetResetStrategy.LATEST.name().equalsIgnoreCase(value)) {
                setStartingOffsets(OffsetsInitializer.latest());
            } else if (OffsetResetStrategy.EARLIEST.name().equalsIgnoreCase(value)) {
                setStartingOffsets(OffsetsInitializer.earliest());
            }
        } else {
            setProperty(key, value);
        }
    }

    public KafkaCDCSourceBuilder setProperties(Map<String, String> properties) {
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            handleOptions(entry.getKey(), entry.getValue());
        }
        return this;
    }

    public KafkaCDCSourceBuilder setProperties(Properties properties) {
        for (Map.Entry<Object, Object> entry : properties.entrySet()) {
            handleOptions(entry.getKey().toString(), entry.getValue().toString());
        }
        return this;
    }
}