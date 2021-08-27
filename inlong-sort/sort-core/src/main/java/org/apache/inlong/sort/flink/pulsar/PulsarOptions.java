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

package org.apache.inlong.sort.flink.pulsar;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/**
 * The configuration options for pulsar sources and sink.
 */
public class PulsarOptions {

    public static final String BOOTSTRAP_MODE_LATEST = "latest";
    public static final String BOOTSTRAP_MODE_EARLIEST = "earliest";

    public static final ConfigOption<String> OPERATION_TIMEOUT =
            ConfigOptions.key("operation-timeout")
                    .defaultValue("30s")
                    .withDescription("Duration of waiting for completing an operation.");

    public static final ConfigOption<String> CONNECTION_TIMEOUT =
            ConfigOptions.key("connection-timeout")
                    .defaultValue("10s")
                    .withDescription("Duration of waiting for a connection to a broker to be established.");

    public static final ConfigOption<String> REQUEST_TIMEOUT =
            ConfigOptions.key("request-timeout")
                    .defaultValue("60s")
                    .withDescription("Duration of waiting for completing a request.");

    public static final ConfigOption<String> KEEPALIVE_INTERVAL =
            ConfigOptions.key("keepalive-interval")
                    .defaultValue("30s")
                    .withDescription("Duration of keeping alive interval for each client broker connection.");

    public static final ConfigOption<String> CONSUMER_BOOTSTRAP_MODE =
            ConfigOptions.key("consumer.bootstrap-mode")
                    .defaultValue(BOOTSTRAP_MODE_LATEST)
                    .withDescription("The behavior when the consumer bootstraps. This only takes effect when not "
                            + "restoring from checkpoints.");

    public static final ConfigOption<Integer> CONSUMER_RECEIVE_QUEUE_SIZE =
            ConfigOptions.key("consumer.receive-queue-size")
                    .defaultValue(1000)
                    .withDescription("The size of a consumer's receiver queue.");

    public static final ConfigOption<String> CONSUMER_RECEIVE_TIMEOUT =
            ConfigOptions.key("consumer.receive-timeout")
                    .defaultValue("120s")
                    .withDescription("The timeout in each receiving from pulsar.");

    public static final ConfigOption<String> CONSUMER_CHECK_PARTITION_INTERVAL =
            ConfigOptions.key("consumer.check-partition-interval")
                    .defaultValue("5s")
                    .withDescription("Duration of checking interval for each partition reader.");

    public static final ConfigOption<String> CONSUMER_MAX_IDLE_TIME =
            ConfigOptions.key("consumer.max-idle-time")
                    .defaultValue("60s")
                    .withDescription("The max idle time for the pulsar consumer.");

    public static final ConfigOption<Boolean> CONSUMER_ENABLE_AUTO_COMMIT =
            ConfigOptions.key("consumer.enable-auto-commit")
                    .defaultValue(true)
                    .withDescription("True if the consumer is enabled to automatically commit offsets.");

    public static final ConfigOption<String> CONSUMER_AUTO_COMMIT_INTERVAL =
            ConfigOptions.key("consumer.auto-commit-interval")
                    .defaultValue("120s")
                    .withDescription("The interval for consumers to commit offset if automatically commit is enabled.");

    public static final ConfigOption<String> PRODUCER_ROUTE_MODE =
            ConfigOptions.key("producer.route-mode")
                    .defaultValue("RoundRobinPartition")
                    .withDescription("Message routing logic for producers on partitioned topics.");

    public static final ConfigOption<Integer> PRODUCER_PENDING_QUEUE_SIZE =
            ConfigOptions.key("producer.pending-queue-size")
                    .defaultValue(1000)
                    .withDescription("The maximum size of a queue holding pending messages.");

    public static final ConfigOption<Integer> PRODUCER_PENDING_SIZE =
            ConfigOptions.key("producer.pending-total-size")
                    .defaultValue(50000)
                    .withDescription("The maximum number of pending messages across partitions.");

    public static final ConfigOption<Boolean> PRODUCER_BLOCK_QUEUE_FULL =
            ConfigOptions.key("producer.block-if-queue-full")
                    .defaultValue(true)
                    .withDescription("When the queue is full, the method is blocked "
                            + "instead of an exception is thrown.");

    public static final ConfigOption<String> PRODUCER_SEND_TIMEOUT =
            ConfigOptions.key("producer.send-timeout")
                    .defaultValue("30s")
                    .withDescription("The timeout in each sending to pulsar.");

    public static final ConfigOption<Boolean> SOURCE_DISABLE_CHAINING =
            ConfigOptions.key("source.disable-chaining")
                    .defaultValue(false)
                    .withDescription("The source operator will not chain with downstream if true.");

    public static final ConfigOption<Boolean> SINK_START_NEW_CHAIN =
            ConfigOptions.key("sink.start-new-chain")
                    .defaultValue(true)
                    .withDescription("The sink operator will start a new chain if true.");
}
