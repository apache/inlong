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

import static org.apache.flink.util.TimeUtils.parseDuration;
import static org.apache.inlong.sort.flink.pulsar.PulsarOptions.CONNECTION_TIMEOUT;
import static org.apache.inlong.sort.flink.pulsar.PulsarOptions.CONSUMER_RECEIVE_QUEUE_SIZE;
import static org.apache.inlong.sort.flink.pulsar.PulsarOptions.KEEPALIVE_INTERVAL;
import static org.apache.inlong.sort.flink.pulsar.PulsarOptions.OPERATION_TIMEOUT;
import static org.apache.inlong.sort.flink.pulsar.PulsarOptions.PRODUCER_BLOCK_QUEUE_FULL;
import static org.apache.inlong.sort.flink.pulsar.PulsarOptions.PRODUCER_PENDING_QUEUE_SIZE;
import static org.apache.inlong.sort.flink.pulsar.PulsarOptions.PRODUCER_PENDING_SIZE;
import static org.apache.inlong.sort.flink.pulsar.PulsarOptions.PRODUCER_ROUTE_MODE;
import static org.apache.inlong.sort.flink.pulsar.PulsarOptions.PRODUCER_SEND_TIMEOUT;
import static org.apache.inlong.sort.flink.pulsar.PulsarOptions.REQUEST_TIMEOUT;
import static org.apache.pulsar.common.naming.TopicName.PARTITIONED_TOPIC_SUFFIX;
import static org.apache.pulsar.common.naming.TopicName.getPartitionIndex;

import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import org.apache.flink.configuration.Configuration;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.admin.internal.PulsarAdminImpl;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.common.policies.data.PersistentTopicInternalStats;
import org.apache.pulsar.common.policies.data.SubscriptionStats;
import org.apache.pulsar.common.policies.data.TopicStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The utility class for pulsar.
 */
public class PulsarUtils {

    private static final Logger LOG =
            LoggerFactory.getLogger(PulsarUtils.class);

    public static PulsarClient createClient(
            String serviceUrl,
            Configuration configuration
    ) throws PulsarClientException {

        Duration operationTimeout =
                parseDuration(configuration.getString(OPERATION_TIMEOUT));
        Duration connectionTimeout =
                parseDuration(configuration.getString(CONNECTION_TIMEOUT));
        Duration requestTimeout =
                parseDuration(configuration.getString(REQUEST_TIMEOUT));
        Duration keepAliveInterval =
                parseDuration(configuration.getString(KEEPALIVE_INTERVAL));

        ClientConfigurationData clientConfigurationData =
                new ClientConfigurationData();
        clientConfigurationData
                .setServiceUrl(serviceUrl);
        clientConfigurationData
                .setOperationTimeoutMs(operationTimeout.toMillis());
        clientConfigurationData
                .setConnectionTimeoutMs((int) connectionTimeout.toMillis());
        clientConfigurationData
                .setRequestTimeoutMs((int) requestTimeout.toMillis());
        clientConfigurationData
                .setKeepAliveIntervalSeconds((int) keepAliveInterval.getSeconds());

        return new PulsarClientImpl(clientConfigurationData);
    }

    public static Reader<byte[]> createReader(
            PulsarClient client,
            String topicPartition,
            MessageId bootstrapOffset,
            Configuration configuration,
            Map<String, Object> readerConf
    ) throws PulsarClientException {

        int receiveQueueSize =
                configuration.getInteger(CONSUMER_RECEIVE_QUEUE_SIZE);

        return client
                .newReader()
                .topic(topicPartition)
                .startMessageId(bootstrapOffset)
                .receiverQueueSize(receiveQueueSize)
                .loadConf(readerConf)
                .create();
    }

    public static Producer<byte[]> createProducer(
            PulsarClient client,
            String topic,
            Configuration configuration
    ) throws PulsarClientException {
        MessageRoutingMode routeMode = Enum.valueOf(MessageRoutingMode.class,
                PRODUCER_ROUTE_MODE.defaultValue());
        Duration sendTimeout =
                parseDuration(configuration.getString(PRODUCER_SEND_TIMEOUT));
        int pendingQueueSize =
                configuration.getInteger(PRODUCER_PENDING_QUEUE_SIZE);
        int pendingSize =
                configuration.getInteger(PRODUCER_PENDING_SIZE);
        boolean blockIfQueueFull =
                configuration.getBoolean(PRODUCER_BLOCK_QUEUE_FULL);

        return client
                .newProducer()
                .topic(topic)
                .messageRoutingMode(routeMode)
                .sendTimeout((int) sendTimeout.toMillis(), TimeUnit.MILLISECONDS)
                .maxPendingMessages(pendingQueueSize)
                .maxPendingMessagesAcrossPartitions(pendingSize)
                .blockIfQueueFull(blockIfQueueFull)
                .create();
    }

    public static PulsarAdmin createAdmin(
            String adminUrl
    ) throws PulsarClientException {
        return new PulsarAdminImpl(adminUrl, new ClientConfigurationData());
    }

    public static Set<String> getTopicPartitions(
            PulsarAdmin admin,
            String topic
    ) throws PulsarAdminException {
        Set<String> topicPartitions = new TreeSet<>();

        int numTopicPartitions =
                admin.topics().getPartitionedTopicMetadata(topic).partitions;
        if (numTopicPartitions == 0) {
            topicPartitions.add(topic);
        } else {
            for (int i = 0; i < numTopicPartitions; ++i) {
                topicPartitions.add(topic + PARTITIONED_TOPIC_SUFFIX + i);
            }
        }

        return topicPartitions;
    }

    public static MessageId getTopicPartitionOffset(
            PulsarAdmin admin,
            String topicPartition,
            String consumerGroup
    ) throws PulsarAdminException {
        TopicStats topicStats = admin.topics().getStats(topicPartition);
        if (topicStats.getSubscriptions().containsKey(consumerGroup)) {
            SubscriptionStats subStats =
                    topicStats.getSubscriptions().get(consumerGroup);
            if (subStats.getConsumers().size() != 0) {
                throw new RuntimeException("Subscription been actively used by other consumers in this situation, the "
                        + "exactly-once semantics cannot be guaranteed.");
            } else {
                PersistentTopicInternalStats.CursorStats cursorStats =
                        admin
                                .topics()
                                .getInternalStats(topicPartition)
                                .cursors
                                .get(consumerGroup);

                String[] ids =
                        cursorStats.markDeletePosition.split(":", 2);
                long ledgerId = Long.parseLong(ids[0]);
                long entryId = Long.parseLong(ids[1]);
                int partitionIndex = getPartitionIndex(topicPartition);

                return new MessageIdImpl(
                        ledgerId,
                        entryId + 1,
                        partitionIndex
                );
            }
        } else {
            throw new RuntimeException("Could not find consumer group '" + consumerGroup + "' for topic partition "
                    + topicPartition + ".");
        }
    }

    public static void commitTopicPartitionOffset(
            PulsarAdmin admin,
            String topicPartition,
            MessageId offset,
            String consumerGroup
    ) {
        try {
            admin.topics().resetCursor(topicPartition, consumerGroup, offset);
        } catch (Throwable e) {
            if (e instanceof PulsarAdminException
                    && (((PulsarAdminException) e).getStatusCode() == 404
                    || ((PulsarAdminException) e).getStatusCode() == 412)) {
                LOG.info("Cannot commit cursor since the topic {} has been deleted during execution", topicPartition);
            } else {
                throw new RuntimeException(String.format("Failed to commit cursor for %s", topicPartition), e);
            }
        }
    }
}
