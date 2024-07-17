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

package org.apache.inlong.manager.service.resource.queue.kafka;

import org.apache.inlong.common.enums.MessageWrapType;
import org.apache.inlong.manager.common.consts.InlongConstants;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.pojo.cluster.kafka.KafkaClusterInfo;
import org.apache.inlong.manager.pojo.consume.BriefMQMessage;
import org.apache.inlong.manager.pojo.group.kafka.InlongKafkaInfo;
import org.apache.inlong.manager.pojo.stream.InlongStreamInfo;
import org.apache.inlong.manager.pojo.stream.QueryMessageRequest;
import org.apache.inlong.manager.service.cluster.InlongClusterServiceImpl;
import org.apache.inlong.manager.service.message.DeserializeOperator;
import org.apache.inlong.manager.service.message.DeserializeOperatorFactory;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

/**
 * kafka operator, supports creating topics and creating subscription.
 */
@Service
public class KafkaOperator {

    private static final Logger LOGGER = LoggerFactory.getLogger(InlongClusterServiceImpl.class);

    @Autowired
    public DeserializeOperatorFactory deserializeOperatorFactory;

    /**
     * Create Kafka topic inlongKafkaInfo
     */
    public void createTopic(InlongKafkaInfo inlongKafkaInfo, KafkaClusterInfo kafkaClusterInfo, String topicName)
            throws InterruptedException, ExecutionException {
        AdminClient adminClient = KafkaUtils.getAdminClient(kafkaClusterInfo);
        NewTopic topic = new NewTopic(topicName,
                inlongKafkaInfo.getNumPartitions(),
                inlongKafkaInfo.getReplicationFactor());
        // Topic will be returned if it exists, and created if it does not exist
        if (topicIsExists(kafkaClusterInfo, topicName)) {
            LOGGER.warn("kafka topic={} already exists", topicName);
            return;
        }
        CreateTopicsResult result = adminClient.createTopics(Collections.singletonList(topic));
        // To prevent the client from disconnecting too quickly and causing the Topic to not be created successfully
        Thread.sleep(500);
        LOGGER.info("success to create kafka topic={}, with={} numPartitions",
                topicName,
                result.numPartitions(topicName).get());
    }

    /**
     * Force delete Kafka topic
     */
    public void forceDeleteTopic(KafkaClusterInfo kafkaClusterInfo, String topicName) {
        AdminClient adminClient = KafkaUtils.getAdminClient(kafkaClusterInfo);
        DeleteTopicsResult result = adminClient.deleteTopics(Collections.singletonList(topicName));
        LOGGER.info("success to delete topic={}, result: {}", topicName, result.all());
    }

    public boolean topicIsExists(KafkaClusterInfo kafkaClusterInfo, String topic)
            throws ExecutionException, InterruptedException {
        AdminClient adminClient = KafkaUtils.getAdminClient(kafkaClusterInfo);
        Set<String> topicList = adminClient.listTopics().names().get();
        return topicList.contains(topic);
    }

    /**
     * Query topic message for the given Kafka cluster.
     */
    public List<BriefMQMessage> queryLatestMessage(KafkaClusterInfo clusterInfo, String topicName,
            String consumeGroup, InlongStreamInfo streamInfo, QueryMessageRequest request) {
        LOGGER.debug("begin to query message for topic {} in cluster: {}", topicName, clusterInfo);

        Properties properties = getProperties(clusterInfo.getUrl(), consumeGroup);
        KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(properties);
        return getLatestMessage(consumer, topicName, streamInfo, request);
    }

    @VisibleForTesting
    public List<BriefMQMessage> getLatestMessage(Consumer<byte[], byte[]> consumer, String topicName,
            InlongStreamInfo streamInfo, QueryMessageRequest request) {
        List<BriefMQMessage> messageList = new ArrayList<>();
        Integer messageCount = request.getMessageCount();
        try {
            List<PartitionInfo> partitionInfoList = consumer.partitionsFor(topicName);
            List<TopicPartition> topicPartitionList = partitionInfoList.stream()
                    .map(topicPartition -> new TopicPartition(topicPartition.topic(), topicPartition.partition()))
                    .collect(Collectors.toList());

            Map<TopicPartition, Long> beginningTopicPartitionList = consumer.beginningOffsets(topicPartitionList);
            Map<TopicPartition, Long> endTopicPartitionList = consumer.endOffsets(topicPartitionList);

            int count = (int) Math.ceil((double) messageCount / topicPartitionList.size());
            Map<TopicPartition, Long> expectedOffsetMap = beginningTopicPartitionList.entrySet()
                    .stream()
                    .map(entry -> {
                        long beginningOffset = entry.getValue();
                        long endOffset = endTopicPartitionList.getOrDefault(entry.getKey(), beginningOffset);
                        Long offset = (endOffset - beginningOffset) >= count ? (endOffset - count) : beginningOffset;
                        return Pair.of(entry.getKey(), offset);
                    }).collect(Collectors.toMap(Pair::getKey, Pair::getValue));

            consumer.assign(topicPartitionList);
            expectedOffsetMap.forEach(consumer::seek);

            int index = 0;
            ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<byte[], byte[]> record : records) {
                Map<String, String> headers = new HashMap<>();
                for (Header header : record.headers()) {
                    headers.put(header.key(), new String(header.value(), StandardCharsets.UTF_8));
                }

                MessageWrapType messageWrapType = MessageWrapType.forType(streamInfo.getWrapType());
                if (headers.get(InlongConstants.MSG_ENCODE_VER) != null) {
                    messageWrapType =
                            MessageWrapType.valueOf(Integer.parseInt(headers.get(InlongConstants.MSG_ENCODE_VER)));
                }
                DeserializeOperator deserializeOperator = deserializeOperatorFactory.getInstance(messageWrapType);
                deserializeOperator.decodeMsg(streamInfo, messageList, record.value(), headers, index, request);
                if (messageList.size() >= messageCount) {
                    break;
                }
            }
        } catch (Exception e) {
            String errMsg = "decode msg error: ";
            LOGGER.error(errMsg, e);
            throw new BusinessException(errMsg + e.getMessage());
        } finally {
            consumer.close();
        }

        LOGGER.debug("success query messages for topic={}, size={}, returned size={}",
                topicName, messageList.size(), messageCount);
        // only return a list of messages of the specified count
        int fromIndex = (messageList.size() > messageCount) ? (messageList.size() - messageCount) : 0;
        List<BriefMQMessage> resultList = messageList.subList(fromIndex, messageList.size());
        for (int i = 0; i < resultList.size(); i++) {
            BriefMQMessage message = resultList.get(i);
            message.setId(i + 1);
        }

        return resultList;
    }

    /**
     * Get a properties instance of consumer group.
     */
    private static Properties getProperties(String clusterUrl, String consumeGroup) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, clusterUrl);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, consumeGroup);

        return properties;
    }
}
