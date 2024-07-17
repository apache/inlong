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

import org.apache.inlong.common.enums.DataTypeEnum;
import org.apache.inlong.common.enums.MessageWrapType;
import org.apache.inlong.common.msg.InLongMsg;
import org.apache.inlong.manager.pojo.consume.BriefMQMessage;
import org.apache.inlong.manager.pojo.stream.InlongStreamInfo;
import org.apache.inlong.manager.pojo.stream.QueryMessageRequest;
import org.apache.inlong.manager.service.ServiceBaseTest;

import com.google.common.collect.Lists;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Test for {@link KafkaOperator}
 */
public class KafkaOperatorTest extends ServiceBaseTest {

    private static final String TOPIC_NAME = "test";
    private static final int PARTITION_NUM = 5;

    private final MockConsumer<byte[], byte[]> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);

    private final InlongStreamInfo streamInfo = new InlongStreamInfo();

    @Autowired
    private KafkaOperator kafkaOperator;

    public byte[] buildInlongMessage(String message) {
        InLongMsg inLongMsg = InLongMsg.newInLongMsg();
        String attr = "t=20230728";
        inLongMsg.addMsg(attr, message.getBytes(StandardCharsets.UTF_8));
        return inLongMsg.buildArray();
    }

    public void addOnceRecord(int partition, long offset, String message) {
        ConsumerRecord<byte[], byte[]> record =
                new ConsumerRecord<>(TOPIC_NAME, partition, offset, 0L, TimestampType.CREATE_TIME,
                        0L, 0, 0, null, buildInlongMessage(message), new RecordHeaders(), Optional.empty());
        consumer.addRecord(record);

        HashMap<TopicPartition, Long> endingOffsets = new HashMap<>();
        endingOffsets.put(new TopicPartition(TOPIC_NAME, partition), offset);
        consumer.updateEndOffsets(endingOffsets);
    }

    public void addRecord(List<String> messages) {
        List<List<String>> partitions =
                Lists.partition(messages, (int) Math.ceil((double) messages.size() / PARTITION_NUM));

        IntStream.range(0, partitions.size()).forEach(index -> {
            List<String> partitionItems = partitions.get(index);
            for (int offset = 0; offset < partitionItems.size(); offset++) {
                addOnceRecord(index, offset + 1, partitionItems.get(offset));
            }
        });
    }

    @BeforeEach
    public void setUp() {
        streamInfo.setDataEncoding("UTF-8");
        streamInfo.setDataType(DataTypeEnum.CSV.getType());
        streamInfo.setWrapType(MessageWrapType.INLONG_MSG_V0.getName());

        List<TopicPartition> topicPartitions = IntStream.range(0, PARTITION_NUM)
                .mapToObj(i -> new TopicPartition(TOPIC_NAME, i)).collect(Collectors.toList());
        consumer.assign(topicPartitions);

        List<PartitionInfo> partitions = IntStream.range(0, PARTITION_NUM)
                .mapToObj(i -> new PartitionInfo(TOPIC_NAME, i, null, null, null)).collect(Collectors.toList());
        consumer.updatePartitions(TOPIC_NAME, partitions);

        Map<TopicPartition, Long> offsets = topicPartitions.stream()
                .collect(Collectors.toMap(Function.identity(), t -> 0L));
        consumer.updateBeginningOffsets(offsets);
        consumer.updateEndOffsets(offsets);

        topicPartitions.forEach(item -> consumer.seek(item, 0));
    }

    @Test
    void testGetKafkaLatestMessage() {
        QueryMessageRequest request = new QueryMessageRequest();
        request.setMessageCount(10);
        List<BriefMQMessage> messages = kafkaOperator.getLatestMessage(consumer, TOPIC_NAME, streamInfo, request);
        Assertions.assertEquals(0, messages.size());
    }

    @Test
    void testGetKafkaLatestMessage_1() {
        addRecord(Collections.singletonList("inlong"));
        QueryMessageRequest request = new QueryMessageRequest();
        request.setMessageCount(10);
        List<BriefMQMessage> messages = kafkaOperator.getLatestMessage(consumer, TOPIC_NAME, streamInfo, request);
        Assertions.assertEquals(1, messages.size());
        Assertions.assertEquals("inlong", messages.get(0).getBody());
    }

    @Test
    void testGetKafkaLatestMessage_2() {
        List<String> records = IntStream.range(0, 9).mapToObj(index -> "name_" + index).collect(Collectors.toList());
        addRecord(records);
        QueryMessageRequest request = new QueryMessageRequest();
        request.setMessageCount(10);
        List<BriefMQMessage> messages = kafkaOperator.getLatestMessage(consumer, TOPIC_NAME, streamInfo, request);
        Assertions.assertEquals(9, messages.size());
    }

    @Test
    void testGetKafkaLatestMessage_4() {
        List<String> records = IntStream.range(0, 21).mapToObj(index -> "name_" + index).collect(Collectors.toList());
        addRecord(records);
        QueryMessageRequest request = new QueryMessageRequest();
        request.setMessageCount(10);
        List<BriefMQMessage> messages = kafkaOperator.getLatestMessage(consumer, TOPIC_NAME, streamInfo, request);
        Assertions.assertEquals(10, messages.size());
    }

}
