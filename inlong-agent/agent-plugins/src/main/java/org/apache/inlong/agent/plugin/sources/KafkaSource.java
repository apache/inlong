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

package org.apache.inlong.agent.plugin.sources;

import org.apache.inlong.agent.conf.InstanceProfile;
import org.apache.inlong.agent.except.FileException;
import org.apache.inlong.agent.plugin.sources.file.AbstractSource;

import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.apache.inlong.agent.constant.TaskConstants.RESTORE_FROM_DB;
import static org.apache.inlong.agent.constant.TaskConstants.TASK_KAFKA_AUTO_COMMIT_OFFSET_RESET;
import static org.apache.inlong.agent.constant.TaskConstants.TASK_KAFKA_BOOTSTRAP_SERVERS;
import static org.apache.inlong.agent.constant.TaskConstants.TASK_KAFKA_OFFSET;
import static org.apache.inlong.agent.constant.TaskConstants.TASK_KAFKA_OFFSET_DELIMITER;
import static org.apache.inlong.agent.constant.TaskConstants.TASK_KAFKA_PARTITION_OFFSET_DELIMITER;

/**
 * kafka source, split kafka source job into multi readers
 */
public class KafkaSource extends AbstractSource {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaSource.class);
    private String topic;
    private Properties props = new Properties();
    private String allPartitionOffsets;
    Map<Integer, Long> partitionOffsets = new HashMap<>();
    private static final String KAFKA_DESERIALIZER_METHOD =
            "org.apache.kafka.common.serialization.ByteArrayDeserializer";
    private static final String KAFKA_SESSION_TIMEOUT = "session.timeout.ms";
    private boolean isRestoreFromDB = false;
    private KafkaConsumer<String, byte[]> kafkaConsumer;
    private long offset = 0L;

    public KafkaSource() {
    }

    @Override
    protected void initSource(InstanceProfile profile) {
        try {
            LOGGER.info("KafkaSource init: {}", profile.toJsonStr());
            topic = profile.getInstanceId();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, profile.get(TASK_KAFKA_BOOTSTRAP_SERVERS));
            props.put(ConsumerConfig.GROUP_ID_CONFIG, taskId);
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KAFKA_DESERIALIZER_METHOD);
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KAFKA_DESERIALIZER_METHOD);
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, profile.get(TASK_KAFKA_AUTO_COMMIT_OFFSET_RESET));
            props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

            allPartitionOffsets = profile.get(TASK_KAFKA_OFFSET, null);
            isRestoreFromDB = profile.getBoolean(RESTORE_FROM_DB, false);
            if (!isRestoreFromDB && StringUtils.isNotBlank(allPartitionOffsets)) {
                // example:0#110_1#666_2#222
                String[] offsets = allPartitionOffsets.split(TASK_KAFKA_OFFSET_DELIMITER);
                for (String offset : offsets) {
                    partitionOffsets.put(Integer.valueOf(offset.split(TASK_KAFKA_PARTITION_OFFSET_DELIMITER)[0]),
                            Long.valueOf(offset.split(TASK_KAFKA_PARTITION_OFFSET_DELIMITER)[1]));
                }
            }
            kafkaConsumer = getKafkaConsumer();
        } catch (Exception ex) {
            stopRunning();
            throw new FileException("error init stream for " + topic, ex);
        }
    }

    @Override
    protected String getThreadName() {
        return "kafka-source-" + taskId + "-" + instanceId;
    }

    @Override
    protected boolean doPrepareToRead() {
        return true;
    }

    @Override
    protected List<SourceData> readFromSource() {
        List<SourceData> dataList = new ArrayList<>();
        ConsumerRecords<String, byte[]> records = kafkaConsumer.poll(Duration.ofMillis(1000));
        for (ConsumerRecord<String, byte[]> record : records) {
            SourceData sourceData = new SourceData(record.value(), Long.toString(record.offset()));
            dataList.add(sourceData);
            offset = record.offset();
        }
        kafkaConsumer.commitSync();
        return dataList;
    }

    private KafkaConsumer<String, byte[]> getKafkaConsumer() {
        List<PartitionInfo> partitionInfoList;
        KafkaConsumer<String, byte[]> kafkaConsumer = null;
        props.put(KAFKA_SESSION_TIMEOUT, 30000);
        try {
            kafkaConsumer = new KafkaConsumer<>(props);
            partitionInfoList = kafkaConsumer.partitionsFor(topic);
            if (partitionInfoList == null) {
                kafkaConsumer.close();
                return null;
            }
            List<TopicPartition> topicPartitions = new ArrayList<>();
            for (PartitionInfo partitionInfo : partitionInfoList) {
                TopicPartition topicPartition = new TopicPartition(partitionInfo.topic(),
                        partitionInfo.partition());
                topicPartitions.add(topicPartition);
            }
            kafkaConsumer.assign(topicPartitions);
            if (!isRestoreFromDB && StringUtils.isNotBlank(allPartitionOffsets)) {
                for (TopicPartition topicPartition : topicPartitions) {
                    Long offset = partitionOffsets.get(topicPartition.partition());
                    if (ObjectUtils.isNotEmpty(offset)) {
                        kafkaConsumer.seek(topicPartition, offset);
                    }
                }
            } else {
                LOGGER.info("Skip to seek offset");
            }
            return kafkaConsumer;
        } catch (Exception e) {
            if (kafkaConsumer != null) {
                kafkaConsumer.close();
            }
            LOGGER.error("get kafka consumer error", e);
        }
        return null;
    }

    @Override
    protected void printCurrentState() {
        LOGGER.info("kafka topic is {}, offset is {}", topic, offset);
    }

    @Override
    protected boolean isRunnable() {
        return runnable;
    }

    @Override
    public boolean sourceExist() {
        return true;
    }

    @Override
    protected void releaseSource() {
        if (kafkaConsumer != null) {
            kafkaConsumer.close();
        }
    }
}
