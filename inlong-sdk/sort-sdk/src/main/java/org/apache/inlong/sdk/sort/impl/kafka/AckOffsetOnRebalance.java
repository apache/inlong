/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.inlong.sdk.sort.impl.kafka;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;

public class AckOffsetOnRebalance implements ConsumerRebalanceListener {

    private final Logger logger = LoggerFactory.getLogger(AckOffsetOnRebalance.class);
    private final String clusterId;
    private final KafkaConsumer<byte[], byte[]> consumer;
    private final ConcurrentHashMap<TopicPartition, OffsetAndMetadata> commitOffsetMap;

    public AckOffsetOnRebalance(String clusterId, KafkaConsumer<byte[], byte[]> consumer,
            ConcurrentHashMap<TopicPartition, OffsetAndMetadata> commitOffsetMap) {
        this.clusterId = clusterId;
        this.consumer = consumer;
        this.commitOffsetMap = commitOffsetMap;
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> collection) {
        logger.debug("*- in ralance:onPartitionsRevoked");
        collection.forEach((v) -> {
            logger.info("clusterId:{},onPartitionsRevoked:{}", clusterId, v.toString());
        });
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> collection) {
        logger.debug("*- in ralance:onPartitionsAssigned  ");
        collection.forEach((v) -> {
            logger.info("clusterId:{},onPartitionsAssigned:{}", clusterId, v.toString());
        });
    }
}
