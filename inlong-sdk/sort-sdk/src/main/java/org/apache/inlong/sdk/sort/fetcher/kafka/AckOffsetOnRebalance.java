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
 *
 */

package org.apache.inlong.sdk.sort.fetcher.kafka;

import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import org.apache.inlong.sdk.sort.api.Seeker;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AckOffsetOnRebalance implements ConsumerRebalanceListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(AckOffsetOnRebalance.class);
    private final String clusterId;
    private final Seeker seeker;
    private final ConcurrentHashMap<TopicPartition, OffsetAndMetadata> commitOffsetMap;
    private final ConcurrentHashMap<TopicPartition, ConcurrentSkipListMap<Long, Boolean>>
            ackOffsetMap;

    public AckOffsetOnRebalance(
            String clusterId,
            Seeker seeker,
            ConcurrentHashMap<TopicPartition, OffsetAndMetadata> commitOffsetMap) {
        this(clusterId, seeker, commitOffsetMap, null);
    }

    public AckOffsetOnRebalance(
            String clusterId,
            Seeker seeker,
            ConcurrentHashMap<TopicPartition, OffsetAndMetadata> commitOffsetMap,
            ConcurrentHashMap<TopicPartition, ConcurrentSkipListMap<Long, Boolean>> ackOffsetMap) {
        this.clusterId = clusterId;
        this.seeker = seeker;
        this.commitOffsetMap = commitOffsetMap;
        this.ackOffsetMap = ackOffsetMap;
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> collection) {
        LOGGER.debug("*- in re-balance:onPartitionsRevoked");
        collection.forEach(
                (v) -> {
                    LOGGER.info("clusterId:{},onPartitionsRevoked:{}", clusterId, v.toString());
                });
        if (Objects.nonNull(ackOffsetMap) && Objects.nonNull(commitOffsetMap)) {
            ackRevokedPartitions(collection);
        }
    }

    private void ackRevokedPartitions(Collection<TopicPartition> collection) {
        collection.forEach(
                tp -> {
                    if (!ackOffsetMap.containsKey(tp)) {
                        return;
                    }
                    ConcurrentSkipListMap<Long, Boolean> tpOffsetMap = ackOffsetMap.remove(tp);
                    long commitOffset = -1;
                    for (Long ackOffset : tpOffsetMap.keySet()) {
                        if (!tpOffsetMap.get(ackOffset)) {
                            break;
                        }
                        commitOffset = ackOffset;
                    }
                    // the first haven't ack, do nothing
                    if (commitOffset == -1) {
                        return;
                    }
                    commitOffsetMap.put(tp, new OffsetAndMetadata(commitOffset));
                });
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> collection) {
        LOGGER.debug("*- in re-balance:onPartitionsAssigned  ");
        collection.forEach(
                (v) -> {
                    LOGGER.info("clusterId:{},onPartitionsAssigned:{}", clusterId, v.toString());
                });
        seeker.seek();
    }
}
