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

package org.apache.inlong.manager.service.resource.queue.pulsar;

import org.apache.inlong.manager.pojo.cluster.pulsar.PulsarClusterInfo;
import org.apache.inlong.manager.pojo.consume.BriefMQMessage;
import org.apache.inlong.manager.pojo.stream.InlongStreamInfo;
import org.apache.inlong.manager.pojo.stream.QueryMessageRequest;

import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Runnable task for querying latest messages from a Pulsar cluster.
 */
public class QueryLatestMessagesRunnable implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(QueryLatestMessagesRunnable.class);

    private final PulsarOperator pulsarOperator;
    private final InlongStreamInfo streamInfo;
    private final PulsarClusterInfo clusterInfo;
    private final boolean serialQueue;
    private final String fullTopicName;
    private final QueryMessageRequest queryMessageRequest;
    private final List<BriefMQMessage> messageResultList;
    private final QueryCountDownLatch latch;

    public QueryLatestMessagesRunnable(
            PulsarOperator pulsarOperator,
            InlongStreamInfo streamInfo,
            PulsarClusterInfo clusterInfo,
            boolean serialQueue,
            String fullTopicName,
            QueryMessageRequest queryMessageRequest,
            List<BriefMQMessage> messageResultList,
            QueryCountDownLatch latch) {
        this.pulsarOperator = pulsarOperator;
        this.streamInfo = streamInfo;
        this.clusterInfo = clusterInfo;
        this.serialQueue = serialQueue;
        this.fullTopicName = fullTopicName;
        this.queryMessageRequest = queryMessageRequest;
        this.messageResultList = messageResultList;
        this.latch = latch;
    }

    @Override
    public void run() {
        String clusterName = clusterInfo.getName();
        LOG.debug("Begin to query messages from cluster={}, topic={}", clusterName, fullTopicName);
        try {
            // Check for interruption before starting the query
            if (Thread.currentThread().isInterrupted()) {
                LOG.info("Task interrupted before query, cluster={}, topic={}", clusterName, fullTopicName);
                return;
            }

            List<BriefMQMessage> messages = pulsarOperator.queryLatestMessage(clusterInfo, fullTopicName,
                    queryMessageRequest, streamInfo, serialQueue);

            // Check for interruption after query completes
            // (IO operations not support interruption, so we check the flag manually after the blocking call returns)
            if (Thread.currentThread().isInterrupted()) {
                LOG.info("Task interrupted after query, discarding results, cluster={}, topic={}",
                        clusterName, fullTopicName);
                return;
            }

            if (CollectionUtils.isNotEmpty(messages)) {
                messageResultList.addAll(messages);
                this.latch.dataCountDown(messages.size());
                LOG.debug("Successfully queried {} messages from cluster={}, topic={}",
                        messages.size(), clusterName, fullTopicName);
            } else {
                LOG.debug("No messages found from cluster={}, topic={}", clusterName, fullTopicName);
            }
        } catch (Exception e) {
            LOG.error("Failed to query messages from cluster={}, groupId={}, streamId={}",
                    clusterName, queryMessageRequest.getGroupId(), queryMessageRequest.getStreamId(), e);
        } finally {
            // Ensure taskCountDown is always called, regardless of success or failure
            this.latch.taskCountDown();
        }
    }
}
