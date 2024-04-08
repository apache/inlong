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

import org.apache.inlong.manager.common.consts.InlongConstants;
import org.apache.inlong.manager.pojo.cluster.pulsar.PulsarClusterInfo;
import org.apache.inlong.manager.pojo.consume.BriefMQMessage;
import org.apache.inlong.manager.pojo.group.pulsar.InlongPulsarInfo;
import org.apache.inlong.manager.pojo.stream.InlongStreamInfo;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * QueryLatestMessagesRunnable
 */
public class QueryLatestMessagesRunnable implements Runnable {

    public static final String PULSAR_SUBSCRIPTION_REALTIME_REVIEW = "%s_%s_consumer_group_realtime_review";

    private InlongPulsarInfo inlongPulsarInfo;
    private InlongStreamInfo streamInfo;
    private PulsarClusterInfo clusterInfo;
    private PulsarOperator pulsarOperator;
    private Integer messageCount;
    private List<BriefMQMessage> briefMQMessages;
    private CountDownLatch latch;

    public QueryLatestMessagesRunnable(InlongPulsarInfo inlongPulsarInfo,
            InlongStreamInfo streamInfo,
            PulsarClusterInfo clusterInfo,
            PulsarOperator pulsarOperator,
            Integer messageCount,
            List<BriefMQMessage> briefMQMessages,
            CountDownLatch latch) {
        this.inlongPulsarInfo = inlongPulsarInfo;
        this.streamInfo = streamInfo;
        this.clusterInfo = clusterInfo;
        this.pulsarOperator = pulsarOperator;
        this.messageCount = messageCount;
        this.briefMQMessages = briefMQMessages;
        this.latch = latch;
    }

    @Override
    public void run() {
        String tenant = inlongPulsarInfo.getPulsarTenant();
        if (StringUtils.isBlank(tenant)) {
            tenant = clusterInfo.getPulsarTenant();
        }

        String namespace = inlongPulsarInfo.getMqResource();
        String topicName = streamInfo.getMqResource();
        String fullTopicName = tenant + "/" + namespace + "/" + topicName;
        String clusterTag = inlongPulsarInfo.getInlongClusterTag();
        String subs = String.format(PULSAR_SUBSCRIPTION_REALTIME_REVIEW, clusterTag, topicName);
        boolean serial = InlongConstants.PULSAR_QUEUE_TYPE_SERIAL.equals(inlongPulsarInfo.getQueueModule());
        List<BriefMQMessage> messages = pulsarOperator.queryLatestMessage(clusterInfo, fullTopicName, subs,
                messageCount, streamInfo, serial);
        if (CollectionUtils.isNotEmpty(messages)) {
            briefMQMessages.addAll(messages);
            messages.forEach(v -> this.latch.countDown());
        }
    }
}
