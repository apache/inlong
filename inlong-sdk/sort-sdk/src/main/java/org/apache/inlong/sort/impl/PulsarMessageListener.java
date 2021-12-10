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

package org.apache.inlong.sort.impl;

import java.util.Base64;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.inlong.sort.api.ClientContext;
import org.apache.inlong.sort.entity.InLongTopic;
import org.apache.inlong.sort.entity.MessageRecord;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PulsarMessageListener implements MessageListener<byte[]> {

    private final Logger logger = LoggerFactory.getLogger(PulsarMessageListener.class);

    private final ClientContext clientContext;
    private final InLongTopic inLongTopic;
    private final InLongPulsarFetcherImpl inLongTopicInFetcher;
    private final ConcurrentHashMap<String, MessageId> offsetCache;

    public PulsarMessageListener(InLongPulsarFetcherImpl inLongTopicInFetcher, ClientContext clientContext,
            InLongTopic inLongTopic, ConcurrentHashMap<String, MessageId> offsetCache) {
        this.inLongTopicInFetcher = inLongTopicInFetcher;
        this.clientContext = clientContext;
        this.inLongTopic = inLongTopic;
        this.offsetCache = offsetCache;
    }

    /**
     * put the received msg to onFinished method
     *
     * @param messageRecord MessageRecord
     */
    public void handleMsg(MessageRecord messageRecord) {
        long start = System.currentTimeMillis();
        try {
            clientContext.getConfig().getCallback().onFinished(messageRecord);
            clientContext.getStatManager()
                    .getStatistics(clientContext.getConfig().getSortTaskId(),
                            inLongTopic.getInLongCluster().getClusterId(), inLongTopic.getTopic())
                    .addCallbackTimeCost(System.currentTimeMillis() - start).addCallbackDoneTimes(1);
        } catch (Exception e) {
            clientContext.getStatManager()
                    .getStatistics(clientContext.getConfig().getSortTaskId(),
                            inLongTopic.getInLongCluster().getClusterId(), inLongTopic.getTopic())
                    .addCallbackErrorTimes(1);
            throw e;
        }
    }

    @Override
    public void received(Consumer<byte[]> consumer, Message<byte[]> msg) {
        String offsetKey;
        try {
            inLongTopicInFetcher.isValidState();
            clientContext.getStatManager()
                    .getStatistics(clientContext.getConfig().getSortTaskId(),
                            inLongTopic.getInLongCluster().getClusterId(), inLongTopic.getTopic())
                    .addConsumeSize(msg.getData().length).addCallbackTimes(1).addMsgCount(1);

            offsetKey = getOffset(msg.getMessageId());
            offsetCache.put(offsetKey, msg.getMessageId());
            callbackMessageRecord(msg, offsetKey);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    private void callbackMessageRecord(Message<byte[]> msg, String offsetKey) {
        handleMsg(new MessageRecord(inLongTopic.getTopicKey(), msg.getData(), msg.getProperties(),
                                offsetKey, System.currentTimeMillis()));
    }

    private String getOffset(MessageId msgId) {
        return Base64.getEncoder().encodeToString(msgId.toByteArray());
    }
}
