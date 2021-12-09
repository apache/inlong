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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.inlong.sort.api.ClientContext;
import org.apache.inlong.sort.api.InLongTopicFetcher;
import org.apache.inlong.sort.entity.InLongTopic;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.shade.org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InLongPulsarFetcherImpl extends InLongTopicFetcher {

    private final Logger logger = LoggerFactory.getLogger(InLongPulsarFetcherImpl.class);
    private final ReentrantReadWriteLock mainLock = new ReentrantReadWriteLock(true);
    private final ConcurrentHashMap<String, MessageId> offsetCache = new ConcurrentHashMap<>();
    private volatile boolean closed = false;
    private Consumer<byte[]> consumer;
    private volatile boolean stopConsume = false;

    public InLongPulsarFetcherImpl(InLongTopic inLongTopic,
            ClientContext context) {
        super(inLongTopic, context);
    }

    @Override
    public void stopConsume(boolean stopConsume) {
        this.stopConsume = stopConsume;
    }

    @Override
    public boolean isConsumeStop() {
        return stopConsume;
    }

    @Override
    public InLongTopic getInLongTopic() {
        return inLongTopic;
    }

    @Override
    public long getConsumedDataSize() {
        return 0;
    }

    @Override
    public long getAckedOffset() {
        return 0;
    }

    @Override
    public void seek(MessageId messageId) throws PulsarClientException {
        if (consumer != null) {
            consumer.seek(messageId);
        }
    }

    private void ackSucc(String offset) {
        logger.info("ack succ:{}", offset);
        offsetCache.remove(offset);
        context.getStatManager().getStatistics(context.getConfig().getSortTaskId(),
                inLongTopic.getInLongCluster().getClusterId(), inLongTopic.getTopic()).addAckSuccTimes(1);
    }

    /**
     * ack Offset
     *
     * @param msgOffset String
     */
    @Override
    public void ack(String msgOffset) throws Exception {
        if (!StringUtils.isEmpty(msgOffset)) {
            logger.debug("## ack {}", msgOffset);
            try {
                if (consumer == null) {
                    context.getStatManager().getStatistics(context.getConfig().getSortTaskId(),
                            inLongTopic.getInLongCluster().getClusterId(), inLongTopic.getTopic())
                            .addAckFailTimes(1);
                    logger.error("consumer == null {}", msgOffset);
                    return;
                }
                MessageId messageId = offsetCache.get(msgOffset);
                if (messageId == null) {
                    context.getStatManager().getStatistics(context.getConfig().getSortTaskId(),
                            inLongTopic.getInLongCluster().getClusterId(), inLongTopic.getTopic())
                            .addAckFailTimes(1);
                    logger.error("messageId == null {}", msgOffset);
                    return;
                }
                consumer.acknowledgeAsync(messageId)
                        .thenAccept(consumer -> ackSucc(msgOffset))
                        .exceptionally(exception -> {
                            logger.error("ack fail:{}", msgOffset);
                            context.getStatManager().getStatistics(context.getConfig().getSortTaskId(),
                                    inLongTopic.getInLongCluster().getClusterId(), inLongTopic.getTopic())
                                    .addAckFailTimes(1);
                            return null;
                        });
            } catch (Exception e) {
                context.getStatManager().getStatistics(context.getConfig().getSortTaskId(),
                        inLongTopic.getInLongCluster().getClusterId(), inLongTopic.getTopic()).addAckFailTimes(1);
                logger.error(e.getMessage(), e);
                throw e;
            }
        }
    }

    /**
     * 创建Consumer
     *
     * @return boolean
     */
    @Override
    public boolean init(PulsarClient pulsarClient) {
        return createConsumer(pulsarClient);
    }

    private boolean createConsumer(PulsarClient client) {
        try {
            consumer = client.newConsumer(Schema.BYTES)
                    .topic(inLongTopic.getTopic())
                    .subscriptionName(context.getConfig().getSortTaskId())
                    .subscriptionType(SubscriptionType.Shared)
                    .startMessageIdInclusive()
                    .ackTimeout(context.getConfig().getAckTimeoutSec(), TimeUnit.SECONDS)
                    .receiverQueueSize(context.getConfig().getPulsarReceiveQueueSize())
                    .messageListener(new PulsarMessageListener(this, context, inLongTopic, offsetCache))
                    .subscribe();
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    /**
     * current fetcher is closed
     */
    public void isValidState() {
        if (closed) {
            throw new IllegalStateException(inLongTopic + " closed.");
        }
    }

    /**
     * pause
     */
    @Override
    public void pause() {
        if (consumer != null) {
            consumer.pause();
        }
    }

    /**
     * resume
     */
    @Override
    public void resume() {
        if (consumer != null) {
            consumer.resume();
        }
    }

    @Override
    public boolean close() {
        mainLock.writeLock().lock();
        try {
            this.closed = true;
            try {
                if (consumer != null) {
                    consumer.close();
                }
            } catch (PulsarClientException e) {
                e.printStackTrace();
            }

            logger.info("{} closed.", inLongTopic);
            return true;
        } finally {
            mainLock.writeLock().unlock();
        }
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

}
