/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tubemq.example;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.tubemq.client.config.ConsumerConfig;
import org.apache.tubemq.client.consumer.ConsumeOffsetInfo;
import org.apache.tubemq.client.consumer.ConsumePosition;
import org.apache.tubemq.client.consumer.ConsumerResult;
import org.apache.tubemq.client.consumer.PullMessageConsumer;
import org.apache.tubemq.client.exception.TubeClientException;
import org.apache.tubemq.client.factory.MessageSessionFactory;
import org.apache.tubemq.client.factory.TubeSingleSessionFactory;
import org.apache.tubemq.corebase.Message;
import org.apache.tubemq.corebase.utils.ThreadUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This demo shows how to consume message by pull.
 *
 * <p>Consume message in pull mode achieved by {@link PullMessageConsumer#getMessage()}.
 * Note that whenever {@link PullMessageConsumer#getMessage()} returns successfully, the
 * return value(whether or not to be {@code null}) should be processed by
 * {@link PullMessageConsumer#confirmConsume(String, boolean)}.
 */
public final class MessagePullConsumerExample {

    private static final Logger logger = LoggerFactory.getLogger(MessagePullConsumerExample.class);
    private static final MsgRecvStats msgRecvStats = new MsgRecvStats();

    private final PullMessageConsumer messagePullConsumer;
    private final MessageSessionFactory messageSessionFactory;

    public MessagePullConsumerExample(String masterHostAndPort, String group) throws Exception {
        ConsumerConfig consumerConfig = new ConsumerConfig(masterHostAndPort, group);
        consumerConfig.setConsumePosition(ConsumePosition.CONSUMER_FROM_LATEST_OFFSET);
        this.messageSessionFactory = new TubeSingleSessionFactory(consumerConfig);
        this.messagePullConsumer = messageSessionFactory.createPullConsumer(consumerConfig);
    }

    public static void main(String[] args) throws Throwable {
        final String masterHostAndPort = args[0];
        final String topics = args[1];
        final String group = args[2];
        final int consumeCount = Integer.parseInt(args[3]);

        final MessagePullConsumerExample messageConsumer = new MessagePullConsumerExample(
            masterHostAndPort,
            group
        );

        final List<String> topicList = Arrays.asList(topics.split(","));
        messageConsumer.subscribe(topicList);
        long startTime = System.currentTimeMillis();

        Thread[] fetchRunners = new Thread[3];
        for (int i = 0; i < fetchRunners.length; i++) {
            fetchRunners[i] = new Thread(new FetchRequestRunner(messageConsumer, consumeCount));
            fetchRunners[i].setName("_fetch_runner_" + i);
        }

        // wait for client to join the exact consumer queue that consumer group allocated
        while (!messageConsumer.isCurConsumeReady(1000)) {
            ThreadUtils.sleep(1000);
        }

        logger.info("Wait and get partitions use time " + (System.currentTimeMillis() - startTime));

        for (Thread thread : fetchRunners) {
            thread.start();
        }

        Thread statisticThread = new Thread(msgRecvStats, "Sent Statistic Thread");
        statisticThread.start();
    }

    public void subscribe(List<String> topicList) throws TubeClientException {
        for (String topic : topicList) {
            messagePullConsumer.subscribe(topic, null);
        }

        messagePullConsumer.completeSubscribe();
    }

    public boolean isCurConsumeReady(long waitTime) {
        return messagePullConsumer.isPartitionsReady(waitTime);
    }

    public ConsumerResult getMessage() throws TubeClientException {
        return messagePullConsumer.getMessage();
    }

    public ConsumerResult confirmConsume(final String confirmContext, boolean isConsumed) throws TubeClientException {
        return messagePullConsumer.confirmConsume(confirmContext, isConsumed);
    }

    public Map<String, ConsumeOffsetInfo> getCurrPartitionOffsetMap() throws TubeClientException {
        return messagePullConsumer.getCurConsumedPartitions();
    }

    private static class FetchRequestRunner implements Runnable {

        final MessagePullConsumerExample messageConsumer;
        final int consumeCount;

        FetchRequestRunner(final MessagePullConsumerExample messageConsumer, int count) {
            this.messageConsumer = messageConsumer;
            this.consumeCount = count;
        }

        @Override
        public void run() {
            try {
                int getCount = consumeCount;
                do {
                    ConsumerResult result = messageConsumer.getMessage();
                    if (result.isSuccess()) {
                        List<Message> messageList = result.getMessageList();
                        if (messageList != null && !messageList.isEmpty()) {
                            msgRecvStats.addMsgCount(result.getTopicName(), messageList.size());
                        }
                        messageConsumer.confirmConsume(result.getConfirmContext(), true);
                    } else {
                        if (result.getErrCode() == 400
                                || result.getErrCode() == 405
                                || result.getErrCode() == 406
                                || result.getErrCode() == 407
                                || result.getErrCode() == 408) {
                            ThreadUtils.sleep(400);
                        } else {
                            if (result.getErrCode() != 404) {
                                logger.info(
                                    "Receive messages errorCode is {}, Error message is {}",
                                    result.getErrCode(),
                                    result.getErrMsg());
                            }
                        }
                    }
                    if (consumeCount > 0) {
                        if (--getCount <= 0) {
                            break;
                        }
                    }
                } while (true);
                msgRecvStats.stopStats();
            } catch (TubeClientException e) {
                logger.error("Create consumer failed!", e);
            }
        }
    }
}


