/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.inlong.tubemq.example;

import static org.apache.inlong.tubemq.corebase.TErrCodeConstants.IGNORE_ERROR_SET;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import org.apache.inlong.tubemq.client.config.ConsumerConfig;
import org.apache.inlong.tubemq.client.consumer.ConsumeOffsetInfo;
import org.apache.inlong.tubemq.client.consumer.ConsumePosition;
import org.apache.inlong.tubemq.client.consumer.ConsumerResult;
import org.apache.inlong.tubemq.client.consumer.PullMessageConsumer;
import org.apache.inlong.tubemq.client.exception.TubeClientException;
import org.apache.inlong.tubemq.client.factory.MessageSessionFactory;
import org.apache.inlong.tubemq.client.factory.TubeSingleSessionFactory;
import org.apache.inlong.tubemq.corebase.Message;
import org.apache.inlong.tubemq.corebase.utils.MixedUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This demo shows how to consume message by pull.
 *
 *Consume message in pull mode achieved by {@link PullMessageConsumer#getMessage()}.
 * Note that whenever {@link PullMessageConsumer#getMessage()} returns successfully, the
 * return value(whether or not to be {@code null}) should be processed by
 * {@link PullMessageConsumer#confirmConsume(String, boolean)}.
 */
public final class MessagePullConsumerExample {

    private static final Logger logger =
            LoggerFactory.getLogger(MessagePullConsumerExample.class);
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
        // get and initial parameters
        final String masterServers = args[0];
        final String topics = args[1];
        final String group = args[2];
        final int msgCount = Integer.parseInt(args[3]);
        final Map<String, TreeSet<String>> topicAndFiltersMap =
                MixedUtils.parseTopicParam(topics);
        // initial consumer object
        final MessagePullConsumerExample messageConsumer =
                new MessagePullConsumerExample(masterServers, group);
        messageConsumer.subscribe(topicAndFiltersMap);
        Thread[] fetchRunners = new Thread[3];
        for (int i = 0; i < fetchRunners.length; i++) {
            fetchRunners[i] = new Thread(new FetchRequestRunner(messageConsumer, msgCount));
            fetchRunners[i].setName("_fetch_runner_" + i);
        }
        // initial fetch threads
        for (Thread thread : fetchRunners) {
            thread.start();
        }
        // initial statistic thread
        Thread statisticThread =
                new Thread(msgRecvStats, "Sent Statistic Thread");
        statisticThread.start();
    }

    public void subscribe(
            Map<String, TreeSet<String>> topicAndFiltersMap) throws TubeClientException {
        for (Map.Entry<String, TreeSet<String>> entry : topicAndFiltersMap.entrySet()) {
            messagePullConsumer.subscribe(entry.getKey(), entry.getValue());
        }
        messagePullConsumer.completeSubscribe();
    }

    public ConsumerResult getMessage() throws TubeClientException {
        return messagePullConsumer.getMessage();
    }

    public ConsumerResult confirmConsume(final String confirmContext,
                                         boolean isConsumed) throws TubeClientException {
        return messagePullConsumer.confirmConsume(confirmContext, isConsumed);
    }

    public Map<String, ConsumeOffsetInfo> getCurrPartitionOffsetMap() throws TubeClientException {
        return messagePullConsumer.getCurConsumedPartitions();
    }

    private static class FetchRequestRunner implements Runnable {

        final MessagePullConsumerExample messageConsumer;
        final int consumeCount;

        FetchRequestRunner(final MessagePullConsumerExample messageConsumer, int msgCount) {
            this.messageConsumer = messageConsumer;
            this.consumeCount = msgCount;
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
                        if (!IGNORE_ERROR_SET.contains(result.getErrCode())) {
                            logger.info(
                                    "Receive messages errorCode is {}, Error message is {}",
                                    result.getErrCode(), result.getErrMsg());
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


