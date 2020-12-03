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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.tubemq.client.common.PeerInfo;
import org.apache.tubemq.client.config.ConsumerConfig;
import org.apache.tubemq.client.consumer.ConsumePosition;
import org.apache.tubemq.client.consumer.MessageV2Listener;
import org.apache.tubemq.client.consumer.PushMessageConsumer;
import org.apache.tubemq.client.exception.TubeClientException;
import org.apache.tubemq.client.factory.MessageSessionFactory;
import org.apache.tubemq.client.factory.TubeSingleSessionFactory;
import org.apache.tubemq.corebase.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This demo shows how to consume message sequentially.
 *
 * <p>Consumer supports subscribe multiple topics in one consume group. Message from subscription
 * sent back to business logic via callback {@link MessageV2Listener}. It is highly recommended NOT
 * to perform any blocking operation inside the callback.
 *
 * <p>As for consumption control of {@link PushMessageConsumer}, business logic is able to monitor
 * current state and adjust consumption by
 *
 * <p><ul>
 *     <li>call {@link PushMessageConsumer#pauseConsume()} to pause consumption when high water mark exceeded.</li>
 *     <li>call {@link PushMessageConsumer#resumeConsume()} to resume consumption</li>
 * </ul>
 */
public final class MessageConsumerExample {

    private static final Logger logger = LoggerFactory.getLogger(MessageConsumerExample.class);
    private static final MsgRecvStats msgRecvStats = new MsgRecvStats();

    private final PushMessageConsumer messageConsumer;

    public MessageConsumerExample(String masterHostAndPort, String group,
            int fetchCount, boolean isFromBegin) throws Exception {
        ConsumerConfig consumerConfig = new ConsumerConfig(masterHostAndPort, group);
        if (isFromBegin) {
            consumerConfig.setConsumePosition(ConsumePosition.CONSUMER_FROM_FIRST_OFFSET);
        } else {
            consumerConfig.setConsumePosition(ConsumePosition.CONSUMER_FROM_LATEST_OFFSET);
        }
        if (fetchCount > 0) {
            consumerConfig.setPushFetchThreadCnt(fetchCount);
        }
        MessageSessionFactory messageSessionFactory = new TubeSingleSessionFactory(consumerConfig);
        this.messageConsumer = messageSessionFactory.createPushConsumer(consumerConfig);
    }

    /**
     * Init options
     * @return options
     */
    public static Options initOptions() {

        Options options = ArgsParserHelper.initCommonOptions();
        options.addOption(null, "batch-size", true, "max number of fetching message in one batch");
        options.addOption(null, "thread-num", true, "thread number of consumers");
        options.addOption(null, "group", true, "consumer group");
        options.addOption(null, "from-begin", false, "default is consuming from latest, "
                + "if option is clarified, then consume from begin");
        return options;

    }

    /**
     * init topic->set(tid) map
     * @param topics - topics string
     * @return - map of topic->set(tid)
     */
    private static Map<String, TreeSet<String>> initTopicList(String topics) {
        Map<String, TreeSet<String>> topicTidsMap = new HashMap<>();
        String[] topicTidsList = topics.split(",");
        for (String topicTids : topicTidsList) {
            String[] topicTidStr = topicTids.split(":");
            TreeSet<String> tids = null;
            if (topicTidStr.length > 1) {
                String tidsStr = topicTidStr[1];
                String[] tidsSet = tidsStr.split(";");
                if (tidsSet.length > 0) {
                    tids = new TreeSet<>(Arrays.asList(tidsSet));
                }
            }
            topicTidsMap.put(topicTidStr[0], tids);
        }
        return topicTidsMap;
    }

    public static void main(String[] args) {
        Options options = null;
        try {
            CommandLineParser parser = new DefaultParser();
            options = initOptions();
            CommandLine cl = parser.parse(options, args);
            if (cl != null) {
                final String masterHostAndPort = cl.getOptionValue("master-list");
                final Map<String, TreeSet<String>> topicTidsMap = initTopicList(
                        cl.getOptionValue("topic"));
                final String group = cl.getOptionValue("group");
                int threadNum = Integer.parseInt(cl.getOptionValue("thread-num", "1"));
                final int fetchCount = Integer.parseInt(cl.getOptionValue("batch-size", "-1"));
                final boolean isFromBegin = cl.hasOption("from-begin");
                ExecutorService executorService = Executors.newFixedThreadPool(threadNum);
                for (int i = 0; i < threadNum; i++) {
                    executorService.submit(new Runnable() {
                        @Override
                        public void run() {
                            try {
                                MessageConsumerExample messageConsumer = new MessageConsumerExample(
                                        masterHostAndPort,
                                        group,
                                        fetchCount,
                                        isFromBegin
                                );
                                messageConsumer.subscribe(topicTidsMap);
                            } catch (Exception e) {
                                logger.error("Create consumer failed!", e);
                            }
                        }
                    });
                }
                final Thread statisticThread = new Thread(msgRecvStats,
                        "Received Statistic Thread");
                statisticThread.start();

                executorService.shutdown();
                try {
                    executorService.awaitTermination(60 * 1000, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    logger.error("Thread Pool shutdown has been interrupted!");
                }
                msgRecvStats.stopStats();
            }
        } catch (Exception ex) {
            logger.error(ex.getMessage(), ex.getMessage());
            if (options != null) {
                ArgsParserHelper.help("./tubemq-consumer-perf-test.sh", options);
            }
        }
    }

    public void subscribe(Map<String, TreeSet<String>> topicTidsMap) throws TubeClientException {
        for (Map.Entry<String, TreeSet<String>> entry : topicTidsMap.entrySet()) {
            MessageV2Listener messageListener = new DefaultMessageListener(entry.getKey());
            messageConsumer.subscribe(entry.getKey(), entry.getValue(), messageListener);
        }
        messageConsumer.completeSubscribe();
    }

    public static class DefaultMessageListener implements MessageV2Listener {

        private String topic;

        public DefaultMessageListener(String topic) {
            this.topic = topic;
        }

        @Override
        public void receiveMessages(PeerInfo peerInfo, List<Message> messages) {
            if (messages != null && !messages.isEmpty()) {
                msgRecvStats.addMsgCount(this.topic, messages.size());
            }
        }

        @Override
        public void receiveMessages(List<Message> messages) {
            // deprecated
        }

        @Override
        public Executor getExecutor() {
            return null;
        }

        @Override
        public void stop() {
        }
    }
}
