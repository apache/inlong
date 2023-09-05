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

package org.apache.inlong.tubemq.server.tools.cli;

import org.apache.inlong.tubemq.client.common.ConfirmResult;
import org.apache.inlong.tubemq.client.common.ConsumeResult;
import org.apache.inlong.tubemq.client.common.PeerInfo;
import org.apache.inlong.tubemq.client.common.QueryMetaResult;
import org.apache.inlong.tubemq.client.config.ConsumerConfig;
import org.apache.inlong.tubemq.client.config.TubeClientConfig;
import org.apache.inlong.tubemq.client.consumer.ClientBalanceConsumer;
import org.apache.inlong.tubemq.client.consumer.ConsumePosition;
import org.apache.inlong.tubemq.client.consumer.ConsumerResult;
import org.apache.inlong.tubemq.client.consumer.MessageListener;
import org.apache.inlong.tubemq.client.consumer.PullMessageConsumer;
import org.apache.inlong.tubemq.client.consumer.PushMessageConsumer;
import org.apache.inlong.tubemq.client.exception.TubeClientException;
import org.apache.inlong.tubemq.client.factory.MessageSessionFactory;
import org.apache.inlong.tubemq.client.factory.TubeSingleSessionFactory;
import org.apache.inlong.tubemq.client.producer.MessageProducer;
import org.apache.inlong.tubemq.client.producer.MessageSentCallback;
import org.apache.inlong.tubemq.client.producer.MessageSentResult;
import org.apache.inlong.tubemq.corebase.Message;
import org.apache.inlong.tubemq.corebase.rv.ProcessResult;
import org.apache.inlong.tubemq.corebase.utils.MixedUtils;
import org.apache.inlong.tubemq.corebase.utils.ThreadUtils;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import org.apache.commons.codec.binary.StringUtils;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Message production and consumption
 */
@Parameters(commandDescription = "Command for message production and consumption")
public class MessageCommand extends AbstractCommand {

    @Parameter()
    private List<String> params;

    public MessageCommand() {
        super("message");

        jcommander.addCommand("produce", new MessageProduce());
        jcommander.addCommand("consume", new MessageConsume());
    }

    @Parameters(commandDescription = "Produce message")
    private static class MessageProduce extends AbstractCommandRunner {

        @Parameter()
        private List<String> params;

        @Parameter(names = {"-ms",
                "--master-servers"}, required = true, order = 1, description = "The master address(es) to connect to. Format is master1_ip:port[,master2_ip:port]")
        private String masterServers;

        @Parameter(names = {"-t",
                "--topic"}, required = true, order = 0, description = "Topic to produce messages")
        private String topicName;

        @Parameter(names = {"-mt",
                "--msg-total"}, order = 2, description = "The total number of messages to be produced. -1 means unlimited.")
        private long msgTotal = -1;

        @Parameter(names = {"-m", "--mode"}, order = 3, description = "Produce mode, must in { sync | async }")
        private String mode = "async";

        private String body = "";

        private MessageProducer messageProducer;

        private AtomicLong msgCount = new AtomicLong(0L);

        /**
         * Send messages in synchronous mode
         *
         * @param message  Message to send
         * @throws TubeClientException
         * @throws InterruptedException
         */
        private void syncProduce(Message message) throws TubeClientException, InterruptedException {
            MessageSentResult result = messageProducer.sendMessage(message);
            if (!result.isSuccess()) {
                System.out.println("sync send message failed : " + result.getErrMsg());
            } else {
                msgCount.getAndIncrement();
            }
        }

        /**
         * Send messages in asynchronous mode
         *
         * @param message  Message to send
         * @throws TubeClientException
         * @throws InterruptedException
         */
        private void asyncProduce(Message message) throws TubeClientException, InterruptedException {
            messageProducer.sendMessage(message, new MessageSentCallback() {

                @Override
                public void onMessageSent(MessageSentResult result) {
                    if (!result.isSuccess()) {
                        System.out.println("async send message failed : " + result.getErrMsg());
                    } else {
                        msgCount.getAndIncrement();
                    }
                }

                @Override
                public void onException(Throwable e) {
                    System.out.println("async send message error : " + e);
                }
            });
        }

        /**
         * Stop a producer and print the total number of messages produced
         *
         * @param v  total number of messages
         * @throws TubeClientException
         * @throws InterruptedException
         */
        private void stopProducer(long v) {
            try {
                messageProducer.shutdown();
            } catch (Throwable e) {
                e.printStackTrace();
            }
            System.out.println("\n" + v + " message(s) has been produced. Exited.");
        }

        @Override
        void run() {
            try {
                Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                    if (msgTotal == -1)
                        stopProducer(msgCount.get());
                }));

                final TubeClientConfig clientConfig = new TubeClientConfig(masterServers);
                final TubeSingleSessionFactory messageSessionFactory = new TubeSingleSessionFactory(clientConfig);
                messageProducer = messageSessionFactory.createProducer();
                messageProducer.publish(topicName);
                byte[] bodyData;
                final Message message = new Message(topicName, null);

                BufferedReader input = new BufferedReader(new InputStreamReader(System.in));
                int c = 0;
                while (msgTotal == -1 || c < msgTotal) {
                    System.out.print(">");
                    body = input.readLine();
                    if (body == null || "".equals(body) || "".equals(body.trim()))
                        continue;
                    bodyData = StringUtils.getBytesUtf8(body);
                    message.setData(bodyData);

                    switch (mode) {
                        case "sync":
                            syncProduce(message);
                            break;
                        case "async":
                            asyncProduce(message);
                            break;
                        default:
                            throw new ParameterException("Produce mode, must in { sync | async }");
                    }
                    c++;
                }
                stopProducer(msgTotal);
            } catch (Exception e) {
                System.out.println(e.getMessage());
            } catch (Throwable e) {
                e.printStackTrace();
            }
        }
    }

    @Parameters(commandDescription = "Consume message")
    private static class MessageConsume extends AbstractCommandRunner {

        @Parameter()
        private List<String> params;

        @Parameter(names = {"-ms",
                "--master-servers"}, required = true, order = 2, description = "The master address(es) to connect to. Format is master1_ip:port[,master2_ip:port]")
        private String masterServers;

        @Parameter(names = {"-t",
                "--topic"}, required = true, order = 0, description = "Topic to consume messages")
        private String topicName;

        @Parameter(names = {"-g", "--group"}, required = true, order = 1, description = "Consumer group")
        private String groupName;

        @Parameter(names = {"-m", "--mode"}, order = 5, description = "Consume mode, must in { pull | push }")
        private String mode = "pull";

        @Parameter(names = {"-p",
                "--position"}, order = 3, description = "Consume position, must in { first | latest | max }")
        private String consumePosition = "first";

        @Parameter(names = {"-po",
                "--partitions-offsets"}, order = 4, description = "Consume partition ids and their offsets, format is id1:offset1[,id2:offset2][...], for example: 0:0,1:0,2:0")
        private String consumePartitionsAndOffsets;

        private ClientBalanceConsumer clientBalanceConsumer;
        private PullMessageConsumer messagePullConsumer;
        private PushMessageConsumer messagePushConsumer;

        private AtomicLong msgCount = new AtomicLong(0L);

        /**
         * Create a pullConsumer and consume messages
         *
         * @param messageSessionFactory
         * @param consumerConfig
         * @throws TubeClientException
         */
        private void pullConsumer(MessageSessionFactory messageSessionFactory, ConsumerConfig consumerConfig)
                throws TubeClientException {
            messagePullConsumer = messageSessionFactory.createPullConsumer(consumerConfig);
            messagePullConsumer.subscribe(topicName, null);
            messagePullConsumer.completeSubscribe();
            while (!messagePullConsumer.isPartitionsReady(1000)) {
                ThreadUtils.sleep(1000);
            }
            System.out.println("Ready to consume messages......");
            while (true) {
                ConsumerResult result = messagePullConsumer.getMessage();
                if (result.isSuccess()) {
                    List<Message> messageList = result.getMessageList();
                    for (Message message : messageList) {
                        System.out.println(new String(message.getData()));
                        msgCount.getAndIncrement();
                    }
                    messagePullConsumer.confirmConsume(result.getConfirmContext(), true);
                }
            }
        }

        /**
         * Create a pushConsumer and consume messages
         *
         * @param messageSessionFactory
         * @param consumerConfig
         * @throws TubeClientException
         * @throws InterruptedException
         */
        private void pushConsumer(MessageSessionFactory messageSessionFactory, ConsumerConfig consumerConfig)
                throws TubeClientException, InterruptedException {
            messagePushConsumer = messageSessionFactory.createPushConsumer(consumerConfig);
            messagePushConsumer.subscribe(topicName, null, new MessageListener() {

                @Override
                public void receiveMessages(PeerInfo peerInfo, List<Message> messages) throws InterruptedException {
                    for (Message message : messages) {
                        System.out.println(new String(message.getData()));
                        msgCount.getAndIncrement();
                    }
                }

                @Override
                public Executor getExecutor() {
                    return null;
                }

                @Override
                public void stop() {
                }
            });
            messagePushConsumer.completeSubscribe();
            CountDownLatch latch = new CountDownLatch(1);
            latch.await(10, TimeUnit.MINUTES);
        }

        /**
         * Create a clientBalanceConsumer and consume messages
         *
         * @param messageSessionFactory
         * @param consumerConfig
         * @throws TubeClientException
         */
        private void balanceConsumer(MessageSessionFactory messageSessionFactory, ConsumerConfig consumerConfig)
                throws TubeClientException {
            clientBalanceConsumer = messageSessionFactory.createBalanceConsumer(consumerConfig);
            ProcessResult procResult = new ProcessResult();
            QueryMetaResult qryResult = new QueryMetaResult();
            final Map<String, TreeSet<String>> topicAndFiltersMap =
                    MixedUtils.parseTopicParam(topicName);
            if (!clientBalanceConsumer.start(topicAndFiltersMap, -1, 0, procResult)) {
                System.out.println("Initial balance consumer failure, errcode is " + procResult.getErrCode()
                        + " errMsg is " + procResult.getErrMsg());
                return;
            }
            clientBalanceConsumer.getPartitionMetaInfo(qryResult);
            Map<String, Boolean> partMetaInfoMap = qryResult.getPartStatusMap();
            if (partMetaInfoMap != null && !partMetaInfoMap.isEmpty()) {
                Set<String> configuredTopicPartitions = partMetaInfoMap.keySet();
                // parse the consumePartitionsAndOffsets parameters
                Map<Long, Long> assignedPartitionsAndOffsets = new HashMap<>();
                for (String str : consumePartitionsAndOffsets.split(",")) {
                    String[] splits = str.split(":");
                    assignedPartitionsAndOffsets.put(Long.parseLong(splits[0]), Long.parseLong(splits[1]));
                }
                Set<Long> assignedPartitionIds = assignedPartitionsAndOffsets.keySet();
                Set<String> assignedPartitions = new HashSet<>();
                for (String partKey : configuredTopicPartitions) {
                    long parId = Long.parseLong(partKey.split(":")[2]);
                    if (partMetaInfoMap.get(partKey) && assignedPartitionIds.contains(parId)) {
                        assignedPartitions.add(partKey);
                        Long boostrapOffset = assignedPartitionsAndOffsets.get(parId);
                        // connect to the partitions based on consumePartitionsAndOffsets parameters
                        if (!clientBalanceConsumer.connect2Partition(partKey,
                                boostrapOffset == null ? -1L : boostrapOffset, procResult))
                            System.out.println("connect2Partition failed.");
                    }
                }

                ConsumeResult csmResult = new ConsumeResult();
                ConfirmResult cfmResult = new ConfirmResult();
                while (!clientBalanceConsumer.isPartitionsReady(1000)) {
                    ThreadUtils.sleep(1000);
                }
                System.out.println("Ready to consume messages......");
                while (true) {
                    // get messages
                    if (clientBalanceConsumer.getMessage(csmResult)) {
                        List<Message> messageList = csmResult.getMessageList();
                        for (Message message : messageList) {
                            System.out.println(new String(message.getData()));
                            msgCount.getAndIncrement();
                        }
                        // confirm messages to server
                        clientBalanceConsumer.confirmConsume(csmResult.getConfirmContext(), true, cfmResult);
                    }
                }

            } else {
                System.out.println("No partitions of the topic are available now.");
            }

        }

        @Override
        void run() {
            try {
                Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                    try {
                        if (clientBalanceConsumer != null)
                            clientBalanceConsumer.shutdown();
                        if (messagePullConsumer != null)
                            messagePullConsumer.shutdown();
                        if (messagePushConsumer != null)
                            messagePushConsumer.shutdown();
                    } catch (Throwable e) {
                        e.printStackTrace();
                    }
                    System.out.println(msgCount.get() + " message(s) has been consumed. Exited.");
                }));

                final ConsumerConfig consumerConfig = new ConsumerConfig(masterServers, groupName);
                switch (consumePosition) {
                    case "first":
                        consumerConfig.setConsumePosition(ConsumePosition.CONSUMER_FROM_FIRST_OFFSET);
                        break;
                    case "latest":
                        consumerConfig.setConsumePosition(ConsumePosition.CONSUMER_FROM_LATEST_OFFSET);
                        break;
                    case "max":
                        consumerConfig.setConsumePosition(ConsumePosition.CONSUMER_FROM_MAX_OFFSET_ALWAYS);
                        break;
                    default:
                        throw new ParameterException("Consume position, must in { first | latest | max }");
                }
                final MessageSessionFactory messageSessionFactory = new TubeSingleSessionFactory(consumerConfig);
                if (consumePartitionsAndOffsets != null) {
                    balanceConsumer(messageSessionFactory, consumerConfig);
                } else {
                    switch (mode) {
                        case "pull":
                            pullConsumer(messageSessionFactory, consumerConfig);
                            break;
                        case "push":
                            pushConsumer(messageSessionFactory, consumerConfig);
                            break;
                        case "balance":
                            balanceConsumer(messageSessionFactory, consumerConfig);
                            break;
                        default:
                            throw new ParameterException("Consume mode, must in { pull | push | balance }");
                    }
                }

            } catch (Exception e) {
                System.out.println(e.getMessage());
            } catch (Throwable e) {
                e.printStackTrace();
            }
        }
    }

}
