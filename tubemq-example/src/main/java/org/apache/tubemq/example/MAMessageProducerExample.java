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

import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.codec.binary.StringUtils;
import org.apache.tubemq.client.config.TubeClientConfig;
import org.apache.tubemq.client.exception.TubeClientException;
import org.apache.tubemq.client.factory.MessageSessionFactory;
import org.apache.tubemq.client.factory.TubeMultiSessionFactory;
import org.apache.tubemq.client.producer.MessageProducer;
import org.apache.tubemq.client.producer.MessageSentCallback;
import org.apache.tubemq.client.producer.MessageSentResult;
import org.apache.tubemq.corebase.Message;
import org.apache.tubemq.corebase.TErrCodeConstants;
import org.apache.tubemq.corebase.utils.ThreadUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This demo shows how to use the multi-connected {@link TubeMultiSessionFactory} in the sample single process.
 * With {@link TubeMultiSessionFactory}, a single process can establish concurrent physical request connections
 * to improve throughput from client to broker.
 */
public class MAMessageProducerExample {
    private static final Logger logger = LoggerFactory.getLogger(MAMessageProducerExample.class);
    private static final AtomicLong SENT_SUCC_COUNTER = new AtomicLong(0);
    private static final List<MessageProducer> PRODUCER_LIST = new ArrayList<>();
    private static final int MAX_PRODUCER_NUM = 100;
    private static final int SESSION_FACTORY_NUM = 10;

    private static Set<String> topicSet;
    private static int msgCount;
    private static int producerCount;
    private static byte[] sendData;

    private final String[] arrayKey = {"aaa", "bbb", "ccc", "ddd", "eee", "fff", "ggg", "hhh"};
    private final Set<String> filters = new TreeSet<>();
    private final Map<MessageProducer, Sender> producerMap = new HashMap<>();
    private final List<MessageSessionFactory> sessionFactoryList = new ArrayList<>();
    private final ExecutorService sendExecutorService =
            Executors.newFixedThreadPool(MAX_PRODUCER_NUM, new ThreadFactory() {
                @Override
                public Thread newThread(Runnable runnable) {
                    return new Thread(runnable, "sender_" + producerMap.size());
                }
            });
    private final AtomicInteger producerIndex = new AtomicInteger(0);

    private int keyCount = 0;
    private int sentCount = 0;

    public MAMessageProducerExample(String localHost, String masterHostAndPort) throws Exception {
        this.filters.add("aaa");
        this.filters.add("bbb");

        TubeClientConfig clientConfig = new TubeClientConfig(localHost, masterHostAndPort);
        for (int i = 0; i < SESSION_FACTORY_NUM; i++) {
            this.sessionFactoryList.add(new TubeMultiSessionFactory(clientConfig));
        }
    }

    public static void main(String[] args) {
        final String localHost = args[0];
        final String masterHostAndPort = args[1];

        final String topics = args[2];
        final List<String> topicList = Arrays.asList(topics.split(","));

        topicSet = new TreeSet<>(topicList);

        msgCount = Integer.parseInt(args[3]);
        producerCount = Math.min(args.length > 4 ? Integer.parseInt(args[4]) : 10, MAX_PRODUCER_NUM);

        logger.info("MAMessageProducerExample.main started...");

        final byte[] transmitData = StringUtils.getBytesUtf8("This is a test message from multi-session factory.");
        final ByteBuffer dataBuffer = ByteBuffer.allocate(1024);

        while (dataBuffer.hasRemaining()) {
            int offset = dataBuffer.arrayOffset();
            dataBuffer.put(transmitData, offset, Math.min(dataBuffer.remaining(), transmitData.length));
        }

        dataBuffer.flip();
        sendData = dataBuffer.array();

        try {
            MAMessageProducerExample messageProducer = new MAMessageProducerExample(localHost, masterHostAndPort);

            messageProducer.startService();

            while (SENT_SUCC_COUNTER.get() < msgCount * producerCount * topicSet.size()) {
                Thread.sleep(1000);
            }
            messageProducer.producerMap.clear();
            messageProducer.shutdown();

        } catch (TubeClientException e) {
            logger.error("TubeClientException: ", e);
        } catch (Throwable e) {
            logger.error("Throwable: ", e);
        }
    }

    public MessageProducer createProducer() throws TubeClientException {
        int index = (producerIndex.incrementAndGet()) % SESSION_FACTORY_NUM;
        return sessionFactoryList.get(index).createProducer();
    }

    private void startService() throws TubeClientException {
        for (int i = 0; i < producerCount; i++) {
            PRODUCER_LIST.add(createProducer());
        }

        for (MessageProducer producer : PRODUCER_LIST) {
            if (producer != null) {
                producerMap.put(producer, new Sender(producer));
                sendExecutorService.submit(producerMap.get(producer));
            }
        }
    }


    public void shutdown() throws Throwable {
        sendExecutorService.shutdownNow();
        for (int i = 0; i < SESSION_FACTORY_NUM; i++) {
            sessionFactoryList.get(i).shutdown();
        }

    }

    public class Sender implements Runnable {
        private MessageProducer producer;

        public Sender(MessageProducer producer) {
            this.producer = producer;
        }

        @Override
        public void run() {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmm");
            try {
                producer.publish(topicSet);
            } catch (Throwable t) {
                logger.error("publish exception: ", t);
            }
            for (int i = 0; i < msgCount; i++) {
                long millis = System.currentTimeMillis();
                for (String topic : topicSet) {
                    try {
                        Message message = new Message(topic, sendData);
                        message.setAttrKeyVal("index", String.valueOf(1));
                        message.setAttrKeyVal("dataTime", String.valueOf(millis));

                        String keyCode = arrayKey[sentCount++ % arrayKey.length];

                        // date format is accurate to minute, not to second
                        message.putSystemHeader(keyCode, sdf.format(new Date(millis)));
                        if (filters.contains(keyCode)) {
                            keyCount++;
                        }

                        // next line sends message synchronously, which is not recommended
                        //producer.sendMessage(message);

                        // send message asynchronously, recommended
                        producer.sendMessage(message, new DefaultSendCallback());
                    } catch (Throwable e1) {
                        logger.error("sendMessage exception: ", e1);
                    }

                    if (i % 5000 == 0) {
                        ThreadUtils.sleep(3000);
                    } else if (i % 4000 == 0) {
                        ThreadUtils.sleep(2000);
                    } else if (i % 2000 == 0) {
                        ThreadUtils.sleep(800);
                    } else if (i % 1000 == 0) {
                        ThreadUtils.sleep(400);
                    }
                }
            }
            try {
                producer.shutdown();
            } catch (Throwable e) {
                logger.error("producer shutdown error: ", e);
            }

        }
    }

    private class DefaultSendCallback implements MessageSentCallback {
        @Override
        public void onMessageSent(MessageSentResult result) {
            if (result.isSuccess()) {
                if (SENT_SUCC_COUNTER.incrementAndGet() % 1000 == 0) {
                    logger.info("Send {} message, keyCount is {}", SENT_SUCC_COUNTER.get(), keyCount);
                }
            } else {
                if (result.getErrCode() != TErrCodeConstants.SERVER_RECEIVE_OVERFLOW) {
                    logger.error("Send message failed!" + result.getErrMsg());
                }
            }
        }

        @Override
        public void onException(Throwable e) {
            logger.error("Send message error!", e);
        }
    }
}
