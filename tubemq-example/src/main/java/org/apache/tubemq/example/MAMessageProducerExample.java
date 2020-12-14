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
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
import org.apache.tubemq.corebase.utils.MixedUtils;
import org.apache.tubemq.corebase.utils.ThreadUtils;
import org.apache.tubemq.corebase.utils.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This demo shows how to use the multi-connected {@link TubeMultiSessionFactory} in the sample single process.
 * With {@link TubeMultiSessionFactory}, a single process can establish concurrent physical request connections
 * to improve throughput from client to broker.
 */
public class MAMessageProducerExample {
    private static final Logger logger =
            LoggerFactory.getLogger(MAMessageProducerExample.class);
    private static final AtomicLong TOTAL_COUNTER = new AtomicLong(0);
    private static final AtomicLong SENT_SUCC_COUNTER = new AtomicLong(0);
    private static final AtomicLong SENT_FAIL_COUNTER = new AtomicLong(0);
    private static final AtomicLong SENT_EXCEPT_COUNTER = new AtomicLong(0);

    private static final List<MessageProducer> PRODUCER_LIST = new ArrayList<>();
    private static final int MAX_PRODUCER_NUM = 100;
    private static final int SESSION_FACTORY_NUM = 10;

    private static Map<String, TreeSet<String>> topicAndFiltersMap;
    private static List<Tuple2<String, String>> topicSendRounds = new ArrayList<>();
    private static int msgCount;
    private static int clientCount;
    private static byte[] sendData;
    private static AtomicLong filterMsgCount = new AtomicLong(0);

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



    public MAMessageProducerExample(String masterHostAndPort) throws Exception {
        TubeClientConfig clientConfig = new TubeClientConfig(masterHostAndPort);
        for (int i = 0; i < SESSION_FACTORY_NUM; i++) {
            this.sessionFactoryList.add(new TubeMultiSessionFactory(clientConfig));
        }
    }

    public static void main(String[] args) {
        // get call parameters
        final String masterServers = args[0];
        final String topics = args[1];
        msgCount = Integer.parseInt(args[2]);
        clientCount = Math.min(args.length > 4 ? Integer.parseInt(args[3]) : 10, MAX_PRODUCER_NUM);
        topicAndFiltersMap = MixedUtils.parseTopicParam(topics);
        // initial topic send round
        for (Map.Entry<String, TreeSet<String>> entry: topicAndFiltersMap.entrySet()) {
            if (entry.getValue().isEmpty()) {
                topicSendRounds.add(new Tuple2<String, String>(entry.getKey()));
            } else {
                for (String filter : entry.getValue()) {
                    topicSendRounds.add(new Tuple2<String, String>(entry.getKey(), filter));
                }
            }
        }
        // build message's body content
        final byte[] transmitData =
                StringUtils.getBytesUtf8("This is a test message from multi-session factory.");
        final ByteBuffer dataBuffer = ByteBuffer.allocate(1024);
        while (dataBuffer.hasRemaining()) {
            int offset = dataBuffer.arrayOffset();
            dataBuffer.put(transmitData, offset,
                    Math.min(dataBuffer.remaining(), transmitData.length));
        }
        dataBuffer.flip();
        sendData = dataBuffer.array();
        // print started log
        logger.info("MAMessageProducerExample.main started...");

        try {
            // initial producer objects
            MAMessageProducerExample messageProducer =
                    new MAMessageProducerExample(masterServers);
            messageProducer.startService();
            // wait util sent message's count reachs required count
            while (TOTAL_COUNTER.get() < msgCount * clientCount) {
                logger.info("Sending, total messages is {}, filter messages is {}",
                        SENT_SUCC_COUNTER.get(), filterMsgCount.get());
                Thread.sleep(5000);
            }
            logger.info("Finished, total messages is {}, filter messages is {}",
                    SENT_SUCC_COUNTER.get(), filterMsgCount.get());
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
        for (int i = 0; i < clientCount; i++) {
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
                producer.publish(topicAndFiltersMap.keySet());
            } catch (Throwable t) {
                logger.error("publish exception: ", t);
            }
            long sentCount = 0;
            int roundIndex = 0;
            int targetCnt = topicSendRounds.size();
            while (msgCount < 0 || sentCount < msgCount) {
                long millis = System.currentTimeMillis();
                roundIndex = (int) (sentCount++ % targetCnt);
                Tuple2<String, String> target = topicSendRounds.get(roundIndex);
                Message message = new Message(target.f0, sendData);
                message.setAttrKeyVal("index", String.valueOf(sentCount));
                message.setAttrKeyVal("dataTime", String.valueOf(millis));
                if (target.f1 != null) {
                    filterMsgCount.incrementAndGet();
                    message.putSystemHeader(target.f1, sdf.format(new Date(millis)));
                }
                try {
                    // next line sends message synchronously, which is not recommended
                    //producer.sendMessage(message);
                    // send message asynchronously, recommended
                    producer.sendMessage(message, new DefaultSendCallback());
                } catch (Throwable e1) {
                    TOTAL_COUNTER.incrementAndGet();
                    SENT_EXCEPT_COUNTER.incrementAndGet();
                    logger.error("sendMessage exception: ", e1);
                }
                TOTAL_COUNTER.incrementAndGet();
                // only for test, delay inflight message's count
                if (sentCount % 5000 == 0) {
                    ThreadUtils.sleep(3000);
                } else if (sentCount % 4000 == 0) {
                    ThreadUtils.sleep(2000);
                } else if (sentCount % 2000 == 0) {
                    ThreadUtils.sleep(800);
                } else if (sentCount % 1000 == 0) {
                    ThreadUtils.sleep(400);
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
            TOTAL_COUNTER.incrementAndGet();
            if (result.isSuccess()) {
                SENT_SUCC_COUNTER.incrementAndGet();
            } else {
                SENT_FAIL_COUNTER.incrementAndGet();
            }
        }

        @Override
        public void onException(Throwable e) {
            TOTAL_COUNTER.incrementAndGet();
            SENT_EXCEPT_COUNTER.incrementAndGet();
            logger.error("Send message error!", e);
        }
    }
}
