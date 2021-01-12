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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.codec.binary.StringUtils;
import org.apache.tubemq.client.config.TubeClientConfig;
import org.apache.tubemq.client.exception.TubeClientException;
import org.apache.tubemq.client.factory.MessageSessionFactory;
import org.apache.tubemq.client.factory.TubeSingleSessionFactory;
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
 * This demo shows how to produce message normally.
 *
 * <p>Producer supports publish one or more topics via {@link MessageProducer#publish(String)}
 * or {@link MessageProducer#publish(Set)}. Note that topic publish asynchronously.
 */
public final class MessageProducerExample {

    private static final Logger logger =
            LoggerFactory.getLogger(MessageProducerExample.class);
    private static final ConcurrentHashMap<String, AtomicLong> counterMap =
            new ConcurrentHashMap<>();

    private final MessageProducer messageProducer;
    private final MessageSessionFactory messageSessionFactory;

    public MessageProducerExample(String masterServers) throws Exception {
        TubeClientConfig clientConfig = new TubeClientConfig(masterServers);
        this.messageSessionFactory = new TubeSingleSessionFactory(clientConfig);
        this.messageProducer = messageSessionFactory.createProducer();
    }

    public static void main(String[] args) {
        // get and initial parameters
        final String masterServers = args[0];
        final String topics = args[1];
        final long msgCount = Long.parseLong(args[2]);
        final Map<String, TreeSet<String>> topicAndFiltersMap =
                MixedUtils.parseTopicParam(topics);
        // initial send target
        final List<Tuple2<String, String>> topicSendRounds = new ArrayList<>();
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
        // initial sent data
        String body = "This is a test message from single-session-factory.";
        byte[] bodyBytes = StringUtils.getBytesUtf8(body);
        final ByteBuffer dataBuffer = ByteBuffer.allocate(1024);
        while (dataBuffer.hasRemaining()) {
            int offset = dataBuffer.arrayOffset();
            dataBuffer.put(bodyBytes, offset, Math.min(dataBuffer.remaining(), bodyBytes.length));
        }
        dataBuffer.flip();
        // send messages
        try {
            long sentCount = 0;
            int roundIndex = 0;
            int targetCnt = topicSendRounds.size();
            MessageProducerExample messageProducer =
                    new MessageProducerExample(masterServers);
            messageProducer.publishTopics(topicAndFiltersMap.keySet());
            SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmm");
            while (msgCount < 0 || sentCount < msgCount) {
                roundIndex = (int) (sentCount++ % targetCnt);
                Tuple2<String, String> target = topicSendRounds.get(roundIndex);
                Message message = new Message(target.getF0(), dataBuffer.array());
                long currTimeMillis = System.currentTimeMillis();
                message.setAttrKeyVal("index", String.valueOf(sentCount));
                message.setAttrKeyVal("dataTime", String.valueOf(currTimeMillis));
                if (target.getF1() != null) {
                    message.putSystemHeader(target.getF1(), sdf.format(new Date(currTimeMillis)));
                }
                try {
                    // 1.1 next line sends message synchronously, which is not recommended
                    // messageProducer.sendMessage(message);
                    // 1.2 send message asynchronously, recommended
                    messageProducer.sendMessageAsync(message,
                            messageProducer.new DefaultSendCallback());
                } catch (Throwable e1) {
                    logger.error("Send Message throw exception  ", e1);
                }
                // only for test, delay inflight message's count
                if (sentCount % 20000 == 0) {
                    ThreadUtils.sleep(4000);
                } else if (sentCount % 10000 == 0) {
                    ThreadUtils.sleep(2000);
                } else if (sentCount % 2500 == 0) {
                    ThreadUtils.sleep(300);
                }
            }
            ThreadUtils.sleep(20000);
            for (Map.Entry<String, AtomicLong> entry : counterMap.entrySet()) {
                logger.info(
                    "********* Current {} Message sent count is {}",
                    entry.getKey(),
                    entry.getValue().get()
                );
            }
        } catch (Throwable e) {
            e.printStackTrace();
        }
    }

    public void publishTopics(Set<String> topicSet) throws TubeClientException {
        this.messageProducer.publish(topicSet);
    }

    /**
     * Send message synchronous.
     */
    public void sendMessage(Message message) {
        // date format is accurate to minute, not to second
        try {
            MessageSentResult result = messageProducer.sendMessage(message);
            if (!result.isSuccess()) {
                logger.error("Sync-send message failed!" + result.getErrMsg());
            }
        } catch (TubeClientException | InterruptedException e) {
            logger.error("Sync-send message failed!", e);
        }
    }

    /**
     * Send message asynchronous. More efficient and recommended.
     */
    public void sendMessageAsync(Message message, MessageSentCallback callback) {
        try {
            messageProducer.sendMessage(message, callback);
        } catch (TubeClientException | InterruptedException e) {
            logger.error("Async-send message failed!", e);
        }
    }

    private class DefaultSendCallback implements MessageSentCallback {

        @Override
        public void onMessageSent(MessageSentResult result) {
            if (result.isSuccess()) {
                String topicName = result.getMessage().getTopic();

                AtomicLong currCount = counterMap.get(topicName);
                if (currCount == null) {
                    AtomicLong tmpCount = new AtomicLong(0);
                    currCount = counterMap.putIfAbsent(topicName, tmpCount);
                    if (currCount == null) {
                        currCount = tmpCount;
                    }
                }
                if (currCount.incrementAndGet() % 1000 == 0) {
                    logger.info("Send " + topicName + " " + currCount.get() + " message!");
                }
            } else {
                logger.error("Send message failed!" + result.getErrMsg());
            }
        }

        @Override
        public void onException(Throwable e) {
            logger.error("Send message error!", e);
        }
    }

}
