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

package org.apache.inlong.dataproxy.sink.pulsar;

import com.google.common.cache.LoadingCache;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.lang3.StringUtils;
import org.apache.flume.Event;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.inlong.common.monitor.LogCounter;
import org.apache.inlong.dataproxy.config.pojo.MQClusterConfig;
import org.apache.inlong.dataproxy.consts.AttributeConstants;
import org.apache.inlong.dataproxy.consts.ConfigConstants;
import org.apache.inlong.dataproxy.sink.EventStat;
import org.apache.inlong.dataproxy.sink.PulsarSink;
import org.apache.inlong.dataproxy.utils.MessageUtils;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.PulsarClientException.NotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SinkTask extends Thread {

    private static final Logger logger = LoggerFactory.getLogger(SinkTask.class);

    private static final LogCounter logPrinterA = new LogCounter(10, 100000, 60 * 1000);

    /*
     * default value
     */
    private static int BATCH_SIZE = 10000;

    private PulsarClientService pulsarClientService;

    private PulsarSink pulsarSink;

    private long logCounter = 0;

    private int poolIndex = 0;

    private LinkedBlockingQueue<EventStat> eventQueue;

    private LinkedBlockingQueue<EventStat> resendQueue;

    private AtomicLong currentInFlightCount;

    private SinkCounter sinkCounter;

    private LoadingCache<String, Long>  agentIdCache;

    private MQClusterConfig pulsarConfig;

    private int maxRetrySendCnt;
    /*
     * whether the SendTask thread can send data to pulsar
     */
    private volatile boolean canSend = false;

    public SinkTask(PulsarClientService pulsarClientService, PulsarSink pulsarSink,
            int eventQueueSize,
            int badEventQueueSize, int poolIndex, boolean canSend) {
        this.pulsarClientService = pulsarClientService;
        this.pulsarSink = pulsarSink;
        this.poolIndex = poolIndex;
        this.canSend = canSend;
        this.currentInFlightCount = pulsarSink.getCurrentInFlightCount();
        this.sinkCounter = pulsarSink.getSinkCounter();
        this.agentIdCache = pulsarSink.getAgentIdCache();
        this.pulsarConfig = pulsarSink.getPulsarConfig();
        this.maxRetrySendCnt = pulsarSink.getMaxRetrySendCnt();
        eventQueue = new LinkedBlockingQueue<>(eventQueueSize);
        resendQueue = new LinkedBlockingQueue<>(badEventQueueSize);
    }

    public boolean processEvent(EventStat eventStat) {
        try {
            return eventQueue.offer(eventStat, 3 * 1000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            logger.error("InterruptedException e", e);
        }
        return false;
    }

    public boolean processReSendEvent(EventStat eventStat) {
        return resendQueue.offer(eventStat);
    }

    public boolean isAllSendFinished() {
        return eventQueue.size() == 0;
    }

    public void close() {
        canSend = false;
    }

    @Override
    public void run() {
        logger.info("Sink task {} started.", Thread.currentThread().getName());
        while (canSend) {
            boolean decrementFlag = false;
            Event event = null;
            EventStat eventStat = null;
            String topic = null;
            try {
                if (!resendQueue.isEmpty()) {
                    /*
                     * Send the data in the retry queue first
                     */
                    eventStat = resendQueue.poll();
                    if (eventStat != null) {
                        event = eventStat.getEvent();
                    }
                } else {
                    if (currentInFlightCount.get() > BATCH_SIZE) {
                        /*
                         * Under the condition that the number of unresponsive messages
                         * is greater than 1w, the number of unresponsive messages sent
                         * to pulsar will be printed periodically
                         */
                        logCounter++;
                        if (logCounter == 1 || logCounter % 100000 == 0) {
                            logger.info(getName()
                                            + " currentInFlightCount={} resendQueue"
                                            + ".size={}",
                                    currentInFlightCount.get(), resendQueue.size());
                        }
                        if (logCounter > Long.MAX_VALUE - 10) {
                            logCounter = 0;
                        }
                    }
                    eventStat = eventQueue.take();
                    sinkCounter.incrementEventDrainAttemptCount();
                    event = eventStat.getEvent();
                }
                // check event status
                if (event == null) {
                    logger.warn("Event is null!");
                    continue;
                }
                // check whether discard or send event
                if (eventStat.getRetryCnt() > maxRetrySendCnt) {
                    logger.warn("Message will be discard! send times reach to max retry cnt."
                            + " topic = {}, max retry cnt = {}", topic, maxRetrySendCnt);
                    continue;
                }
                // get topic
                topic = event.getHeaders().get(ConfigConstants.TOPIC_KEY);
                if (StringUtils.isEmpty(topic)) {
                    String groupId = event.getHeaders().get(AttributeConstants.GROUP_ID);
                    String streamId = event.getHeaders().get(AttributeConstants.STREAM_ID);
                    topic = MessageUtils.getTopic(pulsarSink.getTopicsProperties(), groupId, streamId);
                }
                if (topic == null || topic.equals("")) {
                    pulsarSink.handleMessageSendException(topic, eventStat,
                            new NotFoundException(ConfigConstants.TOPIC_KEY + " info is null"));
                    continue;
                }
                // check whether order-type message
                if (eventStat.isOrderMessage()) {
                    sleep(1000);
                }
                // check whether duplicated event
                String clientSeqId = event.getHeaders().get(ConfigConstants.SEQUENCE_ID);
                if (pulsarConfig.getClientIdCache() && clientSeqId != null) {
                    boolean hasSend = agentIdCache.asMap().containsKey(clientSeqId);
                    agentIdCache.put(clientSeqId, System.currentTimeMillis());
                    if (hasSend) {
                        if (logPrinterA.shouldPrint()) {
                            logger.info("{} agent package {} existed,just discard.",
                                    getName(), clientSeqId);
                        }
                        continue;
                    }
                }
                // send message
                if (!pulsarClientService.sendMessage(
                        poolIndex, topic, event, pulsarSink, eventStat)) {
                    // only for order message
                    processToReTrySend(eventStat);
                }
                currentInFlightCount.incrementAndGet();
                decrementFlag = true;
            } catch (InterruptedException e) {
                logger.error("Thread {} has been interrupted!",
                        Thread.currentThread().getName());
                return;
            } catch (Throwable t) {
                if (t instanceof PulsarClientException) {
                    String message = t.getMessage();
                    if (message != null && (message.contains("No available queue for topic")
                            || message.contains("The brokers of topic are all forbidden"))) {
                        logger.info("IllegalTopicMap.put " + topic);
                        continue;
                    } else {
                        try {
                            /*
                             * The exception of pulsar will cause the sending thread to block
                             * and prevent further pressure on pulsar. Here you should pay
                             * attention to the type of exception to prevent the error of
                             *  a topic from affecting the global
                             */
                            Thread.sleep(100);
                        } catch (InterruptedException e) {
                            //ignore..
                        }
                    }
                }
                if (logPrinterA.shouldPrint()) {
                    logger.error("Sink task fail to send the message, decrementFlag="
                            + decrementFlag
                            + ",sink.name="
                            + Thread.currentThread().getName()
                            + ",event.headers="
                            + eventStat.getEvent().getHeaders(), t);
                }
                /*
                 * producer.sendMessage is abnormal,
                 * so currentInFlightCount is not added,
                 * so there is no need to subtract
                 */
                pulsarSink.handleMessageSendException(topic, eventStat, t);
                processToReTrySend(eventStat);
            }
        }
    }

    public void processToReTrySend(EventStat eventStat) {
        /*
         * order message must be retried in local resendQueue
         */
        if (eventStat.isOrderMessage()) {

            processReSendEvent(eventStat);
        }
    }
}
