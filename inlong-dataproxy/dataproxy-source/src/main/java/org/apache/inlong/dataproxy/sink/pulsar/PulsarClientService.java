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

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import org.apache.inlong.dataproxy.consts.AttributeConstants;
import org.apache.inlong.dataproxy.consts.ConfigConstants;
import org.apache.inlong.dataproxy.sink.EventStat;
import org.apache.inlong.commons.monitor.LogCounter;
import org.apache.inlong.dataproxy.utils.NetworkUtils;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PulsarClientService {

    private static final Logger logger = LoggerFactory.getLogger(PulsarClientService.class);

    private static final LogCounter logPrinterA = new LogCounter(10, 100000, 60 * 1000);

    /*
     * properties key for pulsar client
     */
    private static String PULSAR_SERVER_URL_LIST = "pulsar_server_url_list";

    /*
     * properties key pulsar producer
     */
    private static String SEND_TIMEOUT = "send_timeout_mill";
    private static String CLIENT_TIMEOUT = "client_timeout_second";
    private static String ENABLE_BATCH = "enable_batch";
    private static String BLOCK_IF_QUEUE_FULL = "block_if_queue_full";
    private static String MAX_PENDING_MESSAGES = "max_pending_messages";
    private static String MAX_BATCHING_MESSAGES = "max_batching_messages";
    private static String RETRY_INTERVAL_WHEN_SEND_ERROR_MILL = "retry_interval_when_send_error_ms";

    private static int DEFAULT_SEND_TIMEOUT_MILL = 30 * 1000;
    private static int DEFAULT_CLIENT_TIMEOUT_SECOND = 30;
    private static long DEFAULT_RETRY_INTERVAL_WHEN_SEND_ERROR_MILL = 30 * 1000L;
    private static boolean DEFAULT_ENABLE_BATCH = true;
    private static boolean DEFAULT_BLOCK_IF_QUEUE_FULL = true;
    private static int DEFAULT_MAX_PENDING_MESSAGES = 10000;
    private static int DEFAULT_MAX_BATCHING_MESSAGES = 1000;

    /*
     * for pulsar client
     */
    private String[] pulsarServerUrls;

    /*
     * for producer
     */
    private Integer sendTimeout; // in millsec
    private Integer clientTimeout;
    private boolean enableBatch = true;
    private boolean blockIfQueueFull = true;
    private int maxPendingMessages = 10000;
    private int maxBatchingMessages = 1000;
    private long retryIntervalWhenSendMsgError = 30 * 1000L;
    public Map<String, List<TopicProducerInfo>> producerInfoMap;
    public Map<String, AtomicLong> topicSendIndexMap;
    public List<PulsarClient> pulsarClients;

    private String localIp = "127.0.0.1";

    /**
     * PulsarClientService
     * @param context
     */
    public PulsarClientService(Context context) {

        String pulsarServerUrlList = context.getString(PULSAR_SERVER_URL_LIST);
        Preconditions.checkState(pulsarServerUrlList != null, "No pulsar server url specified");
        pulsarServerUrls = pulsarServerUrlList.split("\\|");
        Preconditions.checkState(pulsarServerUrls != null && pulsarServerUrls.length > 0, "No "
                + "pulsar server url config");
        sendTimeout = context.getInteger(SEND_TIMEOUT, DEFAULT_SEND_TIMEOUT_MILL);
        retryIntervalWhenSendMsgError = context.getLong(RETRY_INTERVAL_WHEN_SEND_ERROR_MILL,
                DEFAULT_RETRY_INTERVAL_WHEN_SEND_ERROR_MILL);
        clientTimeout = context.getInteger(CLIENT_TIMEOUT, DEFAULT_CLIENT_TIMEOUT_SECOND);
        logger.debug("PulsarClientService " + SEND_TIMEOUT + " " + sendTimeout);
        Preconditions.checkArgument(sendTimeout > 0, "sendTimeout must be > 0");

        enableBatch = context.getBoolean(ENABLE_BATCH, DEFAULT_ENABLE_BATCH);
        blockIfQueueFull = context.getBoolean(BLOCK_IF_QUEUE_FULL, DEFAULT_BLOCK_IF_QUEUE_FULL);
        maxPendingMessages = context.getInteger(MAX_PENDING_MESSAGES, DEFAULT_MAX_PENDING_MESSAGES);
        maxBatchingMessages =  context.getInteger(MAX_BATCHING_MESSAGES, DEFAULT_MAX_BATCHING_MESSAGES);
        producerInfoMap = new ConcurrentHashMap<String, List<TopicProducerInfo>>();
        topicSendIndexMap = new ConcurrentHashMap<String, AtomicLong>();
        localIp = NetworkUtils.getLocalIp();

    }

    public void initCreateConnection(CreatePulsarClientCallBack callBack) {
        try {
            createConnection(callBack);
        } catch (FlumeException e) {
            logger.error("Unable to create pulsar client" + ". Exception follows.", e);
            close();
        }
    }

    /**
     * send message
     * @param topic
     * @param event
     * @param sendMessageCallBack
     * @param es
     * @return
     */
    public boolean sendMessage(String topic, Event event,
            SendMessageCallBack sendMessageCallBack, EventStat es) {
        TopicProducerInfo producer = null;
        try {
            producer = getProducer(topic);
        } catch (Exception e) {
            if (logPrinterA.shouldPrint()) {
                /*
                 * If it is not an IllegalTopicException,
                 * the producer may be null,
                 * causing the sendMessage part to report a null pointer later
                 */
                logger.error("Get producer failed!", e);
            }
        }
        /*
         * If the producer is a null value,\ it means that the topic is not yet
         * ready, and it needs to be played back into the file channel
         */
        if (producer == null) {
            /*
             * Data within 30s is placed in the exception channel to
             * prevent frequent checks
             * After 30s, reopen the topic check, if it is still a null value,
             *  put it back into the illegal map
             */
            return false;
        }

        Map<String, String> proMap = new HashMap<>();
        proMap.put("tdbusip", localIp);
        String streamId = "";
        if (event.getHeaders().containsKey(AttributeConstants.INTERFACE_ID)) {
            streamId = event.getHeaders().get(AttributeConstants.INTERFACE_ID);
        } else if (event.getHeaders().containsKey(AttributeConstants.INAME)) {
            streamId = event.getHeaders().get(AttributeConstants.INAME);
        }
        proMap.put(streamId, event.getHeaders().get(ConfigConstants.PKG_TIME_KEY));
        logger.debug("producer send msg!");
        TopicProducerInfo forCallBackP = producer;
        forCallBackP.getProducer().newMessage().properties(proMap).value(event.getBody())
                .sendAsync().thenAccept((msgId) -> {
            forCallBackP.setCanUseSend(true);
            sendMessageCallBack.handleMessageSendSuccess((MessageIdImpl)msgId, es);

        }).exceptionally((e) -> {
            forCallBackP.setCanUseSend(false);
            sendMessageCallBack.handleMessageSendException(es, e);
            return null;
        });
        return true;
    }

    /**
     * If this function is called successively without calling {@see #destroyConnection()}, only the
     * first call has any effect.
     *
     * @throws FlumeException if an RPC client connection could not be opened
     */
    private void createConnection(CreatePulsarClientCallBack callBack) throws FlumeException {
        if (pulsarClients != null) {
            return;
        }
        pulsarClients = new ArrayList<PulsarClient>();
        for (int i = 0; i < pulsarServerUrls.length; i++) {
            try {
                logger.debug("index = {}, url = {}", i, pulsarServerUrls[i]);
                PulsarClient client = initPulsarClient(pulsarServerUrls[i]);
                pulsarClients.add(client);
                callBack.handleCreateClientSuccess(pulsarServerUrls[i]);
            } catch (PulsarClientException e) {
                callBack.handleCreateClientException(pulsarServerUrls[i]);
                logger.error("create connnection error in metasink, "
                        + "maybe pulsar master set error, please re-check.url{}, ex1 {}",
                        pulsarServerUrls[i],
                        e.getMessage());
            } catch (Throwable e) {
                callBack.handleCreateClientException(pulsarServerUrls[i]);
                logger.error("create connnection error in metasink, "
                                + "maybe pulsar master set error/shutdown in progress, please "
                                + "re-check. url{}, ex2 {}",
                        pulsarServerUrls[i],
                        e.getMessage());
            }
        }
        if (pulsarClients.size() == 0) {
            throw new FlumeException("connect to pulsar error1, "
                    + "maybe zkstr/zkroot set error, please re-check");
        }
    }

    private PulsarClient initPulsarClient(String pulsarUrl) throws Exception {
        return PulsarClient.builder()
                .serviceUrl(pulsarUrl)
                .connectionTimeout(clientTimeout, TimeUnit.SECONDS)
                .build();
    }

    public List<TopicProducerInfo> initTopicProducer(String topic) {
        logger.info("initTopicProducer topic = {}", topic);
        List<TopicProducerInfo> producerInfoList = producerInfoMap.computeIfAbsent(topic, (k) -> {
            List<TopicProducerInfo> newList = new ArrayList<>();
            if (pulsarClients != null) {
                for (PulsarClient pulsarClient : pulsarClients) {
                    TopicProducerInfo info = new TopicProducerInfo(pulsarClient, topic);
                    info.initProducer();
                    newList.add(info);
                }
            }
            return newList;
        });
        return producerInfoList;
    }

    private TopicProducerInfo getProducer(String topic) {
        List<TopicProducerInfo> producerList = initTopicProducer(topic);
        AtomicLong topicIndex = topicSendIndexMap.computeIfAbsent(topic,(k) -> {
            return new AtomicLong(0);
        });
        int maxTryToGetProducer = producerList.size();
        if (maxTryToGetProducer == 0) {
            return null;
        }
        int retryTime = 0;
        TopicProducerInfo p = null;
        do {
            int index = (int)(topicIndex.getAndIncrement() % maxTryToGetProducer);
            p = producerList.get(index);
            if (p.isCanUseToSendMessage() && p.getProducer().isConnected()) {
                break;
            }
            retryTime++;
        } while (retryTime < maxTryToGetProducer);
        return p;
    }

    public Map<String, List<TopicProducerInfo>> getProducerInfoMap() {
        return producerInfoMap;
    }

    private void destroyConnection() {
        producerInfoMap.clear();
        if (pulsarClients != null) {
            for (PulsarClient pulsarClient : pulsarClients) {
                try {
                    pulsarClient.shutdown();
                } catch (PulsarClientException e) {
                    logger.error("destroy pulsarClient error in PulsarSink, PulsarClientException {}",
                            e.getMessage());
                } catch (Exception e) {
                    logger.error("destroy pulsarClient error in PulsarSink, ex {}", e.getMessage());
                }
            }
        }
        pulsarClients = null;
        logger.debug("closed meta producer");
    }

    public void close() {
        destroyConnection();
    }

    class TopicProducerInfo {
        private long lastSendMsgErrorTime;

        private Producer producer;

        private PulsarClient pulsarClient;

        private String topic;

        private volatile Boolean isCanUseSend = true;

        private volatile Boolean isFinishInit = false;

        public TopicProducerInfo(PulsarClient pulsarClient,
                String topic) {
            this.pulsarClient = pulsarClient;
            this.topic = topic;
        }

        public void initProducer() {
            try {
                producer = pulsarClient.newProducer().sendTimeout(sendTimeout,
                        TimeUnit.MILLISECONDS)
                        .topic(topic)
                        .enableBatching(enableBatch)
                        .blockIfQueueFull(blockIfQueueFull)
                        .maxPendingMessages(maxPendingMessages)
                        .batchingMaxMessages(maxBatchingMessages)
                        .create();
                isFinishInit = true;
            } catch (PulsarClientException e) {
                logger.error("create pulsar client has error e = {}", e);
                isFinishInit = false;
            }
        }

        public void setCanUseSend(Boolean isCanUseSend) {
            this.isCanUseSend = isCanUseSend;
            if (!isCanUseSend) {
                lastSendMsgErrorTime = System.currentTimeMillis();
            }
        }

        public boolean isCanUseToSendMessage() {
            if (isCanUseSend && isFinishInit) {
                return true;
            } else if (isFinishInit && (System.currentTimeMillis() - lastSendMsgErrorTime)
                    > retryIntervalWhenSendMsgError) {
                lastSendMsgErrorTime = System.currentTimeMillis();
                return true;
            }
            return false;
        }

        public Producer getProducer() {
            return producer;
        }
    }
}
