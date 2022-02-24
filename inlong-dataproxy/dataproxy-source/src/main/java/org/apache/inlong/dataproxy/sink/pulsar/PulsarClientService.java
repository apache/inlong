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
import org.apache.commons.lang.StringUtils;
import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import org.apache.inlong.common.monitor.LogCounter;
import org.apache.inlong.dataproxy.config.ConfigManager;
import org.apache.inlong.dataproxy.config.pojo.ThirdPartyClusterConfig;
import org.apache.inlong.dataproxy.consts.AttributeConstants;
import org.apache.inlong.dataproxy.consts.ConfigConstants;
import org.apache.inlong.dataproxy.metrics.audit.AuditUtils;
import org.apache.inlong.dataproxy.sink.EventStat;
import org.apache.inlong.dataproxy.utils.NetworkUtils;
import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class PulsarClientService {

    private static final Logger logger = LoggerFactory.getLogger(PulsarClientService.class);

    private static final LogCounter logPrinterA = new LogCounter(10, 100000, 60 * 1000);

    /*
     * for pulsar client
     */
    private Map<String, String> pulsarUrl2token;

    private String authType;
    /*
     * for producer
     */
    private Integer sendTimeout; // in millsec
    private Integer clientTimeout;
    private boolean enableBatch = true;
    private boolean blockIfQueueFull = true;
    private int maxPendingMessages = 10000;
    private int maxBatchingBytes = 128 * 1024;
    private int maxBatchingMessages = 1000;
    private long maxBatchingPublishDelayMillis = 1;
    private long retryIntervalWhenSendMsgError = 30 * 1000L;
    public Map<String, List<TopicProducerInfo>> producerInfoMap;
    public Map<String, AtomicLong> topicSendIndexMap;
    public Map<String, PulsarClient> pulsarClients;

    public int pulsarClientIoThreads;
    public int pulsarConnectionsPreBroker;
    private String localIp = "127.0.0.1";

    /**
     * PulsarClientService
     *
     * @param pulsarConfig
     */
    public PulsarClientService(ThirdPartyClusterConfig pulsarConfig) {

//        String pulsarServerUrlList = context.getString(PULSAR_SERVER_URL_LIST);
        authType = pulsarConfig.getAuthType();
        sendTimeout = pulsarConfig.getSendTimeoutMs();
        retryIntervalWhenSendMsgError = pulsarConfig.getRetryIntervalWhenSendErrorMs();
        clientTimeout = pulsarConfig.getClientTimeoutSecond();
        logger.debug("PulsarClientService " + sendTimeout);
        Preconditions.checkArgument(sendTimeout > 0, "sendTimeout must be > 0");

        pulsarClientIoThreads = pulsarConfig.getPulsarClientIoThreads();
        pulsarConnectionsPreBroker = pulsarConfig.getPulsarConnectionsPreBroker();

        enableBatch = pulsarConfig.getEnableBatch();
        blockIfQueueFull = pulsarConfig.getBlockIfQueueFull();
        maxPendingMessages = pulsarConfig.getMaxPendingMessages();
        maxBatchingMessages = pulsarConfig.getMaxBatchingMessages();
        maxBatchingBytes = pulsarConfig.getMaxBatchingBytes();
        maxBatchingPublishDelayMillis = pulsarConfig.getMaxBatchingPublishDelayMillis();
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
     *
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
            sendMessageCallBack.handleMessageSendException(topic, es, new Exception("producer is "
                    + "null"));
            return false;
        }

        Map<String, String> proMap = new HashMap<>();
        proMap.put("data_proxy_ip", localIp);
        String streamId = "";
        if (event.getHeaders().containsKey(AttributeConstants.INTERFACE_ID)) {
            streamId = event.getHeaders().get(AttributeConstants.INTERFACE_ID);
        } else if (event.getHeaders().containsKey(AttributeConstants.INAME)) {
            streamId = event.getHeaders().get(AttributeConstants.INAME);
        }
        proMap.put(streamId, event.getHeaders().get(ConfigConstants.PKG_TIME_KEY));
        TopicProducerInfo forCallBackP = producer;
        forCallBackP.getProducer().newMessage().properties(proMap).value(event.getBody())
                .sendAsync().thenAccept((msgId) -> {
            AuditUtils.add(AuditUtils.AUDIT_ID_DATAPROXY_SEND_SUCCESS, event);
            forCallBackP.setCanUseSend(true);
            sendMessageCallBack.handleMessageSendSuccess(topic, (MessageIdImpl) msgId, es);
        }).exceptionally((e) -> {
            forCallBackP.setCanUseSend(false);
            sendMessageCallBack.handleMessageSendException(topic, es, e);
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
        pulsarClients = new ConcurrentHashMap<>();
        pulsarUrl2token = ConfigManager.getInstance().getThirdPartyClusterUrl2Token();
        Preconditions.checkState(!pulsarUrl2token.isEmpty(), "No pulsar server url specified");
        logger.debug("number of pulsarcluster is {}", pulsarUrl2token.size());
        for (Map.Entry<String, String> info : pulsarUrl2token.entrySet()) {
            try {
                if (logger.isDebugEnabled()) {
                    logger.debug("url = {}, token = {}", info.getKey(), info.getValue());
                }
                PulsarClient client = initPulsarClient(info.getKey(), info.getValue());
                pulsarClients.put(info.getKey(), client);
                callBack.handleCreateClientSuccess(info.getKey());
            } catch (PulsarClientException e) {
                callBack.handleCreateClientException(info.getKey());
                logger.error("create connnection error in metasink, "
                                + "maybe pulsar master set error, please re-check.url{}, ex1 {}",
                        info.getKey(),
                        e.getMessage());
            } catch (Throwable e) {
                callBack.handleCreateClientException(info.getKey());
                logger.error("create connnection error in metasink, "
                                + "maybe pulsar master set error/shutdown in progress, please "
                                + "re-check. url{}, ex2 {}",
                        info.getKey(),
                        e.getMessage());
            }
        }
        if (pulsarClients.size() == 0) {
            throw new FlumeException("connect to pulsar error1, "
                    + "maybe zkstr/zkroot set error, please re-check");
        }
    }

    private PulsarClient initPulsarClient(String pulsarUrl, String token) throws Exception {
        ClientBuilder builder = PulsarClient.builder();
        if (ThirdPartyClusterConfig.PULSAR_DEFAULT_AUTH_TYPE.equals(authType) && StringUtils.isNotEmpty(token)) {
            builder.authentication(AuthenticationFactory.token(token));
        }
        builder.serviceUrl(pulsarUrl)
                .ioThreads(pulsarClientIoThreads)
                .connectionsPerBroker(pulsarConnectionsPreBroker)
                .connectionTimeout(clientTimeout, TimeUnit.SECONDS);
        return builder.build();
    }

    public List<TopicProducerInfo> initTopicProducer(String topic) {
        List<TopicProducerInfo> producerInfoList = producerInfoMap.computeIfAbsent(topic, (k) -> {
            List<TopicProducerInfo> newList = null;
            if (pulsarClients != null) {
                newList = new ArrayList<>();
                for (PulsarClient pulsarClient : pulsarClients.values()) {
                    TopicProducerInfo info = new TopicProducerInfo(pulsarClient, topic);
                    info.initProducer();
                    if (info.isCanUseToSendMessage()) {
                        newList.add(info);
                    }
                }
                if (newList.size() == 0) {
                    newList = null;
                }
            }
            return newList;
        });
        return producerInfoList;
    }

    private TopicProducerInfo getProducer(String topic) {
        List<TopicProducerInfo> producerList = initTopicProducer(topic);
        AtomicLong topicIndex = topicSendIndexMap.computeIfAbsent(topic, (k) -> {
            return new AtomicLong(0);
        });
        int maxTryToGetProducer = producerList == null ? 0 : producerList.size();
        if (maxTryToGetProducer == 0) {
            return null;
        }
        int retryTime = 0;
        TopicProducerInfo p = null;
        do {
            int index = (int) (topicIndex.getAndIncrement() % maxTryToGetProducer);
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
            for (PulsarClient pulsarClient : pulsarClients.values()) {
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

    private void removeProducers(PulsarClient pulsarClient) {
        for (List<TopicProducerInfo> producers : producerInfoMap.values()) {
            for (TopicProducerInfo topicProducer : producers) {
                if (topicProducer.getPulsarClient().equals(pulsarClient)) {
                    topicProducer.close();
                    producers.remove(topicProducer);
                }
            }
        }
    }

    /**
     * close pulsarClients(the related url is removed); start pulsarClients for new url, and create producers for them
     *
     * @param callBack
     * @param needToClose url-token map
     * @param needToStart url-token map
     * @param topicSet    for new pulsarClient, create these topics' producers
     */
    public void updatePulsarClients(CreatePulsarClientCallBack callBack, Map<String, String> needToClose,
                                    Map<String, String> needToStart, Set<String> topicSet) {
        // close
        for (String url : needToClose.keySet()) {
            PulsarClient pulsarClient = pulsarClients.get(url);
            if (pulsarClient != null) {
                try {
                    removeProducers(pulsarClient);
                    pulsarClient.shutdown();
                    pulsarClients.remove(url);
                } catch (PulsarClientException e) {
                    logger.error("shutdown pulsarClient error in PulsarSink, PulsarClientException {}",
                            e.getMessage());
                } catch (Exception e) {
                    logger.error("shutdown pulsarClient error in PulsarSink, ex {}", e.getMessage());
                }
            }
        }
        // new pulsarClient
        for (Map.Entry<String, String> entry : needToStart.entrySet()) {
            String url = entry.getKey();
            String token = entry.getValue();
            try {
                if (logger.isDebugEnabled()) {
                    logger.debug("url = {}, token = {}", url, token);
                }
                PulsarClient client = initPulsarClient(url, token);
                pulsarClients.put(url, client);
                callBack.handleCreateClientSuccess(url);

                //create related topicProducers
                for (String topic : topicSet) {
                    TopicProducerInfo info = new TopicProducerInfo(client, topic);
                    info.initProducer();
                    if (info.isCanUseToSendMessage()) {
                        producerInfoMap.computeIfAbsent(topic, k -> new ArrayList<>()).add(info);
                    }
                }

            } catch (PulsarClientException e) {
                callBack.handleCreateClientException(url);
                logger.error("create connnection error in pulsarsink, "
                        + "maybe pulsar master set error, please re-check.url{}, ex1 {}", url, e.getMessage());
            } catch (Throwable e) {
                callBack.handleCreateClientException(url);
                logger.error("create connnection error in pulsarsink, "
                        + "maybe pulsar master set error/shutdown in progress, please "
                        + "re-check. url{}, ex2 {}", url, e.getMessage());
            }
        }
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
                        .batchingMaxBytes(maxBatchingBytes)
                        .batchingMaxPublishDelay(maxBatchingPublishDelayMillis, TimeUnit.MILLISECONDS)
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

        public void close() {
            try {
                producer.close();
            } catch (PulsarClientException e) {
                logger.error("close pulsar producer has error e = {}", e);
            }
        }

        public Producer getProducer() {
            return producer;
        }

        public PulsarClient getPulsarClient() {
            return pulsarClient;
        }
    }
}
