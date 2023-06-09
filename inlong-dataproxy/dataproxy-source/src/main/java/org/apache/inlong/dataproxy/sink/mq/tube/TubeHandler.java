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

package org.apache.inlong.dataproxy.sink.mq.tube;

import org.apache.inlong.common.enums.DataProxyErrCode;
import org.apache.inlong.common.monitor.LogCounter;
import org.apache.inlong.dataproxy.config.ConfigManager;
import org.apache.inlong.dataproxy.config.pojo.CacheClusterConfig;
import org.apache.inlong.dataproxy.config.pojo.IdTopicConfig;
import org.apache.inlong.dataproxy.consts.ConfigConstants;
import org.apache.inlong.dataproxy.consts.StatConstants;
import org.apache.inlong.dataproxy.sink.common.EventHandler;
import org.apache.inlong.dataproxy.sink.common.TubeUtils;
import org.apache.inlong.dataproxy.sink.mq.BatchPackProfile;
import org.apache.inlong.dataproxy.sink.mq.MessageQueueHandler;
import org.apache.inlong.dataproxy.sink.mq.MessageQueueZoneSinkContext;
import org.apache.inlong.dataproxy.sink.mq.PackProfile;
import org.apache.inlong.dataproxy.sink.mq.SimplePackProfile;
import org.apache.inlong.tubemq.client.config.TubeClientConfig;
import org.apache.inlong.tubemq.client.exception.TubeClientException;
import org.apache.inlong.tubemq.client.factory.TubeMultiSessionFactory;
import org.apache.inlong.tubemq.client.producer.MessageProducer;
import org.apache.inlong.tubemq.client.producer.MessageSentCallback;
import org.apache.inlong.tubemq.client.producer.MessageSentResult;
import org.apache.inlong.tubemq.corebase.Message;

import org.apache.flume.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * TubeHandler
 */
public class TubeHandler implements MessageQueueHandler {

    public static final Logger LOG = LoggerFactory.getLogger(TubeHandler.class);
    // log print count
    private static final LogCounter logCounter = new LogCounter(10, 100000, 30 * 1000);

    private static String MASTER_HOST_PORT_LIST = "master-host-port-list";
    public static final String KEY_NAMESPACE = "namespace";

    private CacheClusterConfig config;
    private String clusterName;
    private MessageQueueZoneSinkContext sinkContext;

    // parameter
    private String masterHostAndPortList;
    private long linkMaxAllowedDelayedMsgCount;
    private long sessionWarnDelayedMsgCount;
    private long sessionMaxAllowedDelayedMsgCount;
    private long nettyWriteBufferHighWaterMark;
    // tube producer
    private TubeMultiSessionFactory sessionFactory;
    private MessageProducer producer;
    private final Set<String> topicSet = new HashSet<>();
    private final ThreadLocal<EventHandler> handlerLocal = new ThreadLocal<>();

    /**
     * init
     * @param config
     * @param sinkContext
     */
    @Override
    public void init(CacheClusterConfig config, MessageQueueZoneSinkContext sinkContext) {
        this.config = config;
        this.clusterName = config.getClusterName();
        this.sinkContext = sinkContext;
    }

    /**
     * start
     */
    @Override
    public void start() {
        // create tube producer
        try {
            // prepare configuration
            TubeClientConfig conf = initTubeConfig();
            LOG.info("try to create producer:{}", conf.toJsonString());
            this.sessionFactory = new TubeMultiSessionFactory(conf);
            this.producer = sessionFactory.createProducer();
            LOG.info("create new producer success:{}", producer);
        } catch (Throwable e) {
            LOG.error(e.getMessage(), e);
        }
    }

    @Override
    public void publishTopic(Set<String> topicSet) {
        if (this.producer == null || topicSet == null || topicSet.isEmpty()) {
            return;
        }
        Set<String> published;
        try {
            published = producer.publish(topicSet);
            topicSet.addAll(published);
            LOG.info("Publish topics to {}, need publish are {}, published are {}",
                    this.clusterName, topicSet, published);
        } catch (Throwable e) {
            LOG.warn("Publish topics to {} failure", this.clusterName, e);
        }
    }

    /**
     * initTubeConfig
     * @return
     *
     * @throws Exception
     */
    private TubeClientConfig initTubeConfig() throws Exception {
        // get parameter
        Context context = sinkContext.getProducerContext();
        Context configContext = new Context(context.getParameters());
        configContext.putAll(this.config.getParams());
        masterHostAndPortList = configContext.getString(MASTER_HOST_PORT_LIST);
        linkMaxAllowedDelayedMsgCount = configContext.getLong(ConfigConstants.LINK_MAX_ALLOWED_DELAYED_MSG_COUNT,
                80000L);
        sessionWarnDelayedMsgCount = configContext.getLong(ConfigConstants.SESSION_WARN_DELAYED_MSG_COUNT,
                2000000L);
        sessionMaxAllowedDelayedMsgCount = configContext.getLong(
                ConfigConstants.SESSION_MAX_ALLOWED_DELAYED_MSG_COUNT,
                4000000L);
        nettyWriteBufferHighWaterMark = configContext.getLong(ConfigConstants.NETTY_WRITE_BUFFER_HIGH_WATER_MARK,
                15 * 1024 * 1024L);
        // config
        final TubeClientConfig tubeClientConfig = new TubeClientConfig(this.masterHostAndPortList);
        tubeClientConfig.setLinkMaxAllowedDelayedMsgCount(linkMaxAllowedDelayedMsgCount);
        tubeClientConfig.setSessionWarnDelayedMsgCount(sessionWarnDelayedMsgCount);
        tubeClientConfig.setSessionMaxAllowedDelayedMsgCount(sessionMaxAllowedDelayedMsgCount);
        tubeClientConfig.setNettyWriteBufferHighWaterMark(nettyWriteBufferHighWaterMark);
        tubeClientConfig.setHeartbeatPeriodMs(15000L);
        tubeClientConfig.setRpcTimeoutMs(20000L);

        return tubeClientConfig;
    }

    /**
     * stop
     */
    @Override
    public void stop() {
        // producer
        if (this.producer != null) {
            try {
                this.producer.shutdown();
            } catch (Throwable e) {
                LOG.error(e.getMessage(), e);
            }
        }
        if (this.sessionFactory != null) {
            try {
                this.sessionFactory.shutdown();
            } catch (TubeClientException e) {
                LOG.error(e.getMessage(), e);
            }
        }
        LOG.info("tube handler stopped");
    }

    /**
     * send
     */
    public boolean send(PackProfile profile) {
        try {
            // idConfig
            IdTopicConfig idConfig = ConfigManager.getInstance().getIdTopicConfig(
                    profile.getInlongGroupId(), profile.getInlongStreamId());
            if (idConfig == null) {
                sinkContext.fileMetricEventInc(StatConstants.EVENT_SINK_NOUID);
                sinkContext.addSendResultMetric(profile, clusterName, profile.getUid(), false, 0);
                sinkContext.getDispatchQueue().release(profile.getSize());
                profile.fail(DataProxyErrCode.GROUPID_OR_STREAMID_NOT_CONFIGURE, "");
                return false;
            }
            String topic = idConfig.getTopicName();
            if (topic == null) {
                sinkContext.fileMetricEventInc(StatConstants.EVENT_SINK_NOTOPIC);
                sinkContext.addSendResultMetric(profile, clusterName, profile.getUid(), false, 0);
                sinkContext.getDispatchQueue().release(profile.getSize());
                profile.fail(DataProxyErrCode.TOPIC_IS_BLANK, "");
                return false;
            }
            // create producer failed
            if (producer == null) {
                sinkContext.fileMetricEventInc(StatConstants.EVENT_SINK_NOPRODUCER);
                sinkContext.processSendFail(profile, clusterName, topic, 0,
                        DataProxyErrCode.PRODUCER_IS_NULL, "");
                LOG.error("producer is null");
                return false;
            }
            // publish
            if (!this.topicSet.contains(topic)) {
                this.producer.publish(topic);
                this.topicSet.add(topic);
            }
            // send
            if (profile instanceof SimplePackProfile) {
                this.sendSimplePackProfile((SimplePackProfile) profile, idConfig, topic);
            } else {
                this.sendBatchPackProfile((BatchPackProfile) profile, idConfig, topic);
            }
            return true;
        } catch (Exception ex) {
            sinkContext.fileMetricEventInc(StatConstants.EVENT_SINK_SENDEXCEPT);
            sinkContext.processSendFail(profile, clusterName, profile.getUid(), 0,
                    DataProxyErrCode.SEND_REQUEST_TO_MQ_FAILURE, ex.getMessage());
            LOG.error(ex.getMessage(), ex);
            return false;
        }
    }

    /**
     * send BatchPackProfile
     */
    private void sendBatchPackProfile(BatchPackProfile batchProfile, IdTopicConfig idConfig,
            String topic) throws Exception {
        EventHandler handler = handlerLocal.get();
        if (handler == null) {
            handler = this.sinkContext.createEventHandler();
            handlerLocal.set(handler);
        }
        // headers
        Map<String, String> headers = handler.parseHeader(idConfig, batchProfile, sinkContext.getNodeId(),
                sinkContext.getCompressType());
        // compress
        byte[] bodyBytes = handler.parseBody(idConfig, batchProfile, sinkContext.getCompressType());
        // metric
        sinkContext.addSendMetric(batchProfile, clusterName, topic, bodyBytes.length);
        // sendAsync
        Message message = new Message(topic, bodyBytes);
        // add headers
        headers.forEach((key, value) -> {
            message.setAttrKeyVal(key, value);
        });
        // callback
        long sendTime = System.currentTimeMillis();
        MessageSentCallback callback = new MessageSentCallback() {

            @Override
            public void onMessageSent(MessageSentResult result) {
                sinkContext.fileMetricEventInc(StatConstants.EVENT_SINK_SUCCESS);
                sinkContext.addSendResultMetric(batchProfile, clusterName, topic, true, sendTime);
                sinkContext.getDispatchQueue().release(batchProfile.getSize());
                batchProfile.ack();
            }

            @Override
            public void onException(Throwable ex) {
                sinkContext.fileMetricEventInc(StatConstants.EVENT_SINK_RECEIVEEXCEPT);
                sinkContext.processSendFail(batchProfile, clusterName, topic, sendTime,
                        DataProxyErrCode.MQ_RETURN_ERROR, ex.getMessage());
                if (logCounter.shouldPrint()) {
                    LOG.error("Send ProfileV1 to tube failure", ex);
                }
            }
        };
        producer.sendMessage(message, callback);
    }

    /**
     * sendSimpleProfileV0
     */
    private void sendSimplePackProfile(SimplePackProfile simpleProfile, IdTopicConfig idConfig,
            String topic) throws Exception {
        // build message
        Message message = TubeUtils.buildMessage(topic, simpleProfile.getEvent());
        // metric
        sinkContext.addSendMetric(simpleProfile, clusterName, topic, simpleProfile.getEvent().getBody().length);
        // callback
        long sendTime = System.currentTimeMillis();
        MessageSentCallback callback = new MessageSentCallback() {

            @Override
            public void onMessageSent(MessageSentResult result) {
                sinkContext.fileMetricEventInc(StatConstants.EVENT_SINK_SUCCESS);
                sinkContext.addSendResultMetric(simpleProfile, clusterName, topic, true, sendTime);
                sinkContext.getDispatchQueue().release(simpleProfile.getSize());
                simpleProfile.ack();
            }

            @Override
            public void onException(Throwable ex) {
                sinkContext.fileMetricEventInc(StatConstants.EVENT_SINK_RECEIVEEXCEPT);
                sinkContext.processSendFail(simpleProfile, clusterName, topic, sendTime,
                        DataProxyErrCode.MQ_RETURN_ERROR, ex.getMessage());
                if (logCounter.shouldPrint()) {
                    LOG.error("Send SimpleProfileV0 to tube failure", ex);
                }
            }
        };
        producer.sendMessage(message, callback);
    }
}
