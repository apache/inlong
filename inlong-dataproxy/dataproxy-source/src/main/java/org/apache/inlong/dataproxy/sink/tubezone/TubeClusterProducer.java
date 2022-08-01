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

package org.apache.inlong.dataproxy.sink.tubezone;

import org.apache.flume.Context;
import org.apache.flume.lifecycle.LifecycleAware;
import org.apache.flume.lifecycle.LifecycleState;
import org.apache.inlong.dataproxy.config.pojo.CacheClusterConfig;
import org.apache.inlong.dataproxy.config.pojo.IdTopicConfig;
import org.apache.inlong.dataproxy.consts.ConfigConstants;
import org.apache.inlong.dataproxy.dispatch.DispatchProfile;
import org.apache.inlong.dataproxy.sink.EventHandler;
import org.apache.inlong.tubemq.client.config.TubeClientConfig;
import org.apache.inlong.tubemq.client.exception.TubeClientException;
import org.apache.inlong.tubemq.client.factory.TubeMultiSessionFactory;
import org.apache.inlong.tubemq.client.producer.MessageProducer;
import org.apache.inlong.tubemq.client.producer.MessageSentCallback;
import org.apache.inlong.tubemq.client.producer.MessageSentResult;
import org.apache.inlong.tubemq.corebase.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * TubeClusterProducer
 */
public class TubeClusterProducer implements LifecycleAware {

    public static final Logger LOG = LoggerFactory.getLogger(TubeClusterProducer.class);
    private static String MASTER_HOST_PORT_LIST = "master-host-port-list";

    protected final String workerName;
    private final CacheClusterConfig config;
    private final TubeZoneSinkContext sinkContext;
    private final Context producerContext;
    private final String cacheClusterName;
    private LifecycleState state;

    // parameter
    private String masterHostAndPortList;
    private long linkMaxAllowedDelayedMsgCount;
    private long sessionWarnDelayedMsgCount;
    private long sessionMaxAllowedDelayedMsgCount;
    private long nettyWriteBufferHighWaterMark;
    // tube producer
    private TubeMultiSessionFactory sessionFactory;
    private MessageProducer producer;
    private Set<String> topicSet = new HashSet<>();
    private EventHandler handler;

    /**
     * Constructor
     * 
     * @param workerName
     * @param config
     * @param context
     */
    public TubeClusterProducer(String workerName, CacheClusterConfig config, TubeZoneSinkContext context) {
        this.workerName = workerName;
        this.config = config;
        this.sinkContext = context;
        this.producerContext = context.getProducerContext();
        this.state = LifecycleState.IDLE;
        this.cacheClusterName = config.getClusterName();
        this.handler = this.sinkContext.createEventHandler();
    }

    /**
     * start
     */
    @Override
    public void start() {
        this.state = LifecycleState.START;
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

    /**
     * initTubeConfig
     * @return
     *
     * @throws Exception
     */
    private TubeClientConfig initTubeConfig() throws Exception {
        // get parameter
        Context configContext = new Context(this.producerContext.getParameters());
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
        this.state = LifecycleState.STOP;
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
    }

    /**
     * getLifecycleState
     * 
     * @return
     */
    @Override
    public LifecycleState getLifecycleState() {
        return state;
    }

    /**
     * send
     * 
     * @param event
     */
    public boolean send(DispatchProfile event) {
        try {
            // idConfig
            IdTopicConfig idConfig = sinkContext.getIdTopicHolder().getIdConfig(event.getUid());
            if (idConfig == null) {
                sinkContext.addSendResultMetric(event, event.getUid(), false, 0);
                return false;
            }
            String topic = idConfig.getTopicName();
            if (topic == null) {
                sinkContext.addSendResultMetric(event, event.getUid(), false, 0);
                return false;
            }
            // metric
            sinkContext.addSendMetric(event, topic);
            // publish
            if (!this.topicSet.contains(topic)) {
                this.producer.publish(topic);
                this.topicSet.add(topic);
            }
            // create producer failed
            if (producer == null) {
                sinkContext.processSendFail(event, topic, 0);
                return false;
            }
            // headers
            Map<String, String> headers = this.handler.parseHeader(idConfig, event, sinkContext.getNodeId(),
                    sinkContext.getCompressType());
            // compress
            byte[] bodyBytes = this.handler.parseBody(idConfig, event, sinkContext.getCompressType());
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
                    sinkContext.addSendResultMetric(event, topic, true, sendTime);
                    event.ack();
                }

                @Override
                public void onException(Throwable ex) {
                    LOG.error("Send fail:{}", ex.getMessage());
                    LOG.error(ex.getMessage(), ex);
                    sinkContext.processSendFail(event, topic, sendTime);
                }
            };
            producer.sendMessage(message, callback);
            return true;
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            sinkContext.processSendFail(event, event.getUid(), 0);
            return false;
        }
    }

    /**
     * get cacheClusterName
     * 
     * @return the cacheClusterName
     */
    public String getCacheClusterName() {
        return cacheClusterName;
    }

}
