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

package org.apache.inlong.dataproxy.sink.mq.kafka;

import org.apache.inlong.common.enums.DataProxyErrCode;
import org.apache.inlong.common.monitor.LogCounter;
import org.apache.inlong.dataproxy.config.CommonConfigHolder;
import org.apache.inlong.dataproxy.config.ConfigManager;
import org.apache.inlong.dataproxy.config.pojo.CacheClusterConfig;
import org.apache.inlong.dataproxy.config.pojo.IdTopicConfig;
import org.apache.inlong.dataproxy.consts.StatConstants;
import org.apache.inlong.dataproxy.sink.common.EventHandler;
import org.apache.inlong.dataproxy.sink.mq.BatchPackProfile;
import org.apache.inlong.dataproxy.sink.mq.MessageQueueHandler;
import org.apache.inlong.dataproxy.sink.mq.MessageQueueZoneSinkContext;
import org.apache.inlong.dataproxy.sink.mq.PackProfile;
import org.apache.inlong.dataproxy.sink.mq.SimplePackProfile;

import org.apache.commons.lang3.StringUtils;
import org.apache.flume.Context;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * KafkaHandler
 * 
 */
public class KafkaHandler implements MessageQueueHandler {

    public static final Logger LOG = LoggerFactory.getLogger(KafkaHandler.class);
    // log print count
    private static final LogCounter logCounter = new LogCounter(10, 100000, 30 * 1000);

    private CacheClusterConfig config;
    private String clusterName;
    private MessageQueueZoneSinkContext sinkContext;

    // kafka producer
    private KafkaProducer<String, byte[]> producer;
    private ThreadLocal<EventHandler> handlerLocal = new ThreadLocal<>();

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
        // create kafka producer
        try {
            // prepare configuration
            Properties props = new Properties();
            Context context = this.sinkContext.getProducerContext();
            props.putAll(context.getParameters());
            props.putAll(config.getParams());
            LOG.info("try to create kafka client:{}", props);
            producer = new KafkaProducer<>(props, new StringSerializer(), new ByteArraySerializer());
            LOG.info("create new producer success:{}", producer);
        } catch (Throwable e) {
            LOG.error(e.getMessage(), e);
        }
    }

    @Override
    public void publishTopic(Set<String> topicSet) {
        //
    }

    /**
     * stop
     */
    @Override
    public void stop() {
        // kafka producer
        this.producer.close();
        LOG.info("kafka handler stopped");
    }

    /**
     * send
     * @param profile
     * @return
     */
    @Override
    public boolean send(PackProfile profile) {
        try {
            String topic;
            // get idConfig
            IdTopicConfig idConfig = ConfigManager.getInstance().getIdTopicConfig(
                    profile.getInlongGroupId(), profile.getInlongStreamId());
            if (idConfig == null) {
                if (!CommonConfigHolder.getInstance().isEnableUnConfigTopicAccept()) {
                    sinkContext.fileMetricIncWithDetailStats(
                            StatConstants.EVENT_SINK_CONFIG_TOPIC_MISSING, profile.getUid());
                    sinkContext.addSendResultMetric(profile, clusterName, profile.getUid(), false, 0);
                    sinkContext.getDispatchQueue().release(profile.getSize());
                    profile.fail(DataProxyErrCode.GROUPID_OR_STREAMID_NOT_CONFIGURE, "");
                    return false;
                }
                topic = CommonConfigHolder.getInstance().getRandDefTopics();
                if (StringUtils.isEmpty(topic)) {
                    sinkContext.fileMetricIncSumStats(StatConstants.EVENT_SINK_DEFAULT_TOPIC_MISSING);
                    sinkContext.addSendResultMetric(profile, clusterName, profile.getUid(), false, 0);
                    sinkContext.getDispatchQueue().release(profile.getSize());
                    profile.fail(DataProxyErrCode.GROUPID_OR_STREAMID_NOT_CONFIGURE, "");
                    return false;
                }
                sinkContext.fileMetricIncSumStats(StatConstants.EVENT_SINK_DEFAULT_TOPIC_USED);
            } else {
                topic = idConfig.getTopicName();
            }
            // create producer failed
            if (producer == null) {
                sinkContext.fileMetricIncSumStats(StatConstants.EVENT_SINK_PRODUCER_NULL);
                sinkContext.processSendFail(profile, clusterName, topic, 0, DataProxyErrCode.PRODUCER_IS_NULL, "");
                return false;
            }
            // send
            if (profile instanceof SimplePackProfile) {
                this.sendSimplePackProfile((SimplePackProfile) profile, idConfig, topic);
            } else {
                this.sendBatchPackProfile((BatchPackProfile) profile, idConfig, topic);
            }
            return true;
        } catch (Exception ex) {
            sinkContext.fileMetricIncSumStats(StatConstants.EVENT_SINK_SEND_EXCEPTION);
            sinkContext.processSendFail(profile, clusterName, profile.getUid(), 0,
                    DataProxyErrCode.SEND_REQUEST_TO_MQ_FAILURE, ex.getMessage());
            if (logCounter.shouldPrint()) {
                LOG.error("Send Message to Kafka failure", ex);
            }
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
        long sendTime = System.currentTimeMillis();

        // prepare ProducerRecord
        ProducerRecord<String, byte[]> producerRecord = new ProducerRecord<>(topic, bodyBytes);
        // add headers
        headers.forEach((key, value) -> {
            producerRecord.headers().add(key, value.getBytes());
        });

        // callback
        Callback callback = new Callback() {

            @Override
            public void onCompletion(RecordMetadata arg0, Exception ex) {
                if (ex != null) {
                    sinkContext.fileMetricIncSumStats(StatConstants.EVENT_SINK_RECEIVEEXCEPT);
                    sinkContext.processSendFail(batchProfile, clusterName, topic, sendTime,
                            DataProxyErrCode.MQ_RETURN_ERROR, ex.getMessage());
                    if (logCounter.shouldPrint()) {
                        LOG.error("Send BatchPackProfile to Kafka failure", ex);
                    }
                } else {
                    sinkContext.fileMetricIncSumStats(StatConstants.EVENT_SINK_SUCCESS);
                    sinkContext.addSendResultMetric(batchProfile, clusterName, topic, true, sendTime);
                    sinkContext.getDispatchQueue().release(batchProfile.getSize());
                    batchProfile.ack();
                }
            }
        };
        producer.send(producerRecord, callback);
    }

    /**
     * send SimplePackProfile
     */
    private void sendSimplePackProfile(SimplePackProfile simpleProfile, IdTopicConfig idConfig,
            String topic) throws Exception {
        // headers
        Map<String, String> headers = simpleProfile.getProperties();
        // body
        byte[] bodyBytes = simpleProfile.getEvent().getBody();
        // metric
        sinkContext.addSendMetric(simpleProfile, clusterName, topic, bodyBytes.length);
        // sendAsync
        long sendTime = System.currentTimeMillis();

        // prepare ProducerRecord
        ProducerRecord<String, byte[]> producerRecord = new ProducerRecord<>(topic, bodyBytes);
        // add headers
        headers.forEach((key, value) -> {
            producerRecord.headers().add(key, value.getBytes());
        });

        // callback
        Callback callback = new Callback() {

            @Override
            public void onCompletion(RecordMetadata arg0, Exception ex) {
                if (ex != null) {
                    sinkContext.fileMetricIncSumStats(StatConstants.EVENT_SINK_RECEIVEEXCEPT);
                    sinkContext.processSendFail(simpleProfile, clusterName, topic, sendTime,
                            DataProxyErrCode.MQ_RETURN_ERROR, ex.getMessage());
                    if (logCounter.shouldPrint()) {
                        LOG.error("Send SimplePackProfile to Kafka failure", ex);
                    }
                } else {
                    sinkContext.fileMetricIncSumStats(StatConstants.EVENT_SINK_SUCCESS);
                    sinkContext.addSendResultMetric(simpleProfile, clusterName, topic, true, sendTime);
                    sinkContext.getDispatchQueue().release(simpleProfile.getSize());
                    simpleProfile.ack();
                }
            }
        };
        producer.send(producerRecord, callback);
    }
}
