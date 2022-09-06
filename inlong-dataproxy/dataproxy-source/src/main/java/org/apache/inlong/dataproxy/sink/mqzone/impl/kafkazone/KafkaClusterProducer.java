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

package org.apache.inlong.dataproxy.sink.mqzone.impl.kafkazone;

import org.apache.flume.lifecycle.LifecycleState;
import org.apache.inlong.dataproxy.config.pojo.CacheClusterConfig;
import org.apache.inlong.dataproxy.dispatch.DispatchProfile;
import org.apache.inlong.dataproxy.sink.mqzone.AbstractZoneClusterProducer;
import org.apache.inlong.sdk.commons.protocol.EventUtils;
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

/**
 * KafkaClusterProducer
 */
public class KafkaClusterProducer extends AbstractZoneClusterProducer {

    public static final Logger LOG = LoggerFactory.getLogger(KafkaClusterProducer.class);

    // kafka producer
    private KafkaProducer<String, byte[]> producer;

    /**
     * Constructor
     *
     * @param workerName
     * @param config
     * @param context
     */
    public KafkaClusterProducer(String workerName, CacheClusterConfig config, KafkaZoneSinkContext context) {
        super(workerName, config, context);
    }

    /**
     * start
     */
    @Override
    public void start() {
        super.state = LifecycleState.START;
        // create kafka producer
        try {
            // prepare configuration
            Properties props = new Properties();
            props.putAll(super.producerContext.getParameters());
            props.putAll(config.getParams());
            LOG.info("try to create kafka client:{}", props);
            producer = new KafkaProducer<>(props, new StringSerializer(), new ByteArraySerializer());
            LOG.info("create new producer success:{}", producer);
        } catch (Throwable e) {
            LOG.error(e.getMessage(), e);
        }
    }

    /**
     * stop
     */
    @Override
    public void stop() {
        super.state = LifecycleState.STOP;
        // kafka producer
        this.producer.close();
    }

    /**
     * send
     * 
     * @param event
     */
    @Override
    public boolean send(DispatchProfile event) {
        try {
            // topic
            String topic = sinkContext.getIdTopicHolder().getTopic(event.getUid());
            if (topic == null) {
                sinkContext.addSendResultMetric(event, event.getUid(), false, 0);
                return false;
            }
            // create producer failed
            if (producer == null) {
                sinkContext.processSendFail(event, topic, 0);
                return false;
            }
            // headers
            Map<String, String> headers = this.encodeCacheMessageHeaders(event);
            // compress
            byte[] bodyBytes = EventUtils.encodeCacheMessageBody(sinkContext.getCompressType(), event.getEvents());
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
                        LOG.error("Send fail:{}", ex.getMessage());
                        LOG.error(ex.getMessage(), ex);
                        if (event.isResend()) {
                            sinkContext.processSendFail(event, topic, sendTime);
                        } else {
                            event.fail();
                        }
                    } else {
                        sinkContext.addSendResultMetric(event, topic, true, sendTime);
                        event.ack();
                    }
                }
            };
            producer.send(producerRecord, callback);
            return true;
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            sinkContext.processSendFail(event, event.getUid(), 0);
            return false;
        }
    }
}
