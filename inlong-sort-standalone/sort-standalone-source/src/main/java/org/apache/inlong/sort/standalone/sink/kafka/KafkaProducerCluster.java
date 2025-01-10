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

package org.apache.inlong.sort.standalone.sink.kafka;

import org.apache.inlong.common.pojo.sort.node.KafkaNodeConfig;
import org.apache.inlong.sort.standalone.channel.ProfileEvent;
import org.apache.inlong.sort.standalone.config.holder.CommonPropertiesHolder;
import org.apache.inlong.sort.standalone.config.pojo.CacheClusterConfig;
import org.apache.inlong.sort.standalone.utils.InlongLoggerFactory;

import com.google.common.base.Preconditions;
import org.apache.flume.Transaction;
import org.apache.flume.lifecycle.LifecycleAware;
import org.apache.flume.lifecycle.LifecycleState;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.datanucleus.util.StringUtils;
import org.slf4j.Logger;

import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Properties;

/**
 * wrapper of kafka producer
 */
public class KafkaProducerCluster implements LifecycleAware {

    public static final Logger LOG = InlongLoggerFactory.getLogger(KafkaProducerCluster.class);

    private static final String KEY_DEFAULT_SELECTOR = "sink.kafka.selector.default";
    private static final String KEY_PRODUCER_CLOSE_TIMEOUT = "sink.kafka.producer.close.timeout";

    private final String workerName;
    protected final KafkaNodeConfig nodeConfig;
    protected final CacheClusterConfig cacheClusterConfig;
    private final KafkaFederationSinkContext sinkContext;

    private LifecycleState state;
    private IEvent2KafkaRecordHandler handler;

    private KafkaProducer<String, byte[]> producer;

    private long configuredMaxPayloadSize = 8388608L;

    public KafkaProducerCluster(
            String workerName,
            CacheClusterConfig cacheClusterConfig,
            KafkaNodeConfig nodeConfig,
            KafkaFederationSinkContext kafkaFederationSinkContext) {
        this.workerName = Preconditions.checkNotNull(workerName);
        this.nodeConfig = nodeConfig;
        this.cacheClusterConfig = cacheClusterConfig;
        this.sinkContext = Preconditions.checkNotNull(kafkaFederationSinkContext);
        this.state = LifecycleState.IDLE;
        this.handler = sinkContext.createEventHandler();
    }

    /**
     * start and init kafka producer
     */
    @Override
    public void start() {
        if (CommonPropertiesHolder.useUnifiedConfiguration()) {
            startByNodeConfig();
        } else {
            startByCacheCluster();
        }
    }

    private void startByCacheCluster() {
        this.state = LifecycleState.START;
        if (cacheClusterConfig == null) {
            LOG.error("start kafka producer cluster failed, cacheClusterConfig config is null");
            return;
        }
        try {
            Properties props = defaultKafkaProperties();
            props.putAll(cacheClusterConfig.getParams());
            props.put(ProducerConfig.ACKS_CONFIG,
                    cacheClusterConfig.getParams().getOrDefault(ProducerConfig.ACKS_CONFIG, "all"));

            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                    cacheClusterConfig.getParams().get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));

            props.put(ProducerConfig.CLIENT_ID_CONFIG,
                    cacheClusterConfig.getParams().get(ProducerConfig.CLIENT_ID_CONFIG) + "-" + workerName);
            LOG.info("init kafka client by cache cluster info: " + props);
            producer = new KafkaProducer<>(props, new StringSerializer(), new ByteArraySerializer());
            Preconditions.checkNotNull(producer);
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }
    }

    private void startByNodeConfig() {
        this.state = LifecycleState.START;
        if (nodeConfig == null) {
            LOG.error("start kafka producer cluster failed, node config is null");
            return;
        }
        try {
            Properties props = defaultKafkaProperties();
            props.putAll(nodeConfig.getProperties() == null ? new HashMap<>() : nodeConfig.getProperties());
            props.put(ProducerConfig.ACKS_CONFIG, nodeConfig.getAcks());
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, nodeConfig.getBootstrapServers());
            props.put(ProducerConfig.CLIENT_ID_CONFIG, nodeConfig.getClientId() + "-" + workerName);
            LOG.info("init kafka client by node config info: " + props);
            configuredMaxPayloadSize = Long.parseLong(props.getProperty(ProducerConfig.MAX_REQUEST_SIZE_CONFIG));
            producer = new KafkaProducer<>(props, new StringSerializer(), new ByteArraySerializer());
            Preconditions.checkNotNull(producer);
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }
    }

    public Properties defaultKafkaProperties() {
        Properties props = new Properties();

        if (!CommonPropertiesHolder.getBoolean(KEY_DEFAULT_SELECTOR, false)) {
            props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, PartitionerSelector.class.getName());
        }
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "false");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, "122880");
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, "44740000");
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip");
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "300000");
        props.put(ProducerConfig.LINGER_MS_CONFIG, "500");
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
        props.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, "8388608");
        props.put(ProducerConfig.METADATA_MAX_AGE_CONFIG, "300000");
        props.put(ProducerConfig.RECEIVE_BUFFER_CONFIG, "32768");
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "30000");
        props.put(ProducerConfig.RETRIES_CONFIG, "100000");
        props.put(ProducerConfig.SEND_BUFFER_CONFIG, "524288");
        props.put("mute.partition.error.max.times", "20");
        props.put("mute.partition.max.percentage", "20");
        props.put("rpc.timeout.ms", "30000");
        props.put("topic.expiry.ms", "86400000");
        props.put("unmute.partition.interval.ms", "600000");
        props.put("metadata.retry.backoff.ms", "500");
        props.put("metadata.fetch.timeout.ms", "1000");
        props.put("maxThreads", "2");
        props.put("enable.replace.partition.for.can.retry", "true");
        props.put("enable.replace.partition.for.not.leader", "true");
        props.put("enable.topic.partition.circuit.breaker", "true");
        return props;
    }

    /**
     * stop and close kafka producer
     */
    @Override
    public void stop() {
        this.state = LifecycleState.STOP;
        try {
            long timeout = CommonPropertiesHolder.getLong(KEY_PRODUCER_CLOSE_TIMEOUT, 60L);
            LOG.info("stop kafka producer, timeout={}", timeout);
            producer.close(Duration.ofSeconds(timeout));
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }
    }

    /**
     * get module state
     *
     * @return state
     */
    @Override
    public LifecycleState getLifecycleState() {
        return this.state;
    }

    /**
     * Send data
     *
     * @param profileEvent data to send
     * @return boolean
     * @throws IOException
     */
    public boolean send(ProfileEvent profileEvent, Transaction tx) throws IOException {
        String topic = sinkContext.getTopic(profileEvent.getUid());
        ProducerRecord<String, byte[]> record = handler.parse(sinkContext, profileEvent);
        long sendTime = System.currentTimeMillis();
        // check
        if (record == null || StringUtils.isEmpty(topic)) {
            tx.commit();
            profileEvent.ack();
            tx.close();
            sinkContext.addSendResultMetric(profileEvent, topic, false, sendTime);
            return true;
        }
        try {
            producer.send(record,
                    (metadata, ex) -> {
                        if (ex == null) {
                            tx.commit();
                            sinkContext.addSendResultMetric(profileEvent, topic, true, sendTime);
                            profileEvent.ack();
                        } else {

                            if (ex instanceof RecordTooLargeException) {
                                // for the message bigger than configuredMaxPayloadSize, just discard it;
                                // otherwise, retry and wait for the server side changes the limitation
                                if (record.value().length > configuredMaxPayloadSize) {
                                    tx.commit();
                                    profileEvent.ack();
                                } else {
                                    tx.rollback();
                                }
                            } else if (ex instanceof UnknownTopicOrPartitionException
                                    || !(ex instanceof RetriableException)) {
                                // for non-retriable exception, just discard it
                                tx.commit();
                                profileEvent.ack();
                            } else {
                                tx.rollback();
                            }
                            LOG.error(String.format("send failed, topic is %s", topic), ex);
                            sinkContext.addSendResultMetric(profileEvent, topic, false, sendTime);
                        }
                        tx.close();
                    });
            return true;
        } catch (Exception e) {
            tx.rollback();
            tx.close();
            LOG.error(e.getMessage(), e);
            sinkContext.addSendResultMetric(profileEvent, topic, false, sendTime);
            return false;
        }
    }

}
