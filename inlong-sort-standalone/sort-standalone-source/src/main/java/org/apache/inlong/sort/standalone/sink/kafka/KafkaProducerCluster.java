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
import org.apache.inlong.sort.standalone.utils.Constants;
import org.apache.inlong.sort.standalone.utils.InlongLoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.apache.flume.Context;
import org.apache.flume.Transaction;
import org.apache.flume.lifecycle.LifecycleAware;
import org.apache.flume.lifecycle.LifecycleState;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.Properties;

/** wrapper of kafka producer */
public class KafkaProducerCluster implements LifecycleAware {

    public static final Logger LOG = InlongLoggerFactory.getLogger(KafkaProducerCluster.class);

    private final String workerName;
    protected final KafkaNodeConfig nodeConfig;
    private final KafkaFederationSinkContext sinkContext;
    private final Context context;

    private final String cacheClusterName;
    private LifecycleState state;
    private IEvent2KafkaRecordHandler handler;

    private KafkaProducer<String, byte[]> producer;

    public KafkaProducerCluster(
            String workerName,
            KafkaNodeConfig nodeConfig,
            KafkaFederationSinkContext kafkaFederationSinkContext) {
        this.workerName = Preconditions.checkNotNull(workerName);
        this.nodeConfig = nodeConfig;
        this.sinkContext = Preconditions.checkNotNull(kafkaFederationSinkContext);
        this.context = new Context(nodeConfig.getProperties() != null ? nodeConfig.getProperties() : Maps.newHashMap());
        this.state = LifecycleState.IDLE;
        this.cacheClusterName = nodeConfig.getNodeName();
        this.handler = sinkContext.createEventHandler();
    }

    /** start and init kafka producer */
    @Override
    public void start() {
        this.state = LifecycleState.START;
        try {
            Properties props = new Properties();
            props.putAll(context.getParameters());
            props.put(
                    ProducerConfig.PARTITIONER_CLASS_CONFIG,
                    context.getString(ProducerConfig.PARTITIONER_CLASS_CONFIG, PartitionerSelector.class.getName()));
            props.put(
                    ProducerConfig.ACKS_CONFIG,
                    context.getString(ProducerConfig.ACKS_CONFIG, "all"));
            props.put(
                    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                    nodeConfig.getBootstrapServers());
            props.put(ProducerConfig.CLIENT_ID_CONFIG,
                    nodeConfig.getClientId() + "-" + workerName);
            LOG.info("init kafka client info: " + props);
            producer = new KafkaProducer<>(props, new StringSerializer(), new ByteArraySerializer());
            Preconditions.checkNotNull(producer);
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }
    }

    /** stop and close kafka producer */
    @Override
    public void stop() {
        this.state = LifecycleState.STOP;
        try {
            LOG.info("stop kafka producer");
            producer.close();
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
     * @param  profileEvent data to send
     * @return              boolean
     * @throws IOException
     */
    public boolean send(ProfileEvent profileEvent, Transaction tx) throws IOException {
        String topic = profileEvent.getHeaders().get(Constants.TOPIC);
        ProducerRecord<String, byte[]> record = handler.parse(sinkContext, profileEvent);
        long sendTime = System.currentTimeMillis();
        // check
        if (record == null) {
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
                            LOG.error(String.format("send failed, topic is %s, partition is %s",
                                    metadata.topic(), metadata.partition()), ex);
                            tx.rollback();
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

    /**
     * get cache cluster name
     *
     * @return cacheClusterName
     */
    public String getCacheClusterName() {
        return cacheClusterName;
    }
}
