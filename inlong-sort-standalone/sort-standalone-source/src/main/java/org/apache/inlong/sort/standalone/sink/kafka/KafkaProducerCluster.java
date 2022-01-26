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

package org.apache.inlong.sort.standalone.sink.kafka;

import com.google.common.base.Preconditions;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.Transaction;
import org.apache.flume.lifecycle.LifecycleAware;
import org.apache.flume.lifecycle.LifecycleState;
import org.apache.inlong.sort.standalone.config.holder.CommonPropertiesHolder;
import org.apache.inlong.sort.standalone.config.pojo.CacheClusterConfig;
import org.apache.inlong.sort.standalone.metrics.SortMetricItem;
import org.apache.inlong.sort.standalone.metrics.audit.AuditUtils;
import org.apache.inlong.sort.standalone.utils.Constants;
import org.apache.inlong.sort.standalone.utils.InlongLoggerFactory;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.pulsar.shade.org.apache.commons.lang.math.NumberUtils;
import org.slf4j.Logger;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/** wrapper of kafka producer */
public class KafkaProducerCluster implements LifecycleAware {
    public static final Logger LOG = InlongLoggerFactory.getLogger(KafkaProducerCluster.class);

    private static final String KEY_RETRIES = "retries";
    private static final String KEY_ACKS = "acks";
    private static final String KEY_BATCH_SIZE = "batch.size";
    private static final String KEY_LINGER_MS = "linger.ms";
    private static final String KEY_MAX_REQUEST_SIZE = "max.request.size";
    private static final String KEY_BUFFER_MEMORY = "buffer.memory";
    private static final String KEY_RECEIVE_BUFFER_BYTES = "receive.buffer.bytes";
    private static final String KEY_SEND_BUFFER_BYTES = "send.buffer.bytes";
    private static final String KEY_METADATA_FETCH_TIMEOUT_MS = "metadata.fetch.timeout.ms";
    private static final String KEY_METADATA_MAX_AGE_MS = "metadata.max.age.ms";
    private static final String KEY_COMPRESSION_TYPE = "compression.type";
    private static final String KEY_MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION =
            "max.in.flight.requests.per.connection";
    private static final String KEY_TIMEOUT_MS = "rpc.timeout.ms";
    private static final String KEY_DELIVERY_TIMEOUT_MS = "delivery.timeout.ms";
    private static final String KEY_REQUEST_TIMEOUT_MS = "request.timeout.ms";
    private static final String KEY_ENABLE_REPLACE_PARTITION_FOR_NOT_LEADER =
            "enable.replace.partition.for.not.leader";
    private static final String KEY_ENABLE_REPLACE_PARTITION_FOR_CAN_RETRY =
            "enable.replace.partition.for.can.retry";
    private static final String KEY_ENABLE_TOPIC_PARTITION_CIRCUIT_BREAKER =
            "enable.topic.partition.circuit.breaker";
    private static final String KEY_MUTE_PARTITION_ERROR_MAX_TIMES =
            "mute.partition.error.max.times";
    private static final String KEY_MUTE_PARTITION_MAX_PERCENTAGE = "mute.partition.max.percentage";
    private static final String KEY_UNMUTE_PARTITION_INTERVAL_MS = "unmute.partition.interval.ms";
    private static final String KEY_MAX_BLOCK_MS = "max.block.ms";
    private static final String KEY_METRIC_REPORTERS = "metric.reporters";
    private static final String KEY_TOPIC_EXPIRY_MS = "topic.expiry.ms";
    private static final String KEY_METADATA_RETRY_BACKOFF_MS = "metadata.retry.backoff.ms";
    private static final String KEY_BOOTSTRAP_SERVERS_CONFIG = "bootstrap.servers";
    private static final String KEY_CLIENT_ID_CONFIG = "client.id";
    private static final String KEY_AUDIT_SET_NAME = "auditSetName";

    private final String workerName;
    private final CacheClusterConfig config;
    private final KafkaFederationSinkContext sinkContext;
    private final Context context;

    private final String cacheClusterName;
    private LifecycleState state;

    private KafkaProducer<String, byte[]> producer;

    /**
     * constructor of KafkaProducerCluster
     *
     * @param workerName workerName
     * @param config config of cluster
     * @param kafkaFederationSinkContext producer context
     */
    public KafkaProducerCluster(
            String workerName,
            CacheClusterConfig config,
            KafkaFederationSinkContext kafkaFederationSinkContext) {
        this.workerName = Preconditions.checkNotNull(workerName);
        this.config = Preconditions.checkNotNull(config);
        this.sinkContext = Preconditions.checkNotNull(kafkaFederationSinkContext);
        this.context = Preconditions.checkNotNull(kafkaFederationSinkContext.getProducerContext());
        this.state = LifecycleState.IDLE;
        this.cacheClusterName = Preconditions.checkNotNull(config.getClusterName());
    }

    /** start and init kafka producer */
    @Override
    public void start() {
        this.state = LifecycleState.START;
        try {
            Properties props = new Properties();
            props.putAll(context.getParameters());
            props.put(
                    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                    context.getString(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
            props.put(
                    ProducerConfig.CLIENT_ID_CONFIG,
                    context.getString(ProducerConfig.CLIENT_ID_CONFIG) + "-" + workerName);
            LOG.info("init kafka client info: " + props);
            producer =
                    new KafkaProducer<>(props, new StringSerializer(), new ByteArraySerializer());
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
     * @param event data to send
     */
    public boolean send(Event event, Transaction tx) {
        String topic = event.getHeaders().get(Constants.TOPIC);
        ProducerRecord<String, byte[]> record = new ProducerRecord<>(topic, event.getBody());
        long sendTime = System.currentTimeMillis();
        try {
            producer.send(
                    record,
                    (metadata, ex) -> {
                        if (ex == null) {
                            tx.commit();
                            addMetric(event, topic, true, sendTime);
                        } else {
                            LOG.error(
                                    String.format(
                                            "send failed, topic is %s, partition is %s",
                                            metadata.topic(), metadata.partition()),
                                    ex);
                            tx.rollback();
                            addMetric(event, topic, false, 0);
                        }
                        tx.close();
                    });
            return true;
        } catch (Exception e) {
            tx.rollback();
            tx.close();
            LOG.error(e.getMessage(), e);
            addMetric(event, topic, false, 0);
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

    /**
     * Report metrics to monitor, including the count, size and duration of sending, sent
     * successfully and sent failed packet.
     *
     * @param currentRecord event to be reported
     * @param topic kafka topic of event sent to
     * @param result send result, send successfully -> true, send failed -> false.
     * @param sendTime the time event sent to kafka
     */
    private void addMetric(Event currentRecord, String topic, boolean result, long sendTime) {
        Map<String, String> dimensions = new HashMap<>();
        dimensions.put(SortMetricItem.KEY_CLUSTER_ID, this.sinkContext.getClusterId());
        // metric
        SortMetricItem.fillInlongId(currentRecord, dimensions);
        dimensions.put(SortMetricItem.KEY_SINK_ID, this.cacheClusterName);
        dimensions.put(SortMetricItem.KEY_SINK_DATA_ID, topic);
        long msgTime =
                NumberUtils.toLong(
                        currentRecord.getHeaders().get(Constants.HEADER_KEY_MSG_TIME), sendTime);
        long auditFormatTime = msgTime - msgTime % CommonPropertiesHolder.getAuditFormatInterval();
        dimensions.put(SortMetricItem.KEY_MESSAGE_TIME, String.valueOf(auditFormatTime));
        String taskName = currentRecord.getHeaders().get(SortMetricItem.KEY_TASK_NAME);
        dimensions.put(SortMetricItem.KEY_TASK_NAME, taskName);
        SortMetricItem.reportDurations(
                currentRecord,
                result,
                sendTime,
                dimensions,
                msgTime,
                this.sinkContext.getMetricItemSet());
        if (result) {
            AuditUtils.add(AuditUtils.AUDIT_ID_SEND_SUCCESS, currentRecord);
        }
    }
}
