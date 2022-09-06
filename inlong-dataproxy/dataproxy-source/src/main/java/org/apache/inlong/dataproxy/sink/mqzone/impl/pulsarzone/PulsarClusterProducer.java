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

package org.apache.inlong.dataproxy.sink.mqzone.impl.pulsarzone;

import static org.apache.inlong.dataproxy.consts.ConfigConstants.KEY_AUTHENTICATION;
import static org.apache.inlong.dataproxy.consts.ConfigConstants.KEY_BATCHINGMAXBYTES;
import static org.apache.inlong.dataproxy.consts.ConfigConstants.KEY_BATCHINGMAXMESSAGES;
import static org.apache.inlong.dataproxy.consts.ConfigConstants.KEY_BATCHINGMAXPUBLISHDELAY;
import static org.apache.inlong.dataproxy.consts.ConfigConstants.KEY_BLOCKIFQUEUEFULL;
import static org.apache.inlong.dataproxy.consts.ConfigConstants.KEY_COMPRESSIONTYPE;
import static org.apache.inlong.dataproxy.consts.ConfigConstants.KEY_CONNECTIONSPERBROKER;
import static org.apache.inlong.dataproxy.consts.ConfigConstants.KEY_ENABLEBATCHING;
import static org.apache.inlong.dataproxy.consts.ConfigConstants.KEY_IOTHREADS;
import static org.apache.inlong.dataproxy.consts.ConfigConstants.KEY_MAXPENDINGMESSAGES;
import static org.apache.inlong.dataproxy.consts.ConfigConstants.KEY_MAXPENDINGMESSAGESACROSSPARTITIONS;
import static org.apache.inlong.dataproxy.consts.ConfigConstants.KEY_MEMORYLIMIT;
import static org.apache.inlong.dataproxy.consts.ConfigConstants.KEY_NAMESPACE;
import static org.apache.inlong.dataproxy.consts.ConfigConstants.KEY_ROUNDROBINROUTERBATCHINGPARTITIONSWITCHFREQUENCY;
import static org.apache.inlong.dataproxy.consts.ConfigConstants.KEY_SENDTIMEOUT;
import static org.apache.inlong.dataproxy.consts.ConfigConstants.KEY_SERVICE_URL;
import static org.apache.inlong.dataproxy.consts.ConfigConstants.KEY_TENANT;
import org.apache.flume.lifecycle.LifecycleState;
import org.apache.inlong.dataproxy.config.pojo.CacheClusterConfig;
import org.apache.inlong.dataproxy.dispatch.DispatchProfile;
import org.apache.inlong.dataproxy.sink.mqzone.AbstractZoneClusterProducer;
import org.apache.inlong.sdk.commons.protocol.EventUtils;
import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerAccessMode;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SizeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.SecureRandom;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * PulsarClusterProducer
 */
public class PulsarClusterProducer extends AbstractZoneClusterProducer {

    public static final Logger LOG = LoggerFactory.getLogger(PulsarClusterProducer.class);

    private String tenant;
    private String namespace;

    /**
     * pulsar client
     */
    private PulsarClient client;
    private ProducerBuilder<byte[]> baseBuilder;
    private Map<String, Producer<byte[]>> producerMap = new ConcurrentHashMap<>();

    /**
     * Constructor
     * 
     * @param workerName Worker name
     * @param config Cache cluster configuration
     * @param context Sink context
     */
    public PulsarClusterProducer(String workerName, CacheClusterConfig config, PulsarZoneSinkContext context) {
        super(workerName, config, context);
        this.tenant = config.getParams().get(KEY_TENANT);
        this.namespace = config.getParams().get(KEY_NAMESPACE);
    }

    /**
     * start
     */
    @Override
    public void start() {
        this.state = LifecycleState.START;
        // create pulsar client
        try {
            String serviceUrl = config.getParams().get(KEY_SERVICE_URL);
            String authentication = config.getParams().get(KEY_AUTHENTICATION);
            this.client = PulsarClient.builder()
                    .serviceUrl(serviceUrl)
                    .authentication(AuthenticationFactory.token(authentication))
                    .ioThreads(producerContext.getInteger(KEY_IOTHREADS, 1))
                    .memoryLimit(producerContext.getLong(KEY_MEMORYLIMIT, 1073741824L), SizeUnit.BYTES)
                    .connectionsPerBroker(producerContext.getInteger(KEY_CONNECTIONSPERBROKER, 10))
                    .build();
            this.baseBuilder = client.newProducer();
            // Map<String, Object> builderConf = new HashMap<>();
            // builderConf.putAll(context.getParameters());
            this.baseBuilder
                    .sendTimeout(producerContext.getInteger(KEY_SENDTIMEOUT, 0), TimeUnit.MILLISECONDS)
                    .maxPendingMessages(producerContext.getInteger(KEY_MAXPENDINGMESSAGES, 500))
                    .maxPendingMessagesAcrossPartitions(
                            producerContext.getInteger(KEY_MAXPENDINGMESSAGESACROSSPARTITIONS, 60000));
            this.baseBuilder
                    .batchingMaxMessages(producerContext.getInteger(KEY_BATCHINGMAXMESSAGES, 500))
                    .batchingMaxPublishDelay(producerContext.getInteger(KEY_BATCHINGMAXPUBLISHDELAY, 100),
                            TimeUnit.MILLISECONDS)
                    .batchingMaxBytes(producerContext.getInteger(KEY_BATCHINGMAXBYTES, 131072));
            this.baseBuilder
                    .accessMode(ProducerAccessMode.Shared)
                    .messageRoutingMode(MessageRoutingMode.RoundRobinPartition)
                    .blockIfQueueFull(producerContext.getBoolean(KEY_BLOCKIFQUEUEFULL, true));
            this.baseBuilder
                    .roundRobinRouterBatchingPartitionSwitchFrequency(
                            producerContext.getInteger(KEY_ROUNDROBINROUTERBATCHINGPARTITIONSWITCHFREQUENCY, 60))
                    .enableBatching(producerContext.getBoolean(KEY_ENABLEBATCHING, true))
                    .compressionType(this.getPulsarCompressionType());
        } catch (Throwable e) {
            LOG.error(e.getMessage(), e);
        }
    }

    /**
     * getPulsarCompressionType
     * 
     * @return CompressionType LZ4/NONE/ZLIB/ZSTD/SNAPPY
     */
    private CompressionType getPulsarCompressionType() {
        String type = this.producerContext.getString(KEY_COMPRESSIONTYPE, CompressionType.SNAPPY.name());
        switch (type) {
            case "LZ4" :
                return CompressionType.LZ4;
            case "NONE" :
                return CompressionType.NONE;
            case "ZLIB" :
                return CompressionType.ZLIB;
            case "ZSTD" :
                return CompressionType.ZSTD;
            case "SNAPPY" :
                return CompressionType.SNAPPY;
            default :
                return CompressionType.NONE;
        }
    }

    /**
     * stop
     */
    @Override
    public void stop() {
        super.state = LifecycleState.STOP;
        //
        for (Entry<String, Producer<byte[]>> entry : this.producerMap.entrySet()) {
            try {
                entry.getValue().close();
            } catch (PulsarClientException e) {
                LOG.error(e.getMessage(), e);
            }
        }
        try {
            this.client.close();
        } catch (PulsarClientException e) {
            LOG.error(e.getMessage(), e);
        }
    }

    /**
     * send DispatchProfile
     * 
     * @param event DispatchProfile
     * @return boolean sendResult
     */
    @Override
    public boolean send(DispatchProfile event) {
        try {
            // topic
            String producerTopic = this.getProducerTopic(event);
            if (producerTopic == null) {
                sinkContext.addSendResultMetric(event, event.getUid(), false, 0);
                event.fail();
                return false;
            }
            // get producer
            Producer<byte[]> producer = this.producerMap.get(producerTopic);
            if (producer == null) {
                try {
                    LOG.info("try to new a object for topic " + producerTopic);
                    SecureRandom secureRandom = new SecureRandom(
                            (workerName + "-" + cacheClusterName + "-" + producerTopic + System.currentTimeMillis())
                                    .getBytes());
                    String producerName = workerName + "-" + cacheClusterName + "-" + producerTopic + "-"
                            + secureRandom.nextLong();
                    producer = baseBuilder.clone().topic(producerTopic)
                            .producerName(producerName)
                            .create();
                    LOG.info("create new producer success:{}", producer.getProducerName());
                    Producer<byte[]> oldProducer = this.producerMap.putIfAbsent(producerTopic, producer);
                    if (oldProducer != null) {
                        producer.close();
                        LOG.info("close producer success:{}", producer.getProducerName());
                        producer = oldProducer;
                    }
                } catch (Throwable ex) {
                    LOG.error("create new producer failed", ex);
                }
            }
            // create producer failed
            if (producer == null) {
                sinkContext.processSendFail(event, producerTopic, 0);
                return false;
            }
            // headers
            Map<String, String> headers = this.encodeCacheMessageHeaders(event);
            // compress
            byte[] bodyBytes = EventUtils.encodeCacheMessageBody(sinkContext.getCompressType(), event.getEvents());
            // sendAsync
            long sendTime = System.currentTimeMillis();
            CompletableFuture<MessageId> future = producer.newMessage().properties(headers)
                    .value(bodyBytes).sendAsync();
            // callback
            future.whenCompleteAsync((msgId, ex) -> {
                if (ex != null) {
                    LOG.error("Send fail:{}", ex.getMessage());
                    LOG.error(ex.getMessage(), ex);
                    sinkContext.processSendFail(event, producerTopic, sendTime);
                } else {
                    sinkContext.addSendResultMetric(event, producerTopic, true, sendTime);
                    event.ack();
                }
            });
            return true;
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            sinkContext.processSendFail(event, event.getUid(), 0);
            return false;
        }
    }

    /**
     * getProducerTopic
     * 
     * @param event DispatchProfile
     * @return String Full topic name
     */
    private String getProducerTopic(DispatchProfile event) {
        String baseTopic = sinkContext.getIdTopicHolder().getTopic(event.getUid());
        if (baseTopic == null) {
            return null;
        }
        StringBuilder builder = new StringBuilder();
        if (tenant != null) {
            builder.append(tenant).append("/");
        }
        if (namespace != null) {
            builder.append(namespace).append("/");
        }
        builder.append(baseTopic);
        return builder.toString();
    }
}
