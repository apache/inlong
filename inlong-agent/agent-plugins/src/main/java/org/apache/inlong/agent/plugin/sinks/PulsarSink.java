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

package org.apache.inlong.agent.plugin.sinks;

import static org.apache.inlong.agent.constants.CommonConstants.DEFAULT_PULSAR_PRODUCER_ASYNC;
import static org.apache.inlong.agent.constants.CommonConstants.DEFAULT_PULSAR_PRODUCER_BATCH_MAXCOUNT;
import static org.apache.inlong.agent.constants.CommonConstants.DEFAULT_PULSAR_PRODUCER_BATCH_MAXSIZE;
import static org.apache.inlong.agent.constants.CommonConstants.DEFAULT_PULSAR_PRODUCER_BLOCK_QUEUE;
import static org.apache.inlong.agent.constants.CommonConstants.DEFAULT_PULSAR_PRODUCER_COMPRESS_TYPE;
import static org.apache.inlong.agent.constants.CommonConstants.DEFAULT_PULSAR_PRODUCER_ENABLE_BATCH;
import static org.apache.inlong.agent.constants.CommonConstants.DEFAULT_PULSAR_PRODUCER_MAX_PENDING_COUNT;
import static org.apache.inlong.agent.constants.CommonConstants.DEFAULT_PULSAR_PRODUCER_THREAD_NUM;
import static org.apache.inlong.agent.constants.CommonConstants.DEFAULT_PULSAR_SINK_CACHE_CAPACITY;
import static org.apache.inlong.agent.constants.CommonConstants.DEFAULT_PULSAR_SINK_POLL_TIMEOUT;
import static org.apache.inlong.agent.constants.CommonConstants.PULSAR_PRODUCER_ASYNC;
import static org.apache.inlong.agent.constants.CommonConstants.PULSAR_PRODUCER_BATCH_MAXCOUNT;
import static org.apache.inlong.agent.constants.CommonConstants.PULSAR_PRODUCER_BATCH_MAXSIZE;
import static org.apache.inlong.agent.constants.CommonConstants.PULSAR_PRODUCER_BLOCK_QUEUE;
import static org.apache.inlong.agent.constants.CommonConstants.PULSAR_PRODUCER_COMPRESS_TYPE;
import static org.apache.inlong.agent.constants.CommonConstants.PULSAR_PRODUCER_ENABLE_BATCH;
import static org.apache.inlong.agent.constants.CommonConstants.PULSAR_PRODUCER_MAX_PENDING_COUNT;
import static org.apache.inlong.agent.constants.CommonConstants.PULSAR_PRODUCER_THREAD_NUM;
import static org.apache.inlong.agent.constants.CommonConstants.PULSAR_SERVERS;
import static org.apache.inlong.agent.constants.CommonConstants.PULSAR_SINK_CACHE_CAPACITY;
import static org.apache.inlong.agent.constants.CommonConstants.PULSAR_SINK_POLL_TIMEOUT;
import static org.apache.inlong.agent.constants.CommonConstants.PULSAR_TOPIC;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.inlong.agent.common.AbstractDaemon;
import org.apache.inlong.agent.conf.JobProfile;
import org.apache.inlong.agent.plugin.Message;
import org.apache.inlong.agent.plugin.Sink;
import org.apache.inlong.agent.plugin.metrics.PluginMetric;
import org.apache.inlong.agent.plugin.utils.PluginUtils;
import org.apache.inlong.agent.utils.AgentUtils;
import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PulsarSink extends AbstractDaemon implements Sink {
    private static final Logger LOGGER = LoggerFactory.getLogger(PulsarSink.class);
    private boolean async;
    private long pollTimeout;
    private int threadNum;
    private volatile boolean running = true;
    private volatile boolean writing = true;
    private JobProfile profile;
    private LinkedBlockingQueue<byte[]> cache;
    private final List<Producer<byte[]>> producerList = new ArrayList<>();

    private final PluginMetric metric = new PluginMetric();
    private PulsarClient client;

    @Override
    public void write(Message message) {
        if (message != null && writing) {
            // if message is not null
            try {
                // put message to cache, wait until cache is not full.
                metric.sendNum.incr();
                cache.put(message.getBody());
            } catch (Exception ignored) {
                // ignore it
            }
        }
    }

    @Override
    public void setSourceFile(String sourceFileName) {

    }

    @Override
    public void init(JobProfile jobConf) {

        threadNum = jobConf.getInt(PULSAR_PRODUCER_THREAD_NUM, DEFAULT_PULSAR_PRODUCER_THREAD_NUM);
        async = jobConf.getBoolean(PULSAR_PRODUCER_ASYNC, DEFAULT_PULSAR_PRODUCER_ASYNC);
        pollTimeout = jobConf.getLong(PULSAR_SINK_POLL_TIMEOUT, DEFAULT_PULSAR_SINK_POLL_TIMEOUT);
        int capacity = jobConf.getInt(PULSAR_SINK_CACHE_CAPACITY, DEFAULT_PULSAR_SINK_CACHE_CAPACITY);
        profile = jobConf;
        cache = new LinkedBlockingQueue<>(capacity);
        start();
    }

    @Override
    public void destroy() {
        try {
            stop();
            LOGGER.info("send success num is {}, failed num is {}",
                metric.sendSuccessNum.snapshot(), metric.sendFailedNum.snapshot());
        } catch (Exception ex) {
            LOGGER.error("exception caught", ex);
        }
    }

    /**
     * sending data with producer.
     * @param item
     * @param producer
     * @throws PulsarClientException
     */
    private void sendingData(final byte[] item, Producer<byte[]> producer) throws PulsarClientException {
        // sending data async
        if (async) {
            CompletableFuture<MessageId> future = producer.sendAsync(item);
            future.whenCompleteAsync((m, t) -> {
                // exception is not null, that means not success.
                // TODO: add metric or retry sending message.
                if (t != null) {
                    metric.sendFailedNum.incr();
                    if (!cache.offer(item)) {
                        LOGGER.warn("message {} not add back to retry", m);
                    }
                } else {
                    metric.sendSuccessNum.incr();
                }
            });
        } else {
            producer.send(item);
        }
    }

    /**
     * thread for sending data.
     * @return runnable thread.
     */
    private Runnable sendThread(Producer<byte[]> producer) {
        return () -> {
            while (running) {
                try {
                    byte[] item = cache.poll(pollTimeout, TimeUnit.MILLISECONDS);
                    if (item != null) {
                      // sending to pulsar
                        sendingData(item, producer);
                    }
                } catch (Exception ex) {
                    LOGGER.warn("exception caught while sending data", ex);
                }
            }
        };
    }

    /**
     * construct producer for every thread.
     * @return
     */
    private Producer<byte[]> constructProducer() {
        if (profile == null) {
            return null;
        }
        try {
            String pulsarServers = profile.get(PULSAR_SERVERS);
            String pulsarTopic = profile.get(PULSAR_TOPIC);
            int pendingNum = profile.getInt(PULSAR_PRODUCER_MAX_PENDING_COUNT,
                DEFAULT_PULSAR_PRODUCER_MAX_PENDING_COUNT);
            int batchSize = profile.getInt(PULSAR_PRODUCER_BATCH_MAXSIZE,
                DEFAULT_PULSAR_PRODUCER_BATCH_MAXSIZE);
            int batchCount = profile.getInt(PULSAR_PRODUCER_BATCH_MAXCOUNT,
                DEFAULT_PULSAR_PRODUCER_BATCH_MAXCOUNT);
            boolean enableBatch = profile.getBoolean(PULSAR_PRODUCER_ENABLE_BATCH,
                DEFAULT_PULSAR_PRODUCER_ENABLE_BATCH);
            boolean blockQueue = profile.getBoolean(PULSAR_PRODUCER_BLOCK_QUEUE, DEFAULT_PULSAR_PRODUCER_BLOCK_QUEUE);
            CompressionType compressionType = PluginUtils.convertType(
                profile.get(PULSAR_PRODUCER_COMPRESS_TYPE,
                DEFAULT_PULSAR_PRODUCER_COMPRESS_TYPE));
            LOGGER.info("init producer, pulsarServers: {}, topic: {}, pendingNum: {}, batchSize: {}, "
                + "batchCount: {}, enableBatch: {}, compressType: {}, blockQueue: {}", pulsarServers, pulsarTopic,
                pendingNum, batchSize, batchCount, enableBatch, compressionType, blockQueue);
            client = PulsarClient.builder()
                .serviceUrl(pulsarServers).build();
            return client.newProducer().topic(pulsarTopic)
                .compressionType(compressionType)
                .batchingMaxBytes(batchSize)
                .batchingMaxMessages(batchCount)
                .blockIfQueueFull(blockQueue)
                .maxPendingMessages(pendingNum)
                .enableBatching(enableBatch).create();
        } catch (Exception exception) {
            LOGGER.error("error while init producer", exception);
            throw new RuntimeException(exception);
        }
    }

    @Override
    public void start() {
        for (int i = 0; i < threadNum; i++) {
            Producer<byte[]> producer = constructProducer();
            if (producer != null) {
                producerList.add(producer);
                submitWorker(sendThread(producer));
            } else {
                LOGGER.warn("producer is null, please check profile");
            }
        }
    }

    @Override
    public void stop() throws Exception {
        writing = false;
        // TODO: wating for cache to clear
        while (cache.size() > 0) {
            AgentUtils.silenceSleepInMs(pollTimeout);
        }
        for (Producer<byte[]> producer : producerList) {
            producer.flush();
            producer.close();
        }
        if (client != null) {
            client.close();
        }
        producerList.clear();
        running = false;
    }
}
