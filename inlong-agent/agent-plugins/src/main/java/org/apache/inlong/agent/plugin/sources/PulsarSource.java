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

package org.apache.inlong.agent.plugin.sources;

import org.apache.inlong.agent.common.AgentThreadFactory;
import org.apache.inlong.agent.conf.InstanceProfile;
import org.apache.inlong.agent.conf.TaskProfile;
import org.apache.inlong.agent.constant.CycleUnitType;
import org.apache.inlong.agent.core.task.MemoryManager;
import org.apache.inlong.agent.except.FileException;
import org.apache.inlong.agent.message.DefaultMessage;
import org.apache.inlong.agent.metrics.audit.AuditUtils;
import org.apache.inlong.agent.plugin.Message;
import org.apache.inlong.agent.plugin.file.Reader;
import org.apache.inlong.agent.plugin.sources.file.AbstractSource;
import org.apache.inlong.agent.utils.AgentUtils;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.inlong.agent.constant.CommonConstants.DEFAULT_PROXY_PACKAGE_MAX_SIZE;
import static org.apache.inlong.agent.constant.CommonConstants.PROXY_KEY_DATA;
import static org.apache.inlong.agent.constant.CommonConstants.PROXY_KEY_STREAM_ID;
import static org.apache.inlong.agent.constant.CommonConstants.PROXY_PACKAGE_MAX_SIZE;
import static org.apache.inlong.agent.constant.CommonConstants.PROXY_SEND_PARTITION_KEY;
import static org.apache.inlong.agent.constant.FetcherConstants.AGENT_GLOBAL_READER_QUEUE_PERMIT;
import static org.apache.inlong.agent.constant.FetcherConstants.AGENT_GLOBAL_READER_SOURCE_PERMIT;
import static org.apache.inlong.agent.constant.TaskConstants.OFFSET;
import static org.apache.inlong.agent.constant.TaskConstants.RESTORE_FROM_DB;
import static org.apache.inlong.agent.constant.TaskConstants.TASK_CYCLE_UNIT;
import static org.apache.inlong.agent.constant.TaskConstants.TASK_PULSAR_RESET_TIME;
import static org.apache.inlong.agent.constant.TaskConstants.TASK_PULSAR_SERVICE_URL;
import static org.apache.inlong.agent.constant.TaskConstants.TASK_PULSAR_SUBSCRIPTION;
import static org.apache.inlong.agent.constant.TaskConstants.TASK_PULSAR_SUBSCRIPTION_POSITION;
import static org.apache.inlong.agent.constant.TaskConstants.TASK_PULSAR_SUBSCRIPTION_TYPE;

public class PulsarSource extends AbstractSource {

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    private class SourceData {

        private byte[] data;
        private Long offset;
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(PulsarSource.class);
    private static final ThreadPoolExecutor EXECUTOR_SERVICE = new ThreadPoolExecutor(
            0, Integer.MAX_VALUE,
            1L, TimeUnit.SECONDS,
            new SynchronousQueue<>(),
            new AgentThreadFactory("pulsar-source"));
    private BlockingQueue<SourceData> queue;
    public InstanceProfile profile;
    private int maxPackSize;
    private String inlongStreamId;
    private String taskId;
    private String instanceId;
    private String topic;
    private String serviceUrl;
    private String subscription;
    private String subscriptionType;
    private String subscriptionPosition;
    private PulsarClient pulsarClient;
    private Long timestamp;
    private volatile boolean running = false;
    private volatile boolean runnable = true;
    private volatile AtomicLong emptyCount = new AtomicLong(0);

    private final Integer CACHE_QUEUE_SIZE = 100000;
    private final Integer READ_WAIT_TIMEOUT_MS = 10;
    private final Integer EMPTY_CHECK_COUNT_AT_LEAST = 5 * 60;
    private final Integer BATCH_TOTAL_LEN = 1024 * 1024;
    private final Integer CORE_THREAD_PRINT_INTERVAL_MS = 1000;
    private final static String PULSAR_SUBSCRIPTION_PREFIX = "inlong-agent-";
    private boolean isRealTime = false;
    private boolean isRestoreFromDB = false;

    public PulsarSource() {
    }

    @Override
    public void init(InstanceProfile profile) {
        try {
            LOGGER.info("PulsarSource init: {}", profile.toJsonStr());
            this.profile = profile;
            super.init(profile);
            String cycleUnit = profile.get(TASK_CYCLE_UNIT);
            if (cycleUnit.compareToIgnoreCase(CycleUnitType.REAL_TIME) == 0) {
                isRealTime = true;
                cycleUnit = CycleUnitType.HOUR;
            }
            queue = new LinkedBlockingQueue<>(CACHE_QUEUE_SIZE);
            maxPackSize = profile.getInt(PROXY_PACKAGE_MAX_SIZE, DEFAULT_PROXY_PACKAGE_MAX_SIZE);
            inlongStreamId = profile.getInlongStreamId();
            taskId = profile.getTaskId();
            instanceId = profile.getInstanceId();
            topic = profile.getInstanceId();
            serviceUrl = profile.get(TASK_PULSAR_SERVICE_URL);
            subscription = profile.get(TASK_PULSAR_SUBSCRIPTION, PULSAR_SUBSCRIPTION_PREFIX + inlongStreamId);
            subscriptionPosition = profile.get(TASK_PULSAR_SUBSCRIPTION_POSITION,
                    SubscriptionInitialPosition.Latest.name());
            subscriptionType = profile.get(TASK_PULSAR_SUBSCRIPTION_TYPE, SubscriptionType.Shared.name());
            timestamp = profile.getLong(TASK_PULSAR_RESET_TIME, 0);
            pulsarClient = PulsarClient.builder().serviceUrl(serviceUrl).build();
            isRestoreFromDB = profile.getBoolean(RESTORE_FROM_DB, false);

            EXECUTOR_SERVICE.execute(run());
        } catch (Exception ex) {
            stopRunning();
            throw new FileException("error init stream for " + topic, ex);
        }
    }

    private Runnable run() {
        return () -> {
            AgentThreadFactory.nameThread("pulsar-source-" + taskId + "-" + instanceId);
            running = true;
            try {
                try (Consumer<byte[]> consumer = pulsarClient.newConsumer(Schema.BYTES)
                        .topic(topic)
                        .subscriptionName(subscription)
                        .subscriptionInitialPosition(SubscriptionInitialPosition.valueOf(subscriptionPosition))
                        .subscriptionType(SubscriptionType.valueOf(subscriptionType))
                        .subscribe()) {

                    if (!isRestoreFromDB && timestamp != 0L) {
                        consumer.seek(timestamp);
                        LOGGER.info("Reset consume from {}", timestamp);
                    } else {
                        LOGGER.info("Skip to reset consume");
                    }

                    doRun(consumer);
                }
            } catch (Throwable e) {
                LOGGER.error("do run error maybe pulsar client is configured incorrectly: ", e);
            }
            running = false;
        };
    }

    private void doRun(Consumer<byte[]> consumer) throws PulsarClientException {
        long lastPrintTime = 0;
        while (isRunnable()) {
            boolean suc = waitForPermit(AGENT_GLOBAL_READER_SOURCE_PERMIT, BATCH_TOTAL_LEN);
            if (!suc) {
                break;
            }
            org.apache.pulsar.client.api.Message<byte[]> message = consumer.receive(0, TimeUnit.MILLISECONDS);
            if (ObjectUtils.isEmpty(message)) {
                if (queue.isEmpty()) {
                    emptyCount.incrementAndGet();
                } else {
                    emptyCount.set(0);
                }
                MemoryManager.getInstance().release(AGENT_GLOBAL_READER_SOURCE_PERMIT, BATCH_TOTAL_LEN);
                AgentUtils.silenceSleepInSeconds(1);
                continue;
            }
            emptyCount.set(0);
            long offset = 0L;
            SourceData sourceData = new SourceData(message.getValue(), 0L);
            boolean suc4Queue = waitForPermit(AGENT_GLOBAL_READER_QUEUE_PERMIT, message.getValue().length);
            if (!suc4Queue) {
                break;
            }
            putIntoQueue(sourceData);
            MemoryManager.getInstance().release(AGENT_GLOBAL_READER_SOURCE_PERMIT, BATCH_TOTAL_LEN);
            consumer.acknowledge(message);

            if (AgentUtils.getCurrentTime() - lastPrintTime > CORE_THREAD_PRINT_INTERVAL_MS) {
                lastPrintTime = AgentUtils.getCurrentTime();
                LOGGER.info("pulsar topic is {}, offset is {}", topic, offset);
            }
        }
    }

    public boolean isRunnable() {
        return runnable;
    }

    private boolean waitForPermit(String permitName, int permitLen) {
        boolean suc = false;
        while (!suc) {
            suc = MemoryManager.getInstance().tryAcquire(permitName, permitLen);
            if (!suc) {
                MemoryManager.getInstance().printDetail(permitName, "log file source");
                if (!isRunnable()) {
                    return false;
                }
                AgentUtils.silenceSleepInSeconds(1);
            }
        }
        return true;
    }

    private void putIntoQueue(SourceData sourceData) {
        if (sourceData == null) {
            return;
        }
        try {
            boolean offerSuc = false;
            if (queue.remainingCapacity() > 0) {
                while (isRunnable() && !offerSuc) {
                    offerSuc = queue.offer(sourceData, 1, TimeUnit.SECONDS);
                }
            }

            if (!offerSuc) {
                MemoryManager.getInstance().release(AGENT_GLOBAL_READER_QUEUE_PERMIT, sourceData.data.length);
            }
            LOGGER.debug("Read {} from pulsar topic {}", sourceData.getData(), topic);
        } catch (InterruptedException e) {
            MemoryManager.getInstance().release(AGENT_GLOBAL_READER_QUEUE_PERMIT, sourceData.data.length);
            LOGGER.error("fetchData offer failed {}", e.getMessage());
        }
    }

    @Override
    public List<Reader> split(TaskProfile conf) {
        return null;
    }

    @Override
    public Message read() {
        SourceData sourceData = null;
        try {
            sourceData = queue.poll(READ_WAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            LOGGER.warn("poll {} data get interrupted.", topic, e);
        }
        if (sourceData == null) {
            return null;
        }
        MemoryManager.getInstance().release(AGENT_GLOBAL_READER_QUEUE_PERMIT, sourceData.data.length);
        Message finalMsg = createMessage(sourceData);
        return finalMsg;
    }

    private Message createMessage(SourceData sourceData) {
        String proxyPartitionKey = profile.get(PROXY_SEND_PARTITION_KEY, DigestUtils.md5Hex(inlongGroupId));
        Map<String, String> header = new HashMap<>();
        header.put(PROXY_KEY_DATA, proxyPartitionKey);
        header.put(OFFSET, sourceData.offset.toString());
        header.put(PROXY_KEY_STREAM_ID, inlongStreamId);

        long auditTime;
        if (isRealTime) {
            auditTime = AgentUtils.getCurrentTime();
        } else {
            auditTime = profile.getSinkDataTime();
        }
        AuditUtils.add(AuditUtils.AUDIT_ID_AGENT_READ_SUCCESS, inlongGroupId, header.get(PROXY_KEY_STREAM_ID),
                auditTime, 1, sourceData.data.length);
        AuditUtils.add(AuditUtils.AUDIT_ID_AGENT_READ_SUCCESS_REAL_TIME, inlongGroupId, header.get(PROXY_KEY_STREAM_ID),
                AgentUtils.getCurrentTime(), 1, sourceData.data.length);
        Message finalMsg = new DefaultMessage(sourceData.data, header);
        if (finalMsg.getBody().length > maxPackSize) {
            LOGGER.warn("message size is {}, greater than max pack size {}, drop it!",
                    finalMsg.getBody().length, maxPackSize);
            return null;
        }
        return finalMsg;
    }

    @Override
    public boolean sourceFinish() {
        if (isRealTime) {
            return false;
        }
        return emptyCount.get() > EMPTY_CHECK_COUNT_AT_LEAST;
    }

    @Override
    public boolean sourceExist() {
        return true;
    }

    /**
     * Stop running threads.
     */
    public void stopRunning() {
        runnable = false;
    }
}
