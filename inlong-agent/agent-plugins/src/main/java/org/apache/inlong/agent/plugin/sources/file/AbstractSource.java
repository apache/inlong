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

package org.apache.inlong.agent.plugin.sources.file;

import org.apache.inlong.agent.common.AgentThreadFactory;
import org.apache.inlong.agent.conf.InstanceProfile;
import org.apache.inlong.agent.conf.OffsetProfile;
import org.apache.inlong.agent.constant.CycleUnitType;
import org.apache.inlong.agent.core.task.MemoryManager;
import org.apache.inlong.agent.core.task.OffsetManager;
import org.apache.inlong.agent.message.DefaultMessage;
import org.apache.inlong.agent.metrics.AgentMetricItem;
import org.apache.inlong.agent.metrics.AgentMetricItemSet;
import org.apache.inlong.agent.metrics.audit.AuditUtils;
import org.apache.inlong.agent.plugin.Message;
import org.apache.inlong.agent.plugin.file.Source;
import org.apache.inlong.agent.plugin.sources.extend.ExtendedHandler;
import org.apache.inlong.agent.utils.AgentUtils;
import org.apache.inlong.agent.utils.ThreadUtils;
import org.apache.inlong.common.metric.MetricRegister;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
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
import static org.apache.inlong.agent.constant.CommonConstants.PROXY_KEY_STREAM_ID;
import static org.apache.inlong.agent.constant.CommonConstants.PROXY_PACKAGE_MAX_SIZE;
import static org.apache.inlong.agent.constant.FetcherConstants.AGENT_GLOBAL_READER_QUEUE_PERMIT;
import static org.apache.inlong.agent.constant.FetcherConstants.AGENT_GLOBAL_READER_SOURCE_PERMIT;
import static org.apache.inlong.agent.constant.TaskConstants.OFFSET;
import static org.apache.inlong.agent.constant.TaskConstants.TASK_AUDIT_VERSION;
import static org.apache.inlong.agent.constant.TaskConstants.TASK_CYCLE_UNIT;
import static org.apache.inlong.agent.metrics.AgentMetricItem.KEY_INLONG_GROUP_ID;
import static org.apache.inlong.agent.metrics.AgentMetricItem.KEY_INLONG_STREAM_ID;
import static org.apache.inlong.agent.metrics.AgentMetricItem.KEY_PLUGIN_ID;

public abstract class AbstractSource implements Source {

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    protected class SourceData {

        private byte[] data;
        private String offset;
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractSource.class);

    protected final Integer BATCH_READ_LINE_COUNT = 10000;
    protected final Integer BATCH_READ_LINE_TOTAL_LEN = 1024 * 1024;
    protected final Integer CACHE_QUEUE_SIZE = 10 * BATCH_READ_LINE_COUNT;
    protected final Integer WAIT_TIMEOUT_MS = 10;
    private final Integer EMPTY_CHECK_COUNT_AT_LEAST = 5 * 60 * 100;
    private final Integer CORE_THREAD_PRINT_INTERVAL_MS = 1000;
    protected BlockingQueue<SourceData> queue;

    protected String inlongGroupId;
    protected String inlongStreamId;
    // metric
    protected AgentMetricItemSet metricItemSet;
    protected AgentMetricItem sourceMetric;
    protected String metricName;
    protected Map<String, String> dimensions;
    protected static final AtomicLong METRIX_INDEX = new AtomicLong(0);
    protected volatile boolean runnable = true;
    protected volatile boolean running = false;
    protected String taskId;
    protected long auditVersion;
    protected String instanceId;
    protected InstanceProfile profile;
    protected String extendClass;
    private ExtendedHandler extendedHandler;
    protected boolean isRealTime = false;
    protected volatile long emptyCount = 0;
    protected int maxPackSize;
    private static final ThreadPoolExecutor EXECUTOR_SERVICE = new ThreadPoolExecutor(
            0, Integer.MAX_VALUE,
            1L, TimeUnit.SECONDS,
            new SynchronousQueue<>(),
            new AgentThreadFactory("source-pool"));
    protected OffsetProfile offsetProfile;
    protected boolean sourceError = false;

    @Override
    public void init(InstanceProfile profile) {
        this.profile = profile;
        taskId = profile.getTaskId();
        auditVersion = Long.parseLong(profile.get(TASK_AUDIT_VERSION));
        instanceId = profile.getInstanceId();
        inlongGroupId = profile.getInlongGroupId();
        inlongStreamId = profile.getInlongStreamId();
        maxPackSize = profile.getInt(PROXY_PACKAGE_MAX_SIZE, DEFAULT_PROXY_PACKAGE_MAX_SIZE);
        queue = new LinkedBlockingQueue<>(CACHE_QUEUE_SIZE);
        String cycleUnit = profile.get(TASK_CYCLE_UNIT);
        if (cycleUnit.compareToIgnoreCase(CycleUnitType.REAL_TIME) == 0) {
            isRealTime = true;
        }
        initOffset();
        registerMetric();
        initExtendHandler();
        initSource(profile);
    }

    protected abstract void initExtendClass();

    protected abstract void initSource(InstanceProfile profile);

    protected void initOffset() {
        offsetProfile = OffsetManager.getInstance().getOffset(taskId, instanceId);
    }

    @Override
    public void start() {
        EXECUTOR_SERVICE.execute(run());
    }

    private Runnable run() {
        return () -> {
            AgentThreadFactory.nameThread(getThreadName());
            running = true;
            try {
                doRun();
            } catch (Throwable e) {
                LOGGER.error("do run error maybe file deleted: ", e);
                ThreadUtils.threadThrowableHandler(Thread.currentThread(), e);
            }
            running = false;
        };
    }

    private void doRun() {
        long lastPrintTime = 0;
        while (isRunnable()) {
            if (!prepareToRead()) {
                break;
            }
            List<SourceData> lines = readFromSource();
            if (lines == null || lines.isEmpty()) {
                if (queue.isEmpty()) {
                    emptyCount++;
                } else {
                    emptyCount = 0;
                }
                MemoryManager.getInstance().release(AGENT_GLOBAL_READER_SOURCE_PERMIT, BATCH_READ_LINE_TOTAL_LEN);
                AgentUtils.silenceSleepInMs(WAIT_TIMEOUT_MS);
                continue;
            }
            emptyCount = 0;
            for (int i = 0; i < lines.size(); i++) {
                boolean suc4Queue = waitForPermit(AGENT_GLOBAL_READER_QUEUE_PERMIT, lines.get(i).getData().length);
                if (!suc4Queue) {
                    break;
                }
                putIntoQueue(lines.get(i));
            }
            MemoryManager.getInstance().release(AGENT_GLOBAL_READER_SOURCE_PERMIT, BATCH_READ_LINE_TOTAL_LEN);
            if (AgentUtils.getCurrentTime() - lastPrintTime > CORE_THREAD_PRINT_INTERVAL_MS) {
                lastPrintTime = AgentUtils.getCurrentTime();
                printCurrentState();
            }
        }
    }

    protected abstract void printCurrentState();

    /**
     * Before reading the data source, some preparation operations need to be done, such as memory control semaphore
     * application and data source legitimacy verification
     *
     * @return true if prepared ok
     */
    private boolean prepareToRead() {
        try {
            if (!doPrepareToRead()) {
                return false;
            }
            return waitForPermit(AGENT_GLOBAL_READER_SOURCE_PERMIT, BATCH_READ_LINE_TOTAL_LEN);
        } catch (Throwable e) {
            LOGGER.error("prepare to read {} error:", instanceId, e);
            sourceError = true;
            return false;
        }
    }

    /**
     * Except for applying for memory control semaphores, all other preparatory work is implemented by this function
     *
     * @return true if prepared ok
     */
    protected abstract boolean doPrepareToRead();

    /**
     * After preparation work, we started to truly read data from the data source
     *
     * @return source data list
     */
    protected abstract List<SourceData> readFromSource();

    private boolean waitForPermit(String permitName, int permitLen) {
        boolean suc = false;
        while (!suc) {
            suc = MemoryManager.getInstance().tryAcquire(permitName, permitLen);
            if (!suc) {
                MemoryManager.getInstance().printDetail(permitName, "source");
                if (!isRunnable()) {
                    return false;
                }
                AgentUtils.silenceSleepInMs(WAIT_TIMEOUT_MS);
            }
        }
        return true;
    }

    /**
     * After preparation work, we started to truly read data from the data source
     */
    private void putIntoQueue(SourceData sourceData) {
        if (sourceData == null) {
            return;
        }
        try {
            boolean offerSuc = false;
            while (isRunnable() && !offerSuc) {
                offerSuc = queue.offer(sourceData, WAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            }
            if (!offerSuc) {
                MemoryManager.getInstance().release(AGENT_GLOBAL_READER_QUEUE_PERMIT, sourceData.getData().length);
            }
            LOGGER.debug("Put in source queue {} {}", new String(sourceData.getData()), inlongGroupId);
        } catch (InterruptedException e) {
            MemoryManager.getInstance().release(AGENT_GLOBAL_READER_QUEUE_PERMIT, sourceData.getData().length);
            LOGGER.error("fetchData offer failed", e);
        }
    }

    /**
     * Returns the name of the thread, used to identify different data sources
     *
     * @return source data list
     */
    protected abstract String getThreadName();

    private void registerMetric() {
        // register metric
        this.dimensions = new HashMap<>();
        dimensions.put(KEY_PLUGIN_ID, this.getClass().getSimpleName());
        dimensions.put(KEY_INLONG_GROUP_ID, inlongGroupId);
        dimensions.put(KEY_INLONG_STREAM_ID, inlongStreamId);
        metricName = String.join("-", this.getClass().getSimpleName(),
                String.valueOf(METRIX_INDEX.incrementAndGet()));
        this.metricItemSet = new AgentMetricItemSet(metricName);
        MetricRegister.register(metricItemSet);
        sourceMetric = metricItemSet.findMetricItem(dimensions);
    }

    private void initExtendHandler() {
        initExtendClass();
        Constructor<?> constructor =
                null;
        try {
            constructor = Class.forName(extendClass)
                    .getDeclaredConstructor(InstanceProfile.class);
        } catch (NoSuchMethodException e) {
            LOGGER.error("init {} NoSuchMethodException error", instanceId, e);
        } catch (ClassNotFoundException e) {
            LOGGER.error("init {} ClassNotFoundException error", instanceId, e);
        }
        constructor.setAccessible(true);
        try {
            extendedHandler = (ExtendedHandler) constructor.newInstance(profile);
        } catch (InstantiationException e) {
            LOGGER.error("init {} InstantiationException error", instanceId, e);
        } catch (IllegalAccessException e) {
            LOGGER.error("init {} IllegalAccessException error", instanceId, e);
        } catch (InvocationTargetException e) {
            LOGGER.error("init {} InvocationTargetException error", instanceId, e);
        }
    }

    @Override
    public Message read() {
        SourceData sourceData = readFromQueue();
        while (sourceData != null) {
            Message msg = createMessage(sourceData);
            if (filterSourceData(msg)) {
                long auditTime = 0;
                if (isRealTime) {
                    auditTime = AgentUtils.getCurrentTime();
                } else {
                    auditTime = profile.getSinkDataTime();
                }
                Map<String, String> header = msg.getHeader();
                AuditUtils.add(AuditUtils.AUDIT_ID_AGENT_READ_SUCCESS, inlongGroupId, header.get(PROXY_KEY_STREAM_ID),
                        auditTime, 1, sourceData.getData().length, auditVersion);
                AuditUtils.add(AuditUtils.AUDIT_ID_AGENT_READ_SUCCESS_REAL_TIME, inlongGroupId,
                        header.get(PROXY_KEY_STREAM_ID),
                        AgentUtils.getCurrentTime(), 1, sourceData.getData().length, auditVersion);
                return msg;
            }
            sourceData = readFromQueue();
        }
        return null;
    }

    private boolean filterSourceData(Message msg) {
        if (extendedHandler != null) {
            return extendedHandler.filterMessage(msg);
        }
        return true;
    }

    private SourceData readFromQueue() {
        SourceData sourceData = null;
        try {
            sourceData = queue.poll(WAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            LOGGER.warn("poll {} data get interrupted.", instanceId);
        }
        if (sourceData == null) {
            LOGGER.debug("Read from source queue null {}", inlongGroupId);
            return null;
        }
        LOGGER.debug("Read from source queue {} {}", new String(sourceData.getData()), inlongGroupId);
        MemoryManager.getInstance().release(AGENT_GLOBAL_READER_QUEUE_PERMIT, sourceData.getData().length);
        return sourceData;
    }

    private Message createMessage(SourceData sourceData) {
        Map<String, String> header = new HashMap<>();
        header.put(OFFSET, sourceData.getOffset());
        header.put(PROXY_KEY_STREAM_ID, inlongStreamId);
        if (extendedHandler != null) {
            extendedHandler.dealWithHeader(header, sourceData.getData());
        }
        Message finalMsg = new DefaultMessage(sourceData.getData(), header);
        // if the message size is greater than max pack size,should drop it.
        if (finalMsg.getBody().length > maxPackSize) {
            LOGGER.warn("message size is {}, greater than max pack size {}, drop it!",
                    finalMsg.getBody().length, maxPackSize);
            return null;
        }
        return finalMsg;
    }

    /**
     * Whether threads can in running state with while loop.
     *
     * @return true if threads can run
     */
    protected abstract boolean isRunnable();

    /**
     * Stop running threads.
     */
    public void stopRunning() {
        runnable = false;
    }

    public void destroy() {
        LOGGER.info("destroy read source name {}", instanceId);
        stopRunning();
        while (running) {
            AgentUtils.silenceSleepInMs(1);
        }
        clearQueue(queue);
        releaseSource();
        LOGGER.info("destroy read source name {} end", instanceId);
    }

    /**
     * Release the source resource if needed
     */
    protected abstract void releaseSource();

    private void clearQueue(BlockingQueue<SourceData> queue) {
        if (queue == null) {
            return;
        }
        while (queue != null && !queue.isEmpty()) {
            SourceData sourceData = null;
            try {
                sourceData = queue.poll(WAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                LOGGER.warn("poll {} data get interrupted.", instanceId, e);
            }
            if (sourceData != null) {
                MemoryManager.getInstance().release(AGENT_GLOBAL_READER_QUEUE_PERMIT, sourceData.getData().length);
            }
        }
        queue.clear();
    }

    @Override
    public boolean sourceFinish() {
        if (sourceError) {
            return true;
        }
        if (isRealTime) {
            return false;
        }
        return emptyCount > EMPTY_CHECK_COUNT_AT_LEAST;
    }
}
