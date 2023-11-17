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

package org.apache.inlong.agent.plugin.sinks.filecollect;

import org.apache.inlong.agent.common.AgentThreadFactory;
import org.apache.inlong.agent.conf.InstanceProfile;
import org.apache.inlong.agent.constant.CommonConstants;
import org.apache.inlong.agent.core.task.file.MemoryManager;
import org.apache.inlong.agent.message.EndMessage;
import org.apache.inlong.agent.message.ProxyMessage;
import org.apache.inlong.agent.message.filecollect.SenderMessage;
import org.apache.inlong.agent.plugin.Message;
import org.apache.inlong.agent.plugin.MessageFilter;
import org.apache.inlong.agent.utils.AgentUtils;
import org.apache.inlong.agent.utils.ThreadUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.apache.inlong.agent.constant.CommonConstants.DEFAULT_FIELD_SPLITTER;
import static org.apache.inlong.agent.constant.FetcherConstants.AGENT_GLOBAL_WRITER_PERMIT;

/**
 * sink message data to inlong-dataproxy
 */
public class ProxySink extends AbstractSink {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProxySink.class);
    private final int WRITE_FAILED_WAIT_TIME_MS = 10;
    private final int DESTROY_LOOP_WAIT_TIME_MS = 10;
    private final Integer NO_WRITE_WAIT_AT_LEAST_MS = 5 * 1000;
    private final Integer SINK_FINISH_AT_LEAST_COUNT = 5;
    private static final ThreadPoolExecutor EXECUTOR_SERVICE = new ThreadPoolExecutor(
            0, Integer.MAX_VALUE,
            1L, TimeUnit.SECONDS,
            new SynchronousQueue<>(),
            new AgentThreadFactory("proxy-sink"));
    private MessageFilter messageFilter;
    private SenderManager senderManager;
    private byte[] fieldSplitter;
    private volatile boolean shutdown = false;
    private volatile boolean running = false;
    private volatile boolean inited = false;
    private volatile long lastWriteTime = 0;
    private volatile long checkSinkFinishCount = 0;

    public ProxySink() {
    }

    @Override
    public void write(Message message) {
        boolean suc = false;
        while (!shutdown && !suc) {
            lastWriteTime = AgentUtils.getCurrentTime();
            suc = putInCache(message);
            if (!suc) {
                AgentUtils.silenceSleepInMs(WRITE_FAILED_WAIT_TIME_MS);
            }
        }
    }

    private boolean putInCache(Message message) {
        try {
            if (message == null) {
                return true;
            }
            message.getHeader().put(CommonConstants.PROXY_KEY_GROUP_ID, inlongGroupId);
            message.getHeader().put(CommonConstants.PROXY_KEY_STREAM_ID, inlongStreamId);
            extractStreamFromMessage(message, fieldSplitter);
            if (message instanceof EndMessage) {
                // increment the count of failed sinks
                sinkMetric.sinkFailCount.incrementAndGet();
                return true;
            }
            ProxyMessage proxyMessage = new ProxyMessage(message);
            boolean writerPermitSuc = MemoryManager.getInstance()
                    .tryAcquire(AGENT_GLOBAL_WRITER_PERMIT, message.getBody().length);
            if (!writerPermitSuc) {
                MemoryManager.getInstance().printDetail(AGENT_GLOBAL_WRITER_PERMIT, "proxy sink");
                return false;
            }
            cache.generateExtraMap(proxyMessage.getDataKey());
            // add message to package proxy
            boolean suc = cache.addProxyMessage(proxyMessage);
            if (!suc) {
                MemoryManager.getInstance().release(AGENT_GLOBAL_WRITER_PERMIT, message.getBody().length);
                // increment the count of failed sinks
                sinkMetric.sinkFailCount.incrementAndGet();
            }
            return suc;
        } catch (Exception e) {
            LOGGER.error("write message to Proxy sink error", e);
        } catch (Throwable t) {
            ThreadUtils.threadThrowableHandler(Thread.currentThread(), t);
        }
        return false;
    }

    /**
     * extract stream id from message if message filter is presented
     */
    private void extractStreamFromMessage(Message message, byte[] fieldSplitter) {
        if (messageFilter != null) {
            message.getHeader().put(CommonConstants.PROXY_KEY_STREAM_ID,
                    messageFilter.filterStreamId(message, fieldSplitter));
        } else {
            message.getHeader().put(CommonConstants.PROXY_KEY_STREAM_ID, inlongStreamId);
        }
    }

    /**
     * flush cache by batch
     *
     * @return thread runner
     */
    private Runnable coreThread() {
        return () -> {
            AgentThreadFactory.nameThread(
                    "flushCache-" + profile.getTaskId() + "-" + profile.getInstanceId());
            LOGGER.info("start flush cache {}:{}", inlongGroupId, sourceName);
            running = true;
            long lastPrintTime = AgentUtils.getCurrentTime();
            while (!shutdown) {
                try {
                    SenderMessage senderMessage = cache.fetchSenderMessage();
                    if (senderMessage != null) {
                        checkSinkFinishCount = 0;
                        senderManager.sendBatch(senderMessage);
                        if (AgentUtils.getCurrentTime() - lastPrintTime > TimeUnit.SECONDS.toMillis(1)) {
                            lastPrintTime = AgentUtils.getCurrentTime();
                            LOGGER.info("send groupId {}, streamId {}, message size {}, taskId {}, "
                                    + "instanceId {} sendTime is {}", inlongGroupId, inlongStreamId,
                                    senderMessage.getDataList().size(), profile.getTaskId(),
                                    profile.getInstanceId(),
                                    senderMessage.getDataTime());
                        }
                    }
                    if (noWriteLongEnough() && senderManager.sendFinished()) {
                        checkSinkFinishCount++;
                    } else {
                        checkSinkFinishCount = 0;
                    }
                } catch (Exception ex) {
                    LOGGER.error("error caught", ex);
                } catch (Throwable t) {
                    ThreadUtils.threadThrowableHandler(Thread.currentThread(), t);
                } finally {
                    AgentUtils.silenceSleepInMs(batchFlushInterval);
                }
            }
            LOGGER.info("stop flush cache {}:{}", inlongGroupId, sourceName);
            running = false;
        };
    }

    @Override
    public void init(InstanceProfile profile) {
        super.init(profile);
        fieldSplitter = profile.get(CommonConstants.FIELD_SPLITTER, DEFAULT_FIELD_SPLITTER).getBytes(
                StandardCharsets.UTF_8);
        sourceName = profile.getInstanceId();
        senderManager = new SenderManager(profile, inlongGroupId, sourceName);
        try {
            senderManager.Start();
            EXECUTOR_SERVICE.execute(coreThread());
            inited = true;
        } catch (Throwable ex) {
            shutdown = true;
            LOGGER.error("error while init sender for group id {}", inlongGroupId);
            ThreadUtils.threadThrowableHandler(Thread.currentThread(), ex);
            throw new IllegalStateException(ex);
        }
    }

    @Override
    public void destroy() {
        LOGGER.info("destroy sink {}", sourceName);
        if (!inited) {
            return;
        }
        shutdown = true;
        while (running) {
            AgentUtils.silenceSleepInMs(DESTROY_LOOP_WAIT_TIME_MS);
        }
        MemoryManager.getInstance().release(AGENT_GLOBAL_WRITER_PERMIT, (int) cache.getCacheSize());
        senderManager.Stop();
        LOGGER.info("destroy sink {} end", sourceName);
    }

    /**
     * check whether all stream id messages finished
     */
    @Override
    public boolean sinkFinish() {
        if (noWriteLongEnough() && sinkFinishLongEnough()) {
            return true;
        } else {
            return false;
        }
    }

    public boolean noWriteLongEnough() {
        return AgentUtils.getCurrentTime() - lastWriteTime > NO_WRITE_WAIT_AT_LEAST_MS;
    }

    public boolean sinkFinishLongEnough() {
        return checkSinkFinishCount > SINK_FINISH_AT_LEAST_COUNT;
    }
}
