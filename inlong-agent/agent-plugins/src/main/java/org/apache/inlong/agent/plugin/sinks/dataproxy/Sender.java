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

package org.apache.inlong.agent.plugin.sinks.dataproxy;

import org.apache.inlong.agent.common.AgentThreadFactory;
import org.apache.inlong.agent.conf.AgentConfiguration;
import org.apache.inlong.agent.conf.InstanceProfile;
import org.apache.inlong.agent.constant.CommonConstants;
import org.apache.inlong.agent.core.AgentStatusManager;
import org.apache.inlong.agent.message.file.SenderMessage;
import org.apache.inlong.agent.metrics.AgentMetricItem;
import org.apache.inlong.agent.metrics.AgentMetricItemSet;
import org.apache.inlong.agent.metrics.audit.AuditUtils;
import org.apache.inlong.agent.plugin.message.SequentialID;
import org.apache.inlong.agent.utils.AgentUtils;
import org.apache.inlong.agent.utils.ThreadUtils;
import org.apache.inlong.common.metric.MetricRegister;
import org.apache.inlong.common.msg.MsgType;
import org.apache.inlong.sdk.dataproxy.common.ProcessResult;
import org.apache.inlong.sdk.dataproxy.exception.ProxySdkException;
import org.apache.inlong.sdk.dataproxy.sender.MsgSendCallback;
import org.apache.inlong.sdk.dataproxy.sender.tcp.InLongTcpMsgSender;
import org.apache.inlong.sdk.dataproxy.sender.tcp.TcpEventInfo;
import org.apache.inlong.sdk.dataproxy.sender.tcp.TcpMsgSender;
import org.apache.inlong.sdk.dataproxy.sender.tcp.TcpMsgSenderConfig;

import io.netty.util.concurrent.DefaultThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.inlong.agent.constant.CommonConstants.DEFAULT_PROXY_BATCH_FLUSH_INTERVAL;
import static org.apache.inlong.agent.constant.CommonConstants.PROXY_BATCH_FLUSH_INTERVAL;
import static org.apache.inlong.agent.constant.FetcherConstants.AGENT_MANAGER_ADDR;
import static org.apache.inlong.agent.constant.FetcherConstants.AGENT_MANAGER_AUTH_SECRET_ID;
import static org.apache.inlong.agent.constant.FetcherConstants.AGENT_MANAGER_AUTH_SECRET_KEY;
import static org.apache.inlong.agent.constant.TaskConstants.DEFAULT_TASK_PROXY_SEND;
import static org.apache.inlong.agent.constant.TaskConstants.TASK_AUDIT_VERSION;
import static org.apache.inlong.agent.constant.TaskConstants.TASK_PROXY_SEND;
import static org.apache.inlong.agent.metrics.AgentMetricItem.KEY_INLONG_GROUP_ID;
import static org.apache.inlong.agent.metrics.AgentMetricItem.KEY_INLONG_STREAM_ID;
import static org.apache.inlong.agent.metrics.AgentMetricItem.KEY_PLUGIN_ID;

/**
 * proxy client
 */
public class Sender {

    private static final Logger LOGGER = LoggerFactory.getLogger(Sender.class);
    private static final SequentialID SEQUENTIAL_ID = SequentialID.getInstance();
    public static final int RESEND_QUEUE_WAIT_MS = 10;
    // cache for group and sender list, share the map cross agent lifecycle.
    private List<TcpMsgSender> senders = new ArrayList<>();
    private AtomicLong senderIndex = new AtomicLong(0);
    private LinkedBlockingQueue<AgentSenderCallback> resendQueue;
    private static final ThreadPoolExecutor EXECUTOR_SERVICE = new ThreadPoolExecutor(
            0, Integer.MAX_VALUE,
            1L, TimeUnit.SECONDS,
            new SynchronousQueue<>(),
            new AgentThreadFactory("sender-manager"));
    // sharing worker threads between sender client
    // in case of thread abusing.
    private ThreadFactory SHARED_FACTORY;
    private static final AtomicLong METRIC_INDEX = new AtomicLong(0);
    private final String managerAddr;
    private final int totalAsyncBufSize;
    private final int aliveConnectionNum;
    private final boolean isCompress;
    private final int msgType;
    private final long maxSenderTimeout;
    private final int maxSenderRetry;
    private final long retrySleepTime;
    private final String inlongGroupId;
    private final int maxSenderPerGroup;
    private final String sourcePath;
    private final boolean proxySend;
    private volatile boolean shutdown = false;
    // metric
    private AgentMetricItemSet metricItemSet;
    private Map<String, String> dimensions;
    private int ioThreadNum;
    private boolean enableBusyWait;
    private String authSecretId;
    private String authSecretKey;
    protected int batchFlushInterval;
    protected InstanceProfile profile;
    private volatile boolean resendRunning = false;
    private volatile boolean started = false;
    private static final AgentConfiguration agentConf = AgentConfiguration.getAgentConf();
    private long auditVersion;

    public Sender(InstanceProfile profile, String inlongGroupId, String sourcePath) {
        this.profile = profile;
        auditVersion = Long.parseLong(profile.get(TASK_AUDIT_VERSION));
        managerAddr = agentConf.get(AGENT_MANAGER_ADDR);
        proxySend = profile.getBoolean(TASK_PROXY_SEND, DEFAULT_TASK_PROXY_SEND);
        totalAsyncBufSize = agentConf.getInt(
                CommonConstants.PROXY_TOTAL_ASYNC_PROXY_SIZE,
                CommonConstants.DEFAULT_PROXY_TOTAL_ASYNC_PROXY_SIZE_KB);
        aliveConnectionNum = agentConf.getInt(
                CommonConstants.PROXY_ALIVE_CONNECTION_NUM, CommonConstants.DEFAULT_PROXY_ALIVE_CONNECTION_NUM);
        isCompress = agentConf.getBoolean(
                CommonConstants.PROXY_IS_COMPRESS, CommonConstants.DEFAULT_PROXY_IS_COMPRESS);
        maxSenderPerGroup = agentConf.getInt(
                CommonConstants.PROXY_MAX_SENDER_PER_GROUP, CommonConstants.DEFAULT_PROXY_MAX_SENDER_PER_GROUP);
        msgType = agentConf.getInt(CommonConstants.PROXY_MSG_TYPE, CommonConstants.DEFAULT_PROXY_MSG_TYPE);
        maxSenderTimeout = agentConf.getInt(
                CommonConstants.PROXY_SENDER_MAX_TIMEOUT, CommonConstants.DEFAULT_PROXY_SENDER_MAX_TIMEOUT);
        maxSenderRetry = agentConf.getInt(
                CommonConstants.PROXY_SENDER_MAX_RETRY, CommonConstants.DEFAULT_PROXY_SENDER_MAX_RETRY);
        retrySleepTime = agentConf.getLong(
                CommonConstants.PROXY_RETRY_SLEEP, CommonConstants.DEFAULT_PROXY_RETRY_SLEEP);
        ioThreadNum = agentConf.getInt(CommonConstants.PROXY_CLIENT_IO_THREAD_NUM,
                CommonConstants.DEFAULT_PROXY_CLIENT_IO_THREAD_NUM);
        enableBusyWait = agentConf.getBoolean(CommonConstants.PROXY_CLIENT_ENABLE_BUSY_WAIT,
                CommonConstants.DEFAULT_PROXY_CLIENT_ENABLE_BUSY_WAIT);
        batchFlushInterval = agentConf.getInt(PROXY_BATCH_FLUSH_INTERVAL, DEFAULT_PROXY_BATCH_FLUSH_INTERVAL);
        authSecretId = agentConf.get(AGENT_MANAGER_AUTH_SECRET_ID);
        authSecretKey = agentConf.get(AGENT_MANAGER_AUTH_SECRET_KEY);

        this.sourcePath = sourcePath;
        this.inlongGroupId = inlongGroupId;

        this.dimensions = new HashMap<>();
        dimensions.put(KEY_PLUGIN_ID, this.getClass().getSimpleName());
        String metricName = String.join("-", this.getClass().getSimpleName(),
                String.valueOf(METRIC_INDEX.incrementAndGet()));
        this.metricItemSet = new AgentMetricItemSet(metricName);
        MetricRegister.register(metricItemSet);
        resendQueue = new LinkedBlockingQueue<>();
    }

    public void Start() throws Exception {
        createMessageSender();
        EXECUTOR_SERVICE.execute(flushResendQueue());
        started = true;
    }

    public void Stop() {
        LOGGER.info("stop send manager");
        shutdown = true;
        if (!started) {
            return;
        }
        while (resendRunning) {
            AgentUtils.silenceSleepInMs(1);
        }
        closeMessageSender();
        LOGGER.info("stop send manager end");
    }

    private void closeMessageSender() {
        Long start = AgentUtils.getCurrentTime();
        senders.forEach(sender -> {
            if (sender != null) {
                sender.close();
            }
        });
        LOGGER.info("close sender elapse {} ms instance {}", AgentUtils.getCurrentTime() - start,
                profile.getInstanceId());
    }

    private AgentMetricItem getMetricItem(Map<String, String> otherDimensions) {
        Map<String, String> dimensions = new HashMap<>();
        dimensions.put(KEY_PLUGIN_ID, this.getClass().getSimpleName());
        dimensions.putAll(otherDimensions);
        return this.metricItemSet.findMetricItem(dimensions);
    }

    private AgentMetricItem getMetricItem(String groupId, String streamId) {
        Map<String, String> dims = new HashMap<>();
        dims.put(KEY_INLONG_GROUP_ID, groupId);
        dims.put(KEY_INLONG_STREAM_ID, streamId);
        return getMetricItem(dims);
    }

    /**
     * createMessageSender
     */
    private void createMessageSender() throws Exception {
        TcpMsgSenderConfig proxyClientConfig = new TcpMsgSenderConfig(
                managerAddr, inlongGroupId, authSecretId, authSecretKey);
        proxyClientConfig.setMaxInFlightSizeInKb(totalAsyncBufSize);
        proxyClientConfig.setAliveConnections(aliveConnectionNum);
        proxyClientConfig.setRequestTimeoutMs(maxSenderTimeout * 1000L);
        proxyClientConfig.setNettyWorkerThreadNum(ioThreadNum);
        proxyClientConfig.setEnableEpollBusyWait(enableBusyWait);
        proxyClientConfig.setSdkMsgType(MsgType.valueOf(msgType));
        proxyClientConfig.setEnableDataCompress(isCompress);
        SHARED_FACTORY = new DefaultThreadFactory("agent-sender-manager-" + sourcePath,
                Thread.currentThread().isDaemon());
        boolean hasError = false;
        ProcessResult procResult = null;
        for (int i = 0; i < maxSenderPerGroup; i++) {
            InLongTcpMsgSender sender = new InLongTcpMsgSender(proxyClientConfig, SHARED_FACTORY);
            procResult = new ProcessResult();
            if (!sender.start(procResult)) {
                hasError = true;
                break;
            }
            senders.add(sender);
        }
        if (hasError) {
            senders.forEach(sender -> {
                sender.close();
            });
            throw new ProxySdkException("Start sender failure, " + procResult);
        }
    }

    public void sendBatch(SenderMessage message) {
        while (!shutdown && !resendQueue.isEmpty()) {
            AgentUtils.silenceSleepInMs(retrySleepTime);
        }
        if (!shutdown) {
            sendBatchWithRetryCount(message, 0);
        }
    }

    /**
     * Send message to proxy by batch, use message cache.
     */
    private void sendBatchWithRetryCount(SenderMessage message, int retry) {
        boolean suc = false;
        while (!suc && !shutdown) {
            try {
                AgentSenderCallback cb = new AgentSenderCallback(message, retry);
                AuditUtils.add(AuditUtils.AUDIT_ID_AGENT_TRY_SEND, message.getGroupId(),
                        message.getStreamId(), message.getDataTime(), message.getMsgCnt(),
                        message.getTotalSize(), auditVersion);
                AuditUtils.add(AuditUtils.AUDIT_ID_AGENT_TRY_SEND_REAL_TIME, message.getGroupId(),
                        message.getStreamId(), AgentUtils.getCurrentTime(), message.getMsgCnt(),
                        message.getTotalSize(), auditVersion);
                asyncSendByMessageSender(cb, message.getDataList(), message.getGroupId(),
                        message.getStreamId(), message.getDataTime(), SEQUENTIAL_ID.getNextUuid(),
                        message.getExtraMap(), proxySend);
                getMetricItem(message.getGroupId(), message.getStreamId()).pluginSendCount.addAndGet(
                        message.getMsgCnt());
                suc = true;
            } catch (Exception exception) {
                AuditUtils.add(AuditUtils.AUDIT_ID_AGENT_SEND_EXCEPTION, message.getGroupId(),
                        message.getStreamId(), message.getDataTime(), message.getMsgCnt(),
                        message.getTotalSize(), auditVersion);
                AuditUtils.add(AuditUtils.AUDIT_ID_AGENT_SEND_EXCEPTION_REAL_TIME, message.getGroupId(),
                        message.getStreamId(), AgentUtils.getCurrentTime(), message.getMsgCnt(),
                        message.getTotalSize(), auditVersion);
                suc = false;
                if (retry > maxSenderRetry) {
                    if (retry % 10 == 0) {
                        LOGGER.error("max retry reached, sample log Exception caught", exception);
                    }
                } else {
                    LOGGER.error("Exception caught", exception);
                }
                retry++;
                AgentUtils.silenceSleepInMs(retrySleepTime);
                ThreadUtils.threadThrowableHandler(Thread.currentThread(), exception);
            }
        }
    }

    private void asyncSendByMessageSender(MsgSendCallback cb,
            List<byte[]> bodyList, String groupId, String streamId, long dataTime, String msgUUID,
            Map<String, String> extraAttrMap, boolean isProxySend) throws Exception {
        boolean isSuccess;
        ProcessResult procResult = new ProcessResult();
        int index = (int) Math.abs(senderIndex.getAndAdd(1) % maxSenderPerGroup);
        if (isProxySend) {
            isSuccess = senders.get(index)
                    .asyncSendMsgWithSinkAck(new TcpEventInfo(
                            groupId, streamId, dataTime, msgUUID, extraAttrMap, bodyList), cb, procResult);
        } else {
            isSuccess =
                    senders.get(index).asyncSendMessage(new TcpEventInfo(
                            groupId, streamId, dataTime, msgUUID, extraAttrMap, bodyList), cb, procResult);
        }
        if (!isSuccess) {
            throw new ProxySdkException("Send message failure, " + procResult);
        }
    }

    /**
     * flushResendQueue
     *
     * @return thread runner
     */
    private Runnable flushResendQueue() {
        return () -> {
            AgentThreadFactory.nameThread(
                    "flushResendQueue-" + profile.getTaskId() + "-" + profile.getInstanceId());
            LOGGER.info("start flush resend queue {}:{}", inlongGroupId, sourcePath);
            resendRunning = true;
            while (!shutdown) {
                try {
                    AgentSenderCallback callback = resendQueue.poll(RESEND_QUEUE_WAIT_MS, TimeUnit.MILLISECONDS);
                    if (callback != null) {
                        SenderMessage message = callback.message;
                        AuditUtils.add(AuditUtils.AUDIT_ID_AGENT_RESEND, message.getGroupId(),
                                message.getStreamId(), message.getDataTime(), message.getMsgCnt(),
                                message.getTotalSize(), auditVersion);
                        AuditUtils.add(AuditUtils.AUDIT_ID_AGENT_RESEND_REAL_TIME, message.getGroupId(),
                                message.getStreamId(), AgentUtils.getCurrentTime(), message.getMsgCnt(),
                                message.getTotalSize(), auditVersion);
                        sendBatchWithRetryCount(callback.message, callback.retry + 1);
                    }
                } catch (Exception e) {
                    LOGGER.error("error caught", e);
                    ThreadUtils.threadThrowableHandler(Thread.currentThread(), e);
                } finally {
                    AgentUtils.silenceSleepInMs(batchFlushInterval);
                }
            }
            LOGGER.info("stop flush resend queue {}:{}", inlongGroupId, sourcePath);
            resendRunning = false;
        };
    }

    /**
     * put the data into resend queue and will be resent later.
     *
     * @param batchMessageCallBack
     */
    private void putInResendQueue(AgentSenderCallback batchMessageCallBack) {
        try {
            resendQueue.put(batchMessageCallBack);
        } catch (Throwable throwable) {
            LOGGER.error("putInResendQueue e = {}", throwable);
        }
    }

    /**
     * sender callback
     */
    private class AgentSenderCallback implements MsgSendCallback {

        private final int retry;
        private final SenderMessage message;
        private final int msgCnt;

        AgentSenderCallback(SenderMessage message, int retry) {
            this.message = message;
            this.retry = retry;
            this.msgCnt = message.getDataList().size();
        }

        @Override
        public void onMessageAck(ProcessResult result) {
            String groupId = message.getGroupId();
            String streamId = message.getStreamId();
            String taskId = message.getTaskId();
            String instanceId = message.getInstanceId();
            long dataTime = message.getDataTime();
            if (result.isSuccess()) {
                message.getOffsetAckList().forEach(ack -> ack.setHasAck(true));
                getMetricItem(groupId, streamId).pluginSendSuccessCount.addAndGet(msgCnt);
                AuditUtils.add(AuditUtils.AUDIT_ID_AGENT_SEND_SUCCESS, groupId, streamId,
                        dataTime, message.getMsgCnt(), message.getTotalSize(), auditVersion);
                AuditUtils.add(AuditUtils.AUDIT_ID_AGENT_SEND_SUCCESS_REAL_TIME, groupId, streamId,
                        AgentUtils.getCurrentTime(), message.getMsgCnt(), message.getTotalSize(), auditVersion);
                AgentStatusManager.sendPackageCount.addAndGet(message.getMsgCnt());
                AgentStatusManager.sendDataLen.addAndGet(message.getTotalSize());
            } else {
                LOGGER.error("send groupId {}, streamId {}, taskId {}, instanceId {}, dataTime {} fail with times {}, "
                        + "error {}", groupId, streamId, taskId, instanceId, dataTime, retry, result);
                getMetricItem(groupId, streamId).pluginSendFailCount.addAndGet(msgCnt);
                putInResendQueue(new AgentSenderCallback(message, retry));
                AuditUtils.add(AuditUtils.AUDIT_ID_AGENT_SEND_FAILED, groupId, streamId,
                        dataTime, message.getMsgCnt(), message.getTotalSize(), auditVersion);
                AuditUtils.add(AuditUtils.AUDIT_ID_AGENT_SEND_FAILED_REAL_TIME, groupId, streamId,
                        AgentUtils.getCurrentTime(), message.getMsgCnt(), message.getTotalSize(), auditVersion);
            }
        }

        @Override
        public void onException(Throwable e) {
            getMetricItem(message.getGroupId(), message.getStreamId()).pluginSendFailCount.addAndGet(msgCnt);
            LOGGER.error("exception caught", e);
        }
    }
}
