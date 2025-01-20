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

package org.apache.inlong.sdk.dataproxy.network;

import org.apache.inlong.sdk.dataproxy.codec.EncodeObject;
import org.apache.inlong.sdk.dataproxy.common.SendMessageCallback;
import org.apache.inlong.sdk.dataproxy.common.SendResult;
import org.apache.inlong.sdk.dataproxy.config.ProxyConfigEntry;
import org.apache.inlong.sdk.dataproxy.exception.ProxySdkException;
import org.apache.inlong.sdk.dataproxy.sender.tcp.TcpMsgSenderConfig;
import org.apache.inlong.sdk.dataproxy.threads.MetricWorkerThread;
import org.apache.inlong.sdk.dataproxy.threads.TimeoutScanThread;
import org.apache.inlong.sdk.dataproxy.utils.LogCounter;
import org.apache.inlong.sdk.dataproxy.utils.ProxyUtils;
import org.apache.inlong.sdk.dataproxy.utils.Tuple2;

import io.netty.channel.Channel;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class Sender {

    private static final Logger logger = LoggerFactory.getLogger(Sender.class);
    private static final LogCounter exptCnt = new LogCounter(10, 100000, 60 * 1000L);
    private static final LogCounter unwritableExptCnt = new LogCounter(10, 100000, 60 * 1000L);
    private static final LogCounter reqChkLoggCount = new LogCounter(10, 100000, 60 * 1000L);
    private static final AtomicLong senderIdGen = new AtomicLong(0L);
    /* Store the callback used by asynchronously message sending. */
    private final ConcurrentHashMap<Channel, ConcurrentHashMap<String, QueueObject>> callbacks =
            new ConcurrentHashMap<>();
    /* Store the synchronous message sending invocations. */
    private final ConcurrentHashMap<String, SyncMessageCallable> syncCallables = new ConcurrentHashMap<>();
    private final ExecutorService threadPool;
    private final int asyncCallbackMaxSize;
    private final AtomicInteger currentBufferSize = new AtomicInteger(0);
    private final TimeoutScanThread scanThread;
    private final AtomicBoolean started = new AtomicBoolean(false);
    private final DefClientMgr clientMgr;
    private final String instanceId;
    private final TcpMsgSenderConfig tcpConfig;
    private MetricWorkerThread metricWorker = null;
    private int clusterId = -1;

    public Sender(TcpMsgSenderConfig tcpConfig) throws Exception {
        this(tcpConfig, null);
    }

    /**
     * Constructor of sender takes two arguments {@link TcpMsgSenderConfig} and {@link ThreadFactory}
     */
    public Sender(TcpMsgSenderConfig tcpConfig, ThreadFactory selfDefineFactory) throws Exception {
        this.tcpConfig = tcpConfig;
        this.instanceId = "sender-" + senderIdGen.incrementAndGet();
        this.asyncCallbackMaxSize = tcpConfig.getTotalAsyncCallbackSize();
        this.threadPool = Executors.newCachedThreadPool();
        this.clientMgr = new DefClientMgr(tcpConfig, this, selfDefineFactory);
        this.scanThread = new TimeoutScanThread(this, tcpConfig);
        if (tcpConfig.isEnableMetric()) {
            metricWorker = new MetricWorkerThread(tcpConfig, this);
        }
        logger.info("Sender({}) instance initialized!", this.instanceId);
    }

    public void start() throws Exception {
        if (!started.compareAndSet(false, true)) {
            return;
        }
        this.clientMgr.start();
        this.scanThread.start();
        ProxyConfigEntry proxyConfigEntry;
        try {
            proxyConfigEntry = this.clientMgr.getGroupIdConfigure();
            setClusterId(proxyConfigEntry.getClusterId());
        } catch (Throwable ex) {
            if (tcpConfig.isOnlyUseLocalProxyConfig()) {
                throw new Exception("Get local proxy configure failure!", ex);
            } else {
                throw new Exception("Visit manager error!", ex);
            }
        }
        if (!proxyConfigEntry.isInterVisit()) {
            if (!tcpConfig.isEnableReportAuthz()) {
                throw new Exception("In OutNetwork isNeedAuthentication must be true!");
            }
            if (!tcpConfig.isEnableReportEncrypt()) {
                throw new Exception("In OutNetwork isNeedDataEncry must be true!");
            }
        }
        if (this.tcpConfig.isEnableMetric()) {
            this.metricWorker.start();
        }
        logger.info("Sender({}) instance started!", this.instanceId);
    }

    public void close() {
        if (!started.compareAndSet(true, false)) {
            return;
        }
        checkCallbackList();
        scanThread.shutDown();
        clientMgr.shutDown();
        threadPool.shutdown();
        if (tcpConfig.isEnableMetric()) {
            metricWorker.close();
        }
        logger.info("Sender({}) instance stopped!", this.instanceId);
    }

    /* Used for asynchronously message sending. */
    public void notifyCallback(Channel channel, String messageId, SendResult result) {
        if (channel == null) {
            return;
        }
        ConcurrentHashMap<String, QueueObject> callBackMap = callbacks.get(channel);
        if (callBackMap == null) {
            return;
        }
        QueueObject callback = callBackMap.remove(messageId);
        if (callback == null) {
            return;
        }
        callback.getCallback().onMessageAck(result);
        currentBufferSize.decrementAndGet();
        callback.done();
    }

    /**
     * Following methods used by synchronously message sending.
     * Meanwhile, update this send channel timeout info(including increase or reset), according to the sendResult
     *
     * @param encodeObject
     * @param msgUUID
     * @return
     */
    public SendResult syncSendMessage(EncodeObject encodeObject, String msgUUID) {
        if (!started.get()) {
            return SendResult.SENDER_CLOSED;
        }
        if (tcpConfig.isEnableMetric()) {
            metricWorker.recordNumByKey(encodeObject.getMessageId(), encodeObject.getGroupId(),
                    encodeObject.getStreamId(), ProxyUtils.getLocalIp(), encodeObject.getDt(),
                    encodeObject.getPackageTime(), encodeObject.getRealCnt());
        }
        SendResult message;
        Tuple2<SendResult, NettyClient> clientResult = null;
        try {
            MutableBoolean allClientMaxInFlight = new MutableBoolean(false);
            clientResult = clientMgr.getClientByRoundRobin(allClientMaxInFlight);
            if (allClientMaxInFlight.booleanValue()) {
                return SendResult.MAX_FLIGHT_ON_ALL_CONNECTION;
            }
            if (clientResult.getF0() != SendResult.OK) {
                return clientResult.getF0();
            }
            if (!clientResult.getF1().getChannel().isWritable()) {
                if (unwritableExptCnt.shouldPrint()) {
                    logger.warn("Sender({}) channel={} touch write_over_water",
                            getInstanceId(), clientResult.getF1().getChannel());
                }
                return SendResult.WRITE_OVER_WATERMARK;
            }
            if (isNotValidateAttr(encodeObject.getCommonattr(), encodeObject.getAttributes())) {
                if (reqChkLoggCount.shouldPrint()) {
                    logger.warn("Sender({}) found error attr format {} {}",
                            getInstanceId(), encodeObject.getCommonattr(), encodeObject.getAttributes());
                }
                return SendResult.INVALID_ATTRIBUTES;
            }
            if (encodeObject.getMsgtype() == 7) {
                if (clientMgr.isIdTransNum()
                        && encodeObject.getGroupId().equals(clientMgr.getGroupId())) {
                    encodeObject.setGroupIdAndStreamIdNum(clientMgr.getGroupIdNum(),
                            clientMgr.getStreamIdNum(encodeObject.getStreamId()));
                }
            }
            if (this.tcpConfig.isEnableReportEncrypt()) {
                encodeObject.setEncryptEntry(true,
                        tcpConfig.getRptUserName(), clientMgr.getEncryptConfigureInfo());
            }
            encodeObject.setMsgUUID(msgUUID);
            SyncMessageCallable callable = new SyncMessageCallable(
                    clientResult.getF1(), encodeObject, tcpConfig.getRequestTimeoutMs());
            syncCallables.put(encodeObject.getMessageId(), callable);
            Future<SendResult> future = threadPool.submit(callable);
            message = future.get(tcpConfig.getRequestTimeoutMs(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            syncCallables.remove(encodeObject.getMessageId());
            return SendResult.THREAD_INTERRUPT;
        } catch (ExecutionException e) {
            syncCallables.remove(encodeObject.getMessageId());
            if (exptCnt.shouldPrint()) {
                logger.warn("Sender({}) sync send msg throw ExecutionException",
                        getInstanceId(), e);
            }
            return SendResult.UNKOWN_ERROR;
        } catch (TimeoutException e) {
            SyncMessageCallable syncMessageCallable = syncCallables.remove(encodeObject.getMessageId());
            if (syncMessageCallable != null) {
                NettyClient tmpClient = syncMessageCallable.getClient();
                if (tmpClient != null) {
                    Channel curChannel = tmpClient.getChannel();
                    if (curChannel != null) {
                        scanThread.addTimeoutChannel(curChannel);
                    }
                }
            }
            if (exptCnt.shouldPrint()) {
                logger.warn("Sender({}) sync send msg throw TimeoutException", getInstanceId(), e);
            }
            return SendResult.TIMEOUT;
        } catch (Throwable e) {
            syncCallables.remove(encodeObject.getMessageId());
            if (exptCnt.shouldPrint()) {
                logger.warn("Sender({}) sync send msg throw exception", getInstanceId(), e);
            }
            return SendResult.UNKOWN_ERROR;
        } finally {
            if (clientResult != null && clientResult.getF1() != null) {
                clientResult.getF1().decMsgInFlight();
            }
        }
        if (message == null) {
            syncCallables.remove(encodeObject.getMessageId());
            return SendResult.UNKOWN_ERROR;
        }
        scanThread.resetTimeoutChannel(clientResult.getF1().getChannel());
        if (message == SendResult.OK) {
            if (tcpConfig.isEnableMetric()) {
                metricWorker.recordSuccessByMessageId(encodeObject.getMessageId());
            }
        }
        return message;
    }

    /**
     * whether is validate
     *
     * @param commonAttr
     * @param oldAttr
     * @return
     */
    private boolean isNotValidateAttr(String commonAttr, String oldAttr) {
        if (!StringUtils.isEmpty(commonAttr) && !validAttribute(commonAttr)) {
            return true;
        }
        return !StringUtils.isEmpty(oldAttr) && !validAttribute(oldAttr);
    }

    /**
     * validate attribute
     *
     * @param attr
     * @return
     */
    private boolean validAttribute(String attr) {
        boolean needEqual = true;
        boolean needAnd = false;
        for (int i = 0; i < attr.length(); i++) {
            char item = attr.charAt(i);
            if (item == '=') {
                // if not must equal, then return false
                if (!needEqual) {
                    return false;
                }
                needEqual = false;
                needAnd = true;
            } else if (item == '&') {
                // if not must and, then return false
                if (!needAnd) {
                    return false;
                }
                needAnd = false;
                needEqual = true;
            }
        }
        return !needEqual;
    }

    /**
     * Following methods used by asynchronously message sending.
     */
    public void asyncSendMessage(EncodeObject encodeObject,
            SendMessageCallback callback, String msgUUID) throws ProxySdkException {
        if (!started.get()) {
            if (callback != null) {
                callback.onMessageAck(SendResult.SENDER_CLOSED);
                return;
            } else {
                throw new ProxySdkException(SendResult.SENDER_CLOSED.toString());
            }
        }
        if (tcpConfig.isEnableMetric()) {
            metricWorker.recordNumByKey(encodeObject.getMessageId(), encodeObject.getGroupId(),
                    encodeObject.getStreamId(), ProxyUtils.getLocalIp(), encodeObject.getPackageTime(),
                    encodeObject.getDt(), encodeObject.getRealCnt());
        }
        // send message package time
        MutableBoolean allClientMaxInFlight = new MutableBoolean(false);
        Tuple2<SendResult, NettyClient> clientResult =
                clientMgr.getClientByRoundRobin(allClientMaxInFlight);
        if (allClientMaxInFlight.booleanValue()) {
            if (callback != null) {
                callback.onMessageAck(SendResult.MAX_FLIGHT_ON_ALL_CONNECTION);
                return;
            } else {
                throw new ProxySdkException(SendResult.MAX_FLIGHT_ON_ALL_CONNECTION.toString());
            }
        }
        if (clientResult.getF0() != SendResult.OK) {
            if (callback != null) {
                callback.onMessageAck(clientResult.getF0());
                return;
            } else {
                throw new ProxySdkException(clientResult.getF0().toString());
            }
        }
        if (!clientResult.getF1().getChannel().isWritable()) {
            if (unwritableExptCnt.shouldPrint()) {
                logger.warn("Sender({}) found channel={} touch write_over_water",
                        getInstanceId(), clientResult.getF1().getChannel());
            }
            clientResult.getF1().decMsgInFlight();
            if (callback != null) {
                callback.onMessageAck(SendResult.WRITE_OVER_WATERMARK);
                return;
            } else {
                throw new ProxySdkException(SendResult.WRITE_OVER_WATERMARK.toString());
            }
        }
        if (currentBufferSize.get() >= asyncCallbackMaxSize) {
            clientResult.getF1().decMsgInFlight();
            if (callback != null) {
                callback.onMessageAck(SendResult.ASYNC_CALLBACK_BUFFER_FULL);
                return;
            } else {
                throw new ProxySdkException(SendResult.ASYNC_CALLBACK_BUFFER_FULL.toString());
            }
        }
        if (isNotValidateAttr(encodeObject.getCommonattr(), encodeObject.getAttributes())) {
            if (reqChkLoggCount.shouldPrint()) {
                logger.warn("Sender({}) found error attr format {} {}",
                        getInstanceId(), encodeObject.getCommonattr(), encodeObject.getAttributes());
            }
            clientResult.getF1().decMsgInFlight();
            if (callback != null) {
                callback.onMessageAck(SendResult.INVALID_ATTRIBUTES);
                return;
            } else {
                throw new ProxySdkException(SendResult.INVALID_ATTRIBUTES.toString());
            }
        }
        int size = 1;
        if (currentBufferSize.incrementAndGet() >= asyncCallbackMaxSize) {
            clientResult.getF1().decMsgInFlight();
            currentBufferSize.decrementAndGet();
            if (callback != null) {
                callback.onMessageAck(SendResult.ASYNC_CALLBACK_BUFFER_FULL);
                return;
            } else {
                throw new ProxySdkException(SendResult.ASYNC_CALLBACK_BUFFER_FULL.toString());
            }
        }
        ConcurrentHashMap<String, QueueObject> msgQueueMap =
                callbacks.computeIfAbsent(clientResult.getF1().getChannel(), (k) -> new ConcurrentHashMap<>());
        QueueObject queueObject = msgQueueMap.putIfAbsent(encodeObject.getMessageId(),
                new QueueObject(clientResult.getF1(), System.currentTimeMillis(), callback,
                        size, tcpConfig.getRequestTimeoutMs()));
        if (queueObject != null) {
            if (reqChkLoggCount.shouldPrint()) {
                logger.warn("Sender({}) found message id {} has existed.",
                        getInstanceId(), encodeObject.getMessageId());
            }
        }
        if (encodeObject.getMsgtype() == 7) {
            if (clientMgr.isIdTransNum()
                    && encodeObject.getGroupId().equals(clientMgr.getGroupId())) {
                encodeObject.setGroupIdAndStreamIdNum(clientMgr.getGroupIdNum(),
                        clientMgr.getStreamIdNum(encodeObject.getStreamId()));
            }
        }
        if (this.tcpConfig.isEnableReportEncrypt()) {
            encodeObject.setEncryptEntry(true,
                    tcpConfig.getRptUserName(), clientMgr.getEncryptConfigureInfo());
        }
        encodeObject.setMsgUUID(msgUUID);
        clientResult.getF1().write(encodeObject);
    }

    /* Deal with feedback. */
    public void notifyFeedback(Channel channel, EncodeObject response) {
        String messageId = response.getMessageId();
        SyncMessageCallable callable = syncCallables.remove(messageId);
        SendResult result = response.getSendResult();
        if (result == SendResult.OK) {
            if (tcpConfig.isEnableMetric()) {
                metricWorker.recordSuccessByMessageId(messageId);
            }
        } else {
            if (logger.isDebugEnabled()) {
                logger.debug("Sender({}) send message to {} exception, errMsg={}",
                        getInstanceId(), channel, response.getErrMsg());
            }
        }
        if (callable != null) { // for syncSend
            callable.update(result);
        }
        notifyCallback(channel, messageId, result); // for asyncSend
    }

    /*
     * deal with connection disconnection, should we restore it and re-send on a new channel?
     */
    public void notifyConnectionDisconnected(Channel channel) {
        if (channel == null) {
            return;
        }
        if (logger.isDebugEnabled()) {
            logger.debug("Sender({}) found channel {} connection is disconnected!",
                    getInstanceId(), channel);
        }
        try {
            ConcurrentHashMap<String, QueueObject> msgQueueMap = callbacks.remove(channel);
            if (msgQueueMap != null) {
                for (String messageId : msgQueueMap.keySet()) {
                    QueueObject queueObject = msgQueueMap.remove(messageId);
                    if (queueObject == null) {
                        continue;
                    }
                    queueObject.getCallback().onMessageAck(SendResult.CONNECTION_BREAK);
                    currentBufferSize.decrementAndGet();
                    queueObject.done();
                }
                msgQueueMap.clear();
            }
        } catch (Throwable e2) {
            if (exptCnt.shouldPrint()) {
                logger.warn("Sender({}) process channel disconnected {} throw error",
                        getInstanceId(), channel, e2);
            }
        }

        try {
            for (String messageId : syncCallables.keySet()) {
                if (messageId == null) {
                    continue;
                }
                SyncMessageCallable messageCallable = syncCallables.get(messageId);
                if (messageCallable == null) {
                    continue;
                }
                NettyClient nettyClient = messageCallable.getClient();
                if (nettyClient == null) {
                    continue;
                }
                Channel netChannel1 = nettyClient.getChannel();
                if (netChannel1 == null) {
                    continue;
                }
                if (netChannel1.id().equals(channel.id())) {
                    messageCallable.update(SendResult.CONNECTION_BREAK);
                    syncCallables.remove(messageId);
                    break;
                }
            }
        } catch (Throwable e) {
            if (exptCnt.shouldPrint()) {
                logger.warn("Sender({}) process channel {} disconnected syncCallables throw error",
                        getInstanceId(), channel, e);
            }
        }
    }

    /* Deal with unexpected exception. only used for async send */
    public void waitForAckForChannel(Channel channel) {
        if (channel == null) {
            return;
        }
        long startTime = System.currentTimeMillis();
        ConcurrentHashMap<String, QueueObject> queueObjMap = callbacks.get(channel);
        if (queueObjMap == null || queueObjMap.isEmpty()) {
            return;
        }
        try {
            while (!queueObjMap.isEmpty()) {
                if (System.currentTimeMillis() - startTime >= tcpConfig.getConCloseWaitPeriodMs()) {
                    break;
                }
                try {
                    Thread.sleep(100L);
                } catch (InterruptedException ex1) {
                    //
                }
            }
        } catch (Throwable ex) {
            if (exptCnt.shouldPrint()) {
                logger.warn("Sender({}) waitForAckForChannel channel {} throw error",
                        getInstanceId(), channel, ex);
            }
        }
    }

    public void clearCallBack() {
        currentBufferSize.set(0);
        callbacks.clear();
    }

    public int getClusterId() {
        return clusterId;
    }

    public void setClusterId(int clusterId) {
        this.clusterId = clusterId;
    }

    public String getInstanceId() {
        return instanceId;
    }

    public AtomicInteger getCurrentBufferSize() {
        return currentBufferSize;
    }

    public ConcurrentHashMap<Channel, ConcurrentHashMap<String, QueueObject>> getCallbacks() {
        return callbacks;
    }

    public DefClientMgr getClientMgr() {
        return clientMgr;
    }

    public TcpMsgSenderConfig getTcpConfig() {
        return tcpConfig;
    }

    private void checkCallbackList() {
        // max wait for 1 min
        try {
            long startTime = System.currentTimeMillis();
            while (currentBufferSize.get() > 0
                    && System.currentTimeMillis() - startTime < tcpConfig.getConCloseWaitPeriodMs()) {
                Thread.sleep(300L);
            }
            if (currentBufferSize.get() > 0) {
                logger.warn("Sender({}) callback size({}) not empty, force quit!",
                        getInstanceId(), currentBufferSize.get());
            }
        } catch (Throwable ex) {
            //
        }
    }
}
