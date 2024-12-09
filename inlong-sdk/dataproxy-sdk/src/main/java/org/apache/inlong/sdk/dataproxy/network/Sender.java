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

import org.apache.inlong.sdk.dataproxy.ProxyClientConfig;
import org.apache.inlong.sdk.dataproxy.codec.EncodeObject;
import org.apache.inlong.sdk.dataproxy.common.SendMessageCallback;
import org.apache.inlong.sdk.dataproxy.common.SendResult;
import org.apache.inlong.sdk.dataproxy.config.ProxyConfigEntry;
import org.apache.inlong.sdk.dataproxy.threads.MetricWorkerThread;
import org.apache.inlong.sdk.dataproxy.threads.TimeoutScanThread;

import io.netty.channel.Channel;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

public class Sender {

    private static final Logger LOGGER = LoggerFactory.getLogger(Sender.class);
    private static final AtomicLong senderIdGen = new AtomicLong(0L);
    /* Store the callback used by asynchronously message sending. */
    private final ConcurrentHashMap<Channel, ConcurrentHashMap<String, QueueObject>> callbacks =
            new ConcurrentHashMap<>();
    /* Store the synchronous message sending invocations. */
    private final ConcurrentHashMap<String, SyncMessageCallable> syncCallables = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, NettyClient> chooseProxy = new ConcurrentHashMap<>();
    private final ReentrantLock stateLock = new ReentrantLock();
    private final ExecutorService threadPool;
    private final int asyncCallbackMaxSize;
    private final AtomicInteger currentBufferSize = new AtomicInteger(0);
    private final TimeoutScanThread scanThread;
    private final ClientMgr clientMgr;
    private final String instanceId;
    private final ProxyClientConfig configure;
    private MetricWorkerThread metricWorker = null;
    private int clusterId = -1;

    public Sender(ProxyClientConfig configure) throws Exception {
        this(configure, null);
    }

    /**
     * Constructor of sender takes two arguments {@link ProxyClientConfig} and {@link ThreadFactory}
     */
    public Sender(ProxyClientConfig configure, ThreadFactory selfDefineFactory) throws Exception {
        this.configure = configure;
        this.instanceId = "sender-" + senderIdGen.incrementAndGet();
        this.asyncCallbackMaxSize = configure.getTotalAsyncCallbackSize();
        this.threadPool = Executors.newCachedThreadPool();
        this.clientMgr = new ClientMgr(configure, this, selfDefineFactory);
        ProxyConfigEntry proxyConfigEntry = null;
        try {
            proxyConfigEntry = this.clientMgr.getGroupIdConfigureInfo();
            setClusterId(proxyConfigEntry.getClusterId());
        } catch (Throwable e) {
            if (configure.isOnlyUseLocalProxyConfig()) {
                throw new Exception("Get local proxy configure failure!", e.getCause());
            } else {
                throw new Exception("Visit manager error!", e.getCause());
            }
        }
        if (!proxyConfigEntry.isInterVisit()) {
            if (!configure.isEnableAuthentication()) {
                throw new Exception("In OutNetwork isNeedAuthentication must be true!");
            }
            if (!configure.isNeedDataEncry()) {
                throw new Exception("In OutNetwork isNeedDataEncry must be true!");
            }
        }
        scanThread = new TimeoutScanThread(callbacks, currentBufferSize, configure, clientMgr);
        scanThread.start();

        if (configure.isEnableMetric()) {
            metricWorker = new MetricWorkerThread(configure, this);
            metricWorker.start();
        }

        LOGGER.info("proxy sdk is starting!");
    }

    private void checkCallbackList() {
        // max wait for 1 min
        LOGGER.info("checking call back list before close, current size is {}",
                currentBufferSize.get());
        int count = 0;
        try {
            while (currentBufferSize.get() > 0 && count < 60) {
                TimeUnit.SECONDS.sleep(1);
                count += 1;
            }
            if (currentBufferSize.get() > 0) {
                LOGGER.warn("callback not empty {}, please check it", currentBufferSize.get());
            }
        } catch (Exception ex) {
            LOGGER.error("exception while checking callback list", ex);
        }
    }

    public void close() {
        checkCallbackList();
        scanThread.shutDown();
        clientMgr.shutDown();
        threadPool.shutdown();
        if (configure.isEnableMetric()) {
            metricWorker.close();
        }
    }

    public String getExceptionStack(Throwable e) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        String exceptStr = null;
        try {
            e.printStackTrace(pw);
            exceptStr = sw.toString();
        } catch (Exception ex) {
            LOGGER.error(getExceptionStack(ex));
        } finally {
            try {
                pw.close();
                sw.close();
            } catch (Exception ex) {
                LOGGER.error(getExceptionStack(ex));
            }
        }
        return exceptStr;
    }

    /* Used for asynchronously message sending. */
    public void notifyCallback(Channel channel, String messageId, SendResult result) {
        LOGGER.debug("Channel = {} , ack messageId = {}", channel, messageId);
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
    }

    private SendResult syncSendInternalMessage(NettyClient client,
            EncodeObject encodeObject, String msgUUID)
            throws ExecutionException, InterruptedException, TimeoutException {
        if (client == null) {
            return SendResult.NO_CONNECTION;
        }
        if (isNotValidateAttr(encodeObject.getCommonattr(), encodeObject.getAttributes())) {
            LOGGER.error("error attr format {} {}", encodeObject.getCommonattr(),
                    encodeObject.getAttributes());
            return SendResult.INVALID_ATTRIBUTES;
        }
        if (encodeObject.getMsgtype() == 7) {
            int groupIdnum = 0;
            int streamIdnum = 0;
            if (encodeObject.getGroupId().equals(clientMgr.getGroupId())) {
                groupIdnum = clientMgr.getGroupIdNum();
                streamIdnum = clientMgr.getStreamIdMap().get(encodeObject.getStreamId()) != null
                        ? clientMgr.getStreamIdMap().get(encodeObject.getStreamId())
                        : 0;
            }
            encodeObject.setGroupIdNum(groupIdnum);
            encodeObject.setStreamIdNum(streamIdnum);
            if (groupIdnum == 0 || streamIdnum == 0) {
                encodeObject.setGroupIdTransfer(false);
            }
        }
        if (this.configure.isNeedDataEncry()) {
            encodeObject.setEncryptEntry(true,
                    configure.getAuthSecretId(), clientMgr.getEncryptConfigEntry());
        } else {
            encodeObject.setEncryptEntry(false, null, null);
        }
        encodeObject.setMsgUUID(msgUUID);
        SyncMessageCallable callable = new SyncMessageCallable(client, encodeObject,
                configure.getRequestTimeoutMs(), TimeUnit.MILLISECONDS);
        syncCallables.put(encodeObject.getMessageId(), callable);

        Future<SendResult> future = threadPool.submit(callable);
        return future.get(configure.getRequestTimeoutMs(), TimeUnit.MILLISECONDS);
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
        if (configure.isEnableMetric()) {
            metricWorker.recordNumByKey(encodeObject.getMessageId(), encodeObject.getGroupId(),
                    encodeObject.getStreamId(), IpUtils.getLocalIp(), encodeObject.getDt(),
                    encodeObject.getPackageTime(), encodeObject.getRealCnt());
        }
        NettyClient client = clientMgr.getClient(clientMgr.getLoadBalance(), encodeObject);
        SendResult message = null;
        try {
            message = syncSendInternalMessage(client, encodeObject, msgUUID);
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            LOGGER.error("send message error {} ", getExceptionStack(e));
            syncCallables.remove(encodeObject.getMessageId());
            return SendResult.THREAD_INTERRUPT;
        } catch (ExecutionException e) {
            // TODO Auto-generated catch block
            LOGGER.error("ExecutionException {} ", getExceptionStack(e));
            syncCallables.remove(encodeObject.getMessageId());
            return SendResult.UNKOWN_ERROR;
        } catch (TimeoutException e) {
            // TODO Auto-generated catch block
            LOGGER.error("TimeoutException {} ", getExceptionStack(e));
            // e.printStackTrace();
            SyncMessageCallable syncMessageCallable = syncCallables.remove(encodeObject.getMessageId());
            if (syncMessageCallable != null) {
                NettyClient tmpClient = syncMessageCallable.getClient();
                if (tmpClient != null) {
                    Channel curChannel = tmpClient.getChannel();
                    if (curChannel != null) {
                        LOGGER.error("channel maybe busy {}", curChannel);
                        scanThread.addTimeoutChannel(curChannel);
                    }
                }
            }
            return SendResult.TIMEOUT;
        } catch (Throwable e) {
            LOGGER.error("syncSendMessage exception {} ", getExceptionStack(e));
            syncCallables.remove(encodeObject.getMessageId());
            return SendResult.UNKOWN_ERROR;
        }
        if (message == null) {
            syncCallables.remove(encodeObject.getMessageId());
            return SendResult.UNKOWN_ERROR;
        }
        if (client != null) {
            scanThread.resetTimeoutChannel(client.getChannel());
        }
        if (message == SendResult.OK) {
            if (configure.isEnableMetric()) {
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
    public void asyncSendMessage(EncodeObject encodeObject, SendMessageCallback callback, String msgUUID,
            long timeout, TimeUnit timeUnit) throws ProxysdkException {
        if (configure.isEnableMetric()) {
            metricWorker.recordNumByKey(encodeObject.getMessageId(), encodeObject.getGroupId(),
                    encodeObject.getStreamId(), IpUtils.getLocalIp(), encodeObject.getPackageTime(),
                    encodeObject.getDt(), encodeObject.getRealCnt());
        }
        // send message package time
        NettyClient client = clientMgr.getClient(clientMgr.getLoadBalance(), encodeObject);
        if (client == null) {
            throw new ProxysdkException(SendResult.NO_CONNECTION.toString());
        }
        if (currentBufferSize.get() >= asyncCallbackMaxSize) {
            throw new ProxysdkException("ASYNC_CALLBACK_BUFFER_FULL");
        }
        if (isNotValidateAttr(encodeObject.getCommonattr(), encodeObject.getAttributes())) {
            LOGGER.error("error attr format {} {}", encodeObject.getCommonattr(),
                    encodeObject.getAttributes());
            throw new ProxysdkException(SendResult.INVALID_ATTRIBUTES.toString());
        }
        int size = 1;
        if (currentBufferSize.incrementAndGet() >= asyncCallbackMaxSize) {
            currentBufferSize.decrementAndGet();
            throw new ProxysdkException("ASYNC_CALLBACK_BUFFER_FULL");
        }
        ConcurrentHashMap<String, QueueObject> msgQueueMap =
                callbacks.computeIfAbsent(client.getChannel(), (k) -> new ConcurrentHashMap<>());
        QueueObject queueObject = msgQueueMap.putIfAbsent(encodeObject.getMessageId(),
                new QueueObject(System.currentTimeMillis(), callback, size, timeout, timeUnit));
        if (queueObject != null) {
            LOGGER.warn("message id {} has existed.", encodeObject.getMessageId());
        }
        if (encodeObject.getMsgtype() == 7) {
            int groupIdnum = 0;
            int streamIdnum = 0;
            if ((clientMgr.getGroupId().length() != 0) && (encodeObject.getGroupId().equals(clientMgr.getGroupId()))) {
                groupIdnum = clientMgr.getGroupIdNum();
                streamIdnum = (clientMgr.getStreamIdMap().get(encodeObject.getStreamId()) != null) ? clientMgr
                        .getStreamIdMap().get(encodeObject.getStreamId()) : 0;
            }
            encodeObject.setGroupIdNum(groupIdnum);
            encodeObject.setStreamIdNum(streamIdnum);
            if (groupIdnum == 0 || streamIdnum == 0) {
                encodeObject.setGroupIdTransfer(false);
            }
        }
        if (this.configure.isNeedDataEncry()) {
            encodeObject.setEncryptEntry(true,
                    configure.getAuthSecretId(), clientMgr.getEncryptConfigEntry());
        } else {
            encodeObject.setEncryptEntry(false, null, null);
        }
        encodeObject.setMsgUUID(msgUUID);
        client.write(encodeObject);
    }

    /* Deal with feedback. */
    public void notifyFeedback(Channel channel, EncodeObject response) {
        String messageId = response.getMessageId();
        chooseProxy.remove(messageId);
        SyncMessageCallable callable = syncCallables.remove(messageId);
        SendResult result = response.getSendResult();
        if (result == SendResult.OK) {
            if (configure.isEnableMetric()) {
                metricWorker.recordSuccessByMessageId(messageId);
            }
        } else {
            LOGGER.error("{} exception happens, error message {}", channel, response.getErrMsg());
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
        LOGGER.info("channel {} connection is disconnected!", channel);
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
                }
                msgQueueMap.clear();
            }
        } catch (Throwable e2) {
            LOGGER.info("process channel {} disconnected callbacks throw error,", channel, e2);
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
            LOGGER.info("process channel {} disconnected syncCallables throw error,", channel, e);
        }
    }

    /* Deal with unexpected exception. only used for asyc send */
    public void waitForAckForChannel(Channel channel) {
        if (channel == null) {
            return;
        }
        LOGGER.info("wait for ack for channel {}", channel);
        try {
            ConcurrentHashMap<String, QueueObject> queueObjMap = callbacks.get(channel);
            if (queueObjMap != null) {
                while (true) {
                    if (queueObjMap.isEmpty()) {
                        LOGGER.info("this channel {} is empty!", channel);
                        break;
                    }
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        // TODO Auto-generated catch block
                        LOGGER.error("wait for ack for channel {}, error {}",
                                channel, e.getMessage());
                        e.printStackTrace();
                    }
                }
            }
            LOGGER.info("waitForAckForChannel finished , channel is {}", channel);
        } catch (Throwable e) {
            LOGGER.error("waitForAckForChannel exception, channel is {}", channel, e);
        }
    }

    public void clearCallBack() {
        currentBufferSize.set(0);
        callbacks.clear();
    }

    public void clearCallBackByChannel(Channel channel) {
        callbacks.remove(channel);
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

    /**
     * check whether clientChannel is idle; if idle, need send hb to keep alive
     *
     * @param client
     * @return
     */
    public boolean isIdleClient(NettyClient client) {
        Channel channel = client.getChannel();
        // used by async send
        if (callbacks.contains(channel) && MapUtils.isNotEmpty(callbacks.get(channel))) {
            return false;
        }
        // used by sync send
        for (SyncMessageCallable syncCallBack : syncCallables.values()) {
            if (ObjectUtils.equals(client, syncCallBack.getClient())) {
                return false;
            }
        }

        return true;
    }

}
