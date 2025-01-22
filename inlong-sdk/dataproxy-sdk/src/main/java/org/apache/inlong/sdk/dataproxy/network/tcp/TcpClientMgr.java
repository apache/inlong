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

package org.apache.inlong.sdk.dataproxy.network.tcp;

import org.apache.inlong.sdk.dataproxy.common.ErrorCode;
import org.apache.inlong.sdk.dataproxy.common.ProcessResult;
import org.apache.inlong.sdk.dataproxy.config.HostInfo;
import org.apache.inlong.sdk.dataproxy.network.ClientMgr;
import org.apache.inlong.sdk.dataproxy.network.SequentialID;
import org.apache.inlong.sdk.dataproxy.network.tcp.codec.DecodeObject;
import org.apache.inlong.sdk.dataproxy.network.tcp.codec.EncodeObject;
import org.apache.inlong.sdk.dataproxy.sender.BaseSender;
import org.apache.inlong.sdk.dataproxy.sender.MsgSendCallback;
import org.apache.inlong.sdk.dataproxy.sender.tcp.TcpMsgSenderConfig;
import org.apache.inlong.sdk.dataproxy.utils.EventLoopUtil;
import org.apache.inlong.sdk.dataproxy.utils.LogCounter;
import org.apache.inlong.sdk.dataproxy.utils.ProxyUtils;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;
import io.netty.util.Timer;
import io.netty.util.TimerTask;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * TCP Client Manager class
 *
 * Used to manage TCP clients, including periodically selecting proxy nodes,
 *  finding available nodes when reporting messages, maintaining inflight message
 *  sending status, finding responses to corresponding requests, etc.
 */
public class TcpClientMgr implements ClientMgr {

    private static final Logger logger = LoggerFactory.getLogger(TcpClientMgr.class);
    private static final LogCounter sendExceptCnt = new LogCounter(10, 100000, 60 * 1000L);
    private static final LogCounter updConExptCnt = new LogCounter(10, 100000, 60 * 1000L);
    private static final LogCounter exptCounter = new LogCounter(10, 100000, 60 * 1000L);
    private static final LogCounter callbackExceptCnt = new LogCounter(10, 100000, 60 * 1000L);
    private static final AtomicLong timerRefCnt = new AtomicLong(0);
    private static Timer timerObj;

    private final BaseSender baseSender;
    private final String senderId;
    private final TcpMsgSenderConfig tcpConfig;
    private final Bootstrap bootstrap;
    private final SequentialID messageIdGen = new SequentialID();
    private ConcurrentHashMap<String, TcpNettyClient> usingClientMaps = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, TcpNettyClient> deletingClientMaps = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, HostInfo> connFailNodeMap = new ConcurrentHashMap<>();
    // current using nodes
    private List<String> activeNodes = new ArrayList<>();
    private volatile long lastUpdateTime = -1;

    private final MaintThread maintThread;
    private final AtomicBoolean started = new AtomicBoolean(false);
    private final AtomicLong channelTermGen = new AtomicLong(0);
    // request cache
    private final ConcurrentHashMap<Integer, TcpCallFuture> reqObjects =
            new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Integer, Timeout> reqTimeouts =
            new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, ConcurrentHashMap<Integer, Boolean>> channelMsgIdMap =
            new ConcurrentHashMap<>();
    // node select index
    private final AtomicInteger reqSendIndex = new AtomicInteger(0);

    /**
     * Build up the connection between the server and client.
     */
    public TcpClientMgr(BaseSender baseSender, TcpMsgSenderConfig tcpConfig, ThreadFactory selfDefineFactory) {
        this.baseSender = baseSender;
        this.senderId = baseSender.getSenderId();
        this.tcpConfig = tcpConfig;
        // Initialize the bootstrap
        this.bootstrap = buildBootstrap(selfDefineFactory);
        this.maintThread = new MaintThread();
    }

    @Override
    public boolean start(ProcessResult procResult) {
        if (!started.compareAndSet(false, true)) {
            return procResult.setSuccess();
        }
        if (timerRefCnt.incrementAndGet() == 1) {
            timerObj = new HashedWheelTimer();
        }
        // start hb thread
        this.maintThread.start();
        logger.info("ClientMgr({}) started", senderId);
        return procResult.setSuccess();
    }

    @Override
    public void stop() {
        if (!started.compareAndSet(true, false)) {
            return;
        }
        if (timerRefCnt.decrementAndGet() == 0) {
            timerObj.stop();
        }
        this.bootstrap.config().group().shutdownGracefully();

        this.maintThread.shutDown();
        if (!channelMsgIdMap.isEmpty()) {
            long startTime = System.currentTimeMillis();
            while (!channelMsgIdMap.isEmpty()) {
                if (System.currentTimeMillis() - startTime >= tcpConfig.getConCloseWaitPeriodMs()) {
                    break;
                }
                ProxyUtils.sleepSomeTime(100L);
            }
        }
        this.activeNodes.clear();
        logger.info("ClientMgr({}) stopped!", senderId);
    }

    @Override
    public int getInflightMsgCnt() {
        return this.reqTimeouts.size();
    }

    @Override
    public int getActiveNodeCnt() {
        return activeNodes.size();
    }

    @Override
    public void updateProxyInfoList(boolean nodeChanged, ConcurrentHashMap<String, HostInfo> hostInfoMap) {
        if (hostInfoMap.isEmpty() || !this.started.get()) {
            return;
        }
        long curTime = System.currentTimeMillis();
        try {
            // shuffle candidate nodes
            List<HostInfo> candidateNodes = new ArrayList<>(hostInfoMap.size());
            candidateNodes.addAll(hostInfoMap.values());
            Collections.sort(candidateNodes);
            Collections.shuffle(candidateNodes);
            int curTotalCnt = candidateNodes.size();
            int needActiveCnt = Math.min(this.tcpConfig.getAliveConnections(), curTotalCnt);
            // build next step nodes
            TcpNettyClient client;
            int maxCycleCnt = 3;
            this.connFailNodeMap.clear();
            List<String> realHosts = new ArrayList<>();
            ConcurrentHashMap<String, TcpNettyClient> tmpClientMaps = new ConcurrentHashMap<>();
            do {
                int selectCnt = 0;
                for (HostInfo hostInfo : candidateNodes) {
                    if (realHosts.contains(hostInfo.getReferenceName())) {
                        continue;
                    }
                    try {
                        client = new TcpNettyClient(this.senderId,
                                this.bootstrap, hostInfo, this.tcpConfig);
                        if (!client.connect(false, channelTermGen.incrementAndGet())) {
                            this.connFailNodeMap.put(hostInfo.getReferenceName(), hostInfo);
                            client.close(false);
                            continue;
                        }
                        realHosts.add(hostInfo.getReferenceName());
                        tmpClientMaps.put(hostInfo.getReferenceName(), client);
                        if (++selectCnt >= needActiveCnt) {
                            break;
                        }
                    } catch (Throwable ex) {
                        if (updConExptCnt.shouldPrint()) {
                            logger.warn("ClientMgr({}) build client {} exception",
                                    senderId, hostInfo.getReferenceName(), ex);
                        }
                    }
                }
                if (!realHosts.isEmpty()) {
                    break;
                }
                ProxyUtils.sleepSomeTime(1000L);
            } while (--maxCycleCnt > 0);
            // update active nodes
            if (realHosts.isEmpty()) {
                if (nodeChanged) {
                    logger.error("ClientMgr({}) changed nodes, but all connect failure, nodes={}!",
                            this.senderId, candidateNodes);
                } else {
                    logger.error("ClientMgr({}) re-choose nodes, but all connect failure, nodes={}!",
                            this.senderId, candidateNodes);
                }
            } else {
                this.lastUpdateTime = System.currentTimeMillis();
                this.deletingClientMaps = this.usingClientMaps;
                this.usingClientMaps = tmpClientMaps;
                this.activeNodes = realHosts;
                if (nodeChanged) {
                    logger.info("ClientMgr({}) changed nodes, wast {}ms, nodeCnt=(r:{}-a:{}), actives={}, fail={}",
                            senderId, (System.currentTimeMillis() - curTime),
                            needActiveCnt, realHosts.size(), realHosts, connFailNodeMap.keySet());
                } else {
                    logger.info("ClientMgr({}) re-choose nodes, wast {}ms, nodeCnt=(r:{}-a:{}), actives={}, fail={}",
                            senderId, (System.currentTimeMillis() - curTime),
                            needActiveCnt, realHosts.size(), realHosts, connFailNodeMap.keySet());
                }
            }
        } catch (Throwable ex) {
            if (updConExptCnt.shouldPrint()) {
                logger.warn("ClientMgr({}) update nodes throw exception", senderId, ex);
            }
        }
    }

    public boolean reportEvent(SendQos sendQos, TcpNettyClient client,
            EncodeObject encObject, MsgSendCallback callback, ProcessResult procResult) {
        if (!this.started.get()) {
            return procResult.setFailResult(ErrorCode.SDK_CLOSED);
        }
        long clientTerm = client.getChanTermId();
        if (sendQos == SendQos.NO_ACK) {
            // process no ack report
            if (client.write(clientTerm, encObject, procResult)) {
                client.decInFlightMsgCnt(clientTerm);
            }
            return procResult.isSuccess();
        }
        TcpCallFuture newFuture = new TcpCallFuture(encObject,
                client.getClientAddr(), clientTerm, client.getChanStr(), callback);
        TcpCallFuture curFuture = reqObjects.putIfAbsent(encObject.getMessageId(), newFuture);
        if (curFuture != null) {
            if (sendExceptCnt.shouldPrint()) {
                logger.warn("ClientMgr({}) found message id {} has existed.",
                        senderId, encObject.getMessageId());
            }
            return procResult.setFailResult(ErrorCode.DUPLICATED_MESSAGE_ID);
        }
        // build channel -- messageId map
        ConcurrentHashMap<Integer, Boolean> msgIdMap = channelMsgIdMap.get(client.getChanStr());
        if (msgIdMap == null) {
            ConcurrentHashMap<Integer, Boolean> tmpMsgIdMap = new ConcurrentHashMap<>();
            msgIdMap = channelMsgIdMap.putIfAbsent(client.getChanStr(), tmpMsgIdMap);
            if (msgIdMap == null) {
                msgIdMap = tmpMsgIdMap;
            }
        }
        msgIdMap.put(encObject.getMessageId(), newFuture.isAsyncCall());
        // send message
        if (newFuture.isAsyncCall()) {
            // process async report
            reqTimeouts.put(encObject.getMessageId(),
                    timerObj.newTimeout(new TimeoutTask(encObject.getMessageId()),
                            tcpConfig.getRequestTimeoutMs(), TimeUnit.MILLISECONDS));
            if (!client.write(clientTerm, encObject, procResult)) {
                Timeout timeout = reqTimeouts.remove(encObject.getMessageId());
                if (timeout != null) {
                    timeout.cancel();
                }
                rmvMsgStubInfo(encObject.getMessageId());
            }
            return procResult.setSuccess();
        } else {
            // process sync report
            if (!client.write(clientTerm, encObject, procResult)) {
                rmvMsgStubInfo(encObject.getMessageId());
                return false;
            }
            boolean retValue = newFuture.get(procResult,
                    tcpConfig.getRequestTimeoutMs(), TimeUnit.MILLISECONDS);
            if (rmvMsgStubInfo(encObject.getMessageId())) {
                if (procResult.getErrCode() == ErrorCode.SEND_WAIT_TIMEOUT.getErrCode()) {
                    client.setBusy(clientTerm);
                }
                client.decInFlightMsgCnt(clientTerm);
            }
            return retValue;
        }
    }

    private boolean rmvMsgStubInfo(int messageId) {
        TcpCallFuture curFuture = reqObjects.remove(messageId);
        if (curFuture == null) {
            return false;
        }
        ConcurrentHashMap<Integer, Boolean> msgIdMap =
                channelMsgIdMap.get(curFuture.getChanStr());
        if (msgIdMap != null) {
            msgIdMap.remove(curFuture.getMessageId());
        }
        return true;
    }

    public boolean getClientByRoundRobin(ProcessResult procResult) {
        if (!this.started.get()) {
            return procResult.setFailResult(ErrorCode.SDK_CLOSED);
        }
        List<String> curNodes = this.activeNodes;
        int curNodeSize = curNodes.size();
        if (curNodeSize == 0) {
            return procResult.setFailResult(ErrorCode.EMPTY_ACTIVE_NODE_SET);
        }
        String curNode;
        TcpNettyClient client;
        TcpNettyClient back1thClient = null;
        int nullClientCnt = 0;
        int unWritableCnt = 0;
        int startPos = reqSendIndex.getAndIncrement();
        for (int step = 0; step < curNodeSize; step++) {
            curNode = curNodes.get(Math.abs(startPos++) % curNodeSize);
            client = usingClientMaps.get(curNode);
            if (client == null) {
                nullClientCnt++;
                continue;
            }
            if (client.isActive()) {
                if (client.getChannel().isWritable()) {
                    if (tcpConfig.getMaxMsgInFlightPerConn() > 0
                            && client.getMsgInflightCnt() > tcpConfig.getMaxMsgInFlightPerConn()) {
                        back1thClient = client;
                    } else {
                        client.incClientUsingCnt();
                        return procResult.setSuccess(client);
                    }
                } else {
                    unWritableCnt++;
                }
            }
        }
        if (nullClientCnt == curNodeSize) {
            return procResult.setFailResult(ErrorCode.EMPTY_ACTIVE_NODE_SET);
        } else if (unWritableCnt + nullClientCnt == curNodeSize) {
            return procResult.setFailResult(ErrorCode.EMPTY_WRITABLE_NODE_SET);
        }
        if (back1thClient != null) {
            back1thClient.incClientUsingCnt();
            return procResult.setSuccess(back1thClient);
        }
        return procResult.setFailResult(ErrorCode.NO_VALID_REMOTE_NODE);
    }

    public void feedbackMsgResponse(String channelStr, DecodeObject decObject) {
        Timeout timeoutTask = this.reqTimeouts.remove(decObject.getMessageId());
        if (timeoutTask != null) {
            timeoutTask.cancel();
        }
        TcpCallFuture callFuture = this.reqObjects.remove(decObject.getMessageId());
        if (callFuture == null) {
            return;
        }
        long curTime = System.currentTimeMillis();
        ConcurrentHashMap<Integer, Boolean> inflightMsgIds = channelMsgIdMap.get(channelStr);
        if (inflightMsgIds != null) {
            inflightMsgIds.remove(decObject.getMessageId());
        }
        try {
            callFuture.onMessageAck(decObject.getSendResult());
        } catch (Throwable ex) {
            if (callbackExceptCnt.shouldPrint()) {
                logger.info("ClientMgr({}) response come, callback exception!",
                        senderId, ex);
            }
        } finally {
            this.descInflightMsgCnt(callFuture);
            if (decObject.getSendResult().isSuccess()) {
                baseSender.getMetricHolder().addCallbackSucMetric(callFuture.getGroupId(),
                        callFuture.getStreamId(), callFuture.getMsgCnt(),
                        (curTime - callFuture.getRtTime()), (System.currentTimeMillis() - curTime));
            } else {
                baseSender.getMetricHolder().addCallbackFailMetric(decObject.getSendResult().getErrCode(),
                        callFuture.getGroupId(), callFuture.getStreamId(), callFuture.getMsgCnt(),
                        (System.currentTimeMillis() - curTime));
            }
        }
    }

    public void setChannelFrozen(String channelStr) {
        Map<Integer, Boolean> inflightMsgIds = channelMsgIdMap.get(channelStr);
        if (inflightMsgIds == null) {
            return;
        }
        TcpCallFuture callFuture;
        TcpNettyClient nettyTcpClient;
        for (Integer messageId : inflightMsgIds.keySet()) {
            if (messageId == null) {
                continue;
            }
            callFuture = this.reqObjects.get(messageId);
            if (callFuture == null) {
                continue;
            }
            // find and process in using clients
            nettyTcpClient = usingClientMaps.get(callFuture.getClientAddr());
            if (nettyTcpClient != null
                    && nettyTcpClient.getChanTermId() == callFuture.getChanTerm()) {
                nettyTcpClient.setFrozen(callFuture.getChanTerm());
                return;
            }
            // find and process in deleting clients
            nettyTcpClient = deletingClientMaps.get(callFuture.getClientAddr());
            if (nettyTcpClient != null
                    && nettyTcpClient.getChanTermId() == callFuture.getChanTerm()) {
                nettyTcpClient.setFrozen(callFuture.getChanTerm());
                return;
            }
            break;
        }
    }

    public void notifyChannelDisconnected(String channelStr) {
        Map<Integer, Boolean> inflightMsgIds =
                channelMsgIdMap.remove(channelStr);
        if (inflightMsgIds == null || inflightMsgIds.isEmpty()) {
            return;
        }
        Timeout timeoutTask;
        TcpNettyClient nettyTcpClient;
        long curTime;
        for (Integer messageId : inflightMsgIds.keySet()) {
            if (messageId == null) {
                continue;
            }
            timeoutTask = this.reqTimeouts.remove(messageId);
            if (timeoutTask != null) {
                timeoutTask.cancel();
            }
            TcpCallFuture callFuture = this.reqObjects.remove(messageId);
            if (callFuture == null) {
                continue;
            }
            curTime = System.currentTimeMillis();
            // find and process in using clients
            nettyTcpClient = usingClientMaps.get(callFuture.getClientAddr());
            if (nettyTcpClient != null
                    && nettyTcpClient.getChanTermId() == callFuture.getChanTerm()) {
                try {
                    nettyTcpClient.getChannel().eventLoop().execute(
                            () -> callFuture.onMessageAck(new ProcessResult(ErrorCode.CONNECTION_BREAK)));
                } catch (Throwable ex) {
                    if (callbackExceptCnt.shouldPrint()) {
                        logger.info("ClientMgr({}) disconnected, callback exception!",
                                senderId, ex);
                    }
                } finally {
                    nettyTcpClient.decInFlightMsgCnt(callFuture.getChanTerm());
                    baseSender.getMetricHolder().addCallbackFailMetric(ErrorCode.CONNECTION_BREAK.getErrCode(),
                            callFuture.getGroupId(), callFuture.getStreamId(), callFuture.getMsgCnt(),
                            (System.currentTimeMillis() - curTime));
                }
                return;
            }
            // find and process in deleting clients
            nettyTcpClient = deletingClientMaps.get(callFuture.getClientAddr());
            if (nettyTcpClient != null
                    && nettyTcpClient.getChanTermId() == callFuture.getChanTerm()) {
                try {
                    nettyTcpClient.getChannel().eventLoop().execute(
                            () -> callFuture.onMessageAck(new ProcessResult(ErrorCode.CONNECTION_BREAK)));
                } catch (Throwable ex) {
                    if (callbackExceptCnt.shouldPrint()) {
                        logger.info("ClientMgr({}) disconnected, callback2 exception!",
                                senderId, ex);
                    }
                } finally {
                    nettyTcpClient.decInFlightMsgCnt(callFuture.getChanTerm());
                    baseSender.getMetricHolder().addCallbackFailMetric(ErrorCode.CONNECTION_BREAK.getErrCode(),
                            callFuture.getGroupId(), callFuture.getStreamId(), callFuture.getMsgCnt(),
                            (System.currentTimeMillis() - curTime));
                }
            }
        }
    }

    public String getSenderId() {
        return this.senderId;
    }

    public int getNextMsgId() {
        return messageIdGen.getNextInt();
    }

    private void descInflightMsgCnt(TcpCallFuture callFuture) {
        TcpNettyClient nettyTcpClient = usingClientMaps.get(callFuture.getClientAddr());
        if (nettyTcpClient != null
                && nettyTcpClient.getChanTermId() == callFuture.getChanTerm()) {
            nettyTcpClient.decInFlightMsgCnt(callFuture.getChanTerm());
            return;
        }
        nettyTcpClient = deletingClientMaps.get(callFuture.getClientAddr());
        if (nettyTcpClient != null
                && nettyTcpClient.getChanTermId() == callFuture.getChanTerm()) {
            nettyTcpClient.decInFlightMsgCnt(callFuture.getChanTerm());
        }
    }

    private Bootstrap buildBootstrap(ThreadFactory selfFactory) {
        if (selfFactory == null) {
            selfFactory = new DefaultThreadFactory(
                    "sdk-netty-workers", Thread.currentThread().isDaemon());
        }
        EventLoopGroup eventLoopGroup = EventLoopUtil.newEventLoopGroup(
                tcpConfig.getNettyWorkerThreadNum(), tcpConfig.isEnableEpollBusyWait(), selfFactory);
        Bootstrap tmpBootstrap = new Bootstrap();
        tmpBootstrap.group(eventLoopGroup);
        tmpBootstrap.option(ChannelOption.TCP_NODELAY, true);
        tmpBootstrap.option(ChannelOption.SO_REUSEADDR, true);
        tmpBootstrap.channel(EventLoopUtil.getClientSocketChannelClass(eventLoopGroup));
        if (tcpConfig.getRcvBufferSize() > 0) {
            tmpBootstrap.option(ChannelOption.SO_RCVBUF, tcpConfig.getRcvBufferSize());
        }
        if (tcpConfig.getSendBufferSize() > 0) {
            tmpBootstrap.option(ChannelOption.SO_SNDBUF, tcpConfig.getSendBufferSize());
        }
        tmpBootstrap.handler(new ClientPipelineFactory(this));
        return tmpBootstrap;
    }

    private class MaintThread extends Thread {

        private volatile boolean bShutDown;

        public MaintThread() {
            bShutDown = false;
            this.setName("ClientMgrMaint-" + senderId);
        }

        public void shutDown() {
            logger.info("ClientMgr({}) shutdown MaintThread!", senderId);
            bShutDown = true;
            this.interrupt();
        }

        @Override
        public void run() {
            logger.info("ClientMgr({}) start MaintThread!", senderId);
            long curTime;
            long checkRound = 0L;
            boolean printFailNodes;
            ProcessResult procResult = new ProcessResult();
            Set<String> failNodes = new HashSet<>();
            while (!bShutDown) {
                printFailNodes = ((Math.abs(checkRound++) % 90L) == 0L);
                try {
                    curTime = System.currentTimeMillis();
                    // clean deleting nodes
                    if (deletingClientMaps != null && !deletingClientMaps.isEmpty()) {
                        if (lastUpdateTime > 0
                                && curTime - lastUpdateTime > tcpConfig.getConCloseWaitPeriodMs()) {
                            for (TcpNettyClient client : deletingClientMaps.values()) {
                                if (client == null) {
                                    continue;
                                }
                                client.close(false);
                            }
                            deletingClientMaps.clear();
                        }
                    }
                    // check and keepalive using nodes
                    for (TcpNettyClient nettyTcpClient : usingClientMaps.values()) {
                        if (nettyTcpClient == null) {
                            continue;
                        }
                        if (nettyTcpClient.isActive()) {
                            if (nettyTcpClient.isIdleClient(curTime)) {
                                nettyTcpClient.sendHeartBeatMsg(procResult);
                            }
                        } else {
                            if ((nettyTcpClient.getClientUsingCnt() <= 0)
                                    && (nettyTcpClient.getMsgInflightCnt() <= 0)
                                    || (curTime - nettyTcpClient.getChanInvalidTime() > tcpConfig
                                            .getConCloseWaitPeriodMs())) {
                                nettyTcpClient.reconnect(false, channelTermGen.incrementAndGet());
                            }
                        }
                        if (printFailNodes) {
                            if (nettyTcpClient.isConFailNodes()) {
                                failNodes.add(nettyTcpClient.getClientAddr());
                            }
                        }
                    }
                } catch (Throwable ex) {
                    if (exptCounter.shouldPrint()) {
                        logger.warn("ClientMgr({}) MaintThread throw exception", senderId, ex);
                    }
                }
                if (printFailNodes && !failNodes.isEmpty()) {
                    logger.warn("ClientMgr({}) found continue connect fail nodes: {}",
                            senderId, failNodes);
                    failNodes.clear();
                }
                if (bShutDown) {
                    break;
                }
                ProxyUtils.sleepSomeTime(2000L);
            }
            logger.info("ClientMgr({}) exit MaintThread!", senderId);
        }
    }

    /**
     * Time out task call back handle
     */
    public class TimeoutTask implements TimerTask {

        private final int messageId;

        public TimeoutTask(int messageId) {
            this.messageId = messageId;
        }

        @Override
        public void run(Timeout timeout) throws Exception {
            Timeout timeoutMsg = reqTimeouts.remove(messageId);
            if (timeoutMsg != null) {
                timeoutMsg.cancel();
            }
            TcpCallFuture future = reqObjects.remove(messageId);
            if (future == null) {
                return;
            }
            long curTime = System.currentTimeMillis();
            ConcurrentHashMap<Integer, Boolean> messageIdMap;
            // find and process in using clients
            TcpNettyClient nettyTcpClient = usingClientMaps.get(future.getClientAddr());
            if (nettyTcpClient != null
                    && nettyTcpClient.getChanTermId() == future.getChanTerm()) {
                messageIdMap = channelMsgIdMap.get(future.getChanStr());
                if (messageIdMap != null) {
                    messageIdMap.remove(messageId);
                }
                try {
                    nettyTcpClient.getChannel().eventLoop().execute(
                            () -> future.onMessageAck(new ProcessResult(ErrorCode.SEND_WAIT_TIMEOUT)));
                } catch (Throwable ex) {
                    if (callbackExceptCnt.shouldPrint()) {
                        logger.info("ClientMgr({}) msg timeout, callback exception!",
                                senderId, ex);
                    }
                } finally {
                    nettyTcpClient.decInFlightMsgCnt(future.getChanTerm());
                    baseSender.getMetricHolder().addCallbackFailMetric(ErrorCode.SEND_WAIT_TIMEOUT.getErrCode(),
                            future.getGroupId(), future.getStreamId(), future.getMsgCnt(),
                            (System.currentTimeMillis() - curTime));
                }
                return;
            }
            // find and process in deleting clients
            nettyTcpClient = deletingClientMaps.get(future.getClientAddr());
            if (nettyTcpClient != null
                    && nettyTcpClient.getChanTermId() == future.getChanTerm()) {
                messageIdMap = channelMsgIdMap.get(future.getChanStr());
                if (messageIdMap != null) {
                    messageIdMap.remove(messageId);
                }
                try {
                    nettyTcpClient.getChannel().eventLoop().execute(
                            () -> future.onMessageAck(new ProcessResult(ErrorCode.SEND_WAIT_TIMEOUT)));
                } catch (Throwable ex) {
                    if (callbackExceptCnt.shouldPrint()) {
                        logger.info("ClientMgr({}) msg timeout, callback2 exception!",
                                senderId, ex);
                    }
                } finally {
                    nettyTcpClient.decInFlightMsgCnt(future.getChanTerm());
                    baseSender.getMetricHolder().addCallbackFailMetric(ErrorCode.SEND_WAIT_TIMEOUT.getErrCode(),
                            future.getGroupId(), future.getStreamId(), future.getMsgCnt(),
                            (System.currentTimeMillis() - curTime));
                }
            }
        }
    }
}
