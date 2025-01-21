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

import org.apache.inlong.common.msg.MsgType;
import org.apache.inlong.sdk.dataproxy.common.ErrorCode;
import org.apache.inlong.sdk.dataproxy.common.ProcessResult;
import org.apache.inlong.sdk.dataproxy.common.ProxyClientConfig;
import org.apache.inlong.sdk.dataproxy.common.SdkConsts;
import org.apache.inlong.sdk.dataproxy.config.HostInfo;
import org.apache.inlong.sdk.dataproxy.network.tcp.codec.EncodeObject;
import org.apache.inlong.sdk.dataproxy.sender.tcp.TcpMsgSenderConfig;
import org.apache.inlong.sdk.dataproxy.utils.AuthzUtils;
import org.apache.inlong.sdk.dataproxy.utils.LogCounter;
import org.apache.inlong.sdk.dataproxy.utils.ProxyUtils;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.security.SecureRandom;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * TCP Netty client class
 *
 * Used to manage TCP netty client, including connection, closing, keep-alive, sending status check, etc.
 */
public class TcpNettyClient {

    private final static int CLIENT_FAIL_CONNECT_WAIT_CNT = 3;
    private static final Logger logger = LoggerFactory.getLogger(TcpNettyClient.class);
    private static final LogCounter conExptCnt = new LogCounter(10, 100000, 60 * 1000L);
    private static final LogCounter hbExptCnt = new LogCounter(10, 100000, 60 * 1000L);
    private final static int CLIENT_STATUS_INIT = -1;
    private final static int CLIENT_STATUS_READY = 0;
    private final static int CLIENT_STATUS_FROZEN = 1;
    private final static int CLIENT_STATUS_DEAD = 2;
    private final static int CLIENT_STATUS_BUSY = 3;

    private final String senderId;
    private final TcpMsgSenderConfig tcpConfig;
    private final Bootstrap bootstrap;
    private final HostInfo hostInfo;
    private final AtomicInteger conStatus = new AtomicInteger(CLIENT_STATUS_INIT);
    private final AtomicLong channelTermId = new AtomicLong(0);
    private final AtomicInteger clientUsingCnt = new AtomicInteger(0);
    private final AtomicInteger msgSentCnt = new AtomicInteger(0);
    private final AtomicInteger msgInflightCnt = new AtomicInteger(0);
    private final AtomicLong chanInvalidTime = new AtomicLong(0);
    private final AtomicInteger conFailCnt = new AtomicInteger(0);
    private final AtomicLong lstConFailTime = new AtomicLong(0);
    private final AtomicInteger chanSyncTimeoutCnt = new AtomicInteger(0);
    private final AtomicLong chanFstBusyTime = new AtomicLong(0);
    private final ReentrantReadWriteLock rw = new ReentrantReadWriteLock();
    private Channel channel = null;
    private String channelStr = "";
    private int lstRoundSentCnt = -1;
    private int clientIdleRounds = 0;
    private long fstIdleTime = 0;

    public TcpNettyClient(String senderId,
            Bootstrap bootstrap, HostInfo hostInfo, TcpMsgSenderConfig tcpConfig) {
        this.conStatus.set(CLIENT_STATUS_INIT);
        this.hostInfo = hostInfo;
        this.tcpConfig = tcpConfig;
        this.senderId = senderId;
        this.bootstrap = bootstrap;
    }

    public boolean connect(boolean needPrint, long termId) {
        // Initial status
        this.conStatus.set(CLIENT_STATUS_INIT);
        long curTime = System.currentTimeMillis();
        final CountDownLatch awaitLatch = new CountDownLatch(1);
        // Build connect to server
        ChannelFuture future = bootstrap.connect(
                new InetSocketAddress(hostInfo.getHostName(), hostInfo.getPortNumber()));
        future.addListener(new ChannelFutureListener() {

            public void operationComplete(ChannelFuture arg0) throws Exception {
                awaitLatch.countDown();
            }
        });
        try {
            // Wait until the connection is built.
            awaitLatch.await(tcpConfig.getConnectTimeoutMs(), TimeUnit.MILLISECONDS);
        } catch (Throwable ex) {
            if (conExptCnt.shouldPrint()) {
                logger.warn("NettyClient({}) connect to {} exception",
                        senderId, hostInfo.getReferenceName(), ex);
            }
            return false;
        }
        // Return if no connection is built.
        if (!future.isSuccess()) {
            this.conFailCnt.getAndIncrement();
            this.lstConFailTime.set(System.currentTimeMillis());
            if (conExptCnt.shouldPrint()) {
                logger.warn("NettyClient({}) connect to {} failure, wast {}ms",
                        senderId, hostInfo.getReferenceName(), (System.currentTimeMillis() - curTime));
            }
            return false;
        }
        this.channelTermId.set(termId);
        this.channel = future.channel();
        this.channelStr = this.channel.toString();
        this.conFailCnt.set(0);
        this.msgSentCnt.set(0);
        this.chanSyncTimeoutCnt.set(0);
        this.msgInflightCnt.set(0);
        this.conStatus.set(CLIENT_STATUS_READY);
        if (needPrint) {
            logger.info("NettyClient({}) connect to {} success, wast {}ms",
                    senderId, channel.toString(), (System.currentTimeMillis() - curTime));
        }
        return true;
    }

    public boolean close(boolean needPrint) {
        this.conStatus.set(CLIENT_STATUS_DEAD);
        long curTime = System.currentTimeMillis();
        this.chanInvalidTime.set(curTime);
        final CountDownLatch awaitLatch = new CountDownLatch(1);
        boolean ret = true;
        String channelStr = "";
        try {
            if (channel == null) {
                channelStr = hostInfo.getReferenceName();
            } else {
                channelStr = channel.toString();
                ChannelFuture future = channel.close();
                future.addListener(new ChannelFutureListener() {

                    public void operationComplete(ChannelFuture arg0) throws Exception {
                        awaitLatch.countDown();
                    }
                });
                // Wait until the connection is close.
                awaitLatch.await(tcpConfig.getConCloseWaitPeriodMs(), TimeUnit.MILLISECONDS);
                // Return if close this connection fail.
                if (!future.isSuccess()) {
                    ret = false;
                }
            }
        } catch (Throwable ex) {
            if (conExptCnt.shouldPrint()) {
                logger.warn("NettyClient({}) close {} exception", senderId, channelStr, ex);
            }
            ret = false;
        } finally {
            this.channel = null;
            this.channelStr = "";
            this.msgInflightCnt.set(0);
        }
        if (needPrint) {
            if (ret) {
                logger.info("NettyClient({}) close {} success, wast {}ms",
                        this.senderId, channelStr, (System.currentTimeMillis() - curTime));
            } else {
                logger.info("NettyClient({}) close {} failure, wast {}ms",
                        this.senderId, channelStr, (System.currentTimeMillis() - curTime));
            }
        }
        return ret;
    }

    public boolean reconnect(boolean needPrint, long termId) {
        long curTime = System.currentTimeMillis();
        if ((this.conFailCnt.get() >= CLIENT_FAIL_CONNECT_WAIT_CNT)
                && (curTime - lstConFailTime.get() < this.tcpConfig.getReconFailWaitMs())) {
            return false;
        }
        int curStatus = this.conStatus.get();
        if (curStatus == CLIENT_STATUS_READY) {
            return true;
        } else if (curStatus == CLIENT_STATUS_BUSY) {
            if (curTime - this.chanInvalidTime.get() < this.tcpConfig.getBusyReconnectWaitMs()) {
                return false;
            }
        } else if (curStatus == CLIENT_STATUS_FROZEN) {
            if (curTime - this.chanInvalidTime.get() < this.tcpConfig.getFrozenReconnectWaitMs()) {
                return false;
            }
        }
        rw.writeLock().lock();
        try {
            if (this.conStatus.get() == CLIENT_STATUS_READY) {
                return true;
            }
            curTime = System.currentTimeMillis();
            this.close(false);
            if (this.connect(false, termId)) {
                if (needPrint) {
                    logger.info("NettyClient({}) re-connect to {} success, wast {}ms",
                            senderId, this.channel.toString(), System.currentTimeMillis() - curTime);
                }
                return true;
            } else {
                if (needPrint) {
                    logger.info("NettyClient({}) re-connect to {} failure",
                            senderId, hostInfo.getReferenceName());
                }
                return false;
            }
        } finally {
            rw.writeLock().unlock();
        }
    }

    public int incClientUsingCnt() {
        return clientUsingCnt.incrementAndGet();
    }

    public int getClientUsingCnt() {
        return clientUsingCnt.get();
    }

    public int decClientUsingCnt() {
        return clientUsingCnt.decrementAndGet();
    }

    public boolean write(long termId, EncodeObject encodeObject, ProcessResult procResult) {
        if (this.conStatus.get() != CLIENT_STATUS_READY) {
            return procResult.setFailResult(ErrorCode.CONNECTION_UNAVAILABLE);
        }
        this.rw.readLock().lock();
        if (this.conStatus.get() != CLIENT_STATUS_READY) {
            return procResult.setFailResult(ErrorCode.CONNECTION_UNAVAILABLE);
        }
        if (this.channelTermId.get() != termId) {
            return procResult.setFailResult(ErrorCode.CONNECTION_BREAK);
        }
        if (!(this.channel.isOpen()
                && this.channel.isActive() && this.channel.isWritable())) {
            return procResult.setFailResult(ErrorCode.CONNECTION_UNWRITABLE);
        }
        try {
            this.msgSentCnt.incrementAndGet();
            this.channel.writeAndFlush(encodeObject);
            this.msgInflightCnt.incrementAndGet();
        } catch (Throwable ex) {
            if (conExptCnt.shouldPrint()) {
                logger.warn("NettyClient({}) write {} exception",
                        this.senderId, this.channel.toString(), ex);
            }
            return procResult.setFailResult(ErrorCode.CONNECTION_WRITE_EXCEPTION, ex.getMessage());
        } finally {
            this.rw.readLock().unlock();
        }
        return procResult.setSuccess();
    }

    public void setFrozen(long termId) {
        if (this.channelTermId.get() != termId
                || this.conStatus.get() != CLIENT_STATUS_READY) {
            return;
        }
        boolean changed = false;
        rw.readLock().lock();
        try {
            if (this.channelTermId.get() != termId) {
                return;
            }
            int curStatus = this.conStatus.get();
            if (curStatus == CLIENT_STATUS_READY) {
                this.conStatus.compareAndSet(curStatus, CLIENT_STATUS_FROZEN);
                this.chanInvalidTime.set(System.currentTimeMillis());
                changed = true;
            }
        } finally {
            rw.readLock().unlock();
        }
        if (changed) {
            logger.warn("NettyClient({}) set {} frozen!", senderId, hostInfo.getReferenceName());
        }
    }

    public void setBusy(long termId) {
        if (this.channelTermId.get() != termId
                || this.conStatus.get() != CLIENT_STATUS_READY) {
            return;
        }
        boolean changed = false;
        long befTime;
        int curTimeoutCnt;
        long curTime = System.currentTimeMillis();
        rw.readLock().lock();
        try {
            if (this.channelTermId.get() != termId) {
                return;
            }
            befTime = this.chanFstBusyTime.get();
            if (curTime - befTime >= tcpConfig.getSyncMsgTimeoutChkDurMs()) {
                if (this.chanFstBusyTime.compareAndSet(befTime, curTime)) {
                    this.chanSyncTimeoutCnt.set(0);
                }
            }
            curTimeoutCnt = this.chanSyncTimeoutCnt.incrementAndGet();
            if (tcpConfig.getMaxAllowedSyncMsgTimeoutCnt() >= 0
                    && curTimeoutCnt < tcpConfig.getMaxAllowedSyncMsgTimeoutCnt()) {
                return;
            }
            int curStatus = this.conStatus.get();
            if (curStatus == CLIENT_STATUS_READY) {
                this.conStatus.compareAndSet(curStatus, CLIENT_STATUS_BUSY);
                this.chanInvalidTime.set(System.currentTimeMillis());
                changed = true;
            }
        } finally {
            rw.readLock().unlock();
        }
        if (changed) {
            logger.warn("NettyClient({}) set {} busy!", senderId, hostInfo.getReferenceName());
        }
    }

    public boolean isActive() {
        if (this.conStatus.get() != CLIENT_STATUS_READY) {
            return false;
        }
        rw.readLock().lock();
        try {
            return ((this.conStatus.get() == CLIENT_STATUS_READY)
                    && channel != null && channel.isOpen() && channel.isActive());
        } finally {
            rw.readLock().unlock();
        }
    }

    public void sendHeartBeatMsg(ProcessResult procResult) {
        if (!isActive()) {
            logger.warn("NettyClient({}) to {} hb inActive",
                    this.senderId, hostInfo.getReferenceName());
            return;
        }
        if (!channel.isWritable()) {
            if (hbExptCnt.shouldPrint()) {
                logger.warn("NettyClient({}) to {} hb write_over_water", this.senderId, channelStr);
            }
            return;
        }
        EncodeObject encodeObject = buildHeartBeatMsg(this.senderId, tcpConfig);
        if (encodeObject == null) {
            if (hbExptCnt.shouldPrint()) {
                logger.warn("NettyClient({}) to {} hb failure:{}!",
                        this.senderId, channelStr, procResult.getErrMsg());
            }
        }
        try {
            write(channelTermId.get(), encodeObject, procResult);
        } catch (Throwable ex) {
            if (hbExptCnt.shouldPrint()) {
                logger.warn("NettyClient({}) send to {} hb exception ",
                        this.senderId, channelStr, ex);
            }
        }
        procResult.setSuccess();
    }

    public boolean isIdleClient(long curTime) {
        int curSentCnt = this.msgSentCnt.get();
        if (curSentCnt != this.lstRoundSentCnt) {
            this.lstRoundSentCnt = curSentCnt;
            this.clientIdleRounds = 0;
            return false;
        }
        if (this.clientIdleRounds++ == 0) {
            this.fstIdleTime = curTime;
            return false;
        }
        return curTime - this.fstIdleTime >= 30000L;
    }

    public String getClientAddr() {
        return hostInfo.getReferenceName();
    }

    public Channel getChannel() {
        return channel;
    }

    public String getChanStr() {
        return channelStr;
    }

    public void decInFlightMsgCnt(long termId) {
        if (this.channelTermId.get() != termId) {
            return;
        }
        this.msgInflightCnt.decrementAndGet();
    }

    public int getMsgInflightCnt() {
        return msgInflightCnt.get();
    }

    public boolean isConFailNodes() {
        return (conFailCnt.get() >= CLIENT_FAIL_CONNECT_WAIT_CNT);
    }

    public long getChanTermId() {
        return channelTermId.get();
    }

    public long getChanInvalidTime() {
        return chanInvalidTime.get();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        TcpNettyClient other = (TcpNettyClient) obj;
        if (channel == null) {
            return other.channel == null;
        } else {
            return channel.equals(other.channel);
        }
    }

    private EncodeObject buildHeartBeatMsg(String senderId, ProxyClientConfig configure) {
        EncodeObject encObject = new EncodeObject(null, null,
                MsgType.MSG_BIN_HEARTBEAT, System.currentTimeMillis());
        encObject.setMessageIdInfo(0);
        int intMsgType = encObject.getMsgType().getValue();
        Map<String, String> newAttrs = new HashMap<>();
        if (configure.isEnableReportAuthz()) {
            intMsgType |= SdkConsts.FLAG_ALLOW_AUTH;
            long timestamp = System.currentTimeMillis();
            int nonce = new SecureRandom(String.valueOf(timestamp).getBytes()).nextInt(Integer.MAX_VALUE);
            String signature = AuthzUtils.generateSignature(
                    configure.getRptUserName(), timestamp, nonce, configure.getRptSecretKey());
            if (StringUtils.isBlank(signature)) {
                return null;
            }
            newAttrs.put("_userName", configure.getRptUserName());
            newAttrs.put("_clientIP", ProxyUtils.getLocalIp());
            newAttrs.put("_signature", signature);
            newAttrs.put("_timeStamp", String.valueOf(timestamp));
            newAttrs.put("_nonce", String.valueOf(nonce));
        }
        encObject.setAttrInfo(intMsgType, false, null, newAttrs);
        return encObject;
    }

}
