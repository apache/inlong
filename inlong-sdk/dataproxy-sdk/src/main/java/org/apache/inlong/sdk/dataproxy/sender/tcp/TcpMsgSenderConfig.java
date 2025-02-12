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

package org.apache.inlong.sdk.dataproxy.sender.tcp;

import org.apache.inlong.common.msg.MsgType;
import org.apache.inlong.sdk.dataproxy.common.ProxyClientConfig;
import org.apache.inlong.sdk.dataproxy.common.ReportProtocol;
import org.apache.inlong.sdk.dataproxy.common.SdkConsts;
import org.apache.inlong.sdk.dataproxy.exception.ProxySdkException;
import org.apache.inlong.sdk.dataproxy.utils.ProxyUtils;

import java.util.Objects;

/**
 * TCP Message Sender configuration:
 *
 * Used to define the setting related to the sender reporting data using the TCP protocol, including
 * the message type, request timeout, whether to enable data compression,
 * the number of netty worker threads, and other configuration settings
 */
public class TcpMsgSenderConfig extends ProxyClientConfig implements Cloneable {

    // msg report type
    private MsgType sdkMsgType = MsgType.MSG_BIN_MULTI_BODY;
    // whether separate event by line feed
    private boolean separateEventByLF = true;
    // whether enable data compress
    private boolean enableDataCompress = true;
    // min compress enable data length
    private int minCompEnableLength = SdkConsts.VAL_DEF_COMPRESS_ENABLE_SIZE;
    // whether enable epoll busy wait
    private boolean enableEpollBusyWait = false;
    // netty worker thread num
    private int nettyWorkerThreadNum = SdkConsts.VAL_DEF_TCP_NETTY_WORKER_THREAD_NUM;
    // socket receive buffer
    private int rcvBufferSize = SdkConsts.DEFAULT_RECEIVE_BUFFER_SIZE;
    // socket send buffer
    private int sendBufferSize = SdkConsts.DEFAULT_SEND_BUFFER_SIZE;
    // connect timeout in milliseconds
    private int connectTimeoutMs = SdkConsts.VAL_DEF_TCP_CONNECT_TIMEOUT_MS;
    // request timeout in milliseconds
    private long requestTimeoutMs = SdkConsts.VAL_DEF_REQUEST_TIMEOUT_MS;
    // connect close wait period in milliseconds
    private long conCloseWaitPeriodMs =
            SdkConsts.VAL_DEF_REQUEST_TIMEOUT_MS + SdkConsts.VAL_DEF_CONNECT_CLOSE_DELAY_MS;
    // max message count per connection
    private int maxMsgInFlightPerConn = SdkConsts.MAX_INFLIGHT_MSG_COUNT_PER_CONNECTION;
    // client reconnect wait period in ms
    private long frozenReconnectWaitMs = SdkConsts.VAL_DEF_FROZEN_RECONNECT_WAIT_MS;
    private long busyReconnectWaitMs = SdkConsts.VAL_DEF_BUSY_RECONNECT_WAIT_MS;
    private long reconFailWaitMs = SdkConsts.VAL_DEF_RECONNECT_FAIL_WAIT_MS;
    // the maximum allowed synchronization message timeout
    private int maxAllowedSyncMsgTimeoutCnt = SdkConsts.VAL_DEF_SYNC_MSG_TIMEOUT_CNT;
    // the synchronization message timeout check duration ms
    private long syncMsgTimeoutChkDurMs = SdkConsts.VAL_DEF_SYNC_TIMEOUT_CHK_DUR_MS;

    public TcpMsgSenderConfig(boolean visitMgrByHttps,
            String managerIP, int managerPort, String groupId) throws ProxySdkException {
        super(visitMgrByHttps, managerIP, managerPort, groupId, ReportProtocol.TCP, null);
    }

    public TcpMsgSenderConfig(String managerAddress, String groupId) throws ProxySdkException {
        super(managerAddress, groupId, ReportProtocol.TCP, null);
    }

    public TcpMsgSenderConfig(boolean visitMgrByHttps, String managerIP, int managerPort,
            String groupId, String mgrAuthSecretId, String mgrAuthSecretKey) throws ProxySdkException {
        super(visitMgrByHttps, managerIP, managerPort, groupId, ReportProtocol.TCP, null);
        this.setMgrAuthzInfo(true, mgrAuthSecretId, mgrAuthSecretKey);
    }

    public TcpMsgSenderConfig(String managerAddress,
            String groupId, String mgrAuthSecretId, String mgrAuthSecretKey) throws ProxySdkException {
        super(managerAddress, groupId, ReportProtocol.TCP, null);
        this.setMgrAuthzInfo(true, mgrAuthSecretId, mgrAuthSecretKey);
    }

    public MsgType getSdkMsgType() {
        return sdkMsgType;
    }

    public void setSdkMsgType(MsgType sdkMsgType) {
        if (!ProxyUtils.SdkAllowedMsgType.contains(sdkMsgType)) {
            throw new IllegalArgumentException(
                    "Only allowed msgType:" + ProxyUtils.SdkAllowedMsgType);
        }
        this.sdkMsgType = sdkMsgType;
    }

    public boolean isSeparateEventByLF() {
        return separateEventByLF;
    }

    public void setSeparateEventByLF(boolean separateEventByLF) {
        this.separateEventByLF = separateEventByLF;
    }

    public boolean isEnableDataCompress() {
        return enableDataCompress;
    }

    public void setEnableDataCompress(boolean enableDataCompress) {
        this.enableDataCompress = enableDataCompress;
    }

    public int getMinCompEnableLength() {
        return minCompEnableLength;
    }

    public void setMinCompEnableLength(int minCompEnableLength) {
        this.minCompEnableLength = Math.max(
                SdkConsts.VAL_MIN_COMPRESS_ENABLE_SIZE, minCompEnableLength);
    }

    public boolean isEnableEpollBusyWait() {
        return enableEpollBusyWait;
    }

    public void setEnableEpollBusyWait(boolean enableEpollBusyWait) {
        this.enableEpollBusyWait = enableEpollBusyWait;
    }

    public int getNettyWorkerThreadNum() {
        return nettyWorkerThreadNum;
    }

    public void setNettyWorkerThreadNum(int nettyWorkerThreadNum) {
        this.nettyWorkerThreadNum = Math.max(
                SdkConsts.VAL_MIN_TCP_NETTY_WORKER_THREAD_NUM, nettyWorkerThreadNum);
    }

    public int getRcvBufferSize() {
        return rcvBufferSize;
    }

    public void setRcvBufferSize(int rcvBufferSize) {
        this.rcvBufferSize = Math.min(rcvBufferSize, Integer.MAX_VALUE - 1);
    }

    public int getSendBufferSize() {
        return sendBufferSize;
    }

    public void setSendBufferSize(int sendBufferSize) {
        this.sendBufferSize = Math.min(sendBufferSize, Integer.MAX_VALUE - 1);
    }

    public int getConnectTimeoutMs() {
        return connectTimeoutMs;
    }

    public void setConnectTimeoutMs(int connectTimeoutMs) {
        if (connectTimeoutMs >= SdkConsts.VAL_MIN_CONNECT_TIMEOUT_MS) {
            this.connectTimeoutMs = connectTimeoutMs;
        }
    }

    public long getRequestTimeoutMs() {
        return requestTimeoutMs;
    }

    public void setRequestTimeoutMs(long requestTimeoutMs) {
        if (requestTimeoutMs >= SdkConsts.VAL_MIN_REQUEST_TIMEOUT_MS) {
            this.requestTimeoutMs = requestTimeoutMs;
            this.conCloseWaitPeriodMs =
                    this.requestTimeoutMs + SdkConsts.VAL_DEF_CONNECT_CLOSE_DELAY_MS;
        }
    }

    public long getConCloseWaitPeriodMs() {
        return conCloseWaitPeriodMs;
    }

    public void setConCloseWaitPeriodMs(long conCloseWaitPeriodMs) {
        if (conCloseWaitPeriodMs >= 0) {
            this.conCloseWaitPeriodMs = conCloseWaitPeriodMs;
        }
    }

    public int getMaxMsgInFlightPerConn() {
        return maxMsgInFlightPerConn;
    }

    public void setMaxMsgInFlightPerConn(int maxMsgInFlightPerConn) {
        this.maxMsgInFlightPerConn = maxMsgInFlightPerConn;
    }

    public long getFrozenReconnectWaitMs() {
        return frozenReconnectWaitMs;
    }

    public void setFrozenReconnectWaitMs(long frozenReconnectWaitMs) {
        if (frozenReconnectWaitMs > SdkConsts.VAL_MAX_RECONNECT_WAIT_MS) {
            this.frozenReconnectWaitMs = SdkConsts.VAL_MAX_RECONNECT_WAIT_MS;
        }
    }

    public long getBusyReconnectWaitMs() {
        return busyReconnectWaitMs;
    }

    public void setBusyReconnectWaitMs(long busyReconnectWaitMs) {
        if (busyReconnectWaitMs > SdkConsts.VAL_MAX_RECONNECT_WAIT_MS) {
            this.busyReconnectWaitMs = SdkConsts.VAL_MAX_RECONNECT_WAIT_MS;
        }
    }

    public long getReconFailWaitMs() {
        return reconFailWaitMs;
    }

    public void setReconFailWaitMs(long reconFailWaitMs) {
        if (reconFailWaitMs > SdkConsts.VAL_MAX_RECONNECT_WAIT_MS) {
            this.reconFailWaitMs = SdkConsts.VAL_MAX_RECONNECT_WAIT_MS;
        }
    }

    public int getMaxAllowedSyncMsgTimeoutCnt() {
        return maxAllowedSyncMsgTimeoutCnt;
    }

    public void setMaxAllowedSyncMsgTimeoutCnt(int maxAllowedSyncMsgTimeoutCnt) {
        this.maxAllowedSyncMsgTimeoutCnt = maxAllowedSyncMsgTimeoutCnt;
    }

    public long getSyncMsgTimeoutChkDurMs() {
        return syncMsgTimeoutChkDurMs;
    }

    public void setSyncMsgTimeoutChkDurMs(long syncMsgTimeoutChkDurMs) {
        this.syncMsgTimeoutChkDurMs = Math.max(
                SdkConsts.VAL_MIN_SYNC_TIMEOUT_CHK_DUR_MS, syncMsgTimeoutChkDurMs);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        if (!super.equals(o))
            return false;
        TcpMsgSenderConfig config = (TcpMsgSenderConfig) o;
        return separateEventByLF == config.separateEventByLF
                && enableDataCompress == config.enableDataCompress
                && minCompEnableLength == config.minCompEnableLength
                && enableEpollBusyWait == config.enableEpollBusyWait
                && nettyWorkerThreadNum == config.nettyWorkerThreadNum
                && rcvBufferSize == config.rcvBufferSize
                && sendBufferSize == config.sendBufferSize
                && connectTimeoutMs == config.connectTimeoutMs
                && requestTimeoutMs == config.requestTimeoutMs
                && conCloseWaitPeriodMs == config.conCloseWaitPeriodMs
                && maxMsgInFlightPerConn == config.maxMsgInFlightPerConn
                && frozenReconnectWaitMs == config.frozenReconnectWaitMs
                && busyReconnectWaitMs == config.busyReconnectWaitMs
                && reconFailWaitMs == config.reconFailWaitMs
                && maxAllowedSyncMsgTimeoutCnt == config.maxAllowedSyncMsgTimeoutCnt
                && syncMsgTimeoutChkDurMs == config.syncMsgTimeoutChkDurMs
                && sdkMsgType == config.sdkMsgType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), sdkMsgType, separateEventByLF,
                enableDataCompress, minCompEnableLength, enableEpollBusyWait,
                nettyWorkerThreadNum, rcvBufferSize, sendBufferSize, connectTimeoutMs,
                requestTimeoutMs, conCloseWaitPeriodMs, maxMsgInFlightPerConn,
                frozenReconnectWaitMs, busyReconnectWaitMs, reconFailWaitMs,
                maxAllowedSyncMsgTimeoutCnt, syncMsgTimeoutChkDurMs);
    }

    @Override
    public TcpMsgSenderConfig clone() {
        try {
            TcpMsgSenderConfig copy = (TcpMsgSenderConfig) super.clone();
            if (copy != null) {
                copy.sdkMsgType = this.sdkMsgType;
            }
            return copy;
        } catch (Throwable ex) {
            logger.warn("Failed to clone TcpMsgSenderConfig", ex);
            return null;
        }
    }

    @Override
    public String toString() {
        final StringBuilder strBuff =
                new StringBuilder("TcpMsgSenderConfig{sdkMsgType=").append(sdkMsgType)
                        .append(", separateEventByLF=").append(separateEventByLF)
                        .append(", enableDataCompress=").append(enableDataCompress)
                        .append(", minCompEnableLength=").append(minCompEnableLength)
                        .append(", enableEpollBusyWait=").append(enableEpollBusyWait)
                        .append(", nettyWorkerThreadNum=").append(nettyWorkerThreadNum)
                        .append(", rcvBufferSize=").append(rcvBufferSize)
                        .append(", sendBufferSize=").append(sendBufferSize)
                        .append(", connectTimeoutMs=").append(connectTimeoutMs)
                        .append(", requestTimeoutMs=").append(requestTimeoutMs)
                        .append(", conCloseWaitPeriodMs=").append(conCloseWaitPeriodMs)
                        .append(", maxMsgInFlightPerConn=").append(maxMsgInFlightPerConn)
                        .append(", frozenReconnectWaitMs=").append(frozenReconnectWaitMs)
                        .append(", busyReconnectWaitMs=").append(busyReconnectWaitMs)
                        .append(", reconFailWaitMs=").append(reconFailWaitMs)
                        .append(", maxAllowedSyncMsgTimeoutCnt=").append(maxAllowedSyncMsgTimeoutCnt)
                        .append(", syncMsgTimeoutChkDurMs=").append(syncMsgTimeoutChkDurMs);
        return super.getSetting(strBuff);
    }
}
