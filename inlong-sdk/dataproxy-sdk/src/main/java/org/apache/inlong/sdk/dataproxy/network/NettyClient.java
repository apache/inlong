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
import org.apache.inlong.sdk.dataproxy.config.HostInfo;
import org.apache.inlong.sdk.dataproxy.utils.LogCounter;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class NettyClient {

    private static final Logger logger = LoggerFactory.getLogger(NettyClient.class);
    private static final LogCounter conExptCnt = new LogCounter(10, 100000, 60 * 1000L);

    private final static int CLIENT_STATUS_INIT = -1;
    private final static int CLIENT_STATUS_READY = 0;
    private final static int CLIENT_STATUS_FROZEN = 1;
    private final static int CLIENT_STATUS_DEAD = 2;
    private final static int CLIENT_STATUS_BUSY = 3;

    private final String callerId;
    private final ProxyClientConfig configure;
    private final Bootstrap bootstrap;
    private final HostInfo hostInfo;
    private Channel channel = null;
    private final AtomicInteger conStatus = new AtomicInteger(CLIENT_STATUS_INIT);
    private final AtomicLong msgInFlight = new AtomicLong(0);
    private final AtomicLong lstSendTime = new AtomicLong(0);
    private final Semaphore reconSemaphore = new Semaphore(1, true);
    private final AtomicLong lstReConTime = new AtomicLong(0);

    public NettyClient(String callerId,
            Bootstrap bootstrap, HostInfo hostInfo, ProxyClientConfig configure) {
        this.callerId = callerId;
        this.bootstrap = bootstrap;
        this.hostInfo = hostInfo;
        this.configure = configure;
        setState(CLIENT_STATUS_INIT);
    }

    public boolean connect(boolean needPrint) {
        // Initial status
        this.setState(CLIENT_STATUS_INIT);
        long curTime = System.currentTimeMillis();
        final CountDownLatch awaitLatch = new CountDownLatch(1);
        // Build connect to server
        ChannelFuture future = bootstrap.connect(
                new InetSocketAddress(hostInfo.getHostName(), hostInfo.getPortNumber()));
        future.addListener(new ChannelFutureListener() {

            public void operationComplete(ChannelFuture arg0) throws Exception {
                awaitLatch.countDown();
                // logger.debug("Connect to {} ack!", hostInfo.getReferenceName());
            }
        });
        try {
            // Wait until the connection is built.
            awaitLatch.await(configure.getConnectTimeoutMs(), TimeUnit.MILLISECONDS);
        } catch (Throwable ex) {
            if (conExptCnt.shouldPrint()) {
                logger.warn("NettyClient({}) connect to {} exception",
                        callerId, hostInfo.getReferenceName(), ex);
            }
            return false;
        }
        // Return if no connection is built.
        if (!future.isSuccess()) {
            if (needPrint) {
                logger.info("NettyClient({}) connect to {} failure, wast {}ms",
                        callerId, hostInfo.getReferenceName(), (System.currentTimeMillis() - curTime));
            }
            return false;
        }
        this.channel = future.channel();
        this.lstSendTime.set(System.currentTimeMillis());
        this.setState(CLIENT_STATUS_READY);
        if (needPrint) {
            logger.info("NettyClient({}) connect to {} success, wast {}ms",
                    callerId, channel.toString(), (System.currentTimeMillis() - curTime));
        }
        return true;
    }

    public boolean close(boolean needPrint) {
        boolean ret = true;
        String channelStr = "";
        setState(CLIENT_STATUS_DEAD);
        long curTime = System.currentTimeMillis();
        final CountDownLatch awaitLatch = new CountDownLatch(1);
        try {
            if (channel == null) {
                channelStr = hostInfo.getReferenceName();
            } else {
                channelStr = channel.toString();
                ChannelFuture future = channel.close();
                future.addListener(new ChannelFutureListener() {

                    public void operationComplete(ChannelFuture arg0) throws Exception {
                        awaitLatch.countDown();
                        // logger.debug("Close client {} acked", hostInfo.getReferenceName());
                    }
                });
                // Wait until the connection is close.
                awaitLatch.await(configure.getConCloseWaitPeriodMs(), TimeUnit.MILLISECONDS);
                // Return if close this connection fail.
                if (!future.isSuccess()) {
                    ret = false;
                }
            }
        } catch (Throwable ex) {
            if (conExptCnt.shouldPrint()) {
                logger.warn("NettyClient({}) close {} exception", callerId, channelStr, ex);
            }
            ret = false;
        } finally {
            this.channel = null;
            this.msgInFlight.set(0);
        }
        if (needPrint) {
            if (ret) {
                logger.info("NettyClient({}) close {} success, wast {}ms",
                        this.callerId, channelStr, (System.currentTimeMillis() - curTime));
            } else {
                logger.info("NettyClient({}) close {} failure, wast {}ms",
                        this.callerId, channelStr, (System.currentTimeMillis() - curTime));
            }
        }
        return ret;
    }

    public ChannelFuture write(EncodeObject encodeObject) {
        ChannelFuture future = null;
        try {
            future = channel.writeAndFlush(encodeObject);
            this.lstSendTime.set(System.currentTimeMillis());
        } catch (Throwable ex) {
            if (conExptCnt.shouldPrint()) {
                logger.warn("NettyClient({}) write {} exception", callerId, channel.toString(), ex);
            }
        }
        return future;
    }

    public boolean reconnect(boolean needPrint) {
        if (this.isActive()
                || this.msgInFlight.get() > 0
                || (System.currentTimeMillis() - this.lstReConTime.get()) < this.configure.getReConnectWaitMs()) {
            return false;
        }
        if (reconSemaphore.tryAcquire()) {
            try {
                if (this.isActive()) {
                    return true;
                }
                this.lstReConTime.set(System.currentTimeMillis());
                this.close(false);
                if (this.connect(false)) {
                    if (needPrint) {
                        logger.info("NettyClient({}) re-connect to {} success",
                                callerId, this.channel.toString());
                    }
                    return true;
                } else {
                    if (needPrint) {
                        logger.info("NettyClient({}) re-connect to {} failure",
                                callerId, hostInfo.getReferenceName());
                    }
                    return false;
                }
            } finally {
                reconSemaphore.release();
            }
        } else {
            return false;
        }
    }

    public String getNodeAddress() {
        return hostInfo.getReferenceName();
    }

    public String getServerIP() {
        return hostInfo.getHostName();
    }

    public Channel getChannel() {
        return channel;
    }

    public void setFrozen(ChannelId channelId) {
        if (this.channel != null && this.channel.id() == channelId) {
            setState(CLIENT_STATUS_FROZEN);
        }
    }

    public void setBusy(ChannelId channelId) {
        if (this.channel != null && this.channel.id() == channelId) {
            setState(CLIENT_STATUS_BUSY);
        }
    }

    public boolean isActive() {
        return ((this.conStatus.get() == CLIENT_STATUS_READY)
                && channel != null && channel.isOpen() && channel.isActive());
    }

    public boolean isIdleClient(long curTime) {
        return (curTime - this.lstSendTime.get() >= 30000L);
    }

    public boolean tryIncMsgInFlight() {
        if (configure.getMaxMsgInFlightPerConn() > 0) {
            if (msgInFlight.getAndIncrement() > configure.getMaxMsgInFlightPerConn()) {
                msgInFlight.decrementAndGet();
                return false;
            }
        }
        return true;
    }

    public void decMsgInFlight() {
        if (configure.getMaxMsgInFlightPerConn() > 0) {
            if (msgInFlight.decrementAndGet() < 0L) {
                logger.warn("NettyClient({}) dec inflight({}) value  < 0", callerId, hostInfo.getReferenceName());
            }
        }
    }

    public long getMsgInFlight() {
        return msgInFlight.get();
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
        NettyClient other = (NettyClient) obj;
        if (channel == null) {
            return other.channel == null;
        } else {
            return channel.equals(other.channel);
        }
    }

    private void setState(int newState) {
        int curState = conStatus.get();
        if (curState == newState) {
            return;
        }
        if (newState == CLIENT_STATUS_DEAD
                || (curState == CLIENT_STATUS_INIT && newState == CLIENT_STATUS_READY)
                || (curState == CLIENT_STATUS_READY && newState > 0)) {
            this.conStatus.compareAndSet(curState, newState);
        }
    }
}
