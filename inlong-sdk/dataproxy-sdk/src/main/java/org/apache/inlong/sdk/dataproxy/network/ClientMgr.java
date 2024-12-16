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
import org.apache.inlong.sdk.dataproxy.common.SendResult;
import org.apache.inlong.sdk.dataproxy.config.EncryptConfigEntry;
import org.apache.inlong.sdk.dataproxy.config.HostInfo;
import org.apache.inlong.sdk.dataproxy.config.ProxyConfigEntry;
import org.apache.inlong.sdk.dataproxy.config.ProxyConfigManager;
import org.apache.inlong.sdk.dataproxy.utils.EventLoopUtil;
import org.apache.inlong.sdk.dataproxy.utils.LogCounter;
import org.apache.inlong.sdk.dataproxy.utils.Tuple2;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.apache.commons.lang.mutable.MutableBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ClientMgr {

    private static final Logger logger = LoggerFactory.getLogger(ClientMgr.class);
    private static final LogCounter logCounter = new LogCounter(10, 100000, 60 * 1000L);
    private static final LogCounter updConExptCnt = new LogCounter(10, 100000, 60 * 1000L);
    private static final LogCounter exptCounter = new LogCounter(10, 100000, 60 * 1000L);
    private static final byte[] hbMsgBody = IpUtils.getLocalIp().getBytes(StandardCharsets.UTF_8);

    private final Sender sender;
    private final ProxyClientConfig configure;
    private final Bootstrap bootstrap;
    private final ProxyConfigManager configManager;
    private final SendHBThread sendHBThread;
    private final AtomicBoolean started = new AtomicBoolean(false);
    // proxy nodes infos
    private final ReentrantReadWriteLock fsLock = new ReentrantReadWriteLock(true);
    private final ConcurrentHashMap<String, HostInfo> proxyNodeInfos = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, NettyClient> usingClientMaps = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, NettyClient> deletingClientMaps = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, HostInfo> connFailNodeMap = new ConcurrentHashMap<>();
    private volatile boolean idTransNum = false;
    // groupId number configure
    private volatile int groupIdNum = 0;
    // steamId number configures
    private Map<String, Integer> streamIdMap = new HashMap<>();
    private volatile long lastUpdateTime = -1;
    private List<String> activeNodes = new ArrayList<>();
    private final AtomicInteger reqSendIndex = new AtomicInteger(0);

    /**
     * Build up the connection between the server and client.
     */
    public ClientMgr(ProxyClientConfig configure, Sender sender) {
        this(configure, sender, null);
    }

    /**
     * Build up the connection between the server and client.
     */
    public ClientMgr(ProxyClientConfig configure, Sender sender, ThreadFactory selfDefineFactory) {
        this.configure = configure;
        this.sender = sender;
        // Initialize the bootstrap
        this.bootstrap = buildBootstrap(this.configure, this.sender, selfDefineFactory);
        // initial configure manager
        this.configManager = new ProxyConfigManager(
                sender.getInstanceId(), configure, this);
        this.sendHBThread = new SendHBThread();
    }

    public void start() {
        if (!started.compareAndSet(false, true)) {
            return;
        }
        try {
            this.configManager.doProxyEntryQueryWork();
        } catch (Throwable ex) {
            if (exptCounter.shouldPrint()) {
                logger.error("ClientMgr({}) query {} exception",
                        sender.getInstanceId(), configure.getInlongGroupId(), ex);
            }
        }
        this.configManager.setDaemon(true);
        this.configManager.start();
        // start hb thread
        this.sendHBThread.start();
        logger.info("ClientMgr({}) started", sender.getInstanceId());
    }

    public void shutDown() {
        if (!started.compareAndSet(true, false)) {
            return;
        }
        this.bootstrap.config().group().shutdownGracefully();
        this.configManager.shutDown();
        this.sendHBThread.shutDown();
        if (!this.deletingClientMaps.isEmpty()) {
            for (NettyClient client : this.deletingClientMaps.values()) {
                if (client == null) {
                    continue;
                }
                if (client.isActive()) {
                    this.sender.waitForAckForChannel(client.getChannel());
                }
                client.close(true);
            }
            this.deletingClientMaps.clear();
        }
        if (!this.usingClientMaps.isEmpty()) {
            for (NettyClient client : this.usingClientMaps.values()) {
                if (client == null) {
                    continue;
                }
                if (client.isActive()) {
                    this.sender.waitForAckForChannel(client.getChannel());
                }
                client.close(true);
            }
            this.usingClientMaps.clear();
        }
        this.activeNodes.clear();
        this.sender.clearCallBack();
        logger.info("ClientMgr({}) stopped!", sender.getInstanceId());
    }

    public ProxyConfigEntry getGroupIdConfigure() throws Exception {
        if (!this.started.get()) {
            throw new Exception("SDK not start or has shutdown!");
        }
        Tuple2<ProxyConfigEntry, String> result =
                configManager.getGroupIdConfigure(true);
        if (result.getF0() == null) {
            throw new Exception(result.getF1());
        }
        return result.getF0();
    }

    public EncryptConfigEntry getEncryptConfigureInfo() {
        if (!this.started.get()) {
            return null;
        }
        Tuple2<EncryptConfigEntry, String> result;
        try {
            result = configManager.getEncryptConfigure(false);
            return result.getF0();
        } catch (Throwable ex) {
            return null;
        }
    }

    public Tuple2<SendResult, NettyClient> getClientByRoundRobin(MutableBoolean allClientMaxInFlight) {
        if (!this.started.get()) {
            return new Tuple2<>(SendResult.SENDER_CLOSED, null);
        }
        if (this.proxyNodeInfos.isEmpty()) {
            return new Tuple2<>(SendResult.NO_REMOTE_NODE_META_INFOS, null);
        }
        List<String> curNodes = this.activeNodes;
        int curNodeSize = curNodes.size();
        if (curNodeSize == 0) {
            return new Tuple2<>(SendResult.EMPTY_ACTIVE_NODE_SET, null);
        }
        String curNode;
        NettyClient client;
        NettyClient backClient = null;
        int nullClientCnt = 0;
        int incFlightFailCnt = 0;
        int startPos = reqSendIndex.getAndIncrement();
        for (int step = 0; step < curNodeSize; step++) {
            curNode = curNodes.get(Math.abs(startPos++) % curNodeSize);
            client = usingClientMaps.get(curNode);
            if (client == null) {
                nullClientCnt++;
                continue;
            }
            if (client.isActive()) {
                if (client.tryIncMsgInFlight()) {
                    return new Tuple2<>(SendResult.OK, client);
                } else {
                    incFlightFailCnt++;
                }
            } else {
                backClient = client;
            }
        }
        if (nullClientCnt == curNodeSize) {
            return new Tuple2<>(SendResult.EMPTY_ACTIVE_NODE_SET, null);
        } else if (incFlightFailCnt + nullClientCnt == curNodeSize) {
            allClientMaxInFlight.setValue(true);
            return new Tuple2<>(SendResult.MAX_FLIGHT_ON_ALL_CONNECTION, null);
        }
        if (backClient != null) {
            if (backClient.reconnect(false) && backClient.isActive()) {
                if (backClient.tryIncMsgInFlight()) {
                    return new Tuple2<>(SendResult.OK, backClient);
                }
            }
        }
        return new Tuple2<>(SendResult.NO_VALID_REMOTE_NODE, null);
    }

    public void setConnectionFrozen(Channel channel) {
        if (channel == null) {
            return;
        }
        boolean found = false;
        for (NettyClient client : deletingClientMaps.values()) {
            if (client == null) {
                continue;
            }
            if (client.getChannel() != null
                    && client.getChannel().id().equals(channel.id())) {
                client.setFrozen(channel.id());
                logger.debug("ClientMgr({}) frozen deleting channel {}",
                        sender.getInstanceId(), channel);
                found = true;
            }
        }
        if (found) {
            return;
        }
        for (NettyClient client : usingClientMaps.values()) {
            if (client == null) {
                continue;
            }
            if (client.getChannel() != null
                    && client.getChannel().id().equals(channel.id())) {
                client.setFrozen(channel.id());
                if (logCounter.shouldPrint()) {
                    logger.info("ClientMgr({}) frozen channel {}", sender.getInstanceId(), channel);
                }
            }
        }
    }

    public void setConnectionBusy(Channel channel) {
        if (channel == null) {
            return;
        }
        boolean found = false;
        for (NettyClient client : deletingClientMaps.values()) {
            if (client == null) {
                continue;
            }
            if (client.getChannel() != null
                    && client.getChannel().id().equals(channel.id())) {
                client.setBusy(channel.id());
                logger.debug("ClientMgr({}) busy deleting channel {}", sender.getInstanceId(), channel);
                found = true;
            }
        }
        if (found) {
            return;
        }
        for (NettyClient client : usingClientMaps.values()) {
            if (client == null) {
                continue;
            }
            if (client.getChannel() != null
                    && client.getChannel().id().equals(channel.id())) {
                client.setBusy(channel.id());
                if (logCounter.shouldPrint()) {
                    logger.info("ClientMgr({}) busy channel {}!", sender.getInstanceId(), channel);
                }
            }
        }
    }

    public void updateProxyInfoList(boolean nodeChanged, List<HostInfo> newNodes) {
        if (newNodes == null || newNodes.isEmpty() || !this.started.get()) {
            return;
        }
        long curTime = System.currentTimeMillis();
        writeLock();
        try {
            // check update info
            this.updateProxyNodes(newNodes);
            // shuffle candidate nodes
            List<HostInfo> candidateNodes = new ArrayList<>(this.proxyNodeInfos.size());
            candidateNodes.addAll(this.proxyNodeInfos.values());
            Collections.sort(candidateNodes);
            Collections.shuffle(candidateNodes);
            int curTotalCnt = candidateNodes.size();
            int needActiveCnt = Math.min(this.configure.getAliveConnections(), curTotalCnt);
            // build next step nodes
            NettyClient client;
            int maxCycleCnt = 3;
            List<String> realHosts = new ArrayList<>();
            this.connFailNodeMap.clear();
            ConcurrentHashMap<String, NettyClient> tmpClientMaps = new ConcurrentHashMap<>();
            do {
                int selectCnt = 0;
                for (HostInfo hostInfo : candidateNodes) {
                    try {
                        client = new NettyClient(this.sender.getInstanceId(),
                                this.bootstrap, hostInfo, this.configure);
                        if (!client.connect(true)) {
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
                                    this.sender.getInstanceId(), hostInfo.getReferenceName(), ex);
                        }
                    }
                }
                if (!realHosts.isEmpty()) {
                    break;
                }
                Thread.sleep(1000L);
            } while (--maxCycleCnt > 0);
            // update active nodes
            if (realHosts.isEmpty()) {
                if (nodeChanged) {
                    logger.error("ClientMgr({}) changed nodes, but all connect failure, nodes={}!",
                            this.sender.getInstanceId(), candidateNodes);
                } else {
                    logger.error("ClientMgr({}) re-choose nodes, but all connect failure, nodes={}!",
                            this.sender.getInstanceId(), candidateNodes);
                }
            } else {
                this.lastUpdateTime = System.currentTimeMillis();
                this.deletingClientMaps = this.usingClientMaps;
                this.usingClientMaps = tmpClientMaps;
                this.activeNodes = realHosts;
                if (nodeChanged) {
                    logger.info("ClientMgr({}) changed nodes, wast {}ms, nodeCnt=(r:{}-a:{}), actives={}, fail={}",
                            sender.getInstanceId(), (System.currentTimeMillis() - curTime),
                            needActiveCnt, realHosts.size(), realHosts, connFailNodeMap.keySet());
                } else {
                    logger.info("ClientMgr({}) re-choose nodes, wast {}ms, nodeCnt=(r:{}-a:{}), actives={}, fail={}",
                            sender.getInstanceId(), (System.currentTimeMillis() - curTime),
                            needActiveCnt, realHosts.size(), realHosts, connFailNodeMap.keySet());
                }
            }
        } catch (Throwable ex) {
            if (updConExptCnt.shouldPrint()) {
                logger.warn("ClientMgr({}) update nodes throw exception", sender.getInstanceId(), ex);
            }
        } finally {
            writeUnlock();
        }
    }

    public String getGroupId() {
        return configure.getInlongGroupId();
    }

    public boolean isIdTransNum() {
        return idTransNum;
    }

    public int getGroupIdNum() {
        return groupIdNum;
    }

    public int getStreamIdNum(String streamId) {
        if (idTransNum) {
            Integer tmpNum = streamIdMap.get(streamId);
            if (tmpNum != null) {
                return tmpNum;
            }
        }
        return 0;
    }

    public void updateGroupIdAndStreamIdNumInfo(
            int groupIdNum, Map<String, Integer> streamIdMap) {
        this.groupIdNum = groupIdNum;
        this.streamIdMap = streamIdMap;
        if (groupIdNum != 0 && streamIdMap != null && !streamIdMap.isEmpty()) {
            this.idTransNum = true;
        }
    }

    private void writeLock() {
        this.fsLock.writeLock().lock();
    }

    private void writeUnlock() {
        this.fsLock.writeLock().unlock();
    }

    private void updateProxyNodes(List<HostInfo> newProxyNodes) {
        boolean found;
        List<String> rmvNodes = new ArrayList<>();
        for (String hostRefName : this.proxyNodeInfos.keySet()) {
            found = false;
            for (HostInfo hostInfo : newProxyNodes) {
                if (hostRefName.equals(hostInfo.getReferenceName())) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                rmvNodes.add(hostRefName);
            }
        }
        for (HostInfo hostInfo : newProxyNodes) {
            if (this.proxyNodeInfos.containsKey(hostInfo.getReferenceName())) {
                continue;
            }
            this.proxyNodeInfos.put(hostInfo.getReferenceName(), hostInfo);
        }
        for (String rmvNode : rmvNodes) {
            this.proxyNodeInfos.remove(rmvNode);
        }
    }

    private Bootstrap buildBootstrap(ProxyClientConfig config, Sender sender, ThreadFactory selfFactory) {
        if (selfFactory == null) {
            selfFactory = new DefaultThreadFactory(
                    "sdk-netty-workers", Thread.currentThread().isDaemon());
        }
        EventLoopGroup eventLoopGroup = EventLoopUtil.newEventLoopGroup(
                config.getIoThreadNum(), config.isEnableBusyWait(), selfFactory);
        Bootstrap tmpBootstrap = new Bootstrap();
        tmpBootstrap.group(eventLoopGroup);
        tmpBootstrap.channel(EventLoopUtil.getClientSocketChannelClass(eventLoopGroup));
        tmpBootstrap.option(ChannelOption.SO_RCVBUF, config.getRecvBufferSize());
        tmpBootstrap.option(ChannelOption.SO_SNDBUF, config.getSendBufferSize());
        tmpBootstrap.handler(new ClientPipelineFactory(this, sender));
        return tmpBootstrap;
    }

    private void sendHeartBeatMsg(NettyClient client) {
        if (!client.isActive()) {
            logger.debug("ClientMgr({}) to {} is inActive",
                    sender.getInstanceId(), client.getNodeAddress());
            return;
        }
        if (!client.getChannel().isWritable()) {
            if (logCounter.shouldPrint()) {
                logger.warn("ClientMgr({}) to {} write_over_water",
                        sender.getInstanceId(), client.getChannel());
            }
            return;
        }
        EncodeObject encodeObject = new EncodeObject(
                Collections.singletonList(hbMsgBody),
                8, false, false, false, System.currentTimeMillis() / 1000, 1, "", "", "");
        try {
            if (configure.isEnableAuthentication()) {
                encodeObject.setAuth(configure.isEnableAuthentication(),
                        configure.getAuthSecretId(), configure.getAuthSecretKey());
            }
            client.write(encodeObject);
        } catch (Throwable ex) {
            if (exptCounter.shouldPrint()) {
                logger.warn("ClientMgr({}) send heartBeat to {} exception ",
                        sender.getInstanceId(), client.getNodeAddress(), ex);
            }
        }
    }

    private class SendHBThread extends Thread {

        private volatile boolean bShutDown;

        public SendHBThread() {
            bShutDown = false;
            this.setName("SendHBThread-" + sender.getInstanceId());
        }

        public void shutDown() {
            logger.info("ClientMgr({}) shutdown SendHBThread!", sender.getInstanceId());
            bShutDown = true;
            this.interrupt();
        }

        @Override
        public void run() {
            logger.info("ClientMgr({}) start SendHBThread!", sender.getInstanceId());
            long curTime;
            while (!bShutDown) {
                try {
                    curTime = System.currentTimeMillis();
                    if (deletingClientMaps != null && !deletingClientMaps.isEmpty()) {
                        if (lastUpdateTime > 0
                                && curTime - lastUpdateTime > configure.getConCloseWaitPeriodMs()) {
                            for (NettyClient client : deletingClientMaps.values()) {
                                if (client == null) {
                                    continue;
                                }
                                client.close(false);
                            }
                            deletingClientMaps.clear();
                        }
                    }
                    for (NettyClient nettyClient : usingClientMaps.values()) {
                        if (nettyClient == null) {
                            continue;
                        }
                        if (nettyClient.isActive()) {
                            if (nettyClient.isIdleClient(curTime)) {
                                sendHeartBeatMsg(nettyClient);
                            }
                        } else {
                            if (nettyClient.getMsgInFlight() <= 0L) {
                                nettyClient.reconnect(false);
                            }
                        }
                    }
                    if (bShutDown) {
                        break;
                    }
                    try {
                        Thread.sleep(4000L);
                    } catch (InterruptedException e) {
                        //
                    }
                } catch (Throwable ex) {
                    if (exptCounter.shouldPrint()) {
                        logger.warn("ClientMgr({}) SendHBThread throw exception", sender.getInstanceId(), ex);
                    }
                }
            }
            logger.info("ClientMgr({}) exit SendHBThread!", sender.getInstanceId());
        }
    }
}
