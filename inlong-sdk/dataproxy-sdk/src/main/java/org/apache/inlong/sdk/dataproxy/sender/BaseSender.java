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

package org.apache.inlong.sdk.dataproxy.sender;

import org.apache.inlong.sdk.dataproxy.MsgSenderFactory;
import org.apache.inlong.sdk.dataproxy.common.ErrorCode;
import org.apache.inlong.sdk.dataproxy.common.ProcessResult;
import org.apache.inlong.sdk.dataproxy.common.ProxyClientConfig;
import org.apache.inlong.sdk.dataproxy.common.SdkConsts;
import org.apache.inlong.sdk.dataproxy.config.ConfigHolder;
import org.apache.inlong.sdk.dataproxy.config.HostInfo;
import org.apache.inlong.sdk.dataproxy.config.ProxyConfigEntry;
import org.apache.inlong.sdk.dataproxy.config.ProxyConfigManager;
import org.apache.inlong.sdk.dataproxy.metric.MetricDataHolder;
import org.apache.inlong.sdk.dataproxy.network.ClientMgr;
import org.apache.inlong.sdk.dataproxy.network.PkgCacheQuota;
import org.apache.inlong.sdk.dataproxy.utils.LogCounter;
import org.apache.inlong.sdk.dataproxy.utils.ProxyUtils;
import org.apache.inlong.sdk.dataproxy.utils.Tuple2;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Base Sender class:
 *
 * Used to manage Sender metadata information, including Sender ID,
 *  status, network interaction, metadata query object, and
 *  Proxy node metadata information obtained from Manager, etc.
 */
public abstract class BaseSender implements ConfigHolder {

    private final int SENDER_STATUS_UNINITIALIZED = -2;
    private final int SENDER_STATUS_INITIALIZING = -1;
    private final int SENDER_STATUS_STARTED = 0;
    private final int SENDER_STATUS_CLOSED = 1;

    protected static final Logger logger = LoggerFactory.getLogger(BaseSender.class);
    protected static final LogCounter exceptCnt = new LogCounter(10, 100000, 60 * 1000L);
    // sender id generator
    private static final AtomicLong senderIdGen = new AtomicLong(0L);
    //
    protected final AtomicInteger senderStatus = new AtomicInteger(SENDER_STATUS_UNINITIALIZED);
    protected final MsgSenderFactory senderFactory;
    private final String factoryClusterIdKey;
    protected final String senderId;
    protected final ProxyClientConfig baseConfig;
    protected ClientMgr clientMgr;
    protected ProxyConfigManager configManager;
    private final ReentrantReadWriteLock fsLock = new ReentrantReadWriteLock(true);
    // proxy node meta infos
    private final ConcurrentHashMap<String, HostInfo> proxyNodeInfos = new ConcurrentHashMap<>();
    // groupId and streamId num info
    private volatile int allowedPkgLength = -1;
    protected volatile boolean idTransNum = false;
    protected volatile int groupIdNum = 0;
    private Map<String, Integer> streamIdMap = new HashMap<>();
    private final PkgCacheQuota globalCacheQuota;
    private final PkgCacheQuota sdkPkgCacheQuota;
    // metric holder
    protected MetricDataHolder metricHolder;

    protected BaseSender(ProxyClientConfig configure, MsgSenderFactory senderFactory, String clusterIdKey) {
        if (configure == null) {
            throw new NullPointerException("configure is null");
        }
        this.baseConfig = configure.clone();
        this.senderFactory = senderFactory;
        this.factoryClusterIdKey = clusterIdKey;
        this.senderId = configure.getDataRptProtocol()
                + "-" + ProxyUtils.getProcessPid() + "-" + senderIdGen.incrementAndGet();
        this.configManager = new ProxyConfigManager(this.senderId, this.baseConfig, this);
        this.configManager.setDaemon(true);
        this.metricHolder = new MetricDataHolder(this);
        if (this.senderFactory == null) {
            this.globalCacheQuota = null;
        } else {
            this.globalCacheQuota = this.senderFactory.getFactoryPkgCacheQuota();
        }
        this.sdkPkgCacheQuota = new PkgCacheQuota(false, this.senderId,
                configure.getMaxInFlightReqCnt(), configure.getMaxInFlightSizeKb(), configure.getPaddingSize());
    }

    public boolean start(ProcessResult procResult) {
        if (!this.senderStatus.compareAndSet(
                SENDER_STATUS_UNINITIALIZED, SENDER_STATUS_INITIALIZING)) {
            return procResult.setSuccess();
        }
        // start metric holder
        if (!this.metricHolder.start(procResult)) {
            return false;
        }
        // start client manager
        if (!this.clientMgr.start(procResult)) {
            return false;
        }
        // query meta info from manager
        if (!this.configManager.doProxyEntryQueryWork(procResult)) {
            this.clientMgr.stop();
            String errInfo = "queryCode=" + procResult.getErrCode()
                    + ", detail=" + procResult.getErrMsg();
            return procResult.setFailResult(ErrorCode.FETCH_PROXY_META_FAILURE, errInfo);
        }
        if (this.baseConfig.isEnableReportEncrypt()
                && !this.configManager.doEncryptConfigEntryQueryWork(procResult)) {
            this.clientMgr.stop();
            String errInfo = "queryCode=" + procResult.getErrCode()
                    + ", detail=" + procResult.getErrMsg();
            return procResult.setFailResult(ErrorCode.FETCH_ENCRYPT_META_FAILURE, errInfo);
        }
        // start configure manager
        this.configManager.start();
        this.senderStatus.set(SENDER_STATUS_STARTED);
        logger.info("Sender({}) instance started!", senderId);
        return procResult.setSuccess();
    }

    public void close() {
        int currentStatus = senderStatus.get();
        if (currentStatus == SENDER_STATUS_CLOSED) {
            return;
        }
        if (!senderStatus.compareAndSet(currentStatus, SENDER_STATUS_CLOSED)) {
            return;
        }
        long startTime = System.currentTimeMillis();
        logger.info("Sender({}) instance is stopping...", senderId);
        configManager.shutDown();
        clientMgr.stop();
        metricHolder.close();
        if (this.senderFactory != null) {
            this.senderFactory.removeClient(this);
        }
        logger.info("Sender({}) instance stopped, cost {} ms!",
                senderId, System.currentTimeMillis() - startTime);
    }

    @Override
    public void updateAllowedMaxPkgLength(int maxPkgLength) {
        this.allowedPkgLength = maxPkgLength;
    }

    @Override
    public void updateProxyNodes(boolean nodeChanged, List<HostInfo> newProxyNodes) {
        if (this.senderStatus.get() == SENDER_STATUS_CLOSED
                || newProxyNodes == null || newProxyNodes.isEmpty()) {
            return;
        }
        this.fsLock.writeLock().lock();
        try {
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
            clientMgr.updateProxyInfoList(nodeChanged, this.proxyNodeInfos);
        } finally {
            this.fsLock.writeLock().unlock();
        }
    }

    @Override
    public MetricDataHolder getMetricHolder() {
        return metricHolder;
    }

    public boolean isStarted() {
        return senderStatus.get() == SENDER_STATUS_STARTED;
    }

    public boolean isGenByFactory() {
        return senderFactory != null;
    }

    public MsgSenderFactory getSenderFactory() {
        return senderFactory;
    }

    public String getFactoryClusterIdKey() {
        return factoryClusterIdKey;
    }

    public String getMetaConfigKey() {
        return this.baseConfig.getGroupMetaConfigKey();
    }

    public ProxyConfigEntry getProxyConfigEntry() {
        return configManager.getProxyConfigEntry();
    }

    public String getSenderId() {
        return senderId;
    }

    public ProxyClientConfig getConfigure() {
        return baseConfig;
    }

    public int getAllowedPkgLength() {
        return allowedPkgLength;
    }

    public String getGroupId() {
        return baseConfig.getInlongGroupId();
    }

    public boolean isMetaInfoUnReady() {
        return this.proxyNodeInfos.isEmpty();
    }

    public Map<String, HostInfo> getProxyNodeInfos() {
        return proxyNodeInfos;
    }

    public int getProxyNodeCnt() {
        return proxyNodeInfos.size();
    }

    public Tuple2<Integer, Integer> getFactoryAvailQuota() {
        if (senderFactory == null || globalCacheQuota == null) {
            return PkgCacheQuota.DISABLE_RET;
        }
        return globalCacheQuota.getPkgCacheAvailQuota();
    }

    public Tuple2<Integer, Integer> getSenderAvailQuota() {
        return sdkPkgCacheQuota.getPkgCacheAvailQuota();
    }

    public int getFactoryPkgCntPermits() {
        if (senderFactory == null
                || senderFactory.getFactoryPkgCacheQuota() == null) {
            return SdkConsts.UNDEFINED_VALUE;
        }
        return senderFactory.getFactoryPkgCacheQuota().getPkgCntPermits();
    }

    public int getFactoryPkgSizeKbPermits() {
        if (senderFactory == null
                || senderFactory.getFactoryPkgCacheQuota() == null) {
            return SdkConsts.UNDEFINED_VALUE;
        }
        return senderFactory.getFactoryPkgCacheQuota().getPkgSizeKbPermits();
    }

    public int getSenderPkgCntPermits() {
        return sdkPkgCacheQuota.getPkgCntPermits();
    }

    public int getSenderPkgSizeKbPermits() {
        return sdkPkgCacheQuota.getPkgSizeKbPermits();
    }

    public boolean tryAcquireCachePermits(int sizeInByte, ProcessResult procResult) {
        if (this.globalCacheQuota == null) {
            return this.sdkPkgCacheQuota.tryAcquire(sizeInByte, procResult);
        } else {
            if (this.globalCacheQuota.tryAcquire(sizeInByte, procResult)) {
                if (this.sdkPkgCacheQuota.tryAcquire(sizeInByte, procResult)) {
                    return true;
                } else {
                    this.globalCacheQuota.release(sizeInByte);
                    return false;
                }
            }
            return false;
        }
    }

    public void releaseCachePermits(int sizeInByte) {
        if (this.globalCacheQuota == null) {
            this.sdkPkgCacheQuota.release(sizeInByte);
        } else {
            this.sdkPkgCacheQuota.release(sizeInByte);
            this.globalCacheQuota.release(sizeInByte);
        }
    }

    public abstract int getActiveNodeCnt();

    public abstract int getInflightMsgCnt();

    public void updateGroupIdAndStreamIdNumInfo(
            int groupIdNum, Map<String, Integer> streamIdMap) {
        this.groupIdNum = groupIdNum;
        this.streamIdMap = streamIdMap;
        if (groupIdNum != 0 && streamIdMap != null && !streamIdMap.isEmpty()) {
            this.idTransNum = true;
        }
    }

    protected int getStreamIdNum(String streamId) {
        if (idTransNum) {
            Integer tmpNum = streamIdMap.get(streamId);
            if (tmpNum != null) {
                return tmpNum;
            }
        }
        return 0;
    }
}
