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

package org.apache.inlong.sdk.dataproxy;

import org.apache.inlong.sdk.dataproxy.common.ProcessResult;
import org.apache.inlong.sdk.dataproxy.common.ProxyClientConfig;
import org.apache.inlong.sdk.dataproxy.common.SdkConsts;
import org.apache.inlong.sdk.dataproxy.config.ProxyConfigEntry;
import org.apache.inlong.sdk.dataproxy.config.ProxyConfigManager;
import org.apache.inlong.sdk.dataproxy.exception.ProxySdkException;
import org.apache.inlong.sdk.dataproxy.network.PkgCacheQuota;
import org.apache.inlong.sdk.dataproxy.sender.BaseSender;
import org.apache.inlong.sdk.dataproxy.sender.http.HttpMsgSenderConfig;
import org.apache.inlong.sdk.dataproxy.sender.http.InLongHttpMsgSender;
import org.apache.inlong.sdk.dataproxy.sender.tcp.InLongTcpMsgSender;
import org.apache.inlong.sdk.dataproxy.sender.tcp.TcpMsgSenderConfig;
import org.apache.inlong.sdk.dataproxy.utils.LogCounter;
import org.apache.inlong.sdk.dataproxy.utils.ProxyUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Base Message Sender Factory
 *
 * Used to manage the instance relationship of the sender factory.
 * Since both singleton and multiple instances involve the same relationship,
 * they are abstracted and maintained separately here.
 */
public class BaseMsgSenderFactory {

    private static final Logger logger = LoggerFactory.getLogger(BaseMsgSenderFactory.class);
    private static final LogCounter exptCounter = new LogCounter(10, 100000, 60 * 1000L);
    // msg send factory
    private final MsgSenderFactory msgSenderFactory;
    private final String factoryNo;
    private final PkgCacheQuota pkgCacheQuota;
    // for senders
    private final ReentrantReadWriteLock senderCacheLock = new ReentrantReadWriteLock();
    // for inlong groupId -- Sender map
    private final ConcurrentHashMap<String, BaseSender> groupIdSenderMap = new ConcurrentHashMap<>();
    // for inlong clusterId -- Sender map
    private final ConcurrentHashMap<String, BaseSender> clusterIdSenderMap = new ConcurrentHashMap<>();

    protected BaseMsgSenderFactory(MsgSenderFactory msgSenderFactory,
            String factoryNo, int factoryPkgCntPermits, int factoryPkgSizeKbPermits) {
        this.msgSenderFactory = msgSenderFactory;
        this.factoryNo = factoryNo;
        this.pkgCacheQuota = new PkgCacheQuota(true, factoryNo,
                factoryPkgCntPermits, factoryPkgSizeKbPermits, SdkConsts.VAL_DEF_PADDING_SIZE);
        logger.info("MsgSenderFactory({}) started, factoryPkgCntPermits={}, factoryPkgSizeKbPermits={}",
                this.factoryNo, factoryPkgCntPermits, factoryPkgSizeKbPermits);
    }

    public void close() {
        int totalSenderCnt;
        long startTime = System.currentTimeMillis();
        logger.info("MsgSenderFactory({}) is closing", this.factoryNo);
        senderCacheLock.writeLock().lock();
        try {
            // release groupId mapped senders
            totalSenderCnt = releaseAllGroupIdSenders(groupIdSenderMap);
            // release clusterId mapped senders
            totalSenderCnt += releaseAllClusterIdSenders(clusterIdSenderMap);
        } finally {
            senderCacheLock.writeLock().unlock();
        }
        logger.info("MsgSenderFactory({}) closed, release {} inlong senders, cost {} ms",
                this.factoryNo, totalSenderCnt, System.currentTimeMillis() - startTime);
    }

    public void removeClient(BaseSender msgSender) {
        if (msgSender == null
                || msgSender.getSenderFactory() == null
                || msgSender.getSenderFactory() != msgSenderFactory) {
            return;
        }
        boolean removed;
        String senderId = msgSender.getSenderId();
        senderCacheLock.writeLock().lock();
        try {
            if (msgSender.getFactoryClusterIdKey() == null) {
                removed = removeGroupIdSender(msgSender, groupIdSenderMap);
            } else {
                removed = removeClusterIdSender(msgSender, clusterIdSenderMap);
            }
        } finally {
            senderCacheLock.writeLock().unlock();
        }
        if (removed) {
            logger.info("MsgSenderFactory({}) removed sender({})", this.factoryNo, senderId);
        }
    }

    public PkgCacheQuota getPkgCacheQuota() {
        return pkgCacheQuota;
    }

    public int getMsgSenderCount() {
        return groupIdSenderMap.size() + clusterIdSenderMap.size();
    }

    public InLongTcpMsgSender genTcpSenderByGroupId(
            TcpMsgSenderConfig configure, ThreadFactory selfDefineFactory) throws ProxySdkException {
        validProxyConfigNotNull(configure);
        // query cached sender
        String metaConfigKey = configure.getGroupMetaConfigKey();
        InLongTcpMsgSender messageSender =
                (InLongTcpMsgSender) groupIdSenderMap.get(metaConfigKey);
        if (messageSender != null) {
            return messageSender;
        }
        // valid configure info
        ProcessResult procResult = new ProcessResult();
        qryProxyMetaConfigure(configure, procResult);
        // generate sender
        senderCacheLock.writeLock().lock();
        try {
            // re-get the created sender based on the groupId key after locked
            messageSender = (InLongTcpMsgSender) groupIdSenderMap.get(metaConfigKey);
            if (messageSender != null) {
                return messageSender;
            }
            // build a new sender based on groupId
            messageSender = new InLongTcpMsgSender(configure, selfDefineFactory, msgSenderFactory, null);
            if (!messageSender.start(procResult)) {
                messageSender.close();
                throw new ProxySdkException("Failed to start groupId sender: " + procResult);
            }
            groupIdSenderMap.put(metaConfigKey, messageSender);
            logger.info("MsgSenderFactory({}) generated a new groupId({}) sender({})",
                    this.factoryNo, metaConfigKey, messageSender.getSenderId());
            return messageSender;
        } catch (Throwable ex) {
            if (exptCounter.shouldPrint()) {
                logger.warn("MsgSenderFactory({}) build groupId sender({}) exception",
                        this.factoryNo, metaConfigKey, ex);
            }
            throw new ProxySdkException("Failed to build groupId sender: " + ex.getMessage());
        } finally {
            senderCacheLock.writeLock().unlock();
        }
    }

    public InLongHttpMsgSender genHttpSenderByGroupId(
            HttpMsgSenderConfig configure) throws ProxySdkException {
        validProxyConfigNotNull(configure);
        // query cached sender
        String metaConfigKey = configure.getGroupMetaConfigKey();
        InLongHttpMsgSender messageSender =
                (InLongHttpMsgSender) groupIdSenderMap.get(metaConfigKey);
        if (messageSender != null) {
            return messageSender;
        }
        // valid configure info
        ProcessResult procResult = new ProcessResult();
        qryProxyMetaConfigure(configure, procResult);
        // generate sender
        senderCacheLock.writeLock().lock();
        try {
            // re-get the created sender based on the groupId key after locked
            messageSender = (InLongHttpMsgSender) groupIdSenderMap.get(metaConfigKey);
            if (messageSender != null) {
                return messageSender;
            }
            // build a new sender based on groupId
            messageSender = new InLongHttpMsgSender(configure, msgSenderFactory, null);
            if (!messageSender.start(procResult)) {
                messageSender.close();
                throw new ProxySdkException("Failed to start groupId sender: " + procResult);
            }
            groupIdSenderMap.put(metaConfigKey, messageSender);
            logger.info("MsgSenderFactory({}) generated a new groupId({}) sender({})",
                    this.factoryNo, metaConfigKey, messageSender.getSenderId());
            return messageSender;
        } catch (Throwable ex) {
            if (exptCounter.shouldPrint()) {
                logger.warn("MsgSenderFactory({}) build groupId sender({}) exception",
                        this.factoryNo, metaConfigKey, ex);
            }
            throw new ProxySdkException("Failed to build groupId sender: " + ex.getMessage());
        } finally {
            senderCacheLock.writeLock().unlock();
        }
    }

    public InLongTcpMsgSender genTcpSenderByClusterId(
            TcpMsgSenderConfig configure, ThreadFactory selfDefineFactory) throws ProxySdkException {
        validProxyConfigNotNull(configure);
        // get groupId's clusterIdKey
        ProcessResult procResult = new ProcessResult();
        ProxyConfigEntry proxyConfigEntry = qryProxyMetaConfigure(configure, procResult);
        String clusterIdKey = ProxyUtils.buildClusterIdKey(
                configure.getDataRptProtocol(), configure.getRegionName(), proxyConfigEntry.getClusterId());
        // get local built sender
        InLongTcpMsgSender messageSender = (InLongTcpMsgSender) clusterIdSenderMap.get(clusterIdKey);
        if (messageSender != null) {
            return messageSender;
        }
        // generate sender
        senderCacheLock.writeLock().lock();
        try {
            // re-get the created sender based on the clusterId Key after locked
            messageSender = (InLongTcpMsgSender) clusterIdSenderMap.get(clusterIdKey);
            if (messageSender != null) {
                return messageSender;
            }
            // build a new sender based on clusterId Key
            messageSender = new InLongTcpMsgSender(configure,
                    selfDefineFactory, msgSenderFactory, clusterIdKey);
            if (!messageSender.start(procResult)) {
                messageSender.close();
                throw new ProxySdkException("Failed to start cluster sender: " + procResult);
            }
            clusterIdSenderMap.put(clusterIdKey, messageSender);
            logger.info("MsgSenderFactory({}) generated a new clusterId({}) sender({})",
                    this.factoryNo, clusterIdKey, messageSender.getSenderId());
            return messageSender;
        } catch (Throwable ex) {
            if (exptCounter.shouldPrint()) {
                logger.warn("MsgSenderFactory({}) build cluster sender({}) exception",
                        this.factoryNo, clusterIdKey, ex);
            }
            throw new ProxySdkException("Failed to build cluster sender: " + ex.getMessage());
        } finally {
            senderCacheLock.writeLock().unlock();
        }
    }

    public InLongHttpMsgSender genHttpSenderByClusterId(
            HttpMsgSenderConfig configure) throws ProxySdkException {
        validProxyConfigNotNull(configure);
        // get groupId's clusterIdKey
        ProcessResult procResult = new ProcessResult();
        ProxyConfigEntry proxyConfigEntry = qryProxyMetaConfigure(configure, procResult);;
        String clusterIdKey = ProxyUtils.buildClusterIdKey(
                configure.getDataRptProtocol(), configure.getRegionName(), proxyConfigEntry.getClusterId());
        // get local built sender
        InLongHttpMsgSender messageSender = (InLongHttpMsgSender) clusterIdSenderMap.get(clusterIdKey);
        if (messageSender != null) {
            return messageSender;
        }
        // generate sender
        senderCacheLock.writeLock().lock();
        try {
            // re-get the created sender based on the clusterId Key after locked
            messageSender = (InLongHttpMsgSender) clusterIdSenderMap.get(clusterIdKey);
            if (messageSender != null) {
                return messageSender;
            }
            // build a new sender based on clusterId Key
            messageSender = new InLongHttpMsgSender(configure, msgSenderFactory, clusterIdKey);
            if (!messageSender.start(procResult)) {
                messageSender.close();
                throw new ProxySdkException("Failed to start cluster sender: " + procResult);
            }
            clusterIdSenderMap.put(clusterIdKey, messageSender);
            logger.info("MsgSenderFactory({}) generated a new clusterId({}) sender({})",
                    this.factoryNo, clusterIdKey, messageSender.getSenderId());
            return messageSender;
        } catch (Throwable ex) {
            if (exptCounter.shouldPrint()) {
                logger.warn("MsgSenderFactory({}) build cluster sender({}) exception",
                        this.factoryNo, clusterIdKey, ex);
            }
            throw new ProxySdkException("Failed to build cluster sender: " + ex.getMessage());
        } finally {
            senderCacheLock.writeLock().unlock();
        }
    }

    private ProxyConfigEntry qryProxyMetaConfigure(
            ProxyClientConfig proxyConfig, ProcessResult procResult) throws ProxySdkException {
        ProxyConfigManager inlongMetaQryMgr = new ProxyConfigManager(proxyConfig);
        // check whether valid configure
        if (!inlongMetaQryMgr.getGroupIdConfigure(true, procResult)) {
            throw new ProxySdkException("Failed to query remote group config: " + procResult);
        }
        if (proxyConfig.isEnableReportEncrypt()
                && !inlongMetaQryMgr.getEncryptConfigure(true, procResult)) {
            throw new ProxySdkException("Failed to query remote encrypt config: " + procResult);
        }
        return (ProxyConfigEntry) procResult.getRetData();
    }

    private boolean removeGroupIdSender(BaseSender msgSender, Map<String, BaseSender> senderMap) {
        BaseSender tmpSender = senderMap.get(msgSender.getMetaConfigKey());
        if (tmpSender == null
                || !tmpSender.getSenderId().equals(msgSender.getSenderId())) {
            return false;
        }
        return senderMap.remove(msgSender.getMetaConfigKey()) != null;
    }

    private boolean removeClusterIdSender(BaseSender msgSender, Map<String, BaseSender> senderMap) {
        BaseSender tmpSender = senderMap.get(msgSender.getFactoryClusterIdKey());
        if (tmpSender == null
                || !tmpSender.getSenderId().equals(msgSender.getSenderId())) {
            return false;
        }
        return senderMap.remove(msgSender.getFactoryClusterIdKey()) != null;
    }

    private int releaseAllGroupIdSenders(Map<String, BaseSender> senderMap) {
        int totalSenderCnt = 0;
        for (Map.Entry<String, BaseSender> entry : senderMap.entrySet()) {
            if (entry == null || entry.getKey() == null || entry.getValue() == null) {
                continue;
            }
            try {
                entry.getValue().close();
            } catch (Throwable ex) {
                if (exptCounter.shouldPrint()) {
                    logger.warn("MsgSenderFactory({}) close groupId({})'s sender failure",
                            this.factoryNo, entry.getKey(), ex);
                }
            }
            totalSenderCnt++;
        }
        senderMap.clear();
        return totalSenderCnt;
    }

    private int releaseAllClusterIdSenders(Map<String, BaseSender> senderMap) {
        int totalSenderCnt = 0;
        for (Map.Entry<String, BaseSender> entry : senderMap.entrySet()) {
            if (entry == null
                    || entry.getKey() == null
                    || entry.getValue() == null) {
                continue;
            }
            try {
                entry.getValue().close();
            } catch (Throwable ex) {
                if (exptCounter.shouldPrint()) {
                    logger.warn("MsgSenderFactory({}) close clusterId({})'s sender failure",
                            this.factoryNo, entry.getKey(), ex);
                }
            }
            totalSenderCnt++;
        }
        senderMap.clear();
        return totalSenderCnt;
    }

    private void validProxyConfigNotNull(ProxyClientConfig configure) throws ProxySdkException {
        if (configure == null) {
            throw new ProxySdkException("configure is null!");
        }
    }
}
