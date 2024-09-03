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

package org.apache.inlong.audit;

import org.apache.inlong.audit.entity.AuditComponent;
import org.apache.inlong.audit.entity.AuditInformation;
import org.apache.inlong.audit.entity.AuditMetric;
import org.apache.inlong.audit.entity.FlowType;
import org.apache.inlong.audit.loader.SocketAddressListLoader;
import org.apache.inlong.audit.protocol.AuditApi;
import org.apache.inlong.audit.send.ProxyManager;
import org.apache.inlong.audit.send.SenderManager;
import org.apache.inlong.audit.util.AuditConfig;
import org.apache.inlong.audit.util.AuditDimensions;
import org.apache.inlong.audit.util.AuditManagerUtils;
import org.apache.inlong.audit.util.AuditValues;
import org.apache.inlong.audit.util.Config;
import org.apache.inlong.audit.util.RequestIdUtils;
import org.apache.inlong.audit.util.StatInfo;

import org.apache.commons.lang3.ClassUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Calendar;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import static org.apache.inlong.audit.consts.ConfigConstants.DEFAULT_AUDIT_TAG;
import static org.apache.inlong.audit.protocol.AuditApi.BaseCommand.Type.AUDIT_REQUEST;

public class AuditReporterImpl implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = LoggerFactory.getLogger(AuditReporterImpl.class);
    private static final String FIELD_SEPARATORS = ":";
    private static final long DEFAULT_AUDIT_VERSION = -1;
    private static final int BATCH_NUM = 100;
    private static final int PERIOD = 1000 * 60;
    // Resource isolation key is used in checkpoint, the default value is 0.
    public static final long DEFAULT_ISOLATE_KEY = 0;
    private final ReentrantLock GLOBAL_LOCK = new ReentrantLock();
    private final ConcurrentHashMap<Long, ConcurrentHashMap<String, StatInfo>> preStatMap =
            new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Long, ConcurrentHashMap<String, StatInfo>> summaryStatMap =
            new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Long, ConcurrentHashMap<String, StatInfo>> expiredStatMap =
            new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Long, HashSet<String>> expiredKeyList = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Long, Long> flushTime = new ConcurrentHashMap<>();
    private final Config config = new Config();
    private final ScheduledExecutorService timeoutExecutor = Executors.newSingleThreadScheduledExecutor();
    private int packageId = 1;
    private int dataId = 0;
    private volatile boolean initialized = false;
    private SenderManager manager;
    private AtomicInteger flushStat = new AtomicInteger(0);
    private AuditConfig auditConfig = null;
    private SocketAddressListLoader loader = null;
    private int flushStatThreshold = 100;
    private boolean autoFlush = true;

    private AuditMetric auditMetric = new AuditMetric();

    /**
     * Set stat threshold
     *
     * @param flushStatThreshold
     */
    public void setFlushStatThreshold(int flushStatThreshold) {
        this.flushStatThreshold = flushStatThreshold;
    }

    /**
     * When the caller needs to isolate resources, please call this method and pass the parameter true.
     * For example, in scenarios such as flink checkpoint
     *
     * @param autoFlush
     */
    public void setAutoFlush(boolean autoFlush) {
        this.autoFlush = autoFlush;
    }

    /**
     * Init
     */
    private void init() {
        if (initialized) {
            return;
        }
        config.init();
        timeoutExecutor.scheduleWithFixedDelay(new Runnable() {

            @Override
            public void run() {
                try {
                    loadIpPortList();
                    checkFlushTime();
                    if (autoFlush) {
                        flush(DEFAULT_ISOLATE_KEY);
                    }
                } catch (Exception e) {
                    LOGGER.error("Audit run has exception!", e);
                }
            }

        }, PERIOD, PERIOD, TimeUnit.MILLISECONDS);
        if (auditConfig == null) {
            auditConfig = new AuditConfig();
        }
        this.manager = new SenderManager(auditConfig);
    }

    private void loadIpPortList() {
        if (loader == null) {
            return;
        }
        try {
            List<String> ipPortList = loader.loadSocketAddressList();
            if (ipPortList != null && ipPortList.size() > 0) {
                HashSet<String> ipPortSet = new HashSet<>();
                ipPortSet.addAll(ipPortList);
                this.setAuditProxy(ipPortSet);
            }
        } catch (Exception e) {
            LOGGER.error(e.getMessage());
        }
    }

    /**
     * Set loader
     *
     * @param loader the loader to set
     */
    public void setLoader(SocketAddressListLoader loader) {
        this.loader = loader;
    }

    /**
     * SetLoaderClass
     *
     * @param loaderClassName
     */
    public void setLoaderClass(String loaderClassName) {
        if (StringUtils.isEmpty(loaderClassName)) {
            return;
        }
        try {
            Class<?> loaderClass = ClassUtils.getClass(loaderClassName);
            Object loaderObject = loaderClass.getDeclaredConstructor().newInstance();
            if (loaderObject instanceof SocketAddressListLoader) {
                SocketAddressListLoader loader = (SocketAddressListLoader) loaderObject;
                this.loader = loader;
                LOGGER.info("Audit list loader class:{}", loaderClassName);
            }
        } catch (Throwable t) {
            LOGGER.error("Fail to init list loader class:{},error:{}",
                    loaderClassName, t.getMessage(), t);
        }
    }

    /**
     * Set AuditProxy from the ip
     */
    public void setAuditProxy(HashSet<String> ipPortList) {
        checkInitStatus();
        ProxyManager.getInstance().setAuditProxy(ipPortList);
    }

    /**
     * Set AuditProxy from the manager host
     */
    public void setAuditProxy(AuditComponent component, String managerHost, String secretId, String secretKey) {
        checkInitStatus();
        ProxyManager.getInstance().setManagerConfig(component, managerHost, secretId, secretKey);
    }

    private synchronized void checkInitStatus() {
        if (initialized) {
            return;
        }
        init();
        initialized = true;
    }

    /**
     * Set audit config
     */
    public void setAuditConfig(AuditConfig config) {
        auditConfig = config;
        manager.setAuditConfig(config);
    }

    /**
     * Add audit data
     */
    public void add(int auditID, String inlongGroupID, String inlongStreamID, long logTime, long count, long size) {
        add(auditID, DEFAULT_AUDIT_TAG, inlongGroupID, inlongStreamID, logTime, count, size, DEFAULT_AUDIT_VERSION);
    }

    public void add(long isolateKey, int auditID, String auditTag, String inlongGroupID, String inlongStreamID,
            long logTime, long count, long size, long auditVersion) {
        long delayTime = System.currentTimeMillis() - logTime;
        add(isolateKey, auditID, auditTag, inlongGroupID, inlongStreamID, logTime,
                count, size, delayTime, auditVersion);
    }

    public void add(int auditID, String auditTag, String inlongGroupID, String inlongStreamID, long logTime,
            long count, long size, long auditVersion) {
        long delayTime = System.currentTimeMillis() - logTime;
        add(DEFAULT_ISOLATE_KEY, auditID, auditTag, inlongGroupID, inlongStreamID, logTime, count, size,
                delayTime * count, auditVersion);
    }

    public void add(int auditID, String inlongGroupID, String inlongStreamID, long logTime, long count, long size,
            long delayTime) {
        add(DEFAULT_ISOLATE_KEY, auditID, DEFAULT_AUDIT_TAG, inlongGroupID, inlongStreamID, logTime, count, size,
                delayTime, DEFAULT_AUDIT_VERSION);
    }

    public void add(long isolateKey, int auditID, String auditTag, String inlongGroupID, String inlongStreamID,
            long logTime, long count, long size, long delayTime, long auditVersion) {
        StringJoiner keyJoiner = new StringJoiner(FIELD_SEPARATORS);
        keyJoiner.add(String.valueOf(logTime / PERIOD));
        keyJoiner.add(inlongGroupID);
        keyJoiner.add(inlongStreamID);
        keyJoiner.add(String.valueOf(auditID));
        keyJoiner.add(auditTag);
        keyJoiner.add(String.valueOf(auditVersion));
        addByKey(isolateKey, keyJoiner.toString(), count, size, delayTime);
    }

    /**
     * When the caller needs to isolate resources, please call this method.
     * For example, in scenarios such as flink checkpoint
     *
     * @param dimensions
     * @param values
     */
    public void add(AuditDimensions dimensions, AuditValues values) {
        StringJoiner keyJoiner = new StringJoiner(FIELD_SEPARATORS);
        keyJoiner.add(String.valueOf(dimensions.getLogTime() / PERIOD));
        keyJoiner.add(dimensions.getInlongGroupID());
        keyJoiner.add(dimensions.getInlongStreamID());
        keyJoiner.add(String.valueOf(dimensions.getAuditID()));
        keyJoiner.add(dimensions.getAuditTag());
        keyJoiner.add(String.valueOf(dimensions.getAuditVersion()));
        addByKey(dimensions.getIsolateKey(), keyJoiner.toString(), values.getCount(),
                values.getSize(), values.getDelayTime());
    }

    /**
     * Add audit info by key.
     */
    private void addByKey(long isolateKey, String statKey, long count, long size, long delayTime) {
        if (null == this.preStatMap.get(isolateKey)) {
            this.preStatMap.putIfAbsent(isolateKey, new ConcurrentHashMap<>());
        }
        ConcurrentHashMap<String, StatInfo> statMap = this.preStatMap.get(isolateKey);
        if (null == statMap.get(statKey)) {
            statMap.putIfAbsent(statKey, new StatInfo(0L, 0L, 0L));
        }
        StatInfo stat = statMap.get(statKey);
        stat.count.addAndGet(count);
        stat.size.addAndGet(size);
        stat.delay.addAndGet(delayTime);
    }

    /**
     * Flush audit data by default audit version
     */
    public synchronized void flush() {
        flush(DEFAULT_ISOLATE_KEY);
    }

    /**
     * Flush audit data
     */
    public synchronized void flush(long isolateKey) {
        if (flushTime.putIfAbsent(isolateKey, System.currentTimeMillis()) != null
                || flushStat.addAndGet(1) > flushStatThreshold) {
            return;
        }
        long startTime = System.currentTimeMillis();
        LOGGER.info("Audit flush isolate key {} ", isolateKey);

        try {
            manager.checkFailedData();
            resetStat();

            summaryExpiredStatMap(isolateKey);

            Iterator<Map.Entry<Long, ConcurrentHashMap<String, StatInfo>>> iterator =
                    this.preStatMap.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<Long, ConcurrentHashMap<String, StatInfo>> entry = iterator.next();
                if (entry.getValue().isEmpty()) {
                    LOGGER.info("Remove the key of pre stat map: {},isolate key: {} ", entry.getKey(), isolateKey);
                    iterator.remove();
                    continue;
                }
                if (entry.getKey() > isolateKey) {
                    continue;
                }
                summaryPreStatMap(entry.getKey(), entry.getValue());
                send(entry.getKey());
            }

            clearExpiredKey(isolateKey);
        } catch (Exception exception) {
            LOGGER.error("Flush audit has exception!", exception);
        } finally {
            manager.closeSocket();
        }

        LOGGER.info(
                "Success report {} package, Failed report {} package, total {} message, memory size {}, cost: {} ms",
                auditMetric.getSuccessPack(), auditMetric.getFailedPack(), auditMetric.getTotalMsg(),
                auditMetric.getMemorySize(),
                System.currentTimeMillis() - startTime);

        auditMetric.reset();
    }

    /**
     * Send base command
     */
    private void sendByBaseCommand(AuditApi.AuditRequest auditRequest) {
        AuditApi.BaseCommand.Builder baseCommand = AuditApi.BaseCommand.newBuilder();
        baseCommand.setType(AUDIT_REQUEST).setAuditRequest(auditRequest).build();
        if (manager.send(baseCommand.build(), auditRequest)) {
            auditMetric.addSuccessPack(1);
        } else {
            auditMetric.addFailedPack(1);
        }
    }

    /**
     * Summary
     */
    private void sumThreadGroup(long isolateKey, String key, StatInfo statInfo) {
        long count = statInfo.count.getAndSet(0);
        if (0 == count) {
            return;
        }
        ConcurrentHashMap<String, StatInfo> sumMap =
                this.summaryStatMap.computeIfAbsent(isolateKey, k -> new ConcurrentHashMap<>());
        StatInfo stat = sumMap.computeIfAbsent(key, k -> new StatInfo(0L, 0L, 0L));
        stat.count.addAndGet(count);
        stat.size.addAndGet(statInfo.size.getAndSet(0));
        stat.delay.addAndGet(statInfo.delay.getAndSet(0));
    }

    /**
     * Reset statistics
     */
    private void resetStat() {
        dataId = 0;
        packageId = 1;
    }

    /**
     * Summary expired stat map
     */
    private void summaryExpiredStatMap(long isolateKey) {
        Iterator<Map.Entry<Long, ConcurrentHashMap<String, StatInfo>>> iterator =
                this.expiredStatMap.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<Long, ConcurrentHashMap<String, StatInfo>> entry = iterator.next();
            if (entry.getValue().isEmpty()) {
                LOGGER.info("Remove the key of expired stat map: {},isolate key: {} ", entry.getKey(), isolateKey);
                iterator.remove();
                continue;
            }
            if (entry.getKey() > isolateKey) {
                continue;
            }
            for (Map.Entry<String, StatInfo> statInfo : entry.getValue().entrySet()) {
                this.sumThreadGroup(isolateKey, statInfo.getKey(), statInfo.getValue());
            }
            entry.getValue().clear();
        }
    }

    /**
     * Summary pre stat map
     */
    private void summaryPreStatMap(long isolateKey, ConcurrentHashMap<String, StatInfo> statInfo) {
        Set<String> expiredKeys = this.expiredKeyList.computeIfAbsent(isolateKey, k -> new HashSet<>());

        for (Map.Entry<String, StatInfo> entry : statInfo.entrySet()) {
            String key = entry.getKey();
            StatInfo value = entry.getValue();
            // If there is no data, enter the list to be eliminated
            if (value.count.get() == 0) {
                expiredKeys.add(key);
                continue;
            }
            sumThreadGroup(isolateKey, key, value);
        }
    }

    /**
     * Clear expired key
     */
    private void clearExpiredKey(long isolateKey) {
        Iterator<Map.Entry<Long, HashSet<String>>> iterator =
                this.expiredKeyList.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<Long, HashSet<String>> entry = iterator.next();
            if (entry.getValue().isEmpty()) {
                LOGGER.info("Remove the key of expired key list: {},isolate key: {}", entry.getKey(), isolateKey);
                iterator.remove();
                continue;
            }
            if (entry.getKey() > isolateKey) {
                continue;
            }

            ConcurrentHashMap<String, StatInfo> preStatInfo = this.preStatMap.get(entry.getKey());
            if (null == preStatInfo) {
                iterator.remove();
                continue;
            }
            ConcurrentHashMap<String, StatInfo> deleteMap =
                    this.expiredStatMap.computeIfAbsent(entry.getKey(), k -> new ConcurrentHashMap<>());
            for (String key : entry.getValue()) {
                StatInfo value = preStatInfo.remove(key);
                deleteMap.put(key, value);
            }
            entry.getValue().clear();
        }
    }

    /**
     * Send Audit data
     */
    private void send(long isolateKey) {
        if (null == summaryStatMap.get(isolateKey)) {
            return;
        }
        if (summaryStatMap.get(isolateKey).isEmpty()) {
            summaryStatMap.remove(isolateKey);
            return;
        }
        long sdkTime = Calendar.getInstance().getTimeInMillis();
        AuditApi.AuditMessageHeader msgHeader = AuditApi.AuditMessageHeader.newBuilder()
                .setIp(config.getLocalIP()).setDockerId(config.getDockerId())
                .setThreadId(String.valueOf(Thread.currentThread().getId()))
                .setSdkTs(sdkTime).setPacketId(packageId)
                .build();
        AuditApi.AuditRequest.Builder requestBuild = AuditApi.AuditRequest.newBuilder();
        requestBuild.setMsgHeader(msgHeader);
        // Process the stat info for all threads
        for (Map.Entry<String, StatInfo> entry : summaryStatMap.get(isolateKey).entrySet()) {
            // Entry key order: logTime inlongGroupID inlongStreamID auditID auditTag auditVersion
            String[] keyArray = entry.getKey().split(FIELD_SEPARATORS);
            if (keyArray.length < 6) {
                LOGGER.error("Number of keys {} <6", keyArray.length);
                continue;
            }

            long logTime;
            long auditVersion;
            try {
                logTime = Long.parseLong(keyArray[0]) * PERIOD;
                auditVersion = Long.parseLong(keyArray[5]);
            } catch (NumberFormatException numberFormatException) {
                LOGGER.error("Failed to parse long from string", numberFormatException);
                continue;
            }

            String inlongGroupID = keyArray[1];
            String inlongStreamID = keyArray[2];
            String auditID = keyArray[3];
            String auditTag = keyArray[4];
            StatInfo value = entry.getValue();
            AuditApi.AuditMessageBody msgBody = AuditApi.AuditMessageBody.newBuilder()
                    .setLogTs(logTime)
                    .setInlongGroupId(inlongGroupID)
                    .setInlongStreamId(inlongStreamID)
                    .setAuditId(auditID)
                    .setAuditTag(auditTag)
                    .setCount(value.count.get())
                    .setSize(value.size.get())
                    .setDelay(value.delay.get())
                    .setAuditVersion(auditVersion)
                    .build();
            requestBuild.addMsgBody(msgBody);

            auditMetric.addMemorySize(msgBody.toByteArray().length);

            if (dataId++ >= BATCH_NUM) {
                dataId = 0;
                packageId++;
                sendData(requestBuild);
            }
        }

        if (requestBuild.getMsgBodyCount() > 0) {
            sendData(requestBuild);
        }
        summaryStatMap.get(isolateKey).clear();
    }

    private void sendData(AuditApi.AuditRequest.Builder requestBuild) {
        requestBuild.setRequestId(RequestIdUtils.nextRequestId());
        sendByBaseCommand(requestBuild.build());
        auditMetric.addTotalMsg(requestBuild.getMsgBodyCount());
        requestBuild.clearMsgBody();
    }

    /**
     * Check flush time
     */
    private void checkFlushTime() {
        flushStat.set(0);
        long currentTime = Calendar.getInstance().getTimeInMillis();
        flushTime.forEach((key, value) -> {
            if ((currentTime - value) > PERIOD) {
                flushTime.remove(key);
            }
        });
    }

    /**
     * Generate Audit item IDs.
     *
     * @param baseAuditId
     * @param success
     * @param isRealtime
     * @param discard
     * @param retry
     * @return
     */
    public int buildAuditId(AuditIdEnum baseAuditId,
            boolean success,
            boolean isRealtime,
            boolean discard,
            boolean retry) {
        return AuditManagerUtils.buildAuditId(baseAuditId, success, isRealtime, discard, retry);
    }

    public int buildSuccessfulAuditId(AuditIdEnum baseAuditId) {
        return buildAuditId(baseAuditId, true, true, false, false);
    }

    public int buildSuccessfulAuditId(AuditIdEnum baseAuditId, boolean isRealtime) {
        return buildAuditId(baseAuditId, true, isRealtime, false, false);
    }

    public int buildFailedAuditId(AuditIdEnum baseAuditId) {
        return buildAuditId(baseAuditId, false, true, false, false);
    }

    public int buildFailedAuditId(AuditIdEnum baseAuditId, boolean isRealtime) {
        return buildAuditId(baseAuditId, false, isRealtime, false, false);
    }

    public int buildDiscardAuditId(AuditIdEnum baseAuditId) {
        return buildAuditId(baseAuditId, true, true, true, false);
    }

    public int buildDiscardAuditId(AuditIdEnum baseAuditId, boolean isRealtime) {
        return buildAuditId(baseAuditId, true, isRealtime, true, false);
    }

    public int buildRetryAuditId(AuditIdEnum baseAuditId) {
        return buildAuditId(baseAuditId, true, true, false, true);
    }

    public int buildRetryAuditId(AuditIdEnum baseAuditId, boolean isRealtime) {
        return buildAuditId(baseAuditId, true, isRealtime, false, true);
    }

    public AuditInformation buildAuditInformation(String auditType,
            FlowType dataFlow,
            boolean success,
            boolean isRealtime,
            boolean discard,
            boolean retry) {
        return AuditManagerUtils.buildAuditInformation(auditType, dataFlow, success, isRealtime, discard, retry);
    }

    public List<AuditInformation> getAllAuditInformation() {
        return AuditManagerUtils.getAllAuditInformation();
    }

    public List<AuditInformation> getAllMetricInformation() {
        return AuditManagerUtils.getAllMetricInformation();
    }

    public List<AuditInformation> getAllAuditInformation(String auditType) {
        return AuditManagerUtils.getAllAuditInformation(auditType);
    }

    public int getStartAuditIdForMetric() {
        return AuditManagerUtils.getStartAuditIdForMetric();
    }

    public void setManagerTimeout(int timeoutMs) {

        ProxyManager.getInstance().setManagerTimeout(timeoutMs);
    }

    public void setAutoUpdateAuditProxy() {
        ProxyManager.getInstance().setAutoUpdateAuditProxy();
    }

    public void setUpdateInterval(int updateInterval) {
        ProxyManager.getInstance().setUpdateInterval(updateInterval);
    }

    public void setMaxGlobalAuditMemory(long maxGlobalAuditMemory) {
        SenderManager.setMaxGlobalAuditMemory(maxGlobalAuditMemory);
    }
}
