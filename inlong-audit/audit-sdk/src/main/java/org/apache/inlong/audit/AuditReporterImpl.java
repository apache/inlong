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

import org.apache.inlong.audit.loader.SocketAddressListLoader;
import org.apache.inlong.audit.protocol.AuditApi;
import org.apache.inlong.audit.send.SenderManager;
import org.apache.inlong.audit.util.AuditConfig;
import org.apache.inlong.audit.util.AuditDimensions;
import org.apache.inlong.audit.util.AuditManagerUtils;
import org.apache.inlong.audit.util.AuditValues;
import org.apache.inlong.audit.util.Config;
import org.apache.inlong.audit.util.StatInfo;

import org.apache.commons.lang3.ClassUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
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
    private static final long DEFAULT_ISOLATE_KEY = 0;
    private final ReentrantLock GLOBAL_LOCK = new ReentrantLock();
    private final ConcurrentHashMap<Long, ConcurrentHashMap<String, StatInfo>> preStatMap =
            new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Long, ConcurrentHashMap<String, StatInfo>> summaryStatMap =
            new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Long, ConcurrentHashMap<String, StatInfo>> expiredStatMap =
            new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Long, List<String>> expiredKeyList = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Long, Long> flushTime = new ConcurrentHashMap<>();
    private final Config config = new Config();
    private final ScheduledExecutorService timeoutExecutor = Executors.newSingleThreadScheduledExecutor();
    private int packageId = 1;
    private int dataId = 0;
    private boolean initialized = false;
    private SenderManager manager;
    private AtomicInteger flushStat = new AtomicInteger(0);
    private AuditConfig auditConfig = null;
    private SocketAddressListLoader loader = null;
    private int flushStatThreshold = 100;
    private boolean autoFlush = true;

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
                    if (autoFlush) {
                        flush(DEFAULT_ISOLATE_KEY);
                    }
                    checkFlushTime();
                } catch (Exception e) {
                    LOGGER.error(e.getMessage());
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
        try {
            GLOBAL_LOCK.lockInterruptibly();
            if (!initialized) {
                init();
                initialized = true;
            }
            this.manager.setAuditProxy(ipPortList);
        } catch (InterruptedException e) {
            LOGGER.error(e.getMessage());
        } finally {
            GLOBAL_LOCK.unlock();
        }
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
        flush(DEFAULT_AUDIT_VERSION);
    }

    /**
     * Flush audit data
     */
    public synchronized void flush(long isolateKey) {
        if (flushTime.putIfAbsent(isolateKey, System.currentTimeMillis()) != null
                || flushStat.addAndGet(1) > flushStatThreshold) {
            return;
        }
        LOGGER.info("Audit flush isolate key {} ", isolateKey);
        manager.clearBuffer();
        resetStat();
        LOGGER.info("pre stat map size {} {} {} {}", this.preStatMap.size(), this.expiredStatMap.size(),
                this.summaryStatMap.size(), this.expiredKeyList.size());

        summaryExpiredStatMap(isolateKey);

        Iterator<Map.Entry<Long, ConcurrentHashMap<String, StatInfo>>> iterator = this.preStatMap.entrySet().iterator();
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

        LOGGER.info("Finish report audit data");
    }

    /**
     * Send base command
     */
    private void sendByBaseCommand(AuditApi.AuditRequest auditRequest) {
        AuditApi.BaseCommand.Builder baseCommand = AuditApi.BaseCommand.newBuilder();
        baseCommand.setType(AUDIT_REQUEST).setAuditRequest(auditRequest).build();
        manager.send(baseCommand.build(), auditRequest);
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
        List<String> expiredKeys = this.expiredKeyList.computeIfAbsent(isolateKey, k -> new ArrayList<>());

        for (Map.Entry<String, StatInfo> entry : statInfo.entrySet()) {
            String key = entry.getKey();
            StatInfo value = entry.getValue();
            // If there is no data, enter the list to be eliminated
            if (value.count.get() == 0) {
                if (!expiredKeys.contains(key)) {
                    expiredKeys.add(key);
                }
                continue;
            }
            sumThreadGroup(isolateKey, key, value);
        }
    }

    /**
     * Clear expired key
     */
    private void clearExpiredKey(long isolateKey) {
        Iterator<Map.Entry<Long, List<String>>> iterator =
                this.expiredKeyList.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<Long, List<String>> entry = iterator.next();
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
        requestBuild.setMsgHeader(msgHeader).setRequestId(manager.nextRequestId());
        // Process the stat info for all threads
        for (Map.Entry<String, StatInfo> entry : summaryStatMap.get(isolateKey).entrySet()) {
            // Entry key order: logTime inlongGroupID inlongStreamID auditID auditTag auditVersion
            String[] keyArray = entry.getKey().split(FIELD_SEPARATORS);
            long logTime = Long.parseLong(keyArray[0]) * PERIOD;
            String inlongGroupID = keyArray[1];
            String inlongStreamID = keyArray[2];
            String auditID = keyArray[3];
            String auditTag = keyArray[4];
            long auditVersion = Long.parseLong(keyArray[5]);
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

            if (dataId++ >= BATCH_NUM) {
                dataId = 0;
                packageId++;
                sendByBaseCommand(requestBuild.build());
                requestBuild.clearMsgBody();
            }
        }

        if (requestBuild.getMsgBodyCount() > 0) {
            sendByBaseCommand(requestBuild.build());
            requestBuild.clearMsgBody();
        }
        summaryStatMap.get(isolateKey).clear();
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
     *  Generate Audit item IDs.
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
}
