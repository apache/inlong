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

import org.apache.inlong.audit.protocol.AuditApi;
import org.apache.inlong.audit.send.SenderManager;
import org.apache.inlong.audit.util.AuditConfig;
import org.apache.inlong.audit.util.Config;
import org.apache.inlong.audit.util.StatInfo;

import org.apache.commons.lang3.ClassUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import static org.apache.inlong.audit.protocol.AuditApi.BaseCommand.Type.AUDIT_REQUEST;

/**
 * Audit operator, which is singleton.
 */
public class AuditOperator implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = LoggerFactory.getLogger(AuditOperator.class);
    private static final String FIELD_SEPARATORS = ":";
    private static final String DEFAULT_AUDIT_TAG = "-1";
    private static final long DEFAULT_AUDIT_VERSION = -1;
    private static final int BATCH_NUM = 100;
    private static final AuditOperator AUDIT_OPERATOR = new AuditOperator();
    private static final ReentrantLock GLOBAL_LOCK = new ReentrantLock();
    private static final int PERIOD = 1000 * 60;
    private final ConcurrentHashMap<String, StatInfo> countMap = new ConcurrentHashMap<>();
    private final HashMap<String, StatInfo> threadCountMap = new HashMap<>();
    private final ConcurrentHashMap<String, StatInfo> deleteCountMap = new ConcurrentHashMap<>();
    private final List<String> deleteKeyList = new ArrayList<>();
    private final Config config = new Config();
    private int packageId = 1;
    private int dataId = 0;
    private boolean initialized = false;
    private SenderManager manager;

    private final ScheduledExecutorService timeoutExecutor = Executors.newSingleThreadScheduledExecutor();
    private AuditConfig auditConfig = null;
    private SocketAddressListLoader loader = null;

    /**
     * Not support create from outer.
     */
    private AuditOperator() {

    }

    /**
     * Get AuditOperator instance.
     */
    public static AuditOperator getInstance() {
        return AUDIT_OPERATOR;
    }

    /**
     * init
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
                    send();
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
     * set loader
     *
     * @param loader the loader to set
     */
    public void setLoader(SocketAddressListLoader loader) {
        this.loader = loader;
    }

    /**
     * setLoaderClass
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
                LOGGER.info("audit IpPortListLoader loaderClass:{}", loaderClassName);
            }
        } catch (Throwable t) {
            LOGGER.error("Fail to init IpPortListLoader,loaderClass:{},error:{}",
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
     * set audit config
     */
    public void setAuditConfig(AuditConfig config) {
        auditConfig = config;
        manager.setAuditConfig(config);
    }

    /**
     * Add audit data
     */
    public void add(int auditID, String inlongGroupID, String inlongStreamID, Long logTime, long count, long size) {
        add(auditID, DEFAULT_AUDIT_TAG, inlongGroupID, inlongStreamID, logTime, count, size);
    }

    public void add(int auditID, String auditTag, String inlongGroupID, String inlongStreamID, Long logTime,
            long count, long size) {
        long delayTime = System.currentTimeMillis() - logTime;
        add(auditID, auditTag, inlongGroupID, inlongStreamID, logTime, count, size,
                delayTime * count, DEFAULT_AUDIT_VERSION);
    }

    public void add(int auditID, String inlongGroupID, String inlongStreamID, Long logTime, long count, long size,
            long delayTime) {
        add(auditID, DEFAULT_AUDIT_TAG, inlongGroupID, inlongStreamID, logTime, count, size,
                delayTime, DEFAULT_AUDIT_VERSION);
    }

    public void add(int auditID, String auditTag, String inlongGroupID, String inlongStreamID, Long logTime,
            long count, long size, long delayTime, long auditVersion) {
        StringJoiner keyJoiner = new StringJoiner(FIELD_SEPARATORS);
        keyJoiner.add(String.valueOf(logTime / PERIOD));
        keyJoiner.add(inlongGroupID);
        keyJoiner.add(inlongStreamID);
        keyJoiner.add(String.valueOf(auditID));
        keyJoiner.add(auditTag);
        keyJoiner.add(String.valueOf(auditVersion));
        addByKey(keyJoiner.toString(), count, size, delayTime);
    }

    /**
     * Add audit info by key.
     */
    private void addByKey(String key, long count, long size, long delayTime) {
        if (countMap.get(key) == null) {
            countMap.put(key, new StatInfo(0L, 0L, 0L));
        }
        countMap.get(key).count.addAndGet(count);
        countMap.get(key).size.addAndGet(size);
        countMap.get(key).delay.addAndGet(delayTime);
    }

    /**
     * Send audit data
     */
    public synchronized void send() {
        manager.clearBuffer();
        resetStat();
        // Retrieve statistics from the list of objects without statistics to be eliminated
        for (Map.Entry<String, StatInfo> entry : this.deleteCountMap.entrySet()) {
            this.sumThreadGroup(entry.getKey(), entry.getValue());
        }
        this.deleteCountMap.clear();
        for (Map.Entry<String, StatInfo> entry : countMap.entrySet()) {
            String key = entry.getKey();
            StatInfo value = entry.getValue();
            // If there is no data, enter the list to be eliminated
            if (value.count.get() == 0) {
                this.deleteKeyList.add(key);
                continue;
            }
            this.sumThreadGroup(key, value);
        }

        // Clean up obsolete statistical data objects
        for (String key : this.deleteKeyList) {
            StatInfo value = this.countMap.remove(key);
            this.deleteCountMap.put(key, value);
        }
        this.deleteKeyList.clear();

        long sdkTime = Calendar.getInstance().getTimeInMillis();
        AuditApi.AuditMessageHeader msgHeader = AuditApi.AuditMessageHeader.newBuilder()
                .setIp(config.getLocalIP()).setDockerId(config.getDockerId())
                .setThreadId(String.valueOf(Thread.currentThread().getId()))
                .setSdkTs(sdkTime).setPacketId(packageId)
                .build();
        AuditApi.AuditRequest.Builder requestBuild = AuditApi.AuditRequest.newBuilder();
        requestBuild.setMsgHeader(msgHeader).setRequestId(manager.nextRequestId());

        // process the stat info for all threads
        for (Map.Entry<String, StatInfo> entry : threadCountMap.entrySet()) {
            // StringJoiner: logTime inlongGroupID inlongStreamID auditID auditTag auditVersion
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
        threadCountMap.clear();

        LOGGER.info("finish report audit data");
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
    private void sumThreadGroup(String key, StatInfo statInfo) {
        long count = statInfo.count.getAndSet(0);
        if (0 == count) {
            return;
        }
        if (threadCountMap.get(key) == null) {
            threadCountMap.put(key, new StatInfo(0, 0, 0));
        }

        long size = statInfo.size.getAndSet(0);
        long delay = statInfo.delay.getAndSet(0);

        threadCountMap.get(key).count.addAndGet(count);
        threadCountMap.get(key).size.addAndGet(size);
        threadCountMap.get(key).delay.addAndGet(delay);
    }

    /**
     * Reset statistics
     */
    private void resetStat() {
        dataId = 0;
        packageId = 1;
    }
}
