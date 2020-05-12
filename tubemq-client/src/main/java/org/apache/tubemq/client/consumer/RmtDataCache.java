/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tubemq.client.consumer;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.tubemq.corebase.TErrCodeConstants;
import org.apache.tubemq.corebase.cluster.BrokerInfo;
import org.apache.tubemq.corebase.cluster.Partition;
import org.apache.tubemq.corebase.cluster.SubscribeInfo;
import org.apache.tubemq.corebase.policies.FlowCtrlRuleHandler;
import org.apache.tubemq.corebase.utils.ThreadUtils;
import org.jboss.netty.util.HashedWheelTimer;
import org.jboss.netty.util.Timeout;
import org.jboss.netty.util.Timer;
import org.jboss.netty.util.TimerTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Remote data cache.
 */
public class RmtDataCache implements Closeable {
    private static final Logger logger = LoggerFactory.getLogger(RmtDataCache.class);
    private static final AtomicLong refCont = new AtomicLong(0);
    private static Timer timer;
    private final FlowCtrlRuleHandler groupFlowCtrlRuleHandler;
    private final FlowCtrlRuleHandler defFlowCtrlRuleHandler;
    private final AtomicInteger waitCont = new AtomicInteger(0);
    private final ConcurrentHashMap<String, Timeout> timeouts =
            new ConcurrentHashMap<>();
    private final BlockingQueue<String> indexPartition =
            new LinkedBlockingQueue<>();
    private final ConcurrentHashMap<String /* index */, PartitionExt> partitionMap =
            new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String /* index */, Long> partitionUsedMap =
            new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String /* index */, Long> partitionOffsetMap =
            new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String /* topic */, ConcurrentLinkedQueue<Partition>> topicPartitionConMap =
            new ConcurrentHashMap<>();
    private final ConcurrentHashMap<BrokerInfo/* broker */, ConcurrentLinkedQueue<Partition>> brokerPartitionConMap =
            new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String/* partitionKey */, Integer> partRegisterBookMap =
            new ConcurrentHashMap<>();
    private AtomicBoolean isClosed = new AtomicBoolean(false);
    private CountDownLatch dataProcessSync = new CountDownLatch(0);


    /**
     * Construct a remote data cache object.
     *
     * @param defFlowCtrlRuleHandler   default flow control rule
     * @param groupFlowCtrlRuleHandler group flow control rule
     * @param partitionList            partition list
     */
    public RmtDataCache(final FlowCtrlRuleHandler defFlowCtrlRuleHandler,
                        final FlowCtrlRuleHandler groupFlowCtrlRuleHandler,
                        List<Partition> partitionList) {
        if (refCont.incrementAndGet() == 1) {
            timer = new HashedWheelTimer();
        }
        this.defFlowCtrlRuleHandler = defFlowCtrlRuleHandler;
        this.groupFlowCtrlRuleHandler = groupFlowCtrlRuleHandler;
        Map<Partition, Long> tmpPartOffsetMap = new HashMap<>();
        if (partitionList != null) {
            for (Partition partition : partitionList) {
                tmpPartOffsetMap.put(partition, -1L);
            }
        }
        addPartitionsInfo(tmpPartOffsetMap);
    }

    /**
     * Set partition context information.
     *
     * @param partitionKey  partition key
     * @param currOffset    current offset
     * @param reqProcType   type information
     * @param errCode       error code
     * @param isEscLimit    if the limitDlt should be ignored
     * @param msgSize       message size
     * @param limitDlt      max offset of the data fetch
     * @param curDataDlt    the offset of current data fetch
     * @param isRequireSlow if the server requires slow down
     */
    public void setPartitionContextInfo(String partitionKey, long currOffset,
                                        int reqProcType, int errCode,
                                        boolean isEscLimit, int msgSize,
                                        long limitDlt, long curDataDlt,
                                        boolean isRequireSlow) {
        PartitionExt partitionExt = partitionMap.get(partitionKey);
        if (partitionExt != null) {
            if (currOffset >= 0) {
                partitionOffsetMap.put(partitionKey, currOffset);
            }
            partitionExt
                    .setPullTempData(reqProcType, errCode,
                            isEscLimit, msgSize, limitDlt,
                            curDataDlt, isRequireSlow);
        }
    }

    /**
     * Check if the partitions are ready.
     *
     * @param maxWaitTime max wait time in milliseconds
     * @return partition status
     */
    public boolean isPartitionsReady(long maxWaitTime) {
        long currTime = System.currentTimeMillis();
        do {
            if (this.isClosed.get()) {
                break;
            }
            if (!partitionMap.isEmpty()) {
                return true;
            }
            ThreadUtils.sleep(250);
        } while (System.currentTimeMillis() - currTime > maxWaitTime);
        return (!partitionMap.isEmpty());
    }

    /**
     * Pull the selected partitions.
     *
     * @return pull result
     */
    public PartitionSelectResult pullSelect() {
        int count = 2;
        do {
            if (this.isClosed.get()) {
                break;
            }
            if (!partitionMap.isEmpty()) {
                break;
            }
            ThreadUtils.sleep(350);
        } while (--count > 0);
        if (partitionMap.isEmpty()) {
            return new PartitionSelectResult(false,
                    TErrCodeConstants.BAD_REQUEST,
                    "No partition info in local, please wait and try later");
        }
        if (indexPartition.isEmpty()) {
            if (hasPartitionWait()) {
                return new PartitionSelectResult(false,
                        TErrCodeConstants.BAD_REQUEST,
                        "All partition in waiting, retry later!");
            } else {
                return new PartitionSelectResult(false,
                        TErrCodeConstants.BAD_REQUEST,
                        "No idle partition to consume, please wait and try later");
            }
        }
        waitCont.incrementAndGet();
        try {
            rebProcessWait();
            if (this.isClosed.get()) {
                return new PartitionSelectResult(false,
                        TErrCodeConstants.BAD_REQUEST,
                        "Client instance has been shutdown!");
            }
            String key = indexPartition.poll();
            if (key == null) {
                if (hasPartitionWait()) {
                    return new PartitionSelectResult(false,
                            TErrCodeConstants.BAD_REQUEST,
                            "All partition in waiting, retry later!");
                } else {
                    return new PartitionSelectResult(false,
                            TErrCodeConstants.BAD_REQUEST,
                            "No idle partition to consume, retry later");
                }
            }
            PartitionExt partitionExt = partitionMap.get(key);
            if (partitionExt == null) {
                return new PartitionSelectResult(false,
                        TErrCodeConstants.BAD_REQUEST,
                        "No valid partition to consume, retry later 1");
            }
            long curTime = System.currentTimeMillis();
            Long newTime = partitionUsedMap.putIfAbsent(key, curTime);
            if (newTime != null) {
                return new PartitionSelectResult(false,
                        TErrCodeConstants.BAD_REQUEST,
                        "No valid partition to consume, retry later 2");
            }
            return new PartitionSelectResult(true, TErrCodeConstants.SUCCESS, "Ok!",
                    partitionExt, curTime, partitionExt.getAndResetLastPackConsumed());
        } catch (Throwable e1) {
            return new PartitionSelectResult(false,
                    TErrCodeConstants.BAD_REQUEST,
                    new StringBuilder(256)
                            .append("Wait partition to consume abnormal : ")
                            .append(e1.getMessage()).toString());
        } finally {
            waitCont.decrementAndGet();
        }
    }

    /**
     * Push the selected partition.
     *
     * @return push result
     */
    public PartitionSelectResult pushSelect() {
        do {
            if (this.isClosed.get()) {
                break;
            }
            if (!partitionMap.isEmpty()) {
                break;
            }
            ThreadUtils.sleep(200);
        } while (true);
        if (this.isClosed.get()) {
            return null;
        }
        waitCont.incrementAndGet();
        try {
            rebProcessWait();
            if (this.isClosed.get()) {
                return null;
            }
            String key = indexPartition.take();
            PartitionExt partitionExt = partitionMap.get(key);
            if (partitionExt == null) {
                return null;
            }
            long curTime = System.currentTimeMillis();
            Long newTime = partitionUsedMap.putIfAbsent(key, curTime);
            if (newTime != null) {
                return null;
            }
            return new PartitionSelectResult(partitionExt,
                    curTime, partitionExt.getAndResetLastPackConsumed());
        } catch (Throwable e1) {
            return null;
        } finally {
            waitCont.decrementAndGet();
        }
    }

    protected boolean isPartitionInUse(String partitionKey, long usedToken) {
        if (partitionMap.containsKey(partitionKey)) {
            Long curToken = partitionUsedMap.get(partitionKey);
            if (curToken != null && curToken == usedToken) {
                return true;
            }
        }
        return false;
    }

    public Partition getPartitionByKey(String partitionKey) {
        return partitionMap.get(partitionKey);
    }

    /**
     * Add a partition.
     *
     * @param partition  partition to be added
     * @param currOffset current offset of the partition
     */
    public void addPartition(Partition partition, long currOffset) {
        if (partition == null) {
            return;
        }
        Map<Partition, Long> tmpPartOffsetMap = new HashMap<>();
        tmpPartOffsetMap.put(partition, currOffset);
        addPartitionsInfo(tmpPartOffsetMap);
    }

    /**
     * book a partition for register event.
     *
     * @param partitionKey  partition key
     *
     *  @return Whether to register for the first time
     */
    public boolean bookPartition(String partitionKey) {
        Integer isReged = partRegisterBookMap.get(partitionKey);
        if (isReged == null) {
            isReged = partRegisterBookMap.putIfAbsent(partitionKey, 1);
            if (isReged == null) {
                return true;
            }
        }
        return false;
    }

    protected void errReqRelease(String partitionKey, long usedToken, boolean isLastPackConsumed) {
        PartitionExt partitionExt = partitionMap.get(partitionKey);
        if (partitionExt != null) {
            if (!indexPartition.contains(partitionKey) && !isTimeWait(partitionKey)) {
                Long oldUsedToken = partitionUsedMap.get(partitionKey);
                if (oldUsedToken != null && oldUsedToken == usedToken) {
                    partitionUsedMap.remove(partitionKey);
                    partitionExt.setLastPackConsumed(isLastPackConsumed);
                    try {
                        indexPartition.put(partitionKey);
                    } catch (Throwable e) {
                        //
                    }
                }
            }
        }
    }

    protected void succRspRelease(String partitionKey, String topicName,
                                  long usedToken, boolean isLastPackConsumed,
                                  boolean isFilterConsume, long currOffset) {
        PartitionExt partitionExt = this.partitionMap.get(partitionKey);
        if (partitionExt != null) {
            if (!indexPartition.contains(partitionKey) && !isTimeWait(partitionKey)) {
                Long oldUsedToken = partitionUsedMap.get(partitionKey);
                if (oldUsedToken != null && oldUsedToken == usedToken) {
                    if (currOffset >= 0) {
                        partitionOffsetMap.put(partitionKey, currOffset);
                    }
                    partitionUsedMap.remove(partitionKey);
                    partitionExt.setLastPackConsumed(isLastPackConsumed);
                    long waitDlt =
                            partitionExt.procConsumeResult(isFilterConsume);
                    if (waitDlt > 10) {
                        timeouts.put(partitionKey,
                                timer.newTimeout(new TimeoutTask(partitionKey), waitDlt, TimeUnit.MILLISECONDS));
                    } else {
                        try {
                            indexPartition.put(partitionKey);
                        } catch (Throwable e) {
                            //
                        }
                    }
                }
            }
        }
    }

    public void errRspRelease(String partitionKey, String topicName,
                              long usedToken, boolean isLastPackConsumed,
                              long currOffset, int reqProcType, int errCode,
                              boolean isEscLimit, int msgSize, long limitDlt,
                              boolean isFilterConsume, long curDataDlt) {
        PartitionExt partitionExt = this.partitionMap.get(partitionKey);
        if (partitionExt != null) {
            if (!indexPartition.contains(partitionKey) && !isTimeWait(partitionKey)) {
                Long oldUsedToken = partitionUsedMap.get(partitionKey);
                if (oldUsedToken != null && oldUsedToken == usedToken) {
                    if (currOffset >= 0) {
                        partitionOffsetMap.put(partitionKey, currOffset);
                    }
                    partitionUsedMap.remove(partitionKey);
                    partitionExt.setLastPackConsumed(isLastPackConsumed);
                    long waitDlt =
                            partitionExt.procConsumeResult(isFilterConsume, reqProcType,
                                    errCode, msgSize, isEscLimit, limitDlt, curDataDlt, false);
                    if (waitDlt > 10) {
                        timeouts.put(partitionKey,
                                timer.newTimeout(new TimeoutTask(partitionKey), waitDlt, TimeUnit.MILLISECONDS));
                    } else {
                        try {
                            indexPartition.put(partitionKey);
                        } catch (Throwable e) {
                            //
                        }
                    }
                }
            }
        }
    }

    /**
     * Close the remote data cache
     */
    @Override
    public void close() {
        if (this.isClosed.get()) {
            return;
        }
        if (this.isClosed.compareAndSet(false, true)) {
            if (refCont.decrementAndGet() == 0) {
                timer.stop();
                timer = null;
            }
            for (int i = this.waitCont.get() + 1; i > 0; i--) {
                try {
                    indexPartition.put("------");
                } catch (Throwable e) {
                    //
                }
            }
        }
    }

    /**
     * Get the subscribe information of the consumer.
     *
     * @param consumerId   consumer id
     * @param consumeGroup consumer group
     * @return subscribe information list
     */
    public List<SubscribeInfo> getSubscribeInfoList(String consumerId, String consumeGroup) {
        List<SubscribeInfo> subscribeInfoList = new ArrayList<>();
        for (Partition partition : partitionMap.values()) {
            if (partition != null) {
                subscribeInfoList.add(new SubscribeInfo(consumerId, consumeGroup, partition));
            }
        }
        return subscribeInfoList;
    }

    public Map<BrokerInfo, List<PartitionSelectResult>> removeAndGetPartition(
            Map<BrokerInfo, List<Partition>> unRegisterInfoMap,
            List<String> partitionKeys, long inUseWaitPeriodMs,
            boolean isWaitTimeoutRollBack) {
        StringBuilder sBuilder = new StringBuilder(512);
        HashMap<BrokerInfo, List<PartitionSelectResult>> unNewRegisterInfoMap =
                new HashMap<>();
        pauseProcess();
        try {
            waitPartitions(partitionKeys, inUseWaitPeriodMs);
            boolean lastPackConsumed = false;
            for (Map.Entry<BrokerInfo, List<Partition>> entry : unRegisterInfoMap.entrySet()) {
                for (Partition partition : entry.getValue()) {
                    PartitionExt partitionExt =
                            partitionMap.remove(partition.getPartitionKey());
                    if (partitionExt != null) {
                        lastPackConsumed = partitionExt.isLastPackConsumed();
                        if (!cancelTimeTask(partition.getPartitionKey())
                                && !indexPartition.remove(partition.getPartitionKey())) {
                            logger.info(sBuilder.append("[Process Interrupt] Partition : ")
                                    .append(partition.toString())
                                    .append(", data in processing, canceled").toString());
                            sBuilder.delete(0, sBuilder.length());
                            if (lastPackConsumed) {
                                if (isWaitTimeoutRollBack) {
                                    lastPackConsumed = false;
                                }
                            }
                        }
                        ConcurrentLinkedQueue<Partition> oldPartitionList =
                                topicPartitionConMap.get(partition.getTopic());
                        if (oldPartitionList != null) {
                            oldPartitionList.remove(partition);
                            if (oldPartitionList.isEmpty()) {
                                topicPartitionConMap.remove(partition.getTopic());
                            }
                        }
                        ConcurrentLinkedQueue<Partition> regMapPartitionList =
                                brokerPartitionConMap.get(entry.getKey());
                        if (regMapPartitionList != null) {
                            regMapPartitionList.remove(partition);
                            if (regMapPartitionList.isEmpty()) {
                                brokerPartitionConMap.remove(entry.getKey());
                            }
                        }
                        partitionOffsetMap.remove(partition.getPartitionKey());
                        partitionUsedMap.remove(partition.getPartitionKey());
                        PartitionSelectResult partitionRet =
                                new PartitionSelectResult(true, TErrCodeConstants.SUCCESS,
                                        "Ok!", partition, 0, lastPackConsumed);
                        List<PartitionSelectResult> targetPartitionList =
                                unNewRegisterInfoMap.get(entry.getKey());
                        if (targetPartitionList == null) {
                            targetPartitionList = new ArrayList<>();
                            unNewRegisterInfoMap.put(entry.getKey(), targetPartitionList);
                        }
                        targetPartitionList.add(partitionRet);
                    }
                }
            }
        } finally {
            resumeProcess();
        }
        return unNewRegisterInfoMap;
    }

    /**
     * Remove a partition.
     *
     * @param partition partition to be removed
     */
    public void removePartition(Partition partition) {
        partitionMap.remove(partition.getPartitionKey());
        cancelTimeTask(partition.getPartitionKey());
        indexPartition.remove(partition.getPartitionKey());
        partitionUsedMap.remove(partition.getPartitionKey());
        partitionOffsetMap.remove(partition.getPartitionKey());
        ConcurrentLinkedQueue<Partition> oldPartitionList =
                topicPartitionConMap.get(partition.getTopic());
        if (oldPartitionList != null) {
            oldPartitionList.remove(partition);
            if (oldPartitionList.isEmpty()) {
                topicPartitionConMap.remove(partition.getTopic());
            }
        }
        ConcurrentLinkedQueue<Partition> regMapPartitionList =
                brokerPartitionConMap.get(partition.getBroker());
        if (regMapPartitionList != null) {
            regMapPartitionList.remove(partition);
            if (regMapPartitionList.isEmpty()) {
                brokerPartitionConMap.remove(partition.getBroker());
            }
        }
    }

    /**
     * Get current partition information.
     *
     * @return consumer offset information map
     */
    public Map<String, ConsumeOffsetInfo> getCurPartitionInfoMap() {
        Map<String, ConsumeOffsetInfo> tmpPartitionMap =
                new ConcurrentHashMap<>();
        for (Map.Entry<String, PartitionExt> entry : partitionMap.entrySet()) {
            if (entry.getKey() == null || entry.getValue() == null) {
                continue;
            }
            Long currOffset = partitionOffsetMap.get(entry.getKey());
            tmpPartitionMap.put(entry.getKey(), new ConsumeOffsetInfo(entry.getKey(), currOffset));
        }
        return tmpPartitionMap;
    }

    public Map<BrokerInfo, List<PartitionSelectResult>> getAllPartitionListWithStatus() {
        Map<BrokerInfo, List<PartitionSelectResult>> registeredInfoMap =
                new HashMap<>();
        for (PartitionExt partitionExt : partitionMap.values()) {
            List<PartitionSelectResult> registerPartitionList =
                    registeredInfoMap.get(partitionExt.getBroker());
            if (registerPartitionList == null) {
                registerPartitionList = new ArrayList<>();
                registeredInfoMap.put(partitionExt.getBroker(), registerPartitionList);
            }
            registerPartitionList.add(new PartitionSelectResult(true,
                    TErrCodeConstants.SUCCESS, "Ok!",
                    partitionExt, 0, partitionExt.isLastPackConsumed()));
        }
        return registeredInfoMap;
    }

    /**
     * Get registered brokers.
     *
     * @return broker information list
     */
    public Set<BrokerInfo> getAllRegisterBrokers() {
        return this.brokerPartitionConMap.keySet();
    }

    /**
     * Get partition list of a broker.
     *
     * @param brokerInfo broker information
     * @return partition list
     */
    public List<Partition> getBrokerPartitionList(BrokerInfo brokerInfo) {
        List<Partition> retPartition = new ArrayList<>();
        ConcurrentLinkedQueue<Partition> partitionList =
                brokerPartitionConMap.get(brokerInfo);
        if (partitionList != null) {
            retPartition.addAll(partitionList);
        }
        return retPartition;
    }

    public void filterCachedPartitionInfo(Map<BrokerInfo, List<Partition>> registerInfoMap,
                                          List<Partition> unRegPartitionList) {
        List<BrokerInfo> brokerInfoList = new ArrayList<>();
        for (Map.Entry<BrokerInfo, List<Partition>> entry : registerInfoMap.entrySet()) {
            if (entry.getKey() == null || entry.getValue() == null) {
                continue;
            }
            ConcurrentLinkedQueue<Partition> partitionList =
                    brokerPartitionConMap.get(entry.getKey());
            if (partitionList == null || partitionList.isEmpty()) {
                unRegPartitionList.addAll(entry.getValue());
            } else {
                boolean isNewBroker = true;
                for (Partition regPartition : entry.getValue()) {
                    if (!partitionList.contains(regPartition)) {
                        unRegPartitionList.add(regPartition);
                        isNewBroker = false;
                    }
                }
                if (isNewBroker) {
                    brokerInfoList.add(entry.getKey());
                }
            }
        }
        for (BrokerInfo brokerInfo : brokerInfoList) {
            registerInfoMap.remove(brokerInfo);
        }
    }

    public ConcurrentLinkedQueue<Partition> getPartitionByBroker(BrokerInfo brokerInfo) {
        return this.brokerPartitionConMap.get(brokerInfo);
    }

    public void resumeTimeoutConsumePartitions(long allowedPeriodTimes) {
        if (!partitionUsedMap.isEmpty()) {
            List<String> partKeys = new ArrayList<>();
            partKeys.addAll(partitionUsedMap.keySet());
            for (String keyId : partKeys) {
                Long oldTime = partitionUsedMap.get(keyId);
                if (oldTime != null && System.currentTimeMillis() - oldTime > allowedPeriodTimes) {
                    partitionUsedMap.remove(keyId);
                    if (partitionMap.containsKey(keyId)) {
                        PartitionExt partitionExt = partitionMap.get(keyId);
                        partitionExt.setLastPackConsumed(false);
                        if (!indexPartition.contains(keyId)) {
                            try {
                                indexPartition.put(keyId);
                            } catch (Throwable e) {
                                //
                            }
                        }
                    }
                }
            }
        }
    }

    private void waitPartitions(List<String> partitionKeys, long inUseWaitPeriodMs) {
        boolean needWait = false;
        long startWaitTime = System.currentTimeMillis();
        do {
            needWait = false;
            for (String partitionKey : partitionKeys) {
                if (partitionUsedMap.get(partitionKey) != null) {
                    needWait = true;
                    break;
                }
            }
            if (needWait) {
                try {
                    Thread.sleep(200);
                } catch (InterruptedException e1) {
                    break;
                }
            }
        } while ((needWait)
                && (!this.isClosed.get())
                && ((System.currentTimeMillis() - startWaitTime) < inUseWaitPeriodMs));

    }

    private void addPartitionsInfo(Map<Partition, Long> partOffsetMap) {
        if (partOffsetMap == null || partOffsetMap.isEmpty()) {
            return;
        }
        for (Map.Entry<Partition, Long> entry : partOffsetMap.entrySet()) {
            if (entry.getKey() == null || entry.getValue() == null) {
                continue;
            }
            Partition partition = entry.getKey();
            if (partitionMap.containsKey(partition.getPartitionKey())) {
                continue;
            }
            ConcurrentLinkedQueue<Partition> topicPartitionQue =
                    topicPartitionConMap.get(partition.getTopic());
            if (topicPartitionQue == null) {
                topicPartitionQue = new ConcurrentLinkedQueue<>();
                ConcurrentLinkedQueue<Partition> tmpTopicPartitionQue =
                        topicPartitionConMap.putIfAbsent(partition.getTopic(), topicPartitionQue);
                if (tmpTopicPartitionQue != null) {
                    topicPartitionQue = tmpTopicPartitionQue;
                }
            }
            if (!topicPartitionQue.contains(partition)) {
                topicPartitionQue.add(partition);
            }
            ConcurrentLinkedQueue<Partition> brokerPartitionQue =
                    brokerPartitionConMap.get(partition.getBroker());
            if (brokerPartitionQue == null) {
                brokerPartitionQue = new ConcurrentLinkedQueue<>();
                ConcurrentLinkedQueue<Partition> tmpBrokerPartQues =
                        brokerPartitionConMap.putIfAbsent(partition.getBroker(), brokerPartitionQue);
                if (tmpBrokerPartQues != null) {
                    brokerPartitionQue = tmpBrokerPartQues;
                }
            }
            if (!brokerPartitionQue.contains(partition)) {
                brokerPartitionQue.add(partition);
            }
            partitionOffsetMap.put(partition.getPartitionKey(), entry.getValue());
            partitionMap.put(partition.getPartitionKey(),
                    new PartitionExt(this.groupFlowCtrlRuleHandler,
                            this.defFlowCtrlRuleHandler, partition.getBroker(),
                            partition.getTopic(), partition.getPartitionId()));
            partitionUsedMap.remove(partition.getPartitionKey());
            if (!indexPartition.contains(partition.getPartitionKey())) {
                try {
                    indexPartition.put(partition.getPartitionKey());
                } catch (Throwable e) {
                    //
                }
            }
        }
    }

    public void rebProcessWait() {
        if (this.dataProcessSync != null
                && this.dataProcessSync.getCount() != 0) {
            try {
                this.dataProcessSync.await();
            } catch (InterruptedException ee) {
                //
            }
        }
    }

    public boolean isRebProcessing() {
        return (this.dataProcessSync != null
                && this.dataProcessSync.getCount() != 0);
    }

    private void pauseProcess() {
        this.dataProcessSync = new CountDownLatch(1);
    }

    private void resumeProcess() {
        this.dataProcessSync.countDown();
    }

    private boolean cancelTimeTask(String indexId) {
        Timeout timeout = timeouts.remove(indexId);
        if (timeout != null) {
            timeout.cancel();
            return true;
        }
        return false;
    }

    private boolean isTimeWait(String indexId) {
        return this.timeouts.containsKey(indexId);
    }

    private boolean hasPartitionWait() {
        return !this.timeouts.isEmpty();
    }

    public class TimeoutTask implements TimerTask {

        private String indexId;

        public TimeoutTask(final String indexId) {
            this.indexId = indexId;
        }

        @Override
        public void run(Timeout timeout) throws Exception {
            Timeout timeout1 = timeouts.remove(indexId);
            if (timeout1 != null) {
                if (partitionMap.containsKey(indexId)) {
                    if (!indexPartition.contains(this.indexId)) {
                        try {
                            indexPartition.put(this.indexId);
                        } catch (Throwable e) {
                            //
                        }
                    }
                }
            }
        }
    }
}




