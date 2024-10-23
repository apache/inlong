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

package org.apache.inlong.tubemq.server.broker.offset;

import org.apache.inlong.tubemq.corebase.TBaseConstants;
import org.apache.inlong.tubemq.corebase.daemon.AbstractDaemonService;
import org.apache.inlong.tubemq.corebase.rv.RetValue;
import org.apache.inlong.tubemq.corebase.utils.MixedUtils;
import org.apache.inlong.tubemq.corebase.utils.TStringUtils;
import org.apache.inlong.tubemq.corebase.utils.Tuple2;
import org.apache.inlong.tubemq.corebase.utils.Tuple3;
import org.apache.inlong.tubemq.corebase.utils.Tuple4;
import org.apache.inlong.tubemq.server.broker.BrokerConfig;
import org.apache.inlong.tubemq.server.broker.metadata.MetadataManager;
import org.apache.inlong.tubemq.server.broker.metadata.TopicMetadata;
import org.apache.inlong.tubemq.server.broker.msgstore.MessageStore;
import org.apache.inlong.tubemq.server.broker.offset.offsetfile.FileOffsetStorage;
import org.apache.inlong.tubemq.server.broker.offset.offsetfile.GroupOffsetStgInfo;
import org.apache.inlong.tubemq.server.broker.offset.offsetfile.OffsetStgInfo;
import org.apache.inlong.tubemq.server.broker.offset.offsetstorage.ZkOffsetStorage;
import org.apache.inlong.tubemq.server.broker.stats.BrokerSrvStatsHolder;
import org.apache.inlong.tubemq.server.broker.utils.DataStoreUtils;
import org.apache.inlong.tubemq.server.common.TServerConstants;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Default offset manager.
 * Conduct consumer's commit offset operation and consumer's offset that has consumed but not committed.
 */
public class DefaultOffsetManager extends AbstractDaemonService implements OffsetService {

    private static final Logger logger = LoggerFactory.getLogger(DefaultOffsetManager.class);
    private final MetadataManager metadataManager;
    private final BrokerConfig brokerConfig;
    private final long startTime;
    private final AtomicBoolean isStart = new AtomicBoolean(false);
    private final AtomicLong sync2FileTime = new AtomicLong(System.currentTimeMillis());
    private OffsetStorage zkOffsetStorage = null;
    private long lstExpireChkTimeMs = System.currentTimeMillis();
    private final FileOffsetStorage fileOffsetStorage;
    private final ConcurrentHashMap<String/* group */, ConcurrentHashMap<String/* topic - partitionId */, OffsetStorageInfo>> cfmOffsetMap =
            new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String/* group */, ConcurrentHashMap<String/* topic - partitionId */, Long>> tmpOffsetMap =
            new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, OffsetHistoryInfo> offlineGroupHisInfoMap =
            new ConcurrentHashMap<>();

    public DefaultOffsetManager(final BrokerConfig brokerConfig,
            final MetadataManager metadataManager) {
        super("[Offset Manager]", brokerConfig.getOffsetStgCacheFlushMs());
        this.brokerConfig = brokerConfig;
        this.metadataManager = metadataManager;
        this.fileOffsetStorage = new FileOffsetStorage(brokerConfig.getBrokerId(),
                brokerConfig.getOffsetStgFilePath(), brokerConfig.getOffsetStgFileSyncMs(),
                brokerConfig.getOffsetStgSyncDurWarnMs());
        if (brokerConfig.getZkConfig() != null
                && (this.fileOffsetStorage.isFistUseFileStg()
                        || this.brokerConfig.isEnableWriteOffset2Zk())) {
            this.zkOffsetStorage = new ZkOffsetStorage(
                    brokerConfig.getZkConfig(), true, brokerConfig.getBrokerId());
        }
        this.startTime = System.currentTimeMillis();
    }

    @Override
    public void start() {
        if (!isStart.compareAndSet(false, true)) {
            return;
        }
        logger.info("[Offset Manager] starting......");
        if (this.fileOffsetStorage.isFistUseFileStg() && this.zkOffsetStorage != null) {
            logger.info("[Offset Manager] begin load group offset info from ZooKeeper");
            GroupOffsetStgInfo curStgInfo = loadZkOffsetStgInfo();
            logger.info("[Offset Manager] loaded offset info from ZooKeeper, wast {} ms, store to file",
                    System.currentTimeMillis() - this.startTime);
            RetValue retValue = FileOffsetStorage.storeOffsetStgInfoToFile(
                    curStgInfo, this.fileOffsetStorage.getOffsetsFileBase());
            if (!retValue.isSuccess()) {
                logger.error("[Offset Manager] store offset stg info failed: {}", retValue.getErrMsg());
                System.exit(2);
            }
            if (!this.brokerConfig.isEnableWriteOffset2Zk()) {
                this.zkOffsetStorage.close();
                this.zkOffsetStorage = null;
            }
            logger.info("[Offset Manager] finished offset info from ZooKeeper to file!");
        }
        this.fileOffsetStorage.start();
        super.start();
        logger.info("[Offset Manager] started, wast {} ms", System.currentTimeMillis() - startTime);
    }

    @Override
    protected void loopProcess(StringBuilder strBuff) {
        if (!this.isStart.get()) {
            return;
        }
        // clean expired groups
        long startTime = System.currentTimeMillis();
        if (startTime - this.lstExpireChkTimeMs > TServerConstants.CFG_GROUP_OFFSETS_STG_EXPIRED_CHECK_DUR_MS) {
            this.lstExpireChkTimeMs = startTime;
            Set<String> cleanGroups = this.fileOffsetStorage.cleanExpiredGroupInfo(
                    startTime, metadataManager.getGrpOffsetsStgExpMs());
            if (!cleanGroups.isEmpty()) {
                for (String group : cleanGroups) {
                    this.cfmOffsetMap.remove(group);
                    this.tmpOffsetMap.remove(group);
                    this.offlineGroupHisInfoMap.remove(group);
                }
                logger.info("[Offset Manager] clean expired group storage info, groups={}, wast={} ms",
                        cleanGroups, System.currentTimeMillis() - startTime);
            }
        }
        // sync cache data to file storage
        try {
            if (commitCfmOffsets(false)) {
                this.sync2FileTime.set(System.currentTimeMillis());
            }
        } catch (Throwable t) {
            logger.error("[Offset Manager] Daemon commit thread throw error ", t);
        }
    }

    @Override
    public void close(long waitTimeMs) {
        if (!isStart.compareAndSet(true, false)) {
            return;
        }
        super.stop();
        logger.info("[Offset Manager] begin reserve temporary Offset.....");
        this.commitTmpOffsets();
        logger.info("[Offset Manager] begin reserve final Offset.....");
        this.commitCfmOffsets(true);
        this.fileOffsetStorage.close();
        if (this.zkOffsetStorage != null) {
            logger.info("[Offset Manager] begin shutdown offset loader.....");
            this.zkOffsetStorage.close();
        }
        logger.info("[Offset Manager] Offset Manager service stopped!");
    }

    /**
     * Load offset.
     *
     * @param msgStore       the message store instance
     * @param group          the consume group name
     * @param topic          the consumed topic name
     * @param partitionId    the consumed partition id
     * @param readStatus     the consume behavior of the consumer group
     * @param reqOffset      the bootstrap offset
     * @param sBuilder       the string buffer
     * @return               the loaded offset information
     */
    @Override
    public OffsetStorageInfo loadOffset(final MessageStore msgStore, final String group,
            final String topic, int partitionId, int readStatus,
            long reqOffset, final StringBuilder sBuilder) {
        OffsetStorageInfo regInfo;
        long indexMaxOffset = msgStore.getIndexMaxOffset();
        long indexMinOffset = msgStore.getIndexMinOffset();
        long defOffset =
                (readStatus == TBaseConstants.CONSUME_MODEL_READ_NORMAL)
                        ? indexMinOffset
                        : indexMaxOffset;
        String offsetCacheKey = OffsetStgInfo.buildOffsetKey(topic, partitionId);
        regInfo = loadOrCreateOffset(group, topic, partitionId, offsetCacheKey, defOffset);
        getAndResetTmpOffset(group, offsetCacheKey);
        final long curOffset = regInfo.getOffset();
        final boolean isFirstCreate = regInfo.isFirstCreate();
        if ((reqOffset >= 0)
                || (readStatus == TBaseConstants.CONSUME_MODEL_READ_FROM_MAX_ALWAYS)) {
            long adjOffset = indexMaxOffset;
            if (readStatus != TBaseConstants.CONSUME_MODEL_READ_FROM_MAX_ALWAYS) {
                adjOffset = MixedUtils.mid(reqOffset, indexMinOffset, indexMaxOffset);
            }
            regInfo.getAndSetOffset(adjOffset);
        }
        sBuilder.append("[Offset Manager]");
        switch (readStatus) {
            case TBaseConstants.CONSUME_MODEL_READ_FROM_MAX_ALWAYS:
                sBuilder.append(" Consume From Max Offset Always");
                break;
            case TBaseConstants.CONSUME_MODEL_READ_FROM_MAX:
                sBuilder.append(" Consume From Max Offset");
                break;
            default: {
                sBuilder.append(" Normal Offset");
            }
        }
        if (!isFirstCreate) {
            sBuilder.append(",Continue");
        }
        long currentOffset = regInfo.getOffset();
        long offsetDelta = indexMaxOffset - currentOffset;
        logger.info(sBuilder.append(",requestOffset=").append(reqOffset)
                .append(",loadedOffset=").append(curOffset)
                .append(",currentOffset=").append(currentOffset)
                .append(",maxOffset=").append(indexMaxOffset)
                .append(",offsetDelta=").append(offsetDelta)
                .append(",lagLevel=").append(getLagLevel(offsetDelta))
                .append(",group=").append(group)
                .append(",topic-part=").append(offsetCacheKey).toString());
        sBuilder.delete(0, sBuilder.length());
        return regInfo;
    }

    /**
     * Get offset by parameters.
     *
     * @param msgStore       the message store instance
     * @param group          the consume group name
     * @param topic          the consumed topic name
     * @param partitionId    the consumed partition id
     * @param isManCommit    whether manual commit offset
     * @param lastConsumed   whether the latest fetch is consumed
     * @param sb             the string buffer
     * @return               the current offset
     */
    @Override
    public long getOffset(final MessageStore msgStore, final String group,
            final String topic, int partitionId,
            boolean isManCommit, boolean lastConsumed,
            final StringBuilder sb) {
        String offsetCacheKey = OffsetStgInfo.buildOffsetKey(topic, partitionId);
        OffsetStorageInfo regInfo =
                loadOrCreateOffset(group, topic, partitionId, offsetCacheKey, 0);
        long requestOffset = regInfo.getOffset();
        if (isManCommit) {
            requestOffset = requestOffset + getTmpOffset(group, topic, partitionId);
        } else {
            if (lastConsumed) {
                requestOffset = commitOffset(group, topic, partitionId, true);
            }
        }
        final long maxOffset = msgStore.getIndexMaxOffset();
        final long minOffset = msgStore.getIndexMinOffset();
        if (requestOffset >= maxOffset) {
            if (requestOffset > maxOffset && brokerConfig.isUpdateConsumerOffsets()) {
                logger.warn(sb
                        .append("[Offset Manager] requestOffset is bigger than maxOffset, reset! requestOffset=")
                        .append(requestOffset).append(",maxOffset=").append(maxOffset)
                        .append(",group=").append(group)
                        .append(",topic-part=").append(offsetCacheKey).toString());
                sb.delete(0, sb.length());
                setTmpOffset(group, offsetCacheKey, maxOffset - requestOffset);
                if (!isManCommit) {
                    requestOffset = commitOffset(group, topic, partitionId, true);
                }
            }
            return -requestOffset;
        } else if (requestOffset < minOffset) {
            logger.warn(sb
                    .append("[Offset Manager] requestOffset is lower than minOffset, reset! requestOffset=")
                    .append(requestOffset).append(",minOffset=").append(minOffset)
                    .append(",group=").append(group)
                    .append(",topic-part=").append(offsetCacheKey).toString());
            sb.delete(0, sb.length());
            setTmpOffset(group, offsetCacheKey, minOffset - requestOffset);
            requestOffset = commitOffset(group, topic, partitionId, true);
        }
        return requestOffset;
    }

    @Override
    public long getOffset(final String group, final String topic, int partitionId) {
        OffsetStorageInfo regInfo =
                loadOrCreateOffset(group, topic, partitionId,
                        OffsetStgInfo.buildOffsetKey(topic, partitionId), 0);
        return regInfo.getOffset();
    }

    @Override
    public void bookOffset(final String group, final String topic, int partitionId,
            int readDalt, boolean isManCommit, boolean isMsgEmpty,
            final StringBuilder sb) {
        if (readDalt == 0) {
            return;
        }
        String offsetCacheKey = OffsetStgInfo.buildOffsetKey(topic, partitionId);
        if (isManCommit) {
            long tmpOffset = getTmpOffset(group, topic, partitionId);
            setTmpOffset(group, offsetCacheKey, readDalt + tmpOffset);
        } else {
            setTmpOffset(group, offsetCacheKey, readDalt);
            if (isMsgEmpty) {
                commitOffset(group, topic, partitionId, true);
            }
        }
    }

    /**
     * Commit offset.
     *
     * @param group         the consume group name
     * @param topic         the consumed topic name
     * @param partitionId   the consumed partition id
     * @param isConsumed    whether the latest fetch is consumed
     * @return              the current offset
     */
    @Override
    public long commitOffset(final String group, final String topic,
            int partitionId, boolean isConsumed) {
        long updatedOffset;
        String offsetCacheKey = OffsetStgInfo.buildOffsetKey(topic, partitionId);
        long tmpOffset = getAndResetTmpOffset(group, offsetCacheKey);
        if (!isConsumed) {
            tmpOffset = 0;
        }
        OffsetStorageInfo regInfo =
                loadOrCreateOffset(group, topic, partitionId, offsetCacheKey, 0);
        if ((tmpOffset == 0) && (!regInfo.isFirstCreate())) {
            return regInfo.getOffset();
        }
        updatedOffset = regInfo.addAndGetOffset(tmpOffset);
        if (logger.isDebugEnabled()) {
            logger.debug(new StringBuilder(512)
                    .append("[Offset Manager] Update offset finished, offset=").append(updatedOffset)
                    .append(",group=").append(group).append(",topic-part=").append(offsetCacheKey).toString());
        }
        return updatedOffset;
    }

    /**
     * Reset offset.
     *
     * @param store          the message store instance
     * @param group          the consume group name
     * @param topic          the consumed topic name
     * @param partitionId    the consumed partition id
     * @param reSetOffset    the reset offset
     * @param modifier       the modifier
     * @return               the current offset
     */
    @Override
    public long resetOffset(final MessageStore store, final String group,
            final String topic, int partitionId,
            long reSetOffset, final String modifier) {
        long oldOffset = -1;
        if (store != null) {
            long indexMaxOffset = store.getIndexMaxOffset();
            reSetOffset = MixedUtils.mid(reSetOffset, store.getIndexMinOffset(), indexMaxOffset);
            String offsetCacheKey = OffsetStgInfo.buildOffsetKey(topic, partitionId);
            getAndResetTmpOffset(group, offsetCacheKey);
            OffsetStorageInfo regInfo =
                    loadOrCreateOffset(group, topic, partitionId, offsetCacheKey, 0);
            oldOffset = regInfo.getAndSetOffset(reSetOffset);
            long currentOffset = regInfo.getOffset();
            long offsetDelta = indexMaxOffset - currentOffset;
            logger.info(new StringBuilder(512)
                    .append("[Offset Manager] Manual update offset by modifier=")
                    .append(modifier).append(",resetOffset=").append(reSetOffset)
                    .append(",loadedOffset=").append(oldOffset)
                    .append(",currentOffset=").append(currentOffset)
                    .append(",maxOffset=").append(indexMaxOffset)
                    .append(",offsetDelta=").append(offsetDelta)
                    .append(",lagLevel=").append(getLagLevel(offsetDelta))
                    .append(",group=").append(group)
                    .append(",topic-part=").append(offsetCacheKey).toString());
        }
        return oldOffset;
    }

    /**
     * Get temp offset.
     *
     * @param group          the consume group name
     * @param topic          the consumed topic name
     * @param partitionId    the consumed partition id
     * @return               the inflight offset
     */
    @Override
    public long getTmpOffset(final String group, final String topic, int partitionId) {
        String offsetCacheKey = OffsetStgInfo.buildOffsetKey(topic, partitionId);
        ConcurrentHashMap<String, Long> partTmpOffsetMap = tmpOffsetMap.get(group);
        if (partTmpOffsetMap != null) {
            Long tmpOffset = partTmpOffsetMap.get(offsetCacheKey);
            if (tmpOffset == null) {
                return 0;
            } else {
                return tmpOffset - tmpOffset % DataStoreUtils.STORE_INDEX_HEAD_LEN;
            }
        }
        return 0;
    }

    /**
     * Get in-memory and in zk group set
     *
     * @return booked group in memory and in zk
     */
    @Override
    public Set<String> getAllBookedGroups() {
        Map<String, Set<String>> totalGroups =
                this.fileOffsetStorage.queryGroupTopicInfo(null);
        return new HashSet<>(totalGroups.keySet());
    }

    /**
     * Get the online group set
     *
     * @return  the online group set
     */
    public Set<String> getOnlineGroups() {
        return new HashSet<>(this.cfmOffsetMap.keySet());
    }

    /**
     * Get the offline group set
     *
     * @return  the offline group set
     */
    @Override
    public Set<String> getOfflineGroups() {
        Map<String, Set<String>> totalGroups =
                this.fileOffsetStorage.queryGroupTopicInfo(null);
        Set<String> offlineGroups = new HashSet<>(totalGroups.keySet());
        offlineGroups.removeAll(this.cfmOffsetMap.keySet());
        return offlineGroups;
    }

    /**
     * Get the topic set subscribed by the consume group
     *
     * @param group    the queries group name
     * @return         the topic set subscribed
     */
    @Override
    public Set<String> getGroupSubInfo(String group) {
        Set<String> result = new HashSet<>();
        Map<String, OffsetStorageInfo> topicPartOffsetMap = cfmOffsetMap.get(group);
        if (topicPartOffsetMap == null) {
            Set<String> groups = new HashSet<>(1);
            groups.add(group);
            Map<String, Set<String>> groupTopicInfo =
                    this.fileOffsetStorage.queryGroupTopicInfo(groups);
            result = groupTopicInfo.get(group);
        } else {
            for (OffsetStorageInfo storageInfo : topicPartOffsetMap.values()) {
                result.add(storageInfo.getTopic());
            }
        }
        return result;
    }

    @Override
    public void cleanRmvTopicInfo(Set<String> rmvTopics) {
        if (rmvTopics == null || rmvTopics.isEmpty()) {
            return;
        }
        int cycleCnt = 0;
        long curSyncTime;
        Set<String> rmvGroups;
        do {
            cycleCnt++;
            curSyncTime = this.sync2FileTime.get();
            rmvGroups = this.fileOffsetStorage.cleanRmvTopicInfo(rmvTopics);
            if (!rmvGroups.isEmpty()) {
                for (String group : rmvGroups) {
                    this.cfmOffsetMap.remove(group);
                    this.tmpOffsetMap.remove(group);
                    this.offlineGroupHisInfoMap.remove(group);
                }
                logger.info("[Offset Manager] clean removed topic storage info, topics={}, groups={}",
                        rmvTopics, rmvGroups);
            }
        } while (cycleCnt < 2 && curSyncTime != this.sync2FileTime.get());
    }

    /**
     * Get group's offset by Specified topic-partitions
     *
     * @param group           the consume group that to query
     * @param topicPartMap    the topic partition map that to query
     * @return group offset info in memory or zk
     */
    @Override
    public Map<String, Map<Integer, Tuple2<Long, Long>>> queryGroupOffset(
            String group, Map<String, Set<Integer>> topicPartMap) {
        Map<String, Map<Integer, Tuple2<Long, Long>>> result = new HashMap<>();
        // search group from memory
        Map<String, OffsetStorageInfo> topicPartOffsetMap = cfmOffsetMap.get(group);
        if (topicPartOffsetMap == null) {
            // query from zookeeper
            for (Map.Entry<String, Set<Integer>> entry : topicPartMap.entrySet()) {
                if (entry == null || entry.getKey() == null || entry.getValue() == null) {
                    continue;
                }
                Map<Integer, Long> qryResult =
                        this.fileOffsetStorage.queryGroupOffsetInfo(group, entry.getKey(), entry.getValue());
                Map<Integer, Tuple2<Long, Long>> offsetMap = new HashMap<>();
                for (Map.Entry<Integer, Long> item : qryResult.entrySet()) {
                    if (item == null || item.getKey() == null || item.getValue() == null) {
                        continue;
                    }
                    offsetMap.put(item.getKey(), new Tuple2<>(item.getValue(), 0L));
                }
                if (!offsetMap.isEmpty()) {
                    result.put(entry.getKey(), offsetMap);
                }
            }
        } else {
            // found in memory, get offset values
            Map<String, Long> tmpPartOffsetMap = tmpOffsetMap.get(group);
            for (Map.Entry<String, Set<Integer>> entry : topicPartMap.entrySet()) {
                Map<Integer, Tuple2<Long, Long>> offsetMap = new HashMap<>();
                for (Integer partitionId : entry.getValue()) {
                    String offsetCacheKey =
                            OffsetStgInfo.buildOffsetKey(entry.getKey(), partitionId);
                    OffsetStorageInfo offsetInfo = topicPartOffsetMap.get(offsetCacheKey);
                    Long tmpOffset = tmpPartOffsetMap.get(offsetCacheKey);
                    if (tmpOffset == null) {
                        tmpOffset = 0L;
                    }
                    if (offsetInfo != null) {
                        offsetMap.put(partitionId,
                                new Tuple2<>(offsetInfo.getOffset(), tmpOffset));
                    }
                }
                if (!offsetMap.isEmpty()) {
                    result.put(entry.getKey(), offsetMap);
                }
            }
        }
        return result;
    }

    /**
     * Get online groups' offset information
     *
     * @return group offset info in memory
     */
    @Override
    public Map<String, OffsetHistoryInfo> getOnlineGroupOffsetInfo() {
        Long tmpOffset;
        OffsetHistoryInfo recordInfo;
        ConcurrentHashMap<String, Long> fetchOffsetMap;
        Map<String, OffsetHistoryInfo> result = new HashMap<>();
        for (Map.Entry<String, ConcurrentHashMap<String, OffsetStorageInfo>> entry : cfmOffsetMap.entrySet()) {
            if (entry == null || entry.getKey() == null || entry.getValue() == null) {
                continue;
            }
            // get fetch offset map
            fetchOffsetMap = tmpOffsetMap.get(entry.getKey());
            // read confirm offset map information
            for (Map.Entry<String, OffsetStorageInfo> partEntry : entry.getValue().entrySet()) {
                if (partEntry == null
                        || partEntry.getKey() == null
                        || partEntry.getValue() == null) {
                    continue;
                }
                recordInfo = result.get(entry.getKey());
                if (recordInfo == null) {
                    recordInfo = new OffsetHistoryInfo(
                            brokerConfig.getBrokerId(), entry.getKey());
                    result.put(entry.getKey(), recordInfo);
                }
                tmpOffset = 0L;
                if (fetchOffsetMap != null) {
                    tmpOffset = fetchOffsetMap.get(partEntry.getKey());
                    if (tmpOffset == null) {
                        tmpOffset = 0L;
                    }
                }
                recordInfo.addCsmOffsets(partEntry.getValue().getTopic(),
                        partEntry.getValue().getPartitionId(),
                        partEntry.getValue().getOffset(), tmpOffset);
            }
        }
        return result;
    }

    /**
     * Get unregistered groups' offset information
     *
     * @return group offset info in zk
     */
    @Override
    public Map<String, OffsetHistoryInfo> getOfflineGroupOffsetInfo() {
        // get offline groups
        Map<String, Set<String>> totalGroups =
                this.fileOffsetStorage.queryGroupTopicInfo(null);
        Set<String> offlineGroups = new HashSet<>(totalGroups.keySet());
        offlineGroups.removeAll(this.cfmOffsetMap.keySet());
        // get offline history offset info
        OffsetHistoryInfo tmpInfo;
        Set<String> offlineGroups2 = new HashSet<>();
        Map<String, OffsetHistoryInfo> result = new HashMap<>();
        for (String group : offlineGroups) {
            tmpInfo = this.offlineGroupHisInfoMap.get(group);
            if (tmpInfo == null) {
                offlineGroups2.add(group);
                continue;
            }
            result.put(group, tmpInfo);
        }
        if (!offlineGroups2.isEmpty()) {
            Map<String, OffsetHistoryInfo> extResult =
                    this.fileOffsetStorage.getOffsetHistoryInfo(offlineGroups2);
            if (extResult.isEmpty()) {
                return result;
            }
            for (OffsetHistoryInfo historyInfo : extResult.values()) {
                tmpInfo = this.offlineGroupHisInfoMap.get(historyInfo.getGroupName());
                if (tmpInfo == null) {
                    tmpInfo = this.offlineGroupHisInfoMap.putIfAbsent(historyInfo.getGroupName(), historyInfo);
                    if (tmpInfo == null) {
                        tmpInfo = historyInfo;
                    }
                    result.put(historyInfo.getGroupName(), tmpInfo);
                }
            }
        }
        return result;
    }

    /**
     * Reset offset.
     *
     * @param groups              the groups to reset offset
     * @param topicPartOffsets    the reset offset information
     * @param modifier            the modifier
     * @return at least one record modified
     */
    @Override
    public boolean modifyGroupOffset(Set<String> groups,
            List<Tuple3<String, Integer, Long>> topicPartOffsets,
            String modifier) {
        long oldOffset;
        boolean changed = false;
        String offsetCacheKey;
        StringBuilder strBuff = new StringBuilder(512);
        // set offset by group
        for (String group : groups) {
            for (Tuple3<String, Integer, Long> tuple3 : topicPartOffsets) {
                if (tuple3 == null
                        || tuple3.getF0() == null
                        || tuple3.getF1() == null
                        || tuple3.getF2() == null) {
                    continue;
                }
                // set offset value
                offsetCacheKey = OffsetStgInfo.buildOffsetKey(tuple3.getF0(), tuple3.getF1());
                getAndResetTmpOffset(group, offsetCacheKey);
                OffsetStorageInfo regInfo = loadOrCreateOffset(group,
                        tuple3.getF0(), tuple3.getF1(), offsetCacheKey, 0);
                oldOffset = regInfo.getAndSetOffset(tuple3.getF2());
                changed = true;
                logger.info(strBuff
                        .append("[Offset Manager] Update offset by modifier=")
                        .append(modifier).append(",resetOffset=").append(tuple3.getF2())
                        .append(",loadedOffset=").append(oldOffset)
                        .append(",currentOffset=").append(regInfo.getOffset())
                        .append(",group=").append(group)
                        .append(",topic-part=").append(offsetCacheKey).toString());
                strBuff.delete(0, strBuff.length());
            }
        }
        return changed;
    }

    /**
     * Reset offset.
     *
     * @param groups              the groups to reset offset
     * @param topicPartOffsets    the reset offset information
     * @param modifier            the modifier
     * @return at least one record modified
     */
    @Override
    public boolean modifyGroupOffset2(Set<String> groups,
            List<Tuple4<Long, String, Integer, Long>> topicPartOffsets,
            String modifier) {
        long oldOffset;
        boolean changed = false;
        String offsetCacheKey;
        StringBuilder strBuff = new StringBuilder(512);
        // set offset by group
        for (String group : groups) {
            for (Tuple4<Long, String, Integer, Long> tuple4 : topicPartOffsets) {
                if (tuple4 == null
                        || tuple4.getF0() == null
                        || tuple4.getF1() == null
                        || tuple4.getF2() == null
                        || tuple4.getF3() == null) {
                    continue;
                }
                // set offset value
                offsetCacheKey = OffsetStgInfo.buildOffsetKey(tuple4.getF1(), tuple4.getF2());
                getAndResetTmpOffset(group, offsetCacheKey);
                OffsetStorageInfo regInfo = loadOrCreateOffset(group,
                        tuple4.getF1(), tuple4.getF2(), offsetCacheKey, 0);
                oldOffset = regInfo.getAndSetOffset(tuple4.getF3());
                changed = true;
                logger.info(strBuff.append("[Offset Manager2] Update offset by modifier=").append(modifier)
                        .append(",recordTime=").append(tuple4.getF0())
                        .append(",resetOffset=").append(tuple4.getF3())
                        .append(",loadedOffset=").append(oldOffset)
                        .append(",currentOffset=").append(regInfo.getOffset())
                        .append(",group=").append(group)
                        .append(",topic-part=").append(offsetCacheKey).toString());
                strBuff.delete(0, strBuff.length());
            }
        }
        return changed;
    }

    /**
     * Delete offset.
     *
     * @param onlyMemory          whether only memory cached value
     * @param groupTopicPartMap   need removed group and topic map
     * @param modifier            the modifier
     */
    @Override
    public void deleteGroupOffset(boolean onlyMemory,
            Map<String, Map<String, Set<Integer>>> groupTopicPartMap,
            String modifier) {
        String printBase;
        StringBuilder strBuff = new StringBuilder(512);
        for (Map.Entry<String, Map<String, Set<Integer>>> entry : groupTopicPartMap.entrySet()) {
            if (entry.getKey() == null
                    || entry.getValue() == null
                    || entry.getValue().isEmpty()) {
                continue;
            }
            rmvOffset(entry.getKey(), entry.getValue());
        }
        this.fileOffsetStorage.deleteGroupOffsetInfo(groupTopicPartMap);
        if (this.zkOffsetStorage != null) {
            if (onlyMemory) {
                printBase = strBuff
                        .append("[Offset Manager] delete offset from memory by modifier=")
                        .append(modifier).toString();
            } else {
                zkOffsetStorage.deleteGroupOffsetInfo(groupTopicPartMap);
                printBase = strBuff
                        .append("[Offset Manager] delete offset from memory and zk by modifier=")
                        .append(modifier).toString();
            }
        } else {
            printBase = strBuff
                    .append("[Offset Manager] delete offset from memory and file by modifier=")
                    .append(modifier).toString();
        }
        strBuff.delete(0, strBuff.length());
        // print log
        for (Map.Entry<String, Map<String, Set<Integer>>> entry : groupTopicPartMap.entrySet()) {
            if (entry.getKey() == null
                    || entry.getValue() == null
                    || entry.getValue().isEmpty()) {
                continue;
            }
            logger.info(strBuff.append(printBase).append(",group=").append(entry.getKey())
                    .append(",topic-partId-map=").append(entry.getValue()).toString());
            strBuff.delete(0, strBuff.length());
        }
    }

    /**
     * Backup group offsets info to the specified path
     *
     * @param backupFilePath           the specified path
     *
     * @return  whether success
     */
    @Override
    public RetValue backupGroupOffsets(String backupFilePath) {
        return this.fileOffsetStorage.backupGroupOffsets(backupFilePath);
    }

    /**
     * Set temp offset.
     *
     * @param group               the consume group name
     * @param offsetCacheKey      the offset store key
     * @param origOffset          the set value
     * @return                    the current inflight offset
     */
    private long setTmpOffset(final String group, final String offsetCacheKey, long origOffset) {
        long tmpOffset = origOffset - origOffset % DataStoreUtils.STORE_INDEX_HEAD_LEN;
        ConcurrentHashMap<String, Long> partTmpOffsetMap = tmpOffsetMap.get(group);
        if (partTmpOffsetMap == null) {
            ConcurrentHashMap<String, Long> tmpMap = new ConcurrentHashMap<>();
            partTmpOffsetMap = tmpOffsetMap.putIfAbsent(group, tmpMap);
            if (partTmpOffsetMap == null) {
                partTmpOffsetMap = tmpMap;
            }
        }
        Long befOffset = partTmpOffsetMap.put(offsetCacheKey, tmpOffset);
        if (befOffset == null) {
            return 0;
        } else {
            return (befOffset - befOffset % DataStoreUtils.STORE_INDEX_HEAD_LEN);
        }
    }

    private long getAndResetTmpOffset(final String group, final String offsetCacheKey) {
        return setTmpOffset(group, offsetCacheKey, 0L);
    }

    /**
     * Commit temp offsets.
     */
    private void commitTmpOffsets() {
        for (Map.Entry<String, ConcurrentHashMap<String, Long>> entry : tmpOffsetMap.entrySet()) {
            if (TStringUtils.isBlank(entry.getKey())
                    || entry.getValue() == null || entry.getValue().isEmpty()) {
                continue;
            }
            for (Map.Entry<String, Long> topicEntry : entry.getValue().entrySet()) {
                if (TStringUtils.isBlank(topicEntry.getKey())) {
                    continue;
                }
                String[] topicPartStrs = topicEntry.getKey().split("-");
                String topic = topicPartStrs[0];
                int partitionId = Integer.parseInt(topicPartStrs[1]);
                try {
                    commitOffset(entry.getKey(), topic, partitionId, true);
                } catch (Exception e) {
                    logger.warn("[Offset Manager] Commit tmp offset error!", e);
                }
            }
        }
    }

    private boolean commitCfmOffsets(boolean retryable) {
        boolean updated = false;
        long startTime = System.currentTimeMillis();
        if (this.zkOffsetStorage == null) {
            for (Map.Entry<String, ConcurrentHashMap<String, OffsetStorageInfo>> entry : cfmOffsetMap.entrySet()) {
                if (TStringUtils.isBlank(entry.getKey())
                        || entry.getValue() == null || entry.getValue().isEmpty()) {
                    continue;
                }
                if (this.fileOffsetStorage.commitOffset(entry.getKey(), entry.getValue().values(), retryable)) {
                    updated = true;
                }
            }
            BrokerSrvStatsHolder.updOffsetFileSyncDataDlt(System.currentTimeMillis() - startTime);
        } else {
            for (Map.Entry<String, ConcurrentHashMap<String, OffsetStorageInfo>> entry : cfmOffsetMap.entrySet()) {
                if (TStringUtils.isBlank(entry.getKey())
                        || entry.getValue() == null || entry.getValue().isEmpty()) {
                    continue;
                }
                if (this.fileOffsetStorage.commitOffset(entry.getKey(), entry.getValue().values(), retryable)) {
                    updated = true;
                }
                this.zkOffsetStorage.commitOffset(entry.getKey(), entry.getValue().values(), retryable);
            }
            long endTime = System.currentTimeMillis();
            BrokerSrvStatsHolder.updZKSyncDataDlt(endTime - startTime);
            BrokerSrvStatsHolder.updOffsetFileSyncDataDlt(endTime - startTime);
        }
        return updated;
    }

    /**
     * Load or create offset.
     *
     * @param group            the consume group name
     * @param topic            the consumed topic name
     * @param partitionId      the consumed partition id
     * @param offsetCacheKey   the offset store key
     * @param defOffset        the default offset if not found
     * @return                 the stored offset object
     */
    private OffsetStorageInfo loadOrCreateOffset(final String group, final String topic,
            int partitionId, final String offsetCacheKey,
            long defOffset) {
        ConcurrentHashMap<String, OffsetStorageInfo> regInfoMap = cfmOffsetMap.get(group);
        if (regInfoMap == null) {
            ConcurrentHashMap<String, OffsetStorageInfo> loadedStgInfo =
                    fileOffsetStorage.loadGroupStgInfo(group);
            regInfoMap = cfmOffsetMap.putIfAbsent(group, loadedStgInfo);
            if (regInfoMap == null) {
                regInfoMap = loadedStgInfo;
            }
        }
        OffsetStorageInfo regInfo = regInfoMap.get(offsetCacheKey);
        if (regInfo == null) {
            OffsetStorageInfo tmpRegInfo =
                    fileOffsetStorage.loadOffset(group, topic, partitionId);
            if (tmpRegInfo == null) {
                tmpRegInfo = new OffsetStorageInfo(topic,
                        brokerConfig.getBrokerId(), partitionId, 0, defOffset, 0);
            }
            regInfo = regInfoMap.putIfAbsent(offsetCacheKey, tmpRegInfo);
            if (regInfo == null) {
                regInfo = tmpRegInfo;
            }
        }
        return regInfo;
    }

    private void rmvOffset(String group, Map<String, Set<Integer>> topicPartMap) {
        if (group == null
                || topicPartMap == null
                || topicPartMap.isEmpty()) {
            return;
        }
        // remove confirm offset
        ConcurrentHashMap<String, OffsetStorageInfo> regInfoMap = cfmOffsetMap.get(group);
        if (regInfoMap != null) {
            for (Map.Entry<String, Set<Integer>> entry : topicPartMap.entrySet()) {
                if (entry.getKey() == null
                        || entry.getValue() == null
                        || entry.getValue().isEmpty()) {
                    continue;
                }
                for (Integer partitionId : entry.getValue()) {
                    regInfoMap.remove(OffsetStgInfo.buildOffsetKey(entry.getKey(), partitionId));
                }
            }
            if (regInfoMap.isEmpty()) {
                cfmOffsetMap.remove(group);
            }
        }
        // remove tmp offset
        ConcurrentHashMap<String, Long> tmpRegInfoMap = tmpOffsetMap.get(group);
        if (tmpRegInfoMap != null) {
            for (Map.Entry<String, Set<Integer>> entry : topicPartMap.entrySet()) {
                if (entry.getKey() == null
                        || entry.getValue() == null
                        || entry.getValue().isEmpty()) {
                    continue;
                }
                for (Integer partitionId : entry.getValue()) {
                    tmpRegInfoMap.remove(OffsetStgInfo.buildOffsetKey(entry.getKey(), partitionId));
                }
            }
            if (tmpRegInfoMap.isEmpty()) {
                tmpOffsetMap.remove(group);
            }
        }
    }

    private GroupOffsetStgInfo loadZkOffsetStgInfo() {
        Set<String> tmpSet;
        Set<String> topicSet;
        TopicMetadata topicMeta;
        Map<String, Set<String>> zkAllGroups =
                zkOffsetStorage.queryGroupTopicInfo(null);
        // filter expired records
        Map<String, Set<String>> notFoundMap = new HashMap<>();
        for (Map.Entry<String, Set<String>> entry : zkAllGroups.entrySet()) {
            for (String topicName : entry.getValue()) {
                topicMeta = metadataManager.getTopicMetadata(topicName);
                if (topicMeta == null || !topicMeta.isValidTopic()) {
                    topicSet = notFoundMap.get(entry.getKey());
                    if (topicSet == null) {
                        tmpSet = new HashSet<>();
                        topicSet = notFoundMap.putIfAbsent(entry.getKey(), tmpSet);
                        if (topicSet == null) {
                            topicSet = tmpSet;
                        }
                    }
                    topicSet.add(topicName);
                }
            }
        }
        for (Map.Entry<String, Set<String>> entry : notFoundMap.entrySet()) {
            topicSet = zkAllGroups.get(entry.getKey());
            topicSet.removeAll(entry.getValue());
            if (topicSet.isEmpty()) {
                zkAllGroups.remove(entry.getKey());
            }
        }
        // get stored offset values
        Set<Integer> partIdSet;
        OffsetStorageInfo tmpRegInfo;
        GroupOffsetStgInfo tmpStgInfo = new GroupOffsetStgInfo(brokerConfig.getBrokerId());
        for (Map.Entry<String, Set<String>> entry : zkAllGroups.entrySet()) {
            if (this.isStopped()) {
                logger.info("[Offset Manager] found service stopped, exit 1!");
                return null;
            }
            topicSet = entry.getValue();
            for (String topicName : topicSet) {
                if (this.isStopped()) {
                    logger.info("[Offset Manager] found service stopped, exit 2!");
                    return null;
                }
                topicMeta = metadataManager.getTopicMetadata(topicName);
                if (topicMeta == null || !topicMeta.isValidTopic()) {
                    continue;
                }
                partIdSet = topicMeta.getAllPartitionIds();
                for (Integer partId : partIdSet) {
                    if (this.isStopped()) {
                        logger.info("[Offset Manager] found service stopped, exit 3!");
                        return null;
                    }
                    tmpRegInfo = zkOffsetStorage.loadOffset(entry.getKey(), topicName, partId);
                    if (tmpRegInfo == null) {
                        continue;
                    }
                    tmpStgInfo.addOffsetStgInfo(entry.getKey(), topicName, partId,
                            tmpRegInfo.getOffset(), tmpRegInfo.getMessageId());
                }
            }
        }
        return tmpStgInfo;
    }

    private int getLagLevel(long lagValue) {
        return (lagValue > TServerConstants.CFG_OFFSET_RESET_MID_ALARM_CHECK)
                ? 2
                : (lagValue > TServerConstants.CFG_OFFSET_RESET_MIN_ALARM_CHECK) ? 1 : 0;
    }
}
