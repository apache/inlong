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

package org.apache.tubemq.server.broker.offset;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.tubemq.corebase.TBaseConstants;
import org.apache.tubemq.corebase.daemon.AbstractDaemonService;
import org.apache.tubemq.corebase.utils.TStringUtils;
import org.apache.tubemq.corebase.utils.Tuple2;
import org.apache.tubemq.corebase.utils.Tuple3;
import org.apache.tubemq.server.broker.BrokerConfig;
import org.apache.tubemq.server.broker.msgstore.MessageStore;
import org.apache.tubemq.server.broker.utils.DataStoreUtils;
import org.apache.tubemq.server.common.offsetstorage.OffsetStorage;
import org.apache.tubemq.server.common.offsetstorage.OffsetStorageInfo;
import org.apache.tubemq.server.common.offsetstorage.ZkOffsetStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/***
 * Default offset manager.
 * Conduct consumer's commit offset operation and consumer's offset that has consumed but not committed.
 */
public class DefaultOffsetManager extends AbstractDaemonService implements OffsetService {
    private static final Logger logger = LoggerFactory.getLogger(DefaultOffsetManager.class);
    private final BrokerConfig brokerConfig;
    private final OffsetStorage zkOffsetStorage;
    private final ConcurrentHashMap<String/* group */,
            ConcurrentHashMap<String/* topic - partitionId*/, OffsetStorageInfo>> cfmOffsetMap =
            new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String/* group */,
            ConcurrentHashMap<String/* topic - partitionId*/, Long>> tmpOffsetMap =
            new ConcurrentHashMap<>();

    public DefaultOffsetManager(final BrokerConfig brokerConfig) {
        super("[Offset Manager]", brokerConfig.getZkConfig().getZkCommitPeriodMs());
        this.brokerConfig = brokerConfig;
        zkOffsetStorage = new ZkOffsetStorage(brokerConfig.getZkConfig(),
                true, brokerConfig.getBrokerId());
        super.start();
    }


    @Override
    protected void loopProcess(long intervalMs) {
        while (!super.isStopped()) {
            try {
                Thread.sleep(intervalMs);
                commitCfmOffsets(false);
            } catch (InterruptedException e) {
                logger.warn("[Offset Manager] Daemon commit thread has been interrupted");
                return;
            } catch (Throwable t) {
                logger.error("[Offset Manager] Daemon commit thread throw error ", t);
            }
        }
    }

    @Override
    public void close(long waitTimeMs) {
        if (super.stop()) {
            return;
        }
        logger.info("[Offset Manager] begin reserve temporary Offset.....");
        this.commitTmpOffsets();
        logger.info("[Offset Manager] begin reserve final Offset.....");
        this.commitCfmOffsets(true);
        this.zkOffsetStorage.close();
        logger.info("[Offset Manager] Offset Manager service stopped!");
    }

    /***
     * Load offset.
     *
     * @param msgStore
     * @param group
     * @param topic
     * @param partitionId
     * @param readStatus
     * @param reqOffset
     * @param sBuilder
     * @return
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
                        ? indexMinOffset : indexMaxOffset;
        String offsetCacheKey = getOffsetCacheKey(topic, partitionId);
        regInfo = loadOrCreateOffset(group, topic, partitionId, offsetCacheKey, defOffset);
        getAndResetTmpOffset(group, offsetCacheKey);
        final long curOffset = regInfo.getOffset();
        final boolean isFirstCreate = regInfo.isFirstCreate();
        if ((reqOffset >= 0)
                || (readStatus == TBaseConstants.CONSUME_MODEL_READ_FROM_MAX_ALWAYS)) {
            long adjOffset = indexMaxOffset;
            if (readStatus != TBaseConstants.CONSUME_MODEL_READ_FROM_MAX_ALWAYS) {
                adjOffset = Math.min(reqOffset, indexMaxOffset);
                adjOffset = Math.max(adjOffset, indexMinOffset);
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
        logger.info(sBuilder.append(",loaded offset=").append(curOffset)
                .append(",required offset=").append(reqOffset)
                .append(",current offset=").append(regInfo.getOffset())
                .append(",maxOffset=").append(indexMaxOffset)
                .append(",offset delta=").append(indexMaxOffset - regInfo.getOffset())
                .append(",group=").append(group).append(",topic=").append(topic)
                .append(",partitionId=").append(partitionId).toString());
        sBuilder.delete(0, sBuilder.length());
        return regInfo;
    }

    /***
     * Get offset by parameters.
     *
     * @param msgStore
     * @param group
     * @param topic
     * @param partitionId
     * @param isManCommit
     * @param lastConsumed
     * @param sb
     * @return
     */
    @Override
    public long getOffset(final MessageStore msgStore, final String group,
                          final String topic, int partitionId,
                          boolean isManCommit, boolean lastConsumed,
                          final StringBuilder sb) {
        String offsetCacheKey = getOffsetCacheKey(topic, partitionId);
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
                        .append("[Offset Manager] Offset is bigger than current max offset, reset! requestOffset=")
                        .append(requestOffset).append(",maxOffset=").append(maxOffset)
                        .append(",group=").append(group).append(",topic=").append(topic)
                        .append(",partitionId=").append(partitionId).toString());
                sb.delete(0, sb.length());
                setTmpOffset(group, offsetCacheKey, maxOffset - requestOffset);
                if (!isManCommit) {
                    requestOffset = commitOffset(group, topic, partitionId, true);
                }
            }
            return -requestOffset;
        } else if (requestOffset < minOffset) {
            logger.warn(sb
                    .append("[Offset Manager] Offset is lower than current min offset, reset! requestOffset=")
                    .append(requestOffset).append(",minOffset=").append(minOffset)
                    .append(",group=").append(group).append(",topic=").append(topic)
                    .append(",partitionId=").append(partitionId).toString());
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
                        getOffsetCacheKey(topic, partitionId), 0);
        return regInfo.getOffset();
    }

    @Override
    public void bookOffset(final String group, final String topic, int partitionId,
                           int readDalt, boolean isManCommit, boolean isMsgEmpty,
                           final StringBuilder sb) {
        if (readDalt == 0) {
            return;
        }
        String offsetCacheKey = getOffsetCacheKey(topic, partitionId);
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

    /***
     * Commit offset.
     *
     * @param group
     * @param topic
     * @param partitionId
     * @param isConsumed
     * @return
     */
    @Override
    public long commitOffset(final String group, final String topic,
                             int partitionId, boolean isConsumed) {
        long updatedOffset = -1;
        String offsetCacheKey = getOffsetCacheKey(topic, partitionId);
        long tmpOffset = getAndResetTmpOffset(group, offsetCacheKey);
        if (!isConsumed) {
            tmpOffset = 0;
        }
        OffsetStorageInfo regInfo =
                loadOrCreateOffset(group, topic, partitionId, offsetCacheKey, 0);
        if ((tmpOffset == 0) && (!regInfo.isFirstCreate())) {
            updatedOffset = regInfo.getOffset();
            return updatedOffset;
        }
        updatedOffset = regInfo.addAndGetOffset(tmpOffset);
        if (logger.isDebugEnabled()) {
            logger.debug(new StringBuilder(512)
                    .append("[Offset Manager] Update offset finished, offset=").append(updatedOffset)
                    .append(",group=").append(group).append(",topic=").append(topic)
                    .append(",partitionId=").append(partitionId).toString());
        }
        return updatedOffset;
    }

    /***
     * Reset offset.
     *
     * @param store
     * @param group
     * @param topic
     * @param partitionId
     * @param reSetOffset
     * @param modifier
     * @return
     */
    @Override
    public long resetOffset(final MessageStore store, final String group,
                            final String topic, int partitionId,
                            long reSetOffset, final String modifier) {
        long oldOffset = -1;
        if (store != null) {
            long firstOffset = store.getIndexMinOffset();
            long lastOffset = store.getIndexMaxOffset();
            reSetOffset = reSetOffset < firstOffset
                    ? firstOffset : Math.min(reSetOffset, lastOffset);
            String offsetCacheKey = getOffsetCacheKey(topic, partitionId);
            getAndResetTmpOffset(group, offsetCacheKey);
            OffsetStorageInfo regInfo =
                    loadOrCreateOffset(group, topic, partitionId, offsetCacheKey, 0);
            oldOffset = regInfo.getAndSetOffset(reSetOffset);
            logger.info(new StringBuilder(512)
                    .append("[Offset Manager] Manual update offset by modifier=")
                    .append(modifier).append(",reset offset=").append(reSetOffset)
                    .append(",old offset=").append(oldOffset)
                    .append(",updated offset=").append(regInfo.getOffset())
                    .append(",group=").append(group).append(",topic=").append(topic)
                    .append(",partitionId=").append(partitionId).toString());
        }
        return oldOffset;
    }

    /***
     * Get temp offset.
     *
     * @param group
     * @param topic
     * @param partitionId
     * @return
     */
    @Override
    public long getTmpOffset(final String group, final String topic, int partitionId) {
        String offsetCacheKey = getOffsetCacheKey(topic, partitionId);
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

    /***
     * Get in-memory and in zk group set
     *
     * @return booked group in memory and in zk
     */
    @Override
    public Set<String> getBookedGroups() {
        Set<String> groupSet = new HashSet<>();
        groupSet.addAll(cfmOffsetMap.keySet());
        Map<String, Set<String>> localGroups =
                zkOffsetStorage.getZkLocalGroupTopicInfos();
        groupSet.addAll(localGroups.keySet());
        return groupSet;
    }

    /***
     * Get in-memory group set
     *
     * @return booked group in memory
     */
    public Set<String> getInMemoryGroups() {
        Set<String> cacheGroup = new HashSet<>();
        cacheGroup.addAll(cfmOffsetMap.keySet());
        return cacheGroup;
    }

    /***
     * Get in-zookeeper but not in memory's group set
     *
     * @return booked group in zookeeper
     */
    @Override
    public Set<String> getUnusedGroupInfo() {
        Set<String> unUsedGroups = new HashSet<>();
        Map<String, Set<String>> localGroups =
                zkOffsetStorage.getZkLocalGroupTopicInfos();
        for (String groupName : localGroups.keySet()) {
            if (!cfmOffsetMap.containsKey(groupName)) {
                unUsedGroups.add(groupName);
            }
        }
        return unUsedGroups;
    }

    /***
     * Get the topic set subscribed by the consumer group
     * @param group
     * @return topic set subscribed
     */
    @Override
    public Set<String> getGroupSubInfo(String group) {
        Set<String> result = new HashSet<>();
        Map<String, OffsetStorageInfo> topicPartOffsetMap = cfmOffsetMap.get(group);
        if (topicPartOffsetMap == null) {
            Map<String, Set<String>> localGroups =
                    zkOffsetStorage.getZkLocalGroupTopicInfos();
            result = localGroups.get(group);
        } else {
            for (OffsetStorageInfo storageInfo : topicPartOffsetMap.values()) {
                result.add(storageInfo.getTopic());
            }
        }
        return result;
    }

    /***
     * Get group's offset by Specified topic-partitions
     * @param group
     * @param topicPartMap
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
                        zkOffsetStorage.queryGroupOffsetInfo(group,
                                entry.getKey(), entry.getValue());
                Map<Integer, Tuple2<Long, Long>> offsetMap = new HashMap<>();
                for (Map.Entry<Integer, Long> item : qryResult.entrySet()) {
                    if (item == null || item.getKey() == null || item.getValue() == null)  {
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
                            getOffsetCacheKey(entry.getKey(), partitionId);
                    OffsetStorageInfo offsetInfo = topicPartOffsetMap.get(offsetCacheKey);
                    Long tmpOffset = tmpPartOffsetMap.get(offsetCacheKey);
                    if (offsetInfo != null) {
                        offsetMap.put(partitionId,
                                new Tuple2<>(offsetInfo.getOffset(),
                                        (tmpOffset == null ? 0 : tmpOffset)));
                    }
                }
                if (!offsetMap.isEmpty()) {
                    result.put(entry.getKey(), offsetMap);
                }
            }
        }
        return result;
    }

    /***
     * Reset offset.
     *
     * @param groups
     * @param topicPartOffsets
     * @param modifier
     * @return at least one record modified
     */
    @Override
    public boolean modifyGroupOffset(Set<String> groups,
                                     List<Tuple3<String, Integer, Long>> topicPartOffsets,
                                     String modifier) {
        long oldOffset = -1;
        boolean changed = false;
        String offsetCacheKey = null;
        StringBuilder strBuidler = new StringBuilder(512);
        // set offset by group
        for (String group : groups) {
            for (Tuple3<String, Integer, Long> tuple3 : topicPartOffsets) {
                if (tuple3 == null
                        || tuple3.f0 == null
                        || tuple3.f1 == null
                        || tuple3.f2 == null) {
                    continue;
                }
                // set offset value
                offsetCacheKey = getOffsetCacheKey(tuple3.f0, tuple3.f1);
                getAndResetTmpOffset(group, offsetCacheKey);
                OffsetStorageInfo regInfo = loadOrCreateOffset(group,
                        tuple3.f0, tuple3.f1, offsetCacheKey, 0);
                oldOffset = regInfo.getAndSetOffset(tuple3.f2);
                changed = true;
                logger.info(strBuidler
                        .append("[Offset Manager] Update offset by modifier=")
                        .append(modifier).append(",reset offset=").append(tuple3.f2)
                        .append(",old offset=").append(oldOffset)
                        .append(",updated offset=").append(regInfo.getOffset())
                        .append(",group=").append(group)
                        .append(",topic-partId=").append(offsetCacheKey).toString());
                strBuidler.delete(0, strBuidler.length());
            }
        }
        return changed;
    }

    /***
     * Set temp offset.
     *
     * @param group
     * @param offsetCacheKey
     * @param origOffset
     * @return
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
        ConcurrentHashMap<String, Long> partTmpOffsetMap = tmpOffsetMap.get(group);
        if (partTmpOffsetMap == null) {
            ConcurrentHashMap<String, Long> tmpMap = new ConcurrentHashMap<>();
            partTmpOffsetMap = tmpOffsetMap.putIfAbsent(group, tmpMap);
            if (partTmpOffsetMap == null) {
                partTmpOffsetMap = tmpMap;
            }
        }
        Long tmpOffset = partTmpOffsetMap.put(offsetCacheKey, 0L);
        if (tmpOffset == null) {
            return 0;
        } else {
            return (tmpOffset - tmpOffset % DataStoreUtils.STORE_INDEX_HEAD_LEN);
        }
    }

    /***
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

    private void commitCfmOffsets(boolean retryable) {
        for (Map.Entry<String, ConcurrentHashMap<String, OffsetStorageInfo>> entry : cfmOffsetMap.entrySet()) {
            if (TStringUtils.isBlank(entry.getKey())
                    || entry.getValue() == null || entry.getValue().isEmpty()) {
                continue;
            }
            zkOffsetStorage.commitOffset(entry.getKey(), entry.getValue().values(), retryable);
        }
    }

    /***
     * Load or create offset.
     *
     * @param group
     * @param topic
     * @param partitionId
     * @param offsetCacheKey
     * @param defOffset
     * @return
     */
    private OffsetStorageInfo loadOrCreateOffset(final String group, final String topic,
                                                 int partitionId, final String offsetCacheKey,
                                                 long defOffset) {
        ConcurrentHashMap<String, OffsetStorageInfo> regInfoMap = cfmOffsetMap.get(group);
        if (regInfoMap == null) {
            ConcurrentHashMap<String, OffsetStorageInfo> tmpRegInfoMap
                    = new ConcurrentHashMap<>();
            regInfoMap = cfmOffsetMap.putIfAbsent(group, tmpRegInfoMap);
            if (regInfoMap == null) {
                regInfoMap = tmpRegInfoMap;
            }
        }
        OffsetStorageInfo regInfo = regInfoMap.get(offsetCacheKey);
        if (regInfo == null) {
            OffsetStorageInfo tmpRegInfo =
                    zkOffsetStorage.loadOffset(group, topic, partitionId);
            if (tmpRegInfo == null) {
                tmpRegInfo = new OffsetStorageInfo(topic,
                        brokerConfig.getBrokerId(), partitionId, defOffset, 0);
            }
            regInfo = regInfoMap.putIfAbsent(offsetCacheKey, tmpRegInfo);
            if (regInfo == null) {
                regInfo = tmpRegInfo;
            }
        }
        return regInfo;
    }

    private String getOffsetCacheKey(String topic, int partitionId) {
        return new StringBuilder(256).append(topic)
                .append("-").append(partitionId).toString();
    }

    private String getOffsetCacheKey(String topic, String partitionId) {
        return new StringBuilder(256).append(topic)
                .append("-").append(partitionId).toString();
    }

}
