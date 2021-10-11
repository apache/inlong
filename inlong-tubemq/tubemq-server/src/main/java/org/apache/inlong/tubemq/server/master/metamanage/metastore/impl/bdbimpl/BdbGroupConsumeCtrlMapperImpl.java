/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.inlong.tubemq.server.master.metamanage.metastore.impl.bdbimpl;

import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.persist.EntityCursor;
import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.PrimaryIndex;
import com.sleepycat.persist.StoreConfig;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.inlong.tubemq.corebase.TBaseConstants;
import org.apache.inlong.tubemq.corebase.utils.ConcurrentHashSet;
import org.apache.inlong.tubemq.corebase.utils.KeyBuilderUtils;
import org.apache.inlong.tubemq.server.common.exception.LoadMetaException;
import org.apache.inlong.tubemq.server.common.utils.ProcessResult;
import org.apache.inlong.tubemq.server.master.bdbstore.bdbentitys.BdbGroupFilterCondEntity;
import org.apache.inlong.tubemq.server.master.metamanage.DataOpErrCode;
import org.apache.inlong.tubemq.server.master.metamanage.metastore.dao.entity.GroupConsumeCtrlEntity;
import org.apache.inlong.tubemq.server.master.metamanage.metastore.dao.mapper.GroupConsumeCtrlMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BdbGroupConsumeCtrlMapperImpl implements GroupConsumeCtrlMapper {

    private static final Logger logger =
            LoggerFactory.getLogger(BdbGroupConsumeCtrlMapperImpl.class);
    // consumer group consume control store
    private EntityStore groupConsumeStore;
    private PrimaryIndex<String/* recordKey */, BdbGroupFilterCondEntity> groupConsumeIndex;
    // configure cache
    private ConcurrentHashMap<String/* recordKey */, GroupConsumeCtrlEntity>
            grpConsumeCtrlCache = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String/* topicName */, ConcurrentHashSet<String>>
            grpConsumeCtrlTopicCache = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String/* groupName */, ConcurrentHashSet<String>>
            grpConsumeCtrlGroupCache = new ConcurrentHashMap<>();

    public BdbGroupConsumeCtrlMapperImpl(ReplicatedEnvironment repEnv, StoreConfig storeConfig) {
        groupConsumeStore = new EntityStore(repEnv,
                TBDBStoreTables.BDB_GROUP_FILTER_COND_STORE_NAME, storeConfig);
        groupConsumeIndex =
                groupConsumeStore.getPrimaryIndex(String.class, BdbGroupFilterCondEntity.class);
    }

    @Override
    public void close() {
        clearCacheData();
        if (groupConsumeStore != null) {
            try {
                groupConsumeStore.close();
                groupConsumeStore = null;
            } catch (Throwable e) {
                logger.error("[BDB Impl] close consume configure failure ", e);
            }
        }
    }

    @Override
    public void loadConfig() throws LoadMetaException {
        long count = 0L;
        EntityCursor<BdbGroupFilterCondEntity> cursor = null;
        logger.info("[BDB Impl] load consume configure start...");
        try {
            clearCacheData();
            cursor = groupConsumeIndex.entities();
            for (BdbGroupFilterCondEntity bdbEntity : cursor) {
                if (bdbEntity == null) {
                    logger.warn("[BDB Impl] found Null data while loading consume configure!");
                    continue;
                }
                GroupConsumeCtrlEntity memEntity =
                        new GroupConsumeCtrlEntity(bdbEntity);
                addOrUpdCacheRecord(memEntity);
                count++;
            }
            logger.info("[BDB Impl] total consume configure records are {}", count);
        } catch (Exception e) {
            logger.error("[BDB Impl] load filter configure failure ", e);
            throw new LoadMetaException(e.getMessage());
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }
        logger.info("[BDB Impl] load consume configure successfully...");
    }

    @Override
    public boolean addGroupConsumeCtrlConf(GroupConsumeCtrlEntity memEntity, ProcessResult result) {
        GroupConsumeCtrlEntity curEntity =
                grpConsumeCtrlCache.get(memEntity.getRecordKey());
        if (curEntity != null) {
            result.setFailResult(DataOpErrCode.DERR_EXISTED.getCode(),
                    new StringBuilder(TBaseConstants.BUILDER_DEFAULT_SIZE)
                            .append("The group consume ").append(memEntity.getRecordKey())
                            .append("'s configure already exists, please delete it first!")
                            .toString());
            return result.isSuccess();
        }
        if (putGroupConsumeCtrlConfig2Bdb(memEntity, result)) {
            addOrUpdCacheRecord(memEntity);
        }
        return result.isSuccess();
    }

    @Override
    public boolean updGroupConsumeCtrlConf(GroupConsumeCtrlEntity memEntity, ProcessResult result) {
        GroupConsumeCtrlEntity curEntity =
                grpConsumeCtrlCache.get(memEntity.getRecordKey());
        if (curEntity == null) {
            result.setFailResult(DataOpErrCode.DERR_NOT_EXIST.getCode(),
                    new StringBuilder(TBaseConstants.BUILDER_DEFAULT_SIZE)
                            .append("The group consume ").append(memEntity.getRecordKey())
                            .append("'s configure is not exists, please add record first!")
                            .toString());
            return result.isSuccess();
        }
        if (curEntity.equals(memEntity)) {
            result.setFailResult(DataOpErrCode.DERR_UNCHANGED.getCode(),
                    new StringBuilder(TBaseConstants.BUILDER_DEFAULT_SIZE)
                            .append("The group consume ").append(memEntity.getRecordKey())
                            .append("'s configure have not changed, please delete it first!")
                            .toString());
            return result.isSuccess();
        }
        if (putGroupConsumeCtrlConfig2Bdb(memEntity, result)) {
            addOrUpdCacheRecord(memEntity);
            result.setRetData(curEntity);
        }
        return result.isSuccess();
    }

    @Override
    public boolean isTopicNameInUsed(String topicName) {
        ConcurrentHashSet<String> consumeCtrlSet =
                grpConsumeCtrlTopicCache.get(topicName);
        return (consumeCtrlSet != null && !consumeCtrlSet.isEmpty());
    }

    @Override
    public boolean hasGroupConsumeCtrlConf(String groupName) {
        ConcurrentHashSet<String> keySet =
                grpConsumeCtrlGroupCache.get(groupName);
        return (keySet != null && !keySet.isEmpty());
    }

    @Override
    public boolean delGroupConsumeCtrlConf(String recordKey, ProcessResult result) {
        GroupConsumeCtrlEntity curEntity =
                grpConsumeCtrlCache.get(recordKey);
        if (curEntity == null) {
            result.setSuccResult(null);
            return true;
        }
        delGroupConsumeCtrlConfigFromBdb(recordKey);
        delCacheRecord(recordKey);
        result.setSuccResult(curEntity);
        return true;
    }

    @Override
    public boolean delGroupConsumeCtrlConf(String groupName,
                                           String topicName,
                                           ProcessResult result) {
        ConcurrentHashSet<String> keySet =
                new ConcurrentHashSet<>();
        // get need deleted record key
        if (groupName == null) {
            if (topicName == null) {
                result.setSuccResult(null);
                return true;
            } else {
                keySet = grpConsumeCtrlTopicCache.get(topicName);
            }
        } else {
            if (topicName == null) {
                keySet = grpConsumeCtrlGroupCache.get(groupName);
            } else {
                keySet.add(KeyBuilderUtils.buildGroupTopicRecKey(groupName, topicName));
            }
        }
        if (keySet == null || keySet.isEmpty()) {
            result.setSuccResult(null);
            return true;
        }
        for (String key : keySet) {
            if (!delGroupConsumeCtrlConf(key, result)) {
                return result.isSuccess();
            }
            result.clear();
        }
        result.setSuccResult(null);
        return true;
    }

    @Override
    public GroupConsumeCtrlEntity getGroupConsumeCtrlConfByRecKey(String recordKey) {
        return grpConsumeCtrlCache.get(recordKey);
    }

    @Override
    public List<GroupConsumeCtrlEntity> getConsumeCtrlByTopicName(String topicName) {
        ConcurrentHashSet<String> keySet =
                grpConsumeCtrlTopicCache.get(topicName);
        if (keySet == null || keySet.isEmpty()) {
            return Collections.emptyList();
        }
        GroupConsumeCtrlEntity entity;
        List<GroupConsumeCtrlEntity> result = new ArrayList<>();
        for (String recordKey : keySet) {
            if (recordKey == null) {
                continue;
            }
            entity = grpConsumeCtrlCache.get(recordKey);
            if (entity != null) {
                result.add(entity);
            }
        }
        return result;
    }

    @Override
    public List<GroupConsumeCtrlEntity> getConsumeCtrlByGroupName(String groupName) {
        ConcurrentHashSet<String> keySet =
                grpConsumeCtrlGroupCache.get(groupName);
        if (keySet == null || keySet.isEmpty()) {
            return Collections.emptyList();
        }
        GroupConsumeCtrlEntity entity;
        List<GroupConsumeCtrlEntity> result = new ArrayList<>();
        for (String recordKey : keySet) {
            entity = grpConsumeCtrlCache.get(recordKey);
            if (entity != null) {
                result.add(entity);
            }
        }
        return result;
    }

    @Override
    public GroupConsumeCtrlEntity getConsumeCtrlByGroupAndTopic(
            String groupName, String topicName) {
        String recKey = KeyBuilderUtils.buildGroupTopicRecKey(groupName, topicName);
        return grpConsumeCtrlCache.get(recKey);
    }

    @Override
    public Map<String/* group */, List<GroupConsumeCtrlEntity>> getConsumeCtrlInfoMap(
            Set<String> groupSet, Set<String> topicSet, GroupConsumeCtrlEntity qryEntry) {
        Map<String, List<GroupConsumeCtrlEntity>> retEntityMap = new HashMap<>();
        // filter matched keys by groupSet and topicSet
        Set<String> totalMatchedSet = getMatchedRecords(groupSet, topicSet);
        // get matched records
        GroupConsumeCtrlEntity tmpEntity;
        List<GroupConsumeCtrlEntity> itemLst;
        if (totalMatchedSet == null) {
            for (GroupConsumeCtrlEntity entity : grpConsumeCtrlCache.values()) {
                if (entity == null || (qryEntry != null && !entity.isMatched(qryEntry))) {
                    continue;
                }
                itemLst = retEntityMap.computeIfAbsent(
                        entity.getGroupName(), k -> new ArrayList<>());
                itemLst.add(entity);
            }
        } else {
            for (String recKey : totalMatchedSet) {
                tmpEntity = grpConsumeCtrlCache.get(recKey);
                if (tmpEntity == null || (qryEntry != null && !tmpEntity.isMatched(qryEntry))) {
                    continue;
                }
                itemLst = retEntityMap.computeIfAbsent(
                        tmpEntity.getGroupName(), k -> new ArrayList<>());
                itemLst.add(tmpEntity);
            }
        }
        return retEntityMap;
    }

    @Override
    public List<GroupConsumeCtrlEntity> getGroupConsumeCtrlConf(GroupConsumeCtrlEntity qryEntity) {
        List<GroupConsumeCtrlEntity> retEntitys = new ArrayList<>();
        if (qryEntity == null) {
            retEntitys.addAll(grpConsumeCtrlCache.values());
        } else {
            for (GroupConsumeCtrlEntity entity : grpConsumeCtrlCache.values()) {
                if (entity != null && entity.isMatched(qryEntity)) {
                    retEntitys.add(entity);
                }
            }
        }
        return retEntitys;
    }

    @Override
    public Set<String> getMatchedRecords(Set<String> groupSet, Set<String> topicSet) {
        Set<String> groupKeySet = null;
        Set<String> topicKeySet = null;
        Set<String> totalMatchedSet = null;
        ConcurrentHashSet<String> recSet;
        // filter group items
        if (groupSet != null && !groupSet.isEmpty()) {
            groupKeySet = new HashSet<>();
            for (String group : groupSet) {
                recSet = grpConsumeCtrlGroupCache.get(group);
                if (recSet != null && !recSet.isEmpty()) {
                    groupKeySet.addAll(recSet);
                }
            }
            if (groupKeySet.isEmpty()) {
                return Collections.emptySet();
            }
        }
        // filter topic items
        if (topicSet != null && !topicSet.isEmpty()) {
            topicKeySet = new HashSet<>();
            for (String topic : topicSet) {
                recSet = grpConsumeCtrlTopicCache.get(topic);
                if (recSet != null && !recSet.isEmpty()) {
                    topicKeySet.addAll(recSet);
                }
            }
            if (topicKeySet.isEmpty()) {
                return Collections.emptySet();
            }
        }
        // get intersection from groupKeySet and topicKeySet
        if (groupKeySet != null || topicKeySet != null) {
            if (groupKeySet == null) {
                totalMatchedSet = new HashSet<>(topicKeySet);
            } else {
                if (topicKeySet == null) {
                    totalMatchedSet = new HashSet<>(groupKeySet);
                } else {
                    totalMatchedSet = new HashSet<>();
                    for (String record : groupKeySet) {
                        if (topicKeySet.contains(record)) {
                            totalMatchedSet.add(record);
                        }
                    }
                }
            }
        }
        return totalMatchedSet;
    }

    /**
     * Put Group consume configure info into bdb store
     *
     * @param memEntity need add record
     * @param result process result with old value
     * @return true sucess, false failue
     */
    private boolean putGroupConsumeCtrlConfig2Bdb(
            GroupConsumeCtrlEntity memEntity, ProcessResult result) {
        BdbGroupFilterCondEntity retData = null;
        BdbGroupFilterCondEntity bdbEntity =
                memEntity.buildBdbGroupFilterCondEntity();
        try {
            retData = groupConsumeIndex.put(bdbEntity);
        } catch (Throwable e) {
            logger.error("[BDB Impl] put consume configure failure ", e);
            result.setFailResult(DataOpErrCode.DERR_STORE_ABNORMAL.getCode(),
                    new StringBuilder(TBaseConstants.BUILDER_DEFAULT_SIZE)
                            .append("Put filter configure failure: ")
                            .append(e.getMessage()).toString());
            return result.isSuccess();
        }
        result.setSuccResult(null);
        return result.isSuccess();
    }

    private boolean delGroupConsumeCtrlConfigFromBdb(String recordKey) {
        try {
            groupConsumeIndex.delete(recordKey);
        } catch (Throwable e) {
            logger.error("[BDB Impl] delete consume configure failure ", e);
            return false;
        }
        return true;
    }

    private void delCacheRecord(String recordKey) {
        GroupConsumeCtrlEntity curEntity =
                grpConsumeCtrlCache.remove(recordKey);
        if (curEntity == null) {
            return;
        }
        // add topic index
        ConcurrentHashSet<String> keySet =
                grpConsumeCtrlTopicCache.get(curEntity.getTopicName());
        if (keySet != null) {
            keySet.remove(recordKey);
            if (keySet.isEmpty()) {
                grpConsumeCtrlTopicCache.remove(curEntity.getTopicName());
            }
        }
        // delete group index
        keySet = grpConsumeCtrlGroupCache.get(curEntity.getGroupName());
        if (keySet != null) {
            keySet.remove(recordKey);
            if (keySet.isEmpty()) {
                grpConsumeCtrlGroupCache.remove(curEntity.getGroupName());
            }
        }
    }

    private void addOrUpdCacheRecord(GroupConsumeCtrlEntity entity) {
        grpConsumeCtrlCache.put(entity.getRecordKey(), entity);
        // add topic index map
        ConcurrentHashSet<String> keySet =
                grpConsumeCtrlTopicCache.get(entity.getTopicName());
        if (keySet == null) {
            ConcurrentHashSet<String> tmpSet = new ConcurrentHashSet<>();
            keySet = grpConsumeCtrlTopicCache.putIfAbsent(entity.getTopicName(), tmpSet);
            if (keySet == null) {
                keySet = tmpSet;
            }
        }
        keySet.add(entity.getRecordKey());
        // add group index map
        keySet = grpConsumeCtrlGroupCache.get(entity.getGroupName());
        if (keySet == null) {
            ConcurrentHashSet<String> tmpSet = new ConcurrentHashSet<>();
            keySet = grpConsumeCtrlGroupCache.putIfAbsent(entity.getGroupName(), tmpSet);
            if (keySet == null) {
                keySet = tmpSet;
            }
        }
        keySet.add(entity.getRecordKey());
    }

    private void clearCacheData() {
        grpConsumeCtrlTopicCache.clear();
        grpConsumeCtrlGroupCache.clear();
        grpConsumeCtrlCache.clear();
    }
}
