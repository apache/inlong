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

package org.apache.tubemq.server.master.metamanage.metastore.impl.bdbimpl;


import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.persist.EntityCursor;
import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.PrimaryIndex;
import com.sleepycat.persist.StoreConfig;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.tubemq.corebase.TBaseConstants;
import org.apache.tubemq.corebase.utils.ConcurrentHashSet;
import org.apache.tubemq.corebase.utils.KeyBuilderUtils;
import org.apache.tubemq.server.common.exception.LoadMetaException;
import org.apache.tubemq.server.common.utils.ProcessResult;
import org.apache.tubemq.server.master.bdbstore.bdbentitys.BdbTopicConfEntity;
import org.apache.tubemq.server.master.metamanage.DataOpErrCode;
import org.apache.tubemq.server.master.metamanage.metastore.dao.entity.TopicDeployEntity;
import org.apache.tubemq.server.master.metamanage.metastore.dao.mapper.TopicDeployMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public class BdbTopicDeployMapperImpl implements TopicDeployMapper {

    private static final Logger logger =
            LoggerFactory.getLogger(BdbTopicDeployMapperImpl.class);

    // Topic configure store
    private EntityStore topicConfStore;
    private PrimaryIndex<String/* recordKey */, BdbTopicConfEntity> topicConfIndex;
    // data cache
    private ConcurrentHashMap<String/* recordKey */, TopicDeployEntity> topicConfCache =
            new ConcurrentHashMap<>();
    private ConcurrentHashMap<Integer/* brokerId */, ConcurrentHashSet<String>>
            brokerIdCacheIndex = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String/* topicName */, ConcurrentHashSet<String>>
            topicNameCacheIndex = new ConcurrentHashMap<>();
    private ConcurrentHashMap<Integer/* brokerId */, ConcurrentHashSet<String>>
            brokerId2TopicCacheIndex = new ConcurrentHashMap<>();





    public BdbTopicDeployMapperImpl(ReplicatedEnvironment repEnv, StoreConfig storeConfig) {
        topicConfStore = new EntityStore(repEnv,
                TBDBStoreTables.BDB_TOPIC_CONFIG_STORE_NAME, storeConfig);
        topicConfIndex =
                topicConfStore.getPrimaryIndex(String.class, BdbTopicConfEntity.class);
    }

    @Override
    public void close() {
        if (topicConfStore != null) {
            try {
                topicConfStore.close();
                topicConfStore = null;
            } catch (Throwable e) {
                logger.error("[BDB Impl] close topic configure failure ", e);
            }
        }
    }

    @Override
    public void loadConfig() throws LoadMetaException {
        long count = 0L;
        EntityCursor<BdbTopicConfEntity> cursor = null;
        logger.info("[BDB Impl] load topic configure start...");
        try {
            cursor = topicConfIndex.entities();
            for (BdbTopicConfEntity bdbEntity : cursor) {
                if (bdbEntity == null) {
                    logger.warn("[BDB Impl] found Null data while loading topic configure!");
                    continue;
                }
                TopicDeployEntity memEntity = new TopicDeployEntity(bdbEntity);
                addOrUpdCacheRecord(memEntity);
                count++;
            }
            logger.info("[BDB Impl] total topic configure records are {}", count);
        } catch (Exception e) {
            logger.error("[BDB Impl] load topic configure failure ", e);
            throw new LoadMetaException(e.getMessage());
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }
        logger.info("[BDB Impl] load topic configure successfully...");
    }

    @Override
    public boolean addTopicConf(TopicDeployEntity memEntity, ProcessResult result) {
        TopicDeployEntity curEntity =
                topicConfCache.get(memEntity.getRecordKey());
        if (curEntity != null) {
            result.setFailResult(DataOpErrCode.DERR_EXISTED.getCode(),
                    new StringBuilder(TBaseConstants.BUILDER_DEFAULT_SIZE)
                            .append("The topic configure ").append(memEntity.getRecordKey())
                            .append("'s configure already exists, please delete it first!")
                            .toString());
            return result.isSuccess();
        }
        if (putTopicConfig2Bdb(memEntity, result)) {
            addOrUpdCacheRecord(memEntity);
        }
        return result.isSuccess();
    }

    @Override
    public boolean updTopicConf(TopicDeployEntity memEntity, ProcessResult result) {
        TopicDeployEntity curEntity =
                topicConfCache.get(memEntity.getRecordKey());
        if (curEntity == null) {
            result.setFailResult(DataOpErrCode.DERR_NOT_EXIST.getCode(),
                    new StringBuilder(TBaseConstants.BUILDER_DEFAULT_SIZE)
                            .append("The topic configure ").append(memEntity.getRecordKey())
                            .append("'s configure is not exists, please add record first!")
                            .toString());
            return result.isSuccess();
        }
        if (curEntity.equals(memEntity)) {
            result.setFailResult(DataOpErrCode.DERR_UNCHANGED.getCode(),
                    new StringBuilder(TBaseConstants.BUILDER_DEFAULT_SIZE)
                            .append("The topic configure ").append(memEntity.getRecordKey())
                            .append("'s configure have not changed, please delete it first!")
                            .toString());
            return result.isSuccess();
        }
        if (putTopicConfig2Bdb(memEntity, result)) {
            addOrUpdCacheRecord(memEntity);
            result.setRetData(curEntity);
        }
        return result.isSuccess();
    }

    @Override
    public boolean delTopicConf(String recordKey, ProcessResult result) {
        TopicDeployEntity curEntity =
                topicConfCache.get(recordKey);
        if (curEntity == null) {
            result.setSuccResult(null);
            return result.isSuccess();
        }
        delTopicConfigFromBdb(recordKey);
        delCacheRecord(recordKey);
        result.setSuccResult(curEntity);
        return result.isSuccess();
    }

    @Override
    public boolean delTopicConfByBrokerId(Integer brokerId, ProcessResult result) {
        ConcurrentHashSet<String> recordKeySet =
                brokerIdCacheIndex.get(brokerId);
        if (recordKeySet == null) {
            result.setSuccResult(null);
            return result.isSuccess();
        }
        for (String recordKey : recordKeySet) {
            delTopicConfigFromBdb(recordKey);
            delCacheRecord(recordKey);
        }
        result.setSuccResult(null);
        return result.isSuccess();
    }

    @Override
    public boolean hasConfiguredTopics(int brokerId) {
        ConcurrentHashSet<String> keySet =
                brokerIdCacheIndex.get(brokerId);
        return (keySet != null && !keySet.isEmpty());
    }

    @Override
    public TopicDeployEntity getTopicConfByeRecKey(String recordKey) {
        return topicConfCache.get(recordKey);
    }

    @Override
    public List<TopicDeployEntity> getTopicConf(TopicDeployEntity qryEntity) {
        List<TopicDeployEntity> retEntitys = new ArrayList<>();
        if (qryEntity == null) {
            retEntitys.addAll(topicConfCache.values());
        } else {
            for (TopicDeployEntity entity : topicConfCache.values()) {
                if (entity != null && entity.isMatched(qryEntity)) {
                    retEntitys.add(entity);
                }
            }
        }
        return retEntitys;
    }

    @Override
    public TopicDeployEntity getTopicConf(int brokerId, String topicName) {
        String recordKey =
                KeyBuilderUtils.buildTopicConfRecKey(brokerId, topicName);
        return topicConfCache.get(recordKey);
    }

    @Override
    public boolean isTopicDeployed(String topicName) {
        ConcurrentHashSet<String> deploySet = topicNameCacheIndex.get(topicName);
        return (deploySet != null && !deploySet.isEmpty());
    }

    @Override
    public Map<String, List<TopicDeployEntity>> getTopicConfMap(Set<String> topicNameSet,
                                                                Set<Integer> brokerIdSet,
                                                                TopicDeployEntity qryEntity) {
        List<TopicDeployEntity> items;
        Set<String> qryTopicKeySet = null;
        ConcurrentHashSet<String> keySet;
        Map<String, List<TopicDeployEntity>> retEntityMap = new HashMap<>();
        // get deploy records set by topicName
        if (topicNameSet != null && !topicNameSet.isEmpty()) {
            qryTopicKeySet = new HashSet<>();
            for (String topicName : topicNameSet) {
                keySet = topicNameCacheIndex.get(topicName);
                if (keySet != null && !keySet.isEmpty()) {
                    qryTopicKeySet.addAll(keySet);
                }
            }
        }
        // get deploy records set by brokerId
        if (brokerIdSet != null && !brokerIdSet.isEmpty()) {
            if (qryTopicKeySet == null) {
                qryTopicKeySet = new HashSet<>();
            }
            for (Integer brokerId : brokerIdSet) {
                keySet = brokerIdCacheIndex.get(brokerId);
                if (keySet != null && !keySet.isEmpty()) {
                    qryTopicKeySet.addAll(keySet);
                }
            }
        }
        // filter record by qryEntity
        if (qryTopicKeySet == null) {
            for (TopicDeployEntity entry :  topicConfCache.values()) {
                if (entry == null || (qryEntity != null && !entry.isMatched(qryEntity))) {
                    continue;
                }
                items = retEntityMap.computeIfAbsent(
                        entry.getTopicName(), k -> new ArrayList<>());
                items.add(entry);
            }
        } else {
            TopicDeployEntity entry;
            for (String recKey : qryTopicKeySet) {
                entry = topicConfCache.get(recKey);
                if (entry == null || (qryEntity != null && !entry.isMatched(qryEntity))) {
                    continue;
                }
                items = retEntityMap.computeIfAbsent(
                        entry.getTopicName(), k -> new ArrayList<>());
                items.add(entry);
            }
        }
        return retEntityMap;
    }

    @Override
    public Map<Integer, List<TopicDeployEntity>> getTopicDeployInfoMap(
            Set<Integer> brokerIdSet, Set<String> topicNameSet) {
        List<TopicDeployEntity> items;
        Set<String> qryTopicKey = null;
        ConcurrentHashSet<String> keySet;
        Map<Integer, List<TopicDeployEntity>> retEntityMap = new HashMap<>();
        if (brokerIdSet != null) {
            for (Integer brokerId : brokerIdSet) {
                retEntityMap.put(brokerId, new ArrayList<>());
            }
        }
        if (topicNameSet != null && !topicNameSet.isEmpty()) {
            qryTopicKey = new HashSet<>();
            for (String topicName : topicNameSet) {
                keySet = topicNameCacheIndex.get(topicName);
                if (keySet != null && !keySet.isEmpty()) {
                    qryTopicKey.addAll(keySet);
                }
            }
        }
        if (brokerIdSet != null && !brokerIdSet.isEmpty()) {
            if (qryTopicKey == null) {
                qryTopicKey = new HashSet<>();
            }
            for (Integer brokerId : brokerIdSet) {
                keySet = brokerIdCacheIndex.get(brokerId);
                if (keySet != null && !keySet.isEmpty()) {
                    qryTopicKey.addAll(keySet);
                }
            }
        }
        if (qryTopicKey == null) {
            qryTopicKey = new HashSet<>(topicConfCache.keySet());
        }
        if (qryTopicKey.isEmpty()) {
            return retEntityMap;
        }
        for (String recordKey: qryTopicKey) {
            TopicDeployEntity entity = topicConfCache.get(recordKey);
            if (entity == null) {
                continue;
            }
            items = retEntityMap.computeIfAbsent(
                    entity.getBrokerId(), k -> new ArrayList<>());
            items.add(entity);
        }
        return retEntityMap;
    }

    @Override
    public Map<String, List<TopicDeployEntity>> getTopicConfMapByTopicAndBrokerIds(
            Set<String> topicSet, Set<Integer> brokerIdSet) {
        TopicDeployEntity tmpEntity;
        List<TopicDeployEntity> itemLst;
        ConcurrentHashSet<String> recSet;
        Set<String> hitKeys = new HashSet<>();
        Map<String, List<TopicDeployEntity>> retEntityMap = new HashMap<>();
        if (((topicSet == null) || (topicSet.isEmpty()))
                && ((brokerIdSet == null) || (brokerIdSet.isEmpty()))) {
            for (TopicDeployEntity entity : topicConfCache.values()) {
                itemLst = retEntityMap.computeIfAbsent(
                        entity.getTopicName(), k -> new ArrayList<>());
                itemLst.add(entity);
            }
            return retEntityMap;
        }
        if ((topicSet == null) || (topicSet.isEmpty())) {
            for (Integer brokerId : brokerIdSet) {
                recSet = brokerIdCacheIndex.get(brokerId);
                if (recSet == null || recSet.isEmpty()) {
                    continue;
                }
                hitKeys.addAll(recSet);
            }
        } else {
            for (String topic : topicSet) {
                recSet = topicNameCacheIndex.get(topic);
                if (recSet == null || recSet.isEmpty()) {
                    continue;
                }
                hitKeys.addAll(recSet);
            }
        }
        for (String key : hitKeys) {
            tmpEntity = topicConfCache.get(key);
            if (tmpEntity == null) {
                continue;
            }
            itemLst = retEntityMap.computeIfAbsent(
                    tmpEntity.getTopicName(), k -> new ArrayList<>());
            itemLst.add(tmpEntity);
        }
        return retEntityMap;
    }

    @Override
    public Map<Integer, Set<String>> getConfiguredTopicInfo(Set<Integer> brokerIdSet) {
        Set<String> topicSet;
        ConcurrentHashSet<String> deploySet;
        Map<Integer, Set<String>> retEntityMap = new HashMap<>();
        if (brokerIdSet == null || brokerIdSet.isEmpty()) {
            for (Map.Entry<Integer, ConcurrentHashSet<String>> entry
                    : brokerId2TopicCacheIndex.entrySet()) {
                if (entry.getKey() == null) {
                    continue;
                }
                topicSet = new HashSet<>();
                if (entry.getValue() != null) {
                    topicSet.addAll(entry.getValue());
                }
                retEntityMap.put(entry.getKey(), topicSet);
            }
        } else {
            for (Integer brokerId : brokerIdSet) {
                if (brokerId == null) {
                    continue;
                }
                topicSet = new HashSet<>();
                deploySet = brokerId2TopicCacheIndex.get(brokerId);
                if (deploySet != null) {
                    topicSet.addAll(deploySet);
                }
                retEntityMap.put(brokerId, topicSet);
            }
        }
        return retEntityMap;
    }

    @Override
    public Map<String, Map<Integer, String>> getTopicBrokerInfo(Set<String> topicNameSet) {
        ConcurrentHashSet<String> keySet;
        Map<Integer, String> brokerInfoMap;
        Map<String, Map<Integer, String>> retEntityMap = new HashMap<>();
        if (topicNameSet == null || topicNameSet.isEmpty()) {
            for (TopicDeployEntity entry : topicConfCache.values()) {
                if (entry == null) {
                    continue;
                }
                brokerInfoMap = retEntityMap.computeIfAbsent(
                        entry.getTopicName(), k -> new HashMap<>());
                brokerInfoMap.put(entry.getBrokerId(), entry.getBrokerIp());
            }
        } else {
            for (String topicName : topicNameSet) {
                if (topicName == null) {
                    continue;
                }
                brokerInfoMap = retEntityMap.computeIfAbsent(topicName, k -> new HashMap<>());
                keySet = topicNameCacheIndex.get(topicName);
                if (keySet != null) {
                    for (String key : keySet) {
                        TopicDeployEntity entry = topicConfCache.get(key);
                        if (entry != null) {
                            brokerInfoMap.put(entry.getBrokerId(), entry.getBrokerIp());
                        }
                    }
                }
            }
        }
        return retEntityMap;
    }

    @Override
    public Set<String> getConfiguredTopicSet() {
        return new HashSet<>(topicNameCacheIndex.keySet());
    }

    @Override
    public Map<String, TopicDeployEntity> getConfiguredTopicInfo(int brokerId) {
        TopicDeployEntity tmpEntity;
        Map<String, TopicDeployEntity> retEntityMap = new HashMap<>();
        ConcurrentHashSet<String> records = brokerIdCacheIndex.get(brokerId);
        if (records == null || records.isEmpty()) {
            return retEntityMap;
        }
        for (String key : records) {
            tmpEntity = topicConfCache.get(key);
            if (tmpEntity == null) {
                continue;
            }
            retEntityMap.put(tmpEntity.getTopicName(), tmpEntity);
        }
        return retEntityMap;
    }

    /**
     * Put topic configure info into bdb store
     *
     * @param memEntity need add record
     * @param result process result with old value
     * @return
     */
    private boolean putTopicConfig2Bdb(TopicDeployEntity memEntity, ProcessResult result) {
        BdbTopicConfEntity retData = null;
        BdbTopicConfEntity bdbEntity =
                memEntity.buildBdbTopicConfEntity();
        try {
            retData = topicConfIndex.put(bdbEntity);
        } catch (Throwable e) {
            logger.error("[BDB Impl] put topic configure failure ", e);
            result.setFailResult(DataOpErrCode.DERR_STORE_ABNORMAL.getCode(),
                    new StringBuilder(TBaseConstants.BUILDER_DEFAULT_SIZE)
                            .append("Put topic configure failure: ")
                            .append(e.getMessage()).toString());
            return result.isSuccess();
        }
        result.setSuccResult(null);
        return result.isSuccess();
    }

    private boolean delTopicConfigFromBdb(String recordKey) {
        try {
            topicConfIndex.delete(recordKey);
        } catch (Throwable e) {
            logger.error("[BDB Impl] delete topic configure failure ", e);
            return false;
        }
        return true;
    }

    private void delCacheRecord(String recordKey) {
        TopicDeployEntity curEntity =
                topicConfCache.remove(recordKey);
        if (curEntity == null) {
            return;
        }
        // add topic index
        ConcurrentHashSet<String> keySet =
                topicNameCacheIndex.get(curEntity.getTopicName());
        if (keySet != null) {
            keySet.remove(recordKey);
            if (keySet.isEmpty()) {
                topicNameCacheIndex.remove(curEntity.getTopicName());
            }
        }
        // delete brokerId index
        keySet = brokerIdCacheIndex.get(curEntity.getBrokerId());
        if (keySet != null) {
            keySet.remove(recordKey);
            if (keySet.isEmpty()) {
                brokerIdCacheIndex.remove(curEntity.getBrokerId());
            }
        }
        // delete broker topic map
        keySet = brokerId2TopicCacheIndex.get(curEntity.getBrokerId());
        if (keySet != null) {
            keySet.remove(curEntity.getTopicName());
            if (keySet.isEmpty()) {
                brokerId2TopicCacheIndex.remove(curEntity.getBrokerId());
            }
        }
    }

    private void addOrUpdCacheRecord(TopicDeployEntity entity) {
        topicConfCache.put(entity.getRecordKey(), entity);
        // add topic index map
        ConcurrentHashSet<String> keySet =
                topicNameCacheIndex.get(entity.getTopicName());
        if (keySet == null) {
            ConcurrentHashSet<String> tmpSet = new ConcurrentHashSet<>();
            keySet = topicNameCacheIndex.putIfAbsent(entity.getTopicName(), tmpSet);
            if (keySet == null) {
                keySet = tmpSet;
            }
        }
        keySet.add(entity.getRecordKey());
        // add brokerId index map
        keySet = brokerIdCacheIndex.get(entity.getBrokerId());
        if (keySet == null) {
            ConcurrentHashSet<String> tmpSet = new ConcurrentHashSet<>();
            keySet = brokerIdCacheIndex.putIfAbsent(entity.getBrokerId(), tmpSet);
            if (keySet == null) {
                keySet = tmpSet;
            }
        }
        keySet.add(entity.getRecordKey());
        // add brokerId topic map
        keySet = brokerId2TopicCacheIndex.get(entity.getBrokerId());
        if (keySet == null) {
            ConcurrentHashSet<String> tmpSet = new ConcurrentHashSet<>();
            keySet = brokerId2TopicCacheIndex.putIfAbsent(entity.getBrokerId(), tmpSet);
            if (keySet == null) {
                keySet = tmpSet;
            }
        }
        keySet.add(entity.getTopicName());
    }

    private void clearCacheData() {
        topicNameCacheIndex.clear();
        brokerIdCacheIndex.clear();
        brokerId2TopicCacheIndex.clear();
        topicConfCache.clear();
    }
}
