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

package org.apache.inlong.tubemq.server.master.metamanage.metastore.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.inlong.tubemq.corebase.rv.ProcessResult;
import org.apache.inlong.tubemq.corebase.utils.ConcurrentHashSet;
import org.apache.inlong.tubemq.corebase.utils.KeyBuilderUtils;
import org.apache.inlong.tubemq.server.master.metamanage.DataOpErrCode;
import org.apache.inlong.tubemq.server.master.metamanage.metastore.dao.entity.TopicDeployEntity;
import org.apache.inlong.tubemq.server.master.metamanage.metastore.dao.mapper.TopicDeployMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbsTopicDeployMapperImpl implements TopicDeployMapper {
    protected static final Logger logger =
            LoggerFactory.getLogger(AbsTopicDeployMapperImpl.class);
    // data cache
    private final ConcurrentHashMap<String/* recordKey */, TopicDeployEntity>
            topicConfCache = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Integer/* brokerId */, ConcurrentHashSet<String>>
            brokerIdCacheIndex = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String/* topicName */, ConcurrentHashSet<String>>
            topicNameCacheIndex = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Integer/* brokerId */, ConcurrentHashSet<String>>
            brokerId2TopicCacheIndex = new ConcurrentHashMap<>();

    public AbsTopicDeployMapperImpl() {
        // Initial instant
    }

    @Override
    public boolean addTopicConf(TopicDeployEntity entity,
                                StringBuilder strBuff, ProcessResult result) {
        TopicDeployEntity curEntity =
                topicConfCache.get(entity.getRecordKey());
        if (curEntity != null) {
            result.setFailResult(DataOpErrCode.DERR_EXISTED.getCode(),
                    strBuff.append("The topic deploy configure ").append(entity.getRecordKey())
                            .append(" already exists, please delete it first!")
                            .toString());
            strBuff.delete(0, strBuff.length());
            return result.isSuccess();
        }
        if (putConfig2Persistent(entity, strBuff, result)) {
            addOrUpdCacheRecord(entity);
        }
        return result.isSuccess();
    }

    @Override
    public boolean updTopicConf(TopicDeployEntity entity,
                                StringBuilder strBuff, ProcessResult result) {
        TopicDeployEntity curEntity =
                topicConfCache.get(entity.getRecordKey());
        if (curEntity == null) {
            result.setFailResult(DataOpErrCode.DERR_NOT_EXIST.getCode(),
                    strBuff.append("The topic deploy configure ").append(entity.getRecordKey())
                            .append(" is not exists, please add it first!")
                            .toString());
            strBuff.delete(0, strBuff.length());
            return result.isSuccess();
        }
        if (curEntity.equals(entity)) {
            result.setFailResult(DataOpErrCode.DERR_UNCHANGED.getCode(),
                    strBuff.append("The topic deploy configure ").append(entity.getRecordKey())
                            .append(" have not changed, please confirm it first!")
                            .toString());
            strBuff.delete(0, strBuff.length());
            return result.isSuccess();
        }
        if (putConfig2Persistent(entity, strBuff, result)) {
            addOrUpdCacheRecord(entity);
            result.setSuccResult(curEntity);
        }
        return result.isSuccess();
    }

    @Override
    public boolean delTopicConf(String recordKey, StringBuilder strBuff, ProcessResult result) {
        TopicDeployEntity curEntity =
                topicConfCache.get(recordKey);
        if (curEntity == null) {
            result.setSuccResult(null);
            return result.isSuccess();
        }
        delConfigFromPersistent(recordKey, strBuff);
        delCacheRecord(recordKey);
        result.setSuccResult(curEntity);
        return result.isSuccess();
    }

    @Override
    public boolean delTopicConfByBrokerId(Integer brokerId, StringBuilder strBuff, ProcessResult result) {
        ConcurrentHashSet<String> recordKeySet =
                brokerIdCacheIndex.get(brokerId);
        if (recordKeySet == null) {
            result.setSuccResult(null);
            return result.isSuccess();
        }
        for (String recordKey : recordKeySet) {
            delConfigFromPersistent(recordKey, strBuff);
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
    public boolean isTopicDeployed(String topicName) {
        ConcurrentHashSet<String> deploySet = topicNameCacheIndex.get(topicName);
        return (deploySet != null && !deploySet.isEmpty());
    }

    @Override
    public List<TopicDeployEntity> getTopicConf(TopicDeployEntity qryEntity) {
        List<TopicDeployEntity> retEntities = new ArrayList<>();
        if (qryEntity == null) {
            retEntities.addAll(topicConfCache.values());
        } else {
            for (TopicDeployEntity entity : topicConfCache.values()) {
                if (entity != null && entity.isMatched(qryEntity)) {
                    retEntities.add(entity);
                }
            }
        }
        return retEntities;
    }

    @Override
    public TopicDeployEntity getTopicConf(int brokerId, String topicName) {
        String recordKey =
                KeyBuilderUtils.buildTopicConfRecKey(brokerId, topicName);
        return topicConfCache.get(recordKey);
    }

    @Override
    public TopicDeployEntity getTopicConfByeRecKey(String recordKey) {
        return topicConfCache.get(recordKey);
    }

    @Override
    public Map<String, List<TopicDeployEntity>> getTopicConfMap(Set<String> topicNameSet,
                                                                Set<Integer> brokerIdSet,
                                                                TopicDeployEntity qryEntity) {
        List<TopicDeployEntity> items;
        Map<String, List<TopicDeployEntity>> retEntityMap = new HashMap<>();
        // get matched keys by topicNameSet and brokerIdSet
        Set<String> matchedKeySet = getMatchedRecords(topicNameSet, brokerIdSet);
        // filter record by qryEntity
        if (matchedKeySet == null) {
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
            for (String recKey : matchedKeySet) {
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
    public Map<Integer, List<TopicDeployEntity>> getTopicDeployInfoMap(Set<String> topicNameSet,
                                                                       Set<Integer> brokerIdSet) {
        List<TopicDeployEntity> items;
        Map<Integer, List<TopicDeployEntity>> retEntityMap = new HashMap<>();
        if (brokerIdSet != null) {
            for (Integer brokerId : brokerIdSet) {
                retEntityMap.put(brokerId, new ArrayList<>());
            }
        }
        // get matched keys by topicNameSet and brokerIdSet
        Set<String> matchedKeySet = getMatchedRecords(topicNameSet, brokerIdSet);
        // get record by keys
        if (matchedKeySet == null) {
            matchedKeySet = new HashSet<>(topicConfCache.keySet());
        }
        for (String recordKey: matchedKeySet) {
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
    public Map<String, List<TopicDeployEntity>> getTopicConfMapByTopicAndBrokerIds(Set<String> topicSet,
                                                                                   Set<Integer> brokerIdSet) {
        TopicDeployEntity tmpEntity;
        List<TopicDeployEntity> itemLst;
        Map<String, List<TopicDeployEntity>> retEntityMap = new HashMap<>();
        // get matched keys by topicNameSet and brokerIdSet
        Set<String> matchedKeySet = getMatchedRecords(topicSet, brokerIdSet);
        // get records by matched keys
        if (matchedKeySet == null) {
            for (TopicDeployEntity entity : topicConfCache.values()) {
                if (entity == null) {
                    continue;
                }
                itemLst = retEntityMap.computeIfAbsent(
                        entity.getTopicName(), k -> new ArrayList<>());
                itemLst.add(entity);
            }
        } else {
            for (String key : matchedKeySet) {
                tmpEntity = topicConfCache.get(key);
                if (tmpEntity == null) {
                    continue;
                }
                itemLst = retEntityMap.computeIfAbsent(
                        tmpEntity.getTopicName(), k -> new ArrayList<>());
                itemLst.add(tmpEntity);
            }
        }
        return retEntityMap;
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

    /**
     * Clear cached data
     */
    protected void clearCachedData() {
        topicNameCacheIndex.clear();
        brokerIdCacheIndex.clear();
        brokerId2TopicCacheIndex.clear();
        topicConfCache.clear();
    }

    /**
     * Add or update a record
     *
     * @param entity  need added or updated entity
     */
    protected void addOrUpdCacheRecord(TopicDeployEntity entity) {
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

    /**
     * Put topic deploy configure information into persistent store
     *
     * @param entity   need add record
     * @param strBuff  the string buffer
     * @param result process result with old value
     * @return the process result
     */
    protected abstract boolean putConfig2Persistent(TopicDeployEntity entity,
                                                    StringBuilder strBuff, ProcessResult result);

    /**
     * Delete topic deploy configure information from persistent storage
     *
     * @param recordKey  the record key
     * @param strBuff    the string buffer
     * @return the process result
     */
    protected abstract boolean delConfigFromPersistent(String recordKey, StringBuilder strBuff);

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

    private Set<String> getMatchedRecords(Set<String> topicNameSet,
                                          Set<Integer> brokerIdSet) {
        ConcurrentHashSet<String> keySet;
        Set<String> topicKeySet = null;
        Set<String> brokerKeySet = null;
        Set<String> matchedKeySet = null;
        // get deploy records set by topicName
        if (topicNameSet != null && !topicNameSet.isEmpty()) {
            topicKeySet = new HashSet<>();
            for (String topicName : topicNameSet) {
                keySet = topicNameCacheIndex.get(topicName);
                if (keySet != null && !keySet.isEmpty()) {
                    topicKeySet.addAll(keySet);
                }
            }
            if (topicKeySet.isEmpty()) {
                return Collections.emptySet();
            }
        }
        // get deploy records set by brokerId
        if (brokerIdSet != null && !brokerIdSet.isEmpty()) {
            brokerKeySet = new HashSet<>();
            for (Integer brokerId : brokerIdSet) {
                keySet = brokerIdCacheIndex.get(brokerId);
                if (keySet != null && !keySet.isEmpty()) {
                    brokerKeySet.addAll(keySet);
                }
            }
            if (brokerKeySet.isEmpty()) {
                return Collections.emptySet();
            }
        }
        // get intersection from topicKeySet and brokerKeySet
        if (topicKeySet != null || brokerKeySet != null) {
            if (topicKeySet == null) {
                matchedKeySet = new HashSet<>(brokerKeySet);
            } else {
                if (brokerKeySet == null) {
                    matchedKeySet = new HashSet<>(topicKeySet);
                } else {
                    matchedKeySet = new HashSet<>();
                    for (String record : topicKeySet) {
                        if (brokerKeySet.contains(record)) {
                            matchedKeySet.add(record);
                        }
                    }
                }
            }
        }
        return matchedKeySet;
    }
}
