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
import org.apache.tubemq.server.common.exception.LoadMetaException;
import org.apache.tubemq.server.common.utils.ProcessResult;
import org.apache.tubemq.server.master.bdbstore.bdbentitys.BdbTopicConfEntity;
import org.apache.tubemq.server.master.metamanage.DataOpErrCode;
import org.apache.tubemq.server.master.metamanage.metastore.dao.entity.TopicConfEntity;
import org.apache.tubemq.server.master.metamanage.metastore.dao.mapper.TopicConfigMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public class BdbTopicConfigMapperImpl implements TopicConfigMapper {

    private static final Logger logger =
            LoggerFactory.getLogger(BdbTopicConfigMapperImpl.class);

    // Topic configure store
    private EntityStore topicConfStore;
    private PrimaryIndex<String/* recordKey */, BdbTopicConfEntity> topicConfIndex;
    // data cache
    private ConcurrentHashMap<String/* recordKey */, TopicConfEntity> topicConfCache =
            new ConcurrentHashMap<>();
    private ConcurrentHashMap<Integer/* brokerId */, ConcurrentHashSet<String>>
            topicConfBrokerCacheIndex = new ConcurrentHashMap<>();
    private ConcurrentHashMap<Integer/* brokerId */, ConcurrentHashSet<String>>
            brokerTopicMapCacheIndex = new ConcurrentHashMap<>();

    private ConcurrentHashMap<String/* topicName */, ConcurrentHashSet<String>>
            topicConfTopicNameCacheIndex = new ConcurrentHashMap<>();




    public BdbTopicConfigMapperImpl(ReplicatedEnvironment repEnv, StoreConfig storeConfig) {
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
                TopicConfEntity memEntity = new TopicConfEntity(bdbEntity);
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
    public boolean addTopicConf(TopicConfEntity memEntity, ProcessResult result) {
        TopicConfEntity curEntity =
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
    public boolean updTopicConf(TopicConfEntity memEntity, ProcessResult result) {
        TopicConfEntity curEntity =
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
        TopicConfEntity curEntity =
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
    public boolean hasConfiguredTopics(int brokerId) {
        ConcurrentHashSet<String> keySet =
                topicConfBrokerCacheIndex.get(brokerId);
        return (keySet != null && !keySet.isEmpty());
    }

    @Override
    public TopicConfEntity getTopicConfByeRecKey(String recordKey) {
        return topicConfCache.get(recordKey);
    }

    @Override
    public List<TopicConfEntity> getTopicConf(TopicConfEntity qryEntity) {
        List<TopicConfEntity> retEntitys = new ArrayList<>();
        if (qryEntity == null) {
            retEntitys.addAll(topicConfCache.values());
        } else {
            for (TopicConfEntity entity : topicConfCache.values()) {
                if (entity.isMatched(qryEntity)) {
                    retEntitys.add(entity);
                }
            }
        }
        return retEntitys;
    }

    @Override
    public Map<String, List<TopicConfEntity>> getTopicConfMap(TopicConfEntity qryEntity) {
        List<TopicConfEntity> items;
        Map<String, List<TopicConfEntity>> retEntityMap = new HashMap<>();
        if (qryEntity == null) {
            for (TopicConfEntity entity : topicConfCache.values()) {
                items = retEntityMap.get(entity.getTopicName());
                if (items == null) {
                    items = new ArrayList<>();
                    retEntityMap.put(entity.getTopicName(), items);
                }
                items.add(entity);
            }
        } else {
            for (TopicConfEntity entity : topicConfCache.values()) {
                if (entity.isMatched(qryEntity)) {
                    items = retEntityMap.get(entity.getTopicName());
                    if (items == null) {
                        items = new ArrayList<>();
                        retEntityMap.put(entity.getTopicName(), items);
                    }
                    items.add(entity);
                }
            }
        }
        return retEntityMap;
    }

    @Override
    public Map<Integer, Set<String>> getConfiguredTopicInfo(Set<Integer> brokerIdSet) {
        Set<String> items;
        Map<Integer, Set<String>> retEntityMap = new HashMap<>();
        if (brokerIdSet == null || brokerIdSet.isEmpty()) {
            brokerIdSet = new HashSet<>();
            brokerIdSet.addAll(brokerTopicMapCacheIndex.keySet());
        }
        for (Integer brokerId : brokerIdSet) {
            if (brokerId == null) {
                continue;
            }
            ConcurrentHashSet<String> topicSet =
                    brokerTopicMapCacheIndex.get(brokerId);
            if (topicSet == null || topicSet.isEmpty()) {
                continue;
            }
            items = retEntityMap.get(brokerId);
            if (items == null) {
                items = new HashSet<>();
                retEntityMap.put(brokerId, items);
            }
            for (String topic : topicSet) {
                if (topic == null) {
                    continue;
                }
                items.add(topic);
            }
        }
        return retEntityMap;
    }

    @Override
    public Map<String, Map<Integer, String>> getTopicBrokerInfo(Set<String> topicNameSet) {
        Map<Integer, String> items;
        Map<String, Map<Integer, String>> retEntityMap = new HashMap<>();
        if (topicNameSet == null || topicNameSet.isEmpty()) {
            topicNameSet = new HashSet<>();
            topicNameSet.addAll(topicConfTopicNameCacheIndex.keySet());
        }
        for (String topicName : topicNameSet) {
            if (topicName == null) {
                continue;
            }
            ConcurrentHashSet<String> keySet =
                    topicConfTopicNameCacheIndex.get(topicName);
            if (keySet == null || keySet.isEmpty()) {
                continue;
            }
            for (String key : keySet) {
                TopicConfEntity entity = topicConfCache.get(key);
                if (entity == null) {
                    continue;
                }
                items = retEntityMap.get(topicName);
                if (items == null) {
                    items = new HashMap<>();
                    retEntityMap.put(topicName, items);
                }
                items.put(entity.getBrokerId(), entity.getBrokerIp());
            }
        }
        return retEntityMap;
    }

    @Override
    public Set<String> getConfiguredTopicSet() {
        Set<String> topicNames = new HashSet<>();
        topicNames.addAll(topicConfTopicNameCacheIndex.keySet());
        return topicNames;
    }

    @Override
    public Map<String, TopicConfEntity> getConfiguredTopicInfo(int brokerId) {
        TopicConfEntity tmpEntity;
        Map<String, TopicConfEntity> retEntityMap = new HashMap<>();
        ConcurrentHashSet<String> records = topicConfBrokerCacheIndex.get(brokerId);
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
    private boolean putTopicConfig2Bdb(TopicConfEntity memEntity, ProcessResult result) {
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
        TopicConfEntity curEntity =
                topicConfCache.remove(recordKey);
        if (curEntity == null) {
            return;
        }
        // add topic index
        ConcurrentHashSet<String> keySet =
                topicConfTopicNameCacheIndex.get(curEntity.getTopicName());
        if (keySet != null) {
            keySet.remove(recordKey);
            if (keySet.isEmpty()) {
                topicConfTopicNameCacheIndex.remove(curEntity.getTopicName());
            }
        }
        // delete brokerId index
        keySet = topicConfBrokerCacheIndex.get(curEntity.getBrokerId());
        if (keySet != null) {
            keySet.remove(recordKey);
            if (keySet.isEmpty()) {
                topicConfBrokerCacheIndex.remove(curEntity.getBrokerId());
            }
        }
        // delete broker topic map
        keySet = brokerTopicMapCacheIndex.get(curEntity.getBrokerId());
        if (keySet != null) {
            keySet.remove(curEntity.getTopicName());
            if (keySet.isEmpty()) {
                brokerTopicMapCacheIndex.remove(curEntity.getBrokerId());
            }
        }
    }

    private void addOrUpdCacheRecord(TopicConfEntity entity) {
        topicConfCache.put(entity.getRecordKey(), entity);
        // add topic index map
        ConcurrentHashSet<String> keySet =
                topicConfTopicNameCacheIndex.get(entity.getTopicName());
        if (keySet == null) {
            ConcurrentHashSet<String> tmpSet = new ConcurrentHashSet<>();
            keySet = topicConfTopicNameCacheIndex.putIfAbsent(entity.getTopicName(), tmpSet);
            if (keySet == null) {
                keySet = tmpSet;
            }
        }
        keySet.add(entity.getRecordKey());
        // add brokerId index map
        keySet = topicConfBrokerCacheIndex.get(entity.getBrokerId());
        if (keySet == null) {
            ConcurrentHashSet<String> tmpSet = new ConcurrentHashSet<>();
            keySet = topicConfBrokerCacheIndex.putIfAbsent(entity.getBrokerId(), tmpSet);
            if (keySet == null) {
                keySet = tmpSet;
            }
        }
        keySet.add(entity.getRecordKey());
        // add brokerId topic map
        keySet = brokerTopicMapCacheIndex.get(entity.getBrokerId());
        if (keySet == null) {
            ConcurrentHashSet<String> tmpSet = new ConcurrentHashSet<>();
            keySet = brokerTopicMapCacheIndex.putIfAbsent(entity.getBrokerId(), tmpSet);
            if (keySet == null) {
                keySet = tmpSet;
            }
        }
        keySet.add(entity.getTopicName());
    }

    private void clearCacheData() {
        topicConfTopicNameCacheIndex.clear();
        topicConfBrokerCacheIndex.clear();
        brokerTopicMapCacheIndex.clear();
        topicConfCache.clear();
    }
}
