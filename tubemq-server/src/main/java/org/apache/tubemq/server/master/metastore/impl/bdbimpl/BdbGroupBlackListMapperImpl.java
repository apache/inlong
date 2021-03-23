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

package org.apache.tubemq.server.master.metastore.impl.bdbimpl;


import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.persist.EntityCursor;
import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.PrimaryIndex;
import com.sleepycat.persist.StoreConfig;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.tubemq.corebase.TBaseConstants;
import org.apache.tubemq.corebase.utils.ConcurrentHashSet;
import org.apache.tubemq.server.common.exception.LoadMetaException;
import org.apache.tubemq.server.common.utils.ProcessResult;
import org.apache.tubemq.server.master.bdbstore.bdbentitys.BdbBlackGroupEntity;
import org.apache.tubemq.server.master.metastore.DataOpErrCode;
import org.apache.tubemq.server.master.metastore.dao.entity.GroupBlackListEntity;
import org.apache.tubemq.server.master.metastore.dao.mapper.GroupBlackListMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public class BdbGroupBlackListMapperImpl implements GroupBlackListMapper {

    private static final Logger logger =
            LoggerFactory.getLogger(BdbGroupBlackListMapperImpl.class);
    // consumer group black list store
    private EntityStore blackGroupStore;
    private PrimaryIndex<String/* recordKey */, BdbBlackGroupEntity> blackGroupIndex;
    private ConcurrentHashMap<String/* recordKey */, GroupBlackListEntity>
            groupBlackListCache = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String/* topicName */, ConcurrentHashSet<String>>
            groupBlackListTopicCache = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String/* groupName */, ConcurrentHashSet<String> >
            groupBlackListGroupCache = new ConcurrentHashMap<>();



    public BdbGroupBlackListMapperImpl(ReplicatedEnvironment repEnv, StoreConfig storeConfig) {
        blackGroupStore = new EntityStore(repEnv,
                TBDBStoreTables.BDB_BLACK_GROUP_STORE_NAME, storeConfig);
        blackGroupIndex =
                blackGroupStore.getPrimaryIndex(String.class, BdbBlackGroupEntity.class);
    }

    @Override
    public void close() {
        // clear cache data
        clearCacheData();
        // close store object
        if (blackGroupStore != null) {
            try {
                blackGroupStore.close();
                blackGroupStore = null;
            } catch (Throwable e) {
                logger.error("[BDB Impl] close blacklist configure failure ", e);
            }
        }
    }

    @Override
    public void loadConfig() throws LoadMetaException {
        long count = 0L;
        EntityCursor<BdbBlackGroupEntity> cursor = null;
        logger.info("[BDB Impl] load blacklist configure start...");
        try {
            // clear cache data
            clearCacheData();
            // read data from bdb
            cursor = blackGroupIndex.entities();
            for (BdbBlackGroupEntity bdbEntity : cursor) {
                if (bdbEntity == null) {
                    logger.warn("[BDB Impl] found Null data while loading blacklist configure!");
                    continue;
                }
                GroupBlackListEntity memEntity =
                        new GroupBlackListEntity(bdbEntity);
                addOrUpdCacheRecord(memEntity);
                count++;
            }
            logger.info("[BDB Impl] total blacklist configure records are {}", count);
        } catch (Exception e) {
            logger.error("[BDB Impl] load blacklist configure failure ", e);
            throw new LoadMetaException(e.getMessage());
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }
        logger.info("[BDB Impl] load blacklist configure successfully...");
    }

    @Override
    public boolean addGroupBlackListConf(GroupBlackListEntity memEntity, ProcessResult result) {
        GroupBlackListEntity curEntity =
                groupBlackListCache.get(memEntity.getRecordKey());
        if (curEntity != null) {
            result.setFailResult(DataOpErrCode.DERR_EXISTED.getCode(),
                    new StringBuilder(TBaseConstants.BUILDER_DEFAULT_SIZE)
                            .append("The blacklist ").append(memEntity.getRecordKey())
                            .append("'s configure already exists, please delete it first!")
                            .toString());
            return result.isSuccess();
        }
        if (putGroupBlackListConfig2Bdb(memEntity, result)) {
            addOrUpdCacheRecord(memEntity);
        }
        return result.isSuccess();
    }

    @Override
    public boolean updGroupBlackListConf(GroupBlackListEntity memEntity, ProcessResult result) {
        GroupBlackListEntity curEntity =
                groupBlackListCache.get(memEntity.getRecordKey());
        if (curEntity == null) {
            result.setFailResult(DataOpErrCode.DERR_NOT_EXIST.getCode(),
                    new StringBuilder(TBaseConstants.BUILDER_DEFAULT_SIZE)
                            .append("The blacklist ").append(memEntity.getRecordKey())
                            .append("'s configure is not exists, please add record first!")
                            .toString());
            return result.isSuccess();
        }
        if (curEntity.equals(memEntity)) {
            result.setFailResult(DataOpErrCode.DERR_UNCHANGED.getCode(),
                    new StringBuilder(TBaseConstants.BUILDER_DEFAULT_SIZE)
                            .append("The blacklist ").append(memEntity.getRecordKey())
                            .append("'s configure have not changed, please delete it first!")
                            .toString());
            return result.isSuccess();
        }
        if (putGroupBlackListConfig2Bdb(memEntity, result)) {
            addOrUpdCacheRecord(memEntity);
        }
        return result.isSuccess();
    }

    @Override
    public boolean delGroupBlackListConf(String recordKey) {
        GroupBlackListEntity curEntity =
                groupBlackListCache.get(recordKey);
        if (curEntity == null) {
            return true;
        }
        delGroupBlackListConfigFromBdb(recordKey);
        delCacheRecord(recordKey);
        return true;
    }

    @Override
    public List<GroupBlackListEntity> getGrpBlkLstConfByGroupName(String groupName) {
        ConcurrentHashSet<String> keySet =
                groupBlackListGroupCache.get(groupName);
        if (keySet == null || keySet.isEmpty()) {
            return Collections.emptyList();
        }
        GroupBlackListEntity entity;
        List<GroupBlackListEntity> result = new ArrayList<>();
        for (String recordKey : keySet) {
            entity = groupBlackListCache.get(recordKey);
            if (entity != null) {
                result.add(entity);
            }
        }
        return result;
    }

    @Override
    public boolean delGroupBlackListConfByGroupName(String groupName) {
        List<GroupBlackListEntity> curEntitys =
                getGrpBlkLstConfByTopicName(groupName);
        if (curEntitys.isEmpty()) {
            return true;
        }
        for (GroupBlackListEntity entity : curEntitys) {
            delGroupBlackListConf(entity.getRecordKey());
        }
        return true;
    }

    @Override
    public List<GroupBlackListEntity> getGrpBlkLstConfByTopicName(String topicName) {
        ConcurrentHashSet<String> keySet =
                groupBlackListTopicCache.get(topicName);
        if (keySet == null || keySet.isEmpty()) {
            return Collections.emptyList();
        }
        GroupBlackListEntity entity;
        List<GroupBlackListEntity> result = new ArrayList<>();
        for (String recordKey : keySet) {
            entity = groupBlackListCache.get(recordKey);
            if (entity != null) {
                result.add(entity);
            }
        }
        return result;
    }

    @Override
    public List<GroupBlackListEntity> getGroupBlackListConf(GroupBlackListEntity qryEntity) {
        List<GroupBlackListEntity> retEntitys = new ArrayList<>();
        if (qryEntity == null) {
            retEntitys.addAll(groupBlackListCache.values());
        } else {
            for (GroupBlackListEntity entity : groupBlackListCache.values()) {
                if (entity.isMatched(qryEntity)) {
                    retEntitys.add(entity);
                }
            }
        }
        return retEntitys;
    }

    /**
     * Put blacklist configure info into bdb store
     *
     * @param memEntity need add record
     * @param result process result with old value
     * @return
     */
    private boolean putGroupBlackListConfig2Bdb(GroupBlackListEntity memEntity,
                                                ProcessResult result) {
        BdbBlackGroupEntity retData = null;
        BdbBlackGroupEntity bdbEntity = memEntity.buildBdbBlackListEntity();
        try {
            retData = blackGroupIndex.put(bdbEntity);
        } catch (Throwable e) {
            logger.error("[BDB Impl] put blacklist configure failure ", e);
            result.setFailResult(DataOpErrCode.DERR_STORE_ABNORMAL.getCode(),
                    new StringBuilder(TBaseConstants.BUILDER_DEFAULT_SIZE)
                            .append("Put blacklist configure failure: ")
                            .append(e.getMessage()).toString());
            return result.isSuccess();
        }
        result.setSuccResult(retData == null);
        return result.isSuccess();
    }

    private boolean delGroupBlackListConfigFromBdb(String recordKey) {
        try {
            blackGroupIndex.delete(recordKey);
        } catch (Throwable e) {
            logger.error("[BDB Impl] delete blacklist configure failure ", e);
            return false;
        }
        return true;
    }


    private void delCacheRecord(String recordKey) {
        GroupBlackListEntity curEntity = groupBlackListCache.get(recordKey);
        if (curEntity == null) {
            return;
        }
        // add topic index
        ConcurrentHashSet<String> keySet =
                groupBlackListTopicCache.get(curEntity.getTopicName());
        if (keySet != null) {
            keySet.remove(recordKey);
            if (keySet.isEmpty()) {
                groupBlackListTopicCache.remove(curEntity.getTopicName());
            }
        }
        // delete group index
        keySet = groupBlackListGroupCache.remove(curEntity.getGroupName());
        if (keySet != null) {
            keySet.remove(recordKey);
            if (keySet.isEmpty()) {
                groupBlackListGroupCache.remove(curEntity.getGroupName());
            }
        }
    }

    private void addOrUpdCacheRecord(GroupBlackListEntity entity) {
        groupBlackListCache.put(entity.getRecordKey(), entity);
        // add topic index map
        ConcurrentHashSet<String> keySet =
                groupBlackListTopicCache.get(entity.getTopicName());
        if (keySet == null) {
            ConcurrentHashSet<String> tmpSet = new ConcurrentHashSet<>();
            keySet = groupBlackListTopicCache.putIfAbsent(entity.getTopicName(), tmpSet);
            if (keySet == null) {
                keySet = tmpSet;
            }
        }
        keySet.add(entity.getRecordKey());
        // add group index map
        keySet = groupBlackListGroupCache.get(entity.getGroupName());
        if (keySet == null) {
            ConcurrentHashSet<String> tmpSet = new ConcurrentHashSet<>();
            keySet = groupBlackListGroupCache.putIfAbsent(entity.getGroupName(), tmpSet);
            if (keySet == null) {
                keySet = tmpSet;
            }
        }
        keySet.add(entity.getRecordKey());
    }

    private void clearCacheData() {
        groupBlackListTopicCache.clear();
        groupBlackListGroupCache.clear();
        groupBlackListCache.clear();
    }

}
