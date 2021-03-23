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
import org.apache.tubemq.server.master.bdbstore.bdbentitys.BdbGroupFilterCondEntity;
import org.apache.tubemq.server.master.metastore.DataOpErrCode;
import org.apache.tubemq.server.master.metastore.dao.entity.GroupFilterCtrlEntity;
import org.apache.tubemq.server.master.metastore.dao.mapper.GroupFilterCtrlMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public class BdbGroupFilterCtrlMapperImpl implements GroupFilterCtrlMapper {

    private static final Logger logger =
            LoggerFactory.getLogger(BdbGroupFilterCtrlMapperImpl.class);
    // consumer group filter control store
    private EntityStore groupFilterStore;
    private PrimaryIndex<String/* recordKey */, BdbGroupFilterCondEntity> groupFilterIndex;
    // configure cache
    private ConcurrentHashMap<String/* recordKey */, GroupFilterCtrlEntity>
            groupFilterCtrlCache = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String/* topicName */, ConcurrentHashSet<String>>
            groupFilterCtrlTopicCache = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String/* groupName */, ConcurrentHashSet<String> >
            groupFilterCtrlGroupCache = new ConcurrentHashMap<>();



    public BdbGroupFilterCtrlMapperImpl(ReplicatedEnvironment repEnv, StoreConfig storeConfig) {
        groupFilterStore = new EntityStore(repEnv,
                TBDBStoreTables.BDB_GROUP_FILTER_COND_STORE_NAME, storeConfig);
        groupFilterIndex =
                groupFilterStore.getPrimaryIndex(String.class, BdbGroupFilterCondEntity.class);
    }

    @Override
    public void close() {
        clearCacheData();
        if (groupFilterStore != null) {
            try {
                groupFilterStore.close();
                groupFilterStore = null;
            } catch (Throwable e) {
                logger.error("[BDB Impl] close filter configure failure ", e);
            }
        }
    }

    @Override
    public void loadConfig() throws LoadMetaException {
        long count = 0L;
        EntityCursor<BdbGroupFilterCondEntity> cursor = null;
        logger.info("[BDB Impl] load filter configure start...");
        try {
            clearCacheData();
            cursor = groupFilterIndex.entities();
            for (BdbGroupFilterCondEntity bdbEntity : cursor) {
                if (bdbEntity == null) {
                    logger.warn("[BDB Impl] found Null data while loading filter configure!");
                    continue;
                }
                GroupFilterCtrlEntity memEntity =
                        new GroupFilterCtrlEntity(bdbEntity);
                addOrUpdCacheRecord(memEntity);
                count++;
            }
            logger.info("[BDB Impl] total filter configure records are {}", count);
        } catch (Exception e) {
            logger.error("[BDB Impl] load filter configure failure ", e);
            throw new LoadMetaException(e.getMessage());
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }
        logger.info("[BDB Impl] load filter configure successfully...");
    }

    @Override
    public boolean addGroupFilterCtrlConf(GroupFilterCtrlEntity memEntity, ProcessResult result) {
        GroupFilterCtrlEntity curEntity =
                groupFilterCtrlCache.get(memEntity.getRecordKey());
        if (curEntity != null) {
            result.setFailResult(DataOpErrCode.DERR_EXISTED.getCode(),
                    new StringBuilder(TBaseConstants.BUILDER_DEFAULT_SIZE)
                            .append("The group filter ").append(memEntity.getRecordKey())
                            .append("'s configure already exists, please delete it first!")
                            .toString());
            return result.isSuccess();
        }
        if (putGroupFilterCtrlConfig2Bdb(memEntity, result)) {
            addOrUpdCacheRecord(memEntity);
        }
        return result.isSuccess();
    }

    @Override
    public boolean updGroupFilterCtrlConf(GroupFilterCtrlEntity memEntity, ProcessResult result) {
        GroupFilterCtrlEntity curEntity =
                groupFilterCtrlCache.get(memEntity.getRecordKey());
        if (curEntity == null) {
            result.setFailResult(DataOpErrCode.DERR_NOT_EXIST.getCode(),
                    new StringBuilder(TBaseConstants.BUILDER_DEFAULT_SIZE)
                            .append("The group filter ").append(memEntity.getRecordKey())
                            .append("'s configure is not exists, please add record first!")
                            .toString());
            return result.isSuccess();
        }
        if (curEntity.equals(memEntity)) {
            result.setFailResult(DataOpErrCode.DERR_UNCHANGED.getCode(),
                    new StringBuilder(TBaseConstants.BUILDER_DEFAULT_SIZE)
                            .append("The group filter ").append(memEntity.getRecordKey())
                            .append("'s configure have not changed, please delete it first!")
                            .toString());
            return result.isSuccess();
        }
        if (putGroupFilterCtrlConfig2Bdb(memEntity, result)) {
            addOrUpdCacheRecord(memEntity);
        }
        return result.isSuccess();
    }

    @Override
    public boolean delGroupFilterCtrlConf(String recordKey) {
        GroupFilterCtrlEntity curEntity =
                groupFilterCtrlCache.get(recordKey);
        if (curEntity == null) {
            return true;
        }
        delGroupFilterCtrlConfigFromBdb(recordKey);
        delCacheRecord(recordKey);
        return true;
    }

    @Override
    public List<GroupFilterCtrlEntity> getGroupFilterCtrlConf(String groupName) {
        ConcurrentHashSet<String> keySet =
                groupFilterCtrlGroupCache.get(groupName);
        if (keySet == null || keySet.isEmpty()) {
            return Collections.emptyList();
        }
        GroupFilterCtrlEntity entity;
        List<GroupFilterCtrlEntity> result = new ArrayList<>();
        for (String recordKey : keySet) {
            entity = groupFilterCtrlCache.get(recordKey);
            if (entity != null) {
                result.add(entity);
            }
        }
        return result;
    }

    @Override
    public List<GroupFilterCtrlEntity> getGroupFilterCtrlConf(GroupFilterCtrlEntity qryEntity) {
        List<GroupFilterCtrlEntity> retEntitys = new ArrayList<>();
        if (qryEntity == null) {
            retEntitys.addAll(groupFilterCtrlCache.values());
        } else {
            for (GroupFilterCtrlEntity entity : groupFilterCtrlCache.values()) {
                if (entity.isMatched(qryEntity)) {
                    retEntitys.add(entity);
                }
            }
        }
        return retEntitys;
    }

    /**
     * Put Group filter configure info into bdb store
     *
     * @param memEntity need add record
     * @param result process result with old value
     * @return
     */
    private boolean putGroupFilterCtrlConfig2Bdb(GroupFilterCtrlEntity memEntity, ProcessResult result) {
        BdbGroupFilterCondEntity retData = null;
        BdbGroupFilterCondEntity bdbEntity =
                memEntity.buildBdbGroupFilterCondEntity();
        try {
            retData = groupFilterIndex.put(bdbEntity);
        } catch (Throwable e) {
            logger.error("[BDB Impl] put filter configure failure ", e);
            result.setFailResult(DataOpErrCode.DERR_STORE_ABNORMAL.getCode(),
                    new StringBuilder(TBaseConstants.BUILDER_DEFAULT_SIZE)
                            .append("Put filter configure failure: ")
                            .append(e.getMessage()).toString());
            return result.isSuccess();
        }
        result.setSuccResult(retData == null);
        return result.isSuccess();
    }

    private boolean delGroupFilterCtrlConfigFromBdb(String recordKey) {
        try {
            groupFilterIndex.delete(recordKey);
        } catch (Throwable e) {
            logger.error("[BDB Impl] delete filter configure failure ", e);
            return false;
        }
        return true;
    }

    private void delCacheRecord(String recordKey) {
        GroupFilterCtrlEntity curEntity = groupFilterCtrlCache.get(recordKey);
        if (curEntity == null) {
            return;
        }
        // add topic index
        ConcurrentHashSet<String> keySet =
                groupFilterCtrlTopicCache.get(curEntity.getTopicName());
        if (keySet != null) {
            keySet.remove(recordKey);
            if (keySet.isEmpty()) {
                groupFilterCtrlTopicCache.remove(curEntity.getTopicName());
            }
        }
        // delete group index
        keySet = groupFilterCtrlGroupCache.remove(curEntity.getGroupName());
        if (keySet != null) {
            keySet.remove(recordKey);
            if (keySet.isEmpty()) {
                groupFilterCtrlGroupCache.remove(curEntity.getGroupName());
            }
        }
    }

    private void addOrUpdCacheRecord(GroupFilterCtrlEntity entity) {
        groupFilterCtrlCache.put(entity.getRecordKey(), entity);
        // add topic index map
        ConcurrentHashSet<String> keySet =
                groupFilterCtrlTopicCache.get(entity.getTopicName());
        if (keySet == null) {
            ConcurrentHashSet<String> tmpSet = new ConcurrentHashSet<>();
            keySet = groupFilterCtrlTopicCache.putIfAbsent(entity.getTopicName(), tmpSet);
            if (keySet == null) {
                keySet = tmpSet;
            }
        }
        keySet.add(entity.getRecordKey());
        // add group index map
        keySet = groupFilterCtrlGroupCache.get(entity.getGroupName());
        if (keySet == null) {
            ConcurrentHashSet<String> tmpSet = new ConcurrentHashSet<>();
            keySet = groupFilterCtrlGroupCache.putIfAbsent(entity.getGroupName(), tmpSet);
            if (keySet == null) {
                keySet = tmpSet;
            }
        }
        keySet.add(entity.getRecordKey());
    }

    private void clearCacheData() {
        groupFilterCtrlTopicCache.clear();
        groupFilterCtrlGroupCache.clear();
        groupFilterCtrlCache.clear();
    }
}
