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
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.inlong.tubemq.corebase.TBaseConstants;
import org.apache.inlong.tubemq.server.common.exception.LoadMetaException;
import org.apache.inlong.tubemq.server.common.utils.ProcessResult;
import org.apache.inlong.tubemq.server.master.bdbstore.bdbentitys.BdbGroupFlowCtrlEntity;
import org.apache.inlong.tubemq.server.master.metamanage.DataOpErrCode;
import org.apache.inlong.tubemq.server.master.metamanage.metastore.dao.entity.GroupResCtrlEntity;
import org.apache.inlong.tubemq.server.master.metamanage.metastore.dao.mapper.GroupResCtrlMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BdbGroupResCtrlMapperImpl implements GroupResCtrlMapper {

    private static final Logger logger =
            LoggerFactory.getLogger(BdbGroupResCtrlMapperImpl.class);
    // consumer group configure store
    private EntityStore groupConfStore;
    private PrimaryIndex<String/* groupName */, BdbGroupFlowCtrlEntity> groupBaseCtrlIndex;
    private ConcurrentHashMap<String/* groupName */, GroupResCtrlEntity> groupBaseCtrlCache =
            new ConcurrentHashMap<>();

    public BdbGroupResCtrlMapperImpl(ReplicatedEnvironment repEnv, StoreConfig storeConfig) {
        groupConfStore = new EntityStore(repEnv,
                TBDBStoreTables.BDB_GROUP_FLOW_CONTROL_STORE_NAME, storeConfig);
        groupBaseCtrlIndex =
                groupConfStore.getPrimaryIndex(String.class, BdbGroupFlowCtrlEntity.class);
    }

    @Override
    public void close() {
        groupBaseCtrlCache.clear();
        if (groupConfStore != null) {
            try {
                groupConfStore.close();
                groupConfStore = null;
            } catch (Throwable e) {
                logger.error("[BDB Impl] close group resource control failure ", e);
            }
        }
    }

    @Override
    public void loadConfig() throws LoadMetaException {
        long count = 0L;
        EntityCursor<BdbGroupFlowCtrlEntity> cursor = null;
        logger.info("[BDB Impl] load group resource control start...");
        try {
            groupBaseCtrlCache.clear();
            cursor = groupBaseCtrlIndex.entities();
            for (BdbGroupFlowCtrlEntity bdbEntity : cursor) {
                if (bdbEntity == null) {
                    logger.warn("[BDB Impl] null data while loading group resource control!");
                    continue;
                }
                GroupResCtrlEntity memEntity =
                        new GroupResCtrlEntity(bdbEntity);
                groupBaseCtrlCache.put(memEntity.getGroupName(), memEntity);
                count++;
            }
            logger.info("[BDB Impl] total group resource control records are {}", count);
        } catch (Exception e) {
            logger.error("[BDB Impl] load group resource control failure ", e);
            throw new LoadMetaException(e.getMessage());
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }
        logger.info("[BDB Impl] load group resource configure successfully...");
    }

    @Override
    public boolean addGroupResCtrlConf(GroupResCtrlEntity memEntity, ProcessResult result) {
        GroupResCtrlEntity curEntity =
                groupBaseCtrlCache.get(memEntity.getGroupName());
        if (curEntity != null) {
            result.setFailResult(DataOpErrCode.DERR_EXISTED.getCode(),
                    new StringBuilder(TBaseConstants.BUILDER_DEFAULT_SIZE)
                            .append("The group ").append(memEntity.getGroupName())
                            .append("'s resource control already exists, please delete it first!")
                            .toString());
            return result.isSuccess();
        }
        if (putGroupConfigConfig2Bdb(memEntity, result)) {
            groupBaseCtrlCache.put(memEntity.getGroupName(), memEntity);
        }
        return result.isSuccess();
    }

    @Override
    public boolean updGroupResCtrlConf(GroupResCtrlEntity memEntity, ProcessResult result) {
        GroupResCtrlEntity curEntity =
                groupBaseCtrlCache.get(memEntity.getGroupName());
        if (curEntity == null) {
            result.setFailResult(DataOpErrCode.DERR_NOT_EXIST.getCode(),
                    new StringBuilder(TBaseConstants.BUILDER_DEFAULT_SIZE)
                            .append("The group ").append(memEntity.getGroupName())
                            .append("'s resource control is not exists, please add record first!")
                            .toString());
            return result.isSuccess();
        }
        if (curEntity.equals(memEntity)) {
            result.setFailResult(DataOpErrCode.DERR_UNCHANGED.getCode(),
                    new StringBuilder(TBaseConstants.BUILDER_DEFAULT_SIZE)
                            .append("The group ").append(memEntity.getGroupName())
                            .append("'s resource control have not changed, please delete it first!")
                            .toString());
            return result.isSuccess();
        }
        if (putGroupConfigConfig2Bdb(memEntity, result)) {
            groupBaseCtrlCache.put(memEntity.getGroupName(), memEntity);
            result.setRetData(curEntity);
        }
        return result.isSuccess();
    }

    @Override
    public boolean delGroupResCtrlConf(String groupName, ProcessResult result) {
        GroupResCtrlEntity curEntity =
                groupBaseCtrlCache.get(groupName);
        if (curEntity == null) {
            result.setSuccResult(null);
            return true;
        }
        delGroupConfigConfigFromBdb(groupName);
        groupBaseCtrlCache.remove(groupName);
        result.setSuccResult(curEntity);
        return true;
    }

    @Override
    public GroupResCtrlEntity getGroupResCtrlConf(String groupName) {
        return groupBaseCtrlCache.get(groupName);
    }

    @Override
    public Map<String, GroupResCtrlEntity> getGroupResCtrlConf(Set<String> groupNameSet,
                                                               GroupResCtrlEntity qryEntry) {
        Map<String, GroupResCtrlEntity> retMap = new HashMap<>();
        if (groupNameSet == null || groupNameSet.isEmpty()) {
            for (GroupResCtrlEntity entry : groupBaseCtrlCache.values()) {
                if (entry == null || (qryEntry != null && !entry.isMatched(qryEntry))) {
                    continue;
                }
                retMap.put(entry.getGroupName(), entry);
            }
        } else {
            GroupResCtrlEntity entry;
            for (String groupName : groupNameSet) {
                entry = groupBaseCtrlCache.get(groupName);
                if (entry == null || (qryEntry != null && !entry.isMatched(qryEntry))) {
                    continue;
                }
                retMap.put(entry.getGroupName(), entry);
            }
        }
        return retMap;
    }

    /**
     * Put Group configure info into bdb store
     *
     * @param memEntity need add record
     * @param result process result with old value
     * @return
     */
    private boolean putGroupConfigConfig2Bdb(GroupResCtrlEntity memEntity, ProcessResult result) {
        BdbGroupFlowCtrlEntity retData = null;
        BdbGroupFlowCtrlEntity bdbEntity =
                memEntity.buildBdbGroupFlowCtrlEntity();
        try {
            retData = groupBaseCtrlIndex.put(bdbEntity);
        } catch (Throwable e) {
            logger.error("[BDB Impl] put group resource control failure ", e);
            result.setFailResult(DataOpErrCode.DERR_STORE_ABNORMAL.getCode(),
                    new StringBuilder(TBaseConstants.BUILDER_DEFAULT_SIZE)
                            .append("Put group resource control failure: ")
                            .append(e.getMessage()).toString());
            return result.isSuccess();
        }
        result.setSuccResult(null);
        return result.isSuccess();
    }

    private boolean delGroupConfigConfigFromBdb(String recordKey) {
        try {
            groupBaseCtrlIndex.delete(recordKey);
        } catch (Throwable e) {
            logger.error("[BDB Impl] delete group resource control failure ", e);
            return false;
        }
        return true;
    }

}
