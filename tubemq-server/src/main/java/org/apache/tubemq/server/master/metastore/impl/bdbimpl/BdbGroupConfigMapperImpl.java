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
import java.util.HashMap;
import java.util.Map;
import org.apache.tubemq.corebase.TBaseConstants;
import org.apache.tubemq.server.common.exception.LoadMetaException;
import org.apache.tubemq.server.common.utils.ProcessResult;
import org.apache.tubemq.server.master.bdbstore.bdbentitys.BdbGroupFlowCtrlEntity;
import org.apache.tubemq.server.master.metastore.dao.entity.GroupConfigEntity;
import org.apache.tubemq.server.master.metastore.dao.mapper.GroupConfigMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public class BdbGroupConfigMapperImpl implements GroupConfigMapper {


    private static final Logger logger =
            LoggerFactory.getLogger(BdbGroupConfigMapperImpl.class);


    // consumer group configure store
    private EntityStore groupConfStore;
    private PrimaryIndex<String/* groupName */, BdbGroupFlowCtrlEntity> groupConfIndex;


    public BdbGroupConfigMapperImpl(ReplicatedEnvironment repEnv, StoreConfig storeConfig) {
        groupConfStore = new EntityStore(repEnv,
                TBDBStoreTables.BDB_GROUP_FLOW_CONTROL_STORE_NAME, storeConfig);
        groupConfIndex =
                groupConfStore.getPrimaryIndex(String.class, BdbGroupFlowCtrlEntity.class);
    }

    @Override
    public void close() {
        if (groupConfStore != null) {
            try {
                groupConfStore.close();
                groupConfStore = null;
            } catch (Throwable e) {
                logger.error("[BDB Impl] close group configure failure ", e);
            }
        }
    }

    @Override
    public void loadConfig(ProcessResult result) throws LoadMetaException {
        long count = 0L;
        Map<String, GroupConfigEntity> metaDataMap = new HashMap<>();
        EntityCursor<BdbGroupFlowCtrlEntity> cursor = null;
        logger.info("[BDB Impl] load group configure start...");
        try {
            cursor = groupConfIndex.entities();
            for (BdbGroupFlowCtrlEntity bdbEntity : cursor) {
                if (bdbEntity == null) {
                    logger.warn("[BDB Impl] found Null data while loading group configure!");
                    continue;
                }
                GroupConfigEntity memEntity =
                        new GroupConfigEntity(bdbEntity);
                metaDataMap.put(memEntity.getGroupName(), memEntity);
                count++;
            }
            logger.info("[BDB Impl] total group configure records are {}", count);
            result.setSuccResult(metaDataMap);
        } catch (Exception e) {
            logger.error("[BDB Impl] load group configure failure ", e);
            throw new LoadMetaException(e.getMessage());
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }
        logger.info("[BDB Impl] load group configure successfully...");
    }

    /**
     * Put Group configure info into bdb store
     *
     * @param memEntity need add record
     * @param result process result with old value
     * @return
     */
    @Override
    public boolean putGroupConfigConfig(GroupConfigEntity memEntity, ProcessResult result) {
        BdbGroupFlowCtrlEntity retData = null;
        BdbGroupFlowCtrlEntity bdbEntity =
                memEntity.buildBdbGroupFlowCtrlEntity();
        try {
            retData = groupConfIndex.put(bdbEntity);
        } catch (Throwable e) {
            logger.error("[BDB Impl] put group configure failure ", e);
            result.setFailResult(new StringBuilder(TBaseConstants.BUILDER_DEFAULT_SIZE)
                    .append("Put group configure failure: ")
                    .append(e.getMessage()).toString());
            return result.isSuccess();
        }
        result.setSuccResult(retData == null);
        return result.isSuccess();
    }

    @Override
    public boolean delGroupConfigConfig(String recordKey) {
        try {
            groupConfIndex.delete(recordKey);
        } catch (Throwable e) {
            logger.error("[BDB Impl] delete group configure failure ", e);
            return false;
        }
        return true;
    }

}
