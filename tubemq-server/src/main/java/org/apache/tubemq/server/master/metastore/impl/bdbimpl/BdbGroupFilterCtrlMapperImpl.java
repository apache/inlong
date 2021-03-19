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
import org.apache.tubemq.server.master.bdbstore.bdbentitys.BdbGroupFilterCondEntity;
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


    public BdbGroupFilterCtrlMapperImpl(ReplicatedEnvironment repEnv, StoreConfig storeConfig) {
        groupFilterStore = new EntityStore(repEnv,
                TBDBStoreTables.BDB_GROUP_FILTER_COND_STORE_NAME, storeConfig);
        groupFilterIndex =
                groupFilterStore.getPrimaryIndex(String.class, BdbGroupFilterCondEntity.class);
    }

    @Override
    public void close() {
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
    public void loadConfig(ProcessResult result) throws LoadMetaException {
        long count = 0L;
        Map<String, GroupFilterCtrlEntity> metaDataMap = new HashMap<>();
        EntityCursor<BdbGroupFilterCondEntity> cursor = null;
        logger.info("[BDB Impl] load filter configure start...");
        try {
            cursor = groupFilterIndex.entities();
            for (BdbGroupFilterCondEntity bdbEntity : cursor) {
                if (bdbEntity == null) {
                    logger.warn("[BDB Impl] found Null data while loading filter configure!");
                    continue;
                }
                GroupFilterCtrlEntity memEntity =
                        new GroupFilterCtrlEntity(bdbEntity);
                metaDataMap.put(memEntity.getRecordKey(), memEntity);
                count++;
            }
            logger.info("[BDB Impl] total filter configure records are {}", count);
            result.setSuccResult(metaDataMap);
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

    /**
     * Put Group filter configure info into bdb store
     *
     * @param memEntity need add record
     * @param result process result with old value
     * @return
     */
    @Override
    public boolean putGroupFilterCtrlConfig(GroupFilterCtrlEntity memEntity, ProcessResult result) {
        BdbGroupFilterCondEntity retData = null;
        BdbGroupFilterCondEntity bdbEntity =
                memEntity.buildBdbGroupFilterCondEntity();
        try {
            retData = groupFilterIndex.put(bdbEntity);
        } catch (Throwable e) {
            logger.error("[BDB Impl] put filter configure failure ", e);
            result.setFailResult(new StringBuilder(TBaseConstants.BUILDER_DEFAULT_SIZE)
                    .append("Put filter configure failure: ")
                    .append(e.getMessage()).toString());
            return result.isSuccess();
        }
        result.setSuccResult(retData == null);
        return result.isSuccess();
    }

    @Override
    public boolean delGroupFilterCtrlConfig(String recordKey) {
        try {
            groupFilterIndex.delete(recordKey);
        } catch (Throwable e) {
            logger.error("[BDB Impl] delete filter configure failure ", e);
            return false;
        }
        return true;
    }

}
