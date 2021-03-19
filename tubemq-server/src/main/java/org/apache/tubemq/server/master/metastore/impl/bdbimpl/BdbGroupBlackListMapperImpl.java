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
import org.apache.tubemq.server.master.bdbstore.bdbentitys.BdbBlackGroupEntity;
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


    public BdbGroupBlackListMapperImpl(ReplicatedEnvironment repEnv, StoreConfig storeConfig) {
        blackGroupStore = new EntityStore(repEnv,
                TBDBStoreTables.BDB_BLACK_GROUP_STORE_NAME, storeConfig);
        blackGroupIndex =
                blackGroupStore.getPrimaryIndex(String.class, BdbBlackGroupEntity.class);
    }

    @Override
    public void close() {
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
    public void loadConfig(ProcessResult result) throws LoadMetaException {
        long count = 0L;
        Map<String, GroupBlackListEntity> metaDataMap = new HashMap<>();
        EntityCursor<BdbBlackGroupEntity> cursor = null;
        logger.info("[BDB Impl] load blacklist configure start...");
        try {
            cursor = blackGroupIndex.entities();
            for (BdbBlackGroupEntity bdbEntity : cursor) {
                if (bdbEntity == null) {
                    logger.warn("[BDB Impl] found Null data while loading blacklist configure!");
                    continue;
                }
                GroupBlackListEntity memEntity =
                        new GroupBlackListEntity(bdbEntity);
                metaDataMap.put(memEntity.getRecordKey(), memEntity);
                count++;
            }
            logger.info("[BDB Impl] total blacklist configure records are {}", count);
            result.setSuccResult(metaDataMap);
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

    /**
     * Put blacklist configure info into bdb store
     *
     * @param memEntity need add record
     * @param result process result with old value
     * @return
     */
    @Override
    public boolean putGroupBlackListConfig(GroupBlackListEntity memEntity, ProcessResult result) {
        BdbBlackGroupEntity retData = null;
        BdbBlackGroupEntity bdbEntity =
                memEntity.buildBdbBlackListEntity();
        try {
            retData = blackGroupIndex.put(bdbEntity);
        } catch (Throwable e) {
            logger.error("[BDB Impl] put blacklist configure failure ", e);
            result.setFailResult(new StringBuilder(TBaseConstants.BUILDER_DEFAULT_SIZE)
                    .append("Put blacklist configure failure: ")
                    .append(e.getMessage()).toString());
            return result.isSuccess();
        }
        result.setSuccResult(retData == null);
        return result.isSuccess();
    }

    @Override
    public boolean delGroupBlackListConfig(String recordKey) {
        try {
            blackGroupIndex.delete(recordKey);
        } catch (Throwable e) {
            logger.error("[BDB Impl] delete blacklist configure failure ", e);
            return false;
        }
        return true;
    }

}
