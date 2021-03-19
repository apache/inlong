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
import org.apache.tubemq.server.master.bdbstore.bdbentitys.BdbClusterSettingEntity;
import org.apache.tubemq.server.master.metastore.dao.entity.ClusterSettingEntity;
import org.apache.tubemq.server.master.metastore.dao.mapper.ClusterConfigMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class BdbClusterConfigMapperImpl implements ClusterConfigMapper {

    private static final Logger logger =
            LoggerFactory.getLogger(BdbClusterConfigMapperImpl.class);

    private EntityStore clsDefSettingStore;
    private PrimaryIndex<String, BdbClusterSettingEntity> clsDefSettingIndex;


    public BdbClusterConfigMapperImpl(ReplicatedEnvironment repEnv, StoreConfig storeConfig) {
        clsDefSettingStore = new EntityStore(repEnv,
                TBDBStoreTables.BDB_CLUSTER_SETTING_STORE_NAME, storeConfig);
        clsDefSettingIndex =
                clsDefSettingStore.getPrimaryIndex(String.class, BdbClusterSettingEntity.class);
    }

    @Override
    public void close() {
        if (clsDefSettingStore != null) {
            try {
                clsDefSettingStore.close();
                clsDefSettingStore = null;
            } catch (Throwable e) {
                logger.error("[BDB Impl] close cluster configure failure ", e);
            }
        }
    }

    @Override
    public void loadConfig(ProcessResult result) throws LoadMetaException {
        long count = 0L;
        Map<String, ClusterSettingEntity> metaDataMap = new HashMap<>();
        EntityCursor<BdbClusterSettingEntity> cursor = null;
        logger.info("[BDB Impl] load cluster configure start...");
        try {
            cursor = clsDefSettingIndex.entities();
            for (BdbClusterSettingEntity bdbEntity : cursor) {
                if (bdbEntity == null) {
                    logger.warn("[BDB Impl] found Null data while loading cluster configure!");
                    continue;
                }
                ClusterSettingEntity memEntity =
                        new ClusterSettingEntity(bdbEntity);
                metaDataMap.put(memEntity.getRecordKey(), memEntity);
                count++;
            }
            logger.info("[BDB Impl] total cluster configure records are {}", count);
            result.setSuccResult(metaDataMap);
        } catch (Exception e) {
            logger.error("[BDB Impl] load cluster configure failure ", e);
            throw new LoadMetaException(e.getMessage());
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }
        logger.info("[BDB Impl] load cluster configure successfully...");
    }

    /**
     * Put cluster setting info into bdb store
     *
     * @param memEntity need add record
     * @param result process result with old value
     * @return
     */
    @Override
    public boolean putClusterConfig(ClusterSettingEntity memEntity, ProcessResult result) {
        BdbClusterSettingEntity retData = null;
        BdbClusterSettingEntity bdbEntity =
                memEntity.buildBdbClsDefSettingEntity();
        try {
            retData = clsDefSettingIndex.put(bdbEntity);
        } catch (Throwable e) {
            logger.error("[BDB Impl] put cluster configure failure ", e);
            result.setFailResult(new StringBuilder(TBaseConstants.BUILDER_DEFAULT_SIZE)
                    .append("Put cluster configure failure: ")
                    .append(e.getMessage()).toString());
            return result.isSuccess();
        }
        result.setSuccResult(retData == null);
        return result.isSuccess();
    }

    @Override
    public boolean delClusterConfig(String key) {
        try {
            clsDefSettingIndex.delete(key);
        } catch (Throwable e) {
            logger.error("[BDB Impl] delete cluster configure failure ", e);
            return false;
        }
        return true;
    }

}
