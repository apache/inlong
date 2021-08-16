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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.inlong.tubemq.corebase.TBaseConstants;
import org.apache.inlong.tubemq.server.common.exception.LoadMetaException;
import org.apache.inlong.tubemq.server.common.utils.ProcessResult;
import org.apache.inlong.tubemq.server.master.bdbstore.bdbentitys.BdbClusterSettingEntity;
import org.apache.inlong.tubemq.server.master.metamanage.DataOpErrCode;
import org.apache.inlong.tubemq.server.master.metamanage.metastore.TStoreConstants;
import org.apache.inlong.tubemq.server.master.metamanage.metastore.dao.entity.ClusterSettingEntity;
import org.apache.inlong.tubemq.server.master.metamanage.metastore.dao.mapper.ClusterConfigMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class BdbClusterConfigMapperImpl implements ClusterConfigMapper {

    private static final Logger logger =
            LoggerFactory.getLogger(BdbClusterConfigMapperImpl.class);

    private EntityStore clsDefSettingStore;
    private PrimaryIndex<String, BdbClusterSettingEntity> clsDefSettingIndex;
    Map<String, ClusterSettingEntity> metaDataCache = new ConcurrentHashMap<>();

    public BdbClusterConfigMapperImpl(ReplicatedEnvironment repEnv, StoreConfig storeConfig) {
        clsDefSettingStore = new EntityStore(repEnv,
                TBDBStoreTables.BDB_CLUSTER_SETTING_STORE_NAME, storeConfig);
        clsDefSettingIndex =
                clsDefSettingStore.getPrimaryIndex(String.class, BdbClusterSettingEntity.class);
    }

    @Override
    public void close() {
        metaDataCache.clear();
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
    public void loadConfig() throws LoadMetaException {
        long count = 0L;
        EntityCursor<BdbClusterSettingEntity> cursor = null;
        logger.info("[BDB Impl] load cluster configure start...");
        try {
            metaDataCache.clear();
            cursor = clsDefSettingIndex.entities();
            for (BdbClusterSettingEntity bdbEntity : cursor) {
                if (bdbEntity == null) {
                    logger.warn("[BDB Impl] found Null data while loading cluster configure!");
                    continue;
                }
                ClusterSettingEntity memEntity =
                        new ClusterSettingEntity(bdbEntity);
                metaDataCache.put(memEntity.getRecordKey(), memEntity);
                count++;
            }
            logger.info("[BDB Impl] total cluster configure records are {}", count);
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
    public boolean addClusterConfig(ClusterSettingEntity memEntity, ProcessResult result) {
        if (!metaDataCache.isEmpty()) {
            result.setFailResult(DataOpErrCode.DERR_EXISTED.getCode(),
                    "The cluster setting already exists, please delete or update!");
            return result.isSuccess();
        }
        if (putClusterConfig2Bdb(memEntity, result)) {
            metaDataCache.put(memEntity.getRecordKey(), memEntity);
        }
        return result.isSuccess();
    }

    /**
     * Update cluster setting info in bdb store
     *
     * @param memEntity need add record
     * @param result process result with old value
     * @return
     */
    @Override
    public boolean updClusterConfig(ClusterSettingEntity memEntity, ProcessResult result) {
        if (metaDataCache.isEmpty()) {
            result.setFailResult(DataOpErrCode.DERR_NOT_EXIST.getCode(),
                    "The cluster setting is null, please add record first!");
            return result.isSuccess();
        }
        ClusterSettingEntity curEntity = metaDataCache.get(memEntity.getRecordKey());
        if (curEntity.equals(memEntity)) {
            result.setFailResult(DataOpErrCode.DERR_UNCHANGED.getCode(),
                    "The cluster settings have not changed!");
            return result.isSuccess();
        }
        if (putClusterConfig2Bdb(memEntity, result)) {
            metaDataCache.put(memEntity.getRecordKey(), memEntity);
            result.setRetData(curEntity);
        }
        return result.isSuccess();
    }

    /**
     * get current cluster setting from bdb store
     * @return current cluster setting, null or object, only read
     */
    @Override
    public ClusterSettingEntity getClusterConfig() {
        return metaDataCache.get(TStoreConstants.TOKEN_DEFAULT_CLUSTER_SETTING);
    }

    /**
     * delete current cluster setting from bdb store
     * @return if success
     */
    @Override
    public boolean delClusterConfig(ProcessResult result) {
        ClusterSettingEntity curEntity =
                metaDataCache.get(TStoreConstants.TOKEN_DEFAULT_CLUSTER_SETTING);
        if (curEntity == null) {
            result.setSuccResult(null);
            return true;
        }
        delClusterConfigFromBdb(TStoreConstants.TOKEN_DEFAULT_CLUSTER_SETTING);
        metaDataCache.remove(TStoreConstants.TOKEN_DEFAULT_CLUSTER_SETTING);
        result.setSuccResult(curEntity);
        return true;
    }

    private boolean putClusterConfig2Bdb(ClusterSettingEntity memEntity, ProcessResult result) {
        BdbClusterSettingEntity bdbEntity =
                memEntity.buildBdbClsDefSettingEntity();
        try {
            clsDefSettingIndex.put(bdbEntity);
        } catch (Throwable e) {
            logger.error("[BDB Impl] put cluster configure failure ", e);
            result.setFailResult(DataOpErrCode.DERR_STORE_ABNORMAL.getCode(),
                    new StringBuilder(TBaseConstants.BUILDER_DEFAULT_SIZE)
                            .append("Put cluster configure failure: ")
                            .append(e.getMessage()).toString());
            return result.isSuccess();
        }
        result.setSuccResult(null);
        return result.isSuccess();
    }

    private boolean delClusterConfigFromBdb(String key) {
        try {
            clsDefSettingIndex.delete(key);
        } catch (Throwable e) {
            logger.error("[BDB Impl] delete cluster configure failure ", e);
            return false;
        }
        return true;
    }

}
