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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.inlong.tubemq.corebase.rv.ProcessResult;
import org.apache.inlong.tubemq.server.master.metamanage.DataOpErrCode;
import org.apache.inlong.tubemq.server.master.metamanage.metastore.TStoreConstants;
import org.apache.inlong.tubemq.server.master.metamanage.metastore.dao.entity.ClusterSettingEntity;
import org.apache.inlong.tubemq.server.master.metamanage.metastore.dao.mapper.ClusterConfigMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbsClusterConfigMapperImpl implements ClusterConfigMapper {
    protected static final Logger logger =
            LoggerFactory.getLogger(AbsClusterConfigMapperImpl.class);
    // data cache
    private final Map<String, ClusterSettingEntity> metaDataCache = new ConcurrentHashMap<>();

    public AbsClusterConfigMapperImpl() {
        // initial instance
    }

    @Override
    public boolean addClusterConfig(ClusterSettingEntity entity,
                                    StringBuilder strBuff, ProcessResult result) {
        if (!metaDataCache.isEmpty()) {
            result.setFailResult(DataOpErrCode.DERR_EXISTED.getCode(),
                    "The cluster configure already exists, please delete or update it first!");
            return result.isSuccess();
        }
        if (putConfig2Persistent(entity, strBuff, result)) {
            metaDataCache.put(entity.getRecordKey(), entity);
        }
        return result.isSuccess();
    }

    @Override
    public boolean updClusterConfig(ClusterSettingEntity entity,
                                    StringBuilder strBuff, ProcessResult result) {
        if (metaDataCache.isEmpty()) {
            result.setFailResult(DataOpErrCode.DERR_NOT_EXIST.getCode(),
                    "The cluster configure is null, please add it first!");
            return result.isSuccess();
        }
        ClusterSettingEntity curEntity = metaDataCache.get(entity.getRecordKey());
        if (curEntity.equals(entity)) {
            result.setFailResult(DataOpErrCode.DERR_UNCHANGED.getCode(),
                    "The cluster configure have not changed!");
            return result.isSuccess();
        }
        if (putConfig2Persistent(entity, strBuff, result)) {
            metaDataCache.put(entity.getRecordKey(), entity);
            result.setSuccResult(curEntity);
        }
        return result.isSuccess();
    }

    @Override
    public ClusterSettingEntity getClusterConfig() {
        return metaDataCache.get(TStoreConstants.TOKEN_DEFAULT_CLUSTER_SETTING);
    }

    @Override
    public boolean delClusterConfig(StringBuilder strBuff, ProcessResult result) {
        ClusterSettingEntity curEntity =
                metaDataCache.get(TStoreConstants.TOKEN_DEFAULT_CLUSTER_SETTING);
        if (curEntity == null) {
            result.setSuccResult(null);
            return true;
        }
        delConfigFromPersistent(strBuff, TStoreConstants.TOKEN_DEFAULT_CLUSTER_SETTING);
        metaDataCache.remove(TStoreConstants.TOKEN_DEFAULT_CLUSTER_SETTING);
        result.setSuccResult(curEntity);
        return true;
    }

    /**
     * Clear cached data
     */
    protected void clearCachedData() {
        metaDataCache.clear();
    }

    /**
     * Add or update a record
     *
     * @param entity  need added or updated entity
     */
    protected void addOrUpdCacheRecord(ClusterSettingEntity entity) {
        metaDataCache.put(entity.getRecordKey(), entity);
    }

    /**
     * Put cluster configure information into persistent storage
     *
     * @param entity   need add record
     * @param strBuff  the string buffer
     * @param result process result with old value
     * @return the process result
     */
    protected abstract boolean putConfig2Persistent(ClusterSettingEntity entity,
                                                    StringBuilder strBuff, ProcessResult result);

    /**
     * Delete cluster configure information from persistent storage
     *
     * @param key      the record key
     * @param strBuff  the string buffer
     * @return the process result
     */
    protected abstract boolean delConfigFromPersistent(StringBuilder strBuff, String key);
}
