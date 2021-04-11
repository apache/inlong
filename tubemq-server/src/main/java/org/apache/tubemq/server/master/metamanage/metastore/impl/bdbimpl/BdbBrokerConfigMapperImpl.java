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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.tubemq.corebase.TBaseConstants;
import org.apache.tubemq.server.common.exception.LoadMetaException;
import org.apache.tubemq.server.common.utils.ProcessResult;
import org.apache.tubemq.server.master.bdbstore.bdbentitys.BdbBrokerConfEntity;
import org.apache.tubemq.server.master.metamanage.DataOpErrCode;
import org.apache.tubemq.server.master.metamanage.metastore.dao.entity.BrokerConfEntity;
import org.apache.tubemq.server.master.metamanage.metastore.dao.mapper.BrokerConfigMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public class BdbBrokerConfigMapperImpl implements BrokerConfigMapper {

    private static final Logger logger =
            LoggerFactory.getLogger(BdbBrokerConfigMapperImpl.class);

    // broker config store
    private EntityStore brokerConfStore;
    private PrimaryIndex<Integer/* brokerId */, BdbBrokerConfEntity> brokerConfIndex;
    private ConcurrentHashMap<Integer/* brokerId */, BrokerConfEntity> brokerConfCache =
            new ConcurrentHashMap<>();
    private ConcurrentHashMap<String/* brokerIP */, Integer/* brokerId */> brokerIpIndexCache =
            new ConcurrentHashMap<>();

    public BdbBrokerConfigMapperImpl(ReplicatedEnvironment repEnv, StoreConfig storeConfig) {
        brokerConfStore = new EntityStore(repEnv,
                TBDBStoreTables.BDB_BROKER_CONFIG_STORE_NAME, storeConfig);
        brokerConfIndex =
                brokerConfStore.getPrimaryIndex(Integer.class, BdbBrokerConfEntity.class);
    }

    @Override
    public void close() {
        clearCacheData();
        if (brokerConfStore != null) {
            try {
                brokerConfStore.close();
                brokerConfStore = null;
            } catch (Throwable e) {
                logger.error("[BDB Impl] close broker configure failure ", e);
            }
        }
    }

    @Override
    public void loadConfig() throws LoadMetaException {
        long count = 0L;
        EntityCursor<BdbBrokerConfEntity> cursor = null;
        logger.info("[BDB Impl] load broker configure start...");
        try {
            clearCacheData();
            cursor = brokerConfIndex.entities();
            for (BdbBrokerConfEntity bdbEntity : cursor) {
                if (bdbEntity == null) {
                    logger.warn("[BDB Impl] found Null data while loading broker configure!");
                    continue;
                }
                BrokerConfEntity memEntity =
                        new BrokerConfEntity(bdbEntity);
                addOrUpdCacheRecord(memEntity);
                count++;
            }
            logger.info("[BDB Impl] total broker configure records are {}", count);
        } catch (Exception e) {
            logger.error("[BDB Impl] load broker configure failure ", e);
            throw new LoadMetaException(e.getMessage());
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }
        logger.info("[BDB Impl] load broker configure successfully...");
    }

    @Override
    public boolean addBrokerConf(BrokerConfEntity memEntity, ProcessResult result) {
        BrokerConfEntity curEntity =
                brokerConfCache.get(memEntity.getBrokerId());
        if (curEntity != null) {
            result.setFailResult(DataOpErrCode.DERR_EXISTED.getCode(),
                    new StringBuilder(TBaseConstants.BUILDER_DEFAULT_SIZE)
                            .append("The broker's brokerId ").append(memEntity.getBrokerId())
                            .append(" has already exists, the value must be unique!")
                            .toString());
            return result.isSuccess();
        }
        Integer curBrokerId = brokerIpIndexCache.get(memEntity.getBrokerIp());
        if (curBrokerId != null) {
            result.setFailResult(DataOpErrCode.DERR_EXISTED.getCode(),
                    new StringBuilder(TBaseConstants.BUILDER_DEFAULT_SIZE)
                            .append("The broker's brokerIp ").append(memEntity.getBrokerIp())
                            .append(" has already exists, the value must be unique!")
                            .toString());
            return result.isSuccess();
        }
        if (putBrokerConfig2Bdb(memEntity, result)) {
            addOrUpdCacheRecord(memEntity);
        }
        return result.isSuccess();
    }

    @Override
    public boolean updBrokerConf(BrokerConfEntity memEntity, ProcessResult result) {
        BrokerConfEntity curEntity =
                brokerConfCache.get(memEntity.getBrokerId());
        if (curEntity == null) {
            result.setFailResult(DataOpErrCode.DERR_NOT_EXIST.getCode(),
                    new StringBuilder(TBaseConstants.BUILDER_DEFAULT_SIZE)
                            .append("The broker ").append(memEntity.getBrokerIp())
                            .append("'s configure is not exists, please add record first!")
                            .toString());
            return result.isSuccess();
        }
        if (curEntity.equals(memEntity)) {
            result.setFailResult(DataOpErrCode.DERR_UNCHANGED.getCode(),
                    new StringBuilder(TBaseConstants.BUILDER_DEFAULT_SIZE)
                            .append("The broker ").append(memEntity.getBrokerIp())
                            .append("'s configure have not changed, please delete it first!")
                            .toString());
            return result.isSuccess();
        }
        if (putBrokerConfig2Bdb(memEntity, result)) {
            addOrUpdCacheRecord(memEntity);
            result.setRetData(curEntity);
        }
        return result.isSuccess();
    }

    /**
     * delete broker configure info from bdb store
     * @return
     */
    @Override
    public boolean delBrokerConf(int brokerId, ProcessResult result) {
        BrokerConfEntity curEntity =
                brokerConfCache.get(brokerId);
        if (curEntity == null) {
            result.setSuccResult(null);
            return result.isSuccess();
        }
        delBrokerConfigFromBdb(brokerId);
        delCacheRecord(brokerId);
        result.setSuccResult(curEntity);
        return result.isSuccess();
    }

    /**
     * get broker configure info from bdb store
     * @return result, only read
     */
    @Override
    public Map<Integer, BrokerConfEntity> getBrokerConfInfo(BrokerConfEntity qryEntity) {
        Map<Integer, BrokerConfEntity> retMap = new HashMap<>();
        if (qryEntity == null) {
            for (BrokerConfEntity entity : brokerConfCache.values()) {
                retMap.put(entity.getBrokerId(), entity);
            }
        } else {
            for (BrokerConfEntity entity : brokerConfCache.values()) {
                if (entity.isMatched(qryEntity)) {
                    retMap.put(entity.getBrokerId(), entity);
                }
            }
        }
        return retMap;
    }

    /**
     * get broker configure info from bdb store
     * @return result, only read
     */
    @Override
    public Map<Integer, BrokerConfEntity> getBrokerConfInfo(Set<Integer> brokerIdSet,
                                                            Set<String> brokerIpSet,
                                                            BrokerConfEntity qryEntity) {
        Set<Integer> qryBrokerKey = null;
        Map<Integer, BrokerConfEntity> retMap = new HashMap<>();
        if (brokerIdSet != null && !brokerIdSet.isEmpty()) {
            qryBrokerKey = new HashSet<>(brokerIdSet);
        }
        if (brokerIpSet != null && !brokerIpSet.isEmpty()) {
            if (qryBrokerKey == null) {
                qryBrokerKey = new HashSet<>();
            }
            for (String brokerIp : brokerIpSet) {
                Integer brokerId = brokerIpIndexCache.get(brokerIp);
                if (brokerId != null) {
                    qryBrokerKey.add(brokerId);
                }
            }
        }
        if (qryBrokerKey == null) {
            qryBrokerKey = new HashSet<>(brokerConfCache.keySet());
        }
        if (qryBrokerKey.isEmpty()) {
            return retMap;
        }
        for (Integer brokerId : qryBrokerKey) {
            BrokerConfEntity entity = brokerConfCache.get(brokerId);
            if (entity == null
                    || (qryEntity != null && !qryEntity.isMatched(entity))) {
                continue;
            }
            retMap.put(entity.getBrokerId(), entity);
        }
        return retMap;
    }



    /**
     * get broker configure info from bdb store
     * @return result, only read
     */
    @Override
    public BrokerConfEntity getBrokerConfByBrokerId(int brokerId) {
        return brokerConfCache.get(brokerId);
    }

    /**
     * get broker configure info from bdb store
     * @return result, only read
     */
    @Override
    public BrokerConfEntity getBrokerConfByBrokerIp(String brokerIp) {
        Integer brokerId = brokerIpIndexCache.get(brokerIp);
        if (brokerId == null) {
            return null;
        }
        return brokerConfCache.get(brokerId);
    }

    /**
     * Put cluster setting info into bdb store
     *
     * @param memEntity need add record
     * @param result process result with old value
     * @return
     */
    private boolean putBrokerConfig2Bdb(BrokerConfEntity memEntity, ProcessResult result) {
        BdbBrokerConfEntity retData = null;
        BdbBrokerConfEntity bdbEntity =
                memEntity.buildBdbBrokerConfEntity();
        try {
            retData = brokerConfIndex.put(bdbEntity);
        } catch (Throwable e) {
            logger.error("[BDB Impl] put broker configure failure ", e);
            result.setFailResult(DataOpErrCode.DERR_STORE_ABNORMAL.getCode(),
                    new StringBuilder(TBaseConstants.BUILDER_DEFAULT_SIZE)
                            .append("Put broker configure failure: ")
                            .append(e.getMessage()).toString());
            return result.isSuccess();
        }
        result.setSuccResult(null);
        return result.isSuccess();
    }

    private boolean delBrokerConfigFromBdb(int brokerId) {
        try {
            brokerConfIndex.delete(brokerId);
        } catch (Throwable e) {
            logger.error("[BDB Impl] delete broker configure failure ", e);
            return false;
        }
        return true;
    }


    private void delCacheRecord(int brokerId) {
        BrokerConfEntity curEntity =
                brokerConfCache.remove(brokerId);
        if (curEntity == null) {
            return;
        }
        brokerIpIndexCache.remove(curEntity.getBrokerIp());
    }

    private void addOrUpdCacheRecord(BrokerConfEntity entity) {
        brokerConfCache.put(entity.getBrokerId(), entity);
        // add brokerId info
        Integer brokerId = brokerIpIndexCache.get(entity.getBrokerIp());
        if (brokerId == null || brokerId != entity.getBrokerId()) {
            brokerIpIndexCache.put(entity.getBrokerIp(), entity.getBrokerId());
        }
    }

    private void clearCacheData() {
        brokerIpIndexCache.clear();
        brokerConfCache.clear();
    }
}
