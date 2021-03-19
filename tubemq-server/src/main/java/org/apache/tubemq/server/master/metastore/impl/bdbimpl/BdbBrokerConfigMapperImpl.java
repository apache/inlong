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
import org.apache.tubemq.server.master.bdbstore.bdbentitys.BdbBrokerConfEntity;
import org.apache.tubemq.server.master.metastore.dao.entity.BrokerConfEntity;
import org.apache.tubemq.server.master.metastore.dao.mapper.BrokerConfigMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public class BdbBrokerConfigMapperImpl implements BrokerConfigMapper {

    private static final Logger logger =
            LoggerFactory.getLogger(BdbBrokerConfigMapperImpl.class);


    // broker config store
    private EntityStore brokerConfStore;
    private PrimaryIndex<Integer/* brokerId */, BdbBrokerConfEntity> brokerConfIndex;


    public BdbBrokerConfigMapperImpl(ReplicatedEnvironment repEnv, StoreConfig storeConfig) {
        brokerConfStore = new EntityStore(repEnv,
                TBDBStoreTables.BDB_BROKER_CONFIG_STORE_NAME, storeConfig);
        brokerConfIndex =
                brokerConfStore.getPrimaryIndex(Integer.class, BdbBrokerConfEntity.class);
    }

    @Override
    public void close() {
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
    public void loadConfig(ProcessResult result) throws LoadMetaException {
        long count = 0L;
        Map<Integer, BrokerConfEntity> metaDataMap = new HashMap<>();
        EntityCursor<BdbBrokerConfEntity> cursor = null;
        logger.info("[BDB Impl] load broker configure start...");
        try {
            cursor = brokerConfIndex.entities();
            for (BdbBrokerConfEntity bdbEntity : cursor) {
                if (bdbEntity == null) {
                    logger.warn("[BDB Impl] found Null data while loading broker configure!");
                    continue;
                }
                BrokerConfEntity memEntity =
                        new BrokerConfEntity(bdbEntity);
                metaDataMap.put(memEntity.getBrokerId(), memEntity);
                count++;
            }
            logger.info("[BDB Impl] total broker configure records are {}", count);
            result.setSuccResult(metaDataMap);
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

    /**
     * Put cluster setting info into bdb store
     *
     * @param memEntity need add record
     * @param result process result with old value
     * @return
     */
    @Override
    public boolean putBrokerConfig(BrokerConfEntity memEntity, ProcessResult result) {
        BdbBrokerConfEntity retData = null;
        BdbBrokerConfEntity bdbEntity =
                memEntity.buildBdbBrokerConfEntity();
        try {
            retData = brokerConfIndex.put(bdbEntity);
        } catch (Throwable e) {
            logger.error("[BDB Impl] put broker configure failure ", e);
            result.setFailResult(new StringBuilder(TBaseConstants.BUILDER_DEFAULT_SIZE)
                    .append("Put broker configure failure: ")
                    .append(e.getMessage()).toString());
            return result.isSuccess();
        }
        result.setSuccResult(retData == null);
        return result.isSuccess();
    }

    @Override
    public boolean delBrokerConfig(int brokerId) {
        try {
            brokerConfIndex.delete(brokerId);
        } catch (Throwable e) {
            logger.error("[BDB Impl] delete broker configure failure ", e);
            return false;
        }
        return true;
    }

}
