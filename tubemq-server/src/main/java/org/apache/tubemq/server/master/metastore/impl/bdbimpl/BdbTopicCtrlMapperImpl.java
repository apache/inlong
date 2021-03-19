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
import org.apache.tubemq.server.master.bdbstore.bdbentitys.BdbTopicAuthControlEntity;
import org.apache.tubemq.server.master.metastore.dao.entity.TopicCtrlEntity;
import org.apache.tubemq.server.master.metastore.dao.mapper.TopicCtrlMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public class BdbTopicCtrlMapperImpl implements TopicCtrlMapper {


    private static final Logger logger =
            LoggerFactory.getLogger(BdbTopicCtrlMapperImpl.class);


    // Topic control store
    private EntityStore topicCtrlStore;
    private PrimaryIndex<String/* recordKey */, BdbTopicAuthControlEntity> topicCtrlIndex;


    public BdbTopicCtrlMapperImpl(ReplicatedEnvironment repEnv, StoreConfig storeConfig) {
        topicCtrlStore = new EntityStore(repEnv,
                TBDBStoreTables.BDB_TOPIC_AUTH_CONTROL_STORE_NAME, storeConfig);
        topicCtrlIndex =
                topicCtrlStore.getPrimaryIndex(String.class, BdbTopicAuthControlEntity.class);
    }

    @Override
    public void close() {
        if (topicCtrlStore != null) {
            try {
                topicCtrlStore.close();
                topicCtrlStore = null;
            } catch (Throwable e) {
                logger.error("[BDB Impl] close topic control failure ", e);
            }
        }
    }

    @Override
    public void loadConfig(ProcessResult result) throws LoadMetaException {
        long count = 0L;
        Map<String, TopicCtrlEntity> metaDataMap = new HashMap<>();
        EntityCursor<BdbTopicAuthControlEntity> cursor = null;
        logger.info("[BDB Impl] load topic configure start...");
        try {
            cursor = topicCtrlIndex.entities();
            for (BdbTopicAuthControlEntity bdbEntity : cursor) {
                if (bdbEntity == null) {
                    logger.warn("[BDB Impl] found Null data while loading topic control!");
                    continue;
                }
                TopicCtrlEntity memEntity =
                        new TopicCtrlEntity(bdbEntity);
                metaDataMap.put(memEntity.getTopicName(), memEntity);
                count++;
            }
            logger.info("[BDB Impl] total topic control records are {}", count);
            result.setSuccResult(metaDataMap);
        } catch (Exception e) {
            logger.error("[BDB Impl] load topic control failure ", e);
            throw new LoadMetaException(e.getMessage());
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }
        logger.info("[BDB Impl] load topic control successfully...");
    }

    /**
     * Put topic control configure info into bdb store
     *
     * @param memEntity need add record
     * @param result process result with old value
     * @return
     */
    @Override
    public boolean putTopicCtrlConfig(TopicCtrlEntity memEntity, ProcessResult result) {
        BdbTopicAuthControlEntity retData = null;
        BdbTopicAuthControlEntity bdbEntity =
                memEntity.buildBdbTopicAuthControlEntity();
        try {
            retData = topicCtrlIndex.put(bdbEntity);
        } catch (Throwable e) {
            logger.error("[BDB Impl] put topic control failure ", e);
            result.setFailResult(new StringBuilder(TBaseConstants.BUILDER_DEFAULT_SIZE)
                    .append("Put topic control failure: ")
                    .append(e.getMessage()).toString());
            return result.isSuccess();
        }
        result.setSuccResult(retData == null);
        return result.isSuccess();
    }

    @Override
    public boolean delTopicCtrlConfig(String recordKey) {
        try {
            topicCtrlIndex.delete(recordKey);
        } catch (Throwable e) {
            logger.error("[BDB Impl] delete topic control failure ", e);
            return false;
        }
        return true;
    }

}
