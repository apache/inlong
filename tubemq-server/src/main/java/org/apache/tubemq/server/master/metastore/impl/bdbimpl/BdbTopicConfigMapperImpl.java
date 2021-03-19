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
import org.apache.tubemq.server.master.bdbstore.bdbentitys.BdbTopicConfEntity;
import org.apache.tubemq.server.master.metastore.dao.entity.TopicConfEntity;
import org.apache.tubemq.server.master.metastore.dao.mapper.TopicConfigMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public class BdbTopicConfigMapperImpl implements TopicConfigMapper {


    private static final Logger logger =
            LoggerFactory.getLogger(BdbTopicConfigMapperImpl.class);


    // Topic configure store
    private EntityStore topicConfStore;
    private PrimaryIndex<String/* recordKey */, BdbTopicConfEntity> topicConfIndex;


    public BdbTopicConfigMapperImpl(ReplicatedEnvironment repEnv, StoreConfig storeConfig) {
        topicConfStore = new EntityStore(repEnv,
                TBDBStoreTables.BDB_TOPIC_CONFIG_STORE_NAME, storeConfig);
        topicConfIndex =
                topicConfStore.getPrimaryIndex(String.class, BdbTopicConfEntity.class);
    }

    @Override
    public void close() {
        if (topicConfStore != null) {
            try {
                topicConfStore.close();
                topicConfStore = null;
            } catch (Throwable e) {
                logger.error("[BDB Impl] close topic configure failure ", e);
            }
        }
    }

    @Override
    public void loadConfig(ProcessResult result) throws LoadMetaException {
        long count = 0L;
        Map<String, TopicConfEntity> metaDataMap = new HashMap<>();
        EntityCursor<BdbTopicConfEntity> cursor = null;
        logger.info("[BDB Impl] load topic configure start...");
        try {
            cursor = topicConfIndex.entities();
            for (BdbTopicConfEntity bdbEntity : cursor) {
                if (bdbEntity == null) {
                    logger.warn("[BDB Impl] found Null data while loading topic configure!");
                    continue;
                }
                TopicConfEntity memEntity =
                        new TopicConfEntity(bdbEntity);
                metaDataMap.put(memEntity.getRecordKey(), memEntity);
                count++;
            }
            logger.info("[BDB Impl] total topic configure records are {}", count);
            result.setSuccResult(metaDataMap);
        } catch (Exception e) {
            logger.error("[BDB Impl] load topic configure failure ", e);
            throw new LoadMetaException(e.getMessage());
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }
        logger.info("[BDB Impl] load topic configure successfully...");
    }

    /**
     * Put topic configure info into bdb store
     *
     * @param memEntity need add record
     * @param result process result with old value
     * @return
     */
    @Override
    public boolean putTopicConfig(TopicConfEntity memEntity, ProcessResult result) {
        BdbTopicConfEntity retData = null;
        BdbTopicConfEntity bdbEntity =
                memEntity.buildBdbTopicConfEntity();
        try {
            retData = topicConfIndex.put(bdbEntity);
        } catch (Throwable e) {
            logger.error("[BDB Impl] put topic configure failure ", e);
            result.setFailResult(new StringBuilder(TBaseConstants.BUILDER_DEFAULT_SIZE)
                    .append("Put topic configure failure: ")
                    .append(e.getMessage()).toString());
            return result.isSuccess();
        }
        result.setSuccResult(retData == null);
        return result.isSuccess();
    }

    @Override
    public boolean delTopicConfig(String recordKey) {
        try {
            topicConfIndex.delete(recordKey);
        } catch (Throwable e) {
            logger.error("[BDB Impl] delete topic configure failure ", e);
            return false;
        }
        return true;
    }

}
