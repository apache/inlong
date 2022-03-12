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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.inlong.tubemq.corebase.rv.ProcessResult;
import org.apache.inlong.tubemq.server.master.metamanage.DataOpErrCode;
import org.apache.inlong.tubemq.server.master.metamanage.metastore.dao.entity.TopicCtrlEntity;
import org.apache.inlong.tubemq.server.master.metamanage.metastore.dao.mapper.TopicCtrlMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbsTopicCtrlMapperImpl implements TopicCtrlMapper {
    protected static final Logger logger =
            LoggerFactory.getLogger(AbsTopicCtrlMapperImpl.class);
    // data cache
    private final ConcurrentHashMap<String/* topicName */, TopicCtrlEntity>
            topicCtrlCache = new ConcurrentHashMap<>();

    public AbsTopicCtrlMapperImpl() {
        // Initial instant
    }

    @Override
    public boolean addTopicCtrlConf(TopicCtrlEntity entity,
                                    StringBuilder strBuff, ProcessResult result) {
        TopicCtrlEntity curEntity =
                topicCtrlCache.get(entity.getTopicName());
        if (curEntity != null) {
            result.setFailResult(DataOpErrCode.DERR_EXISTED.getCode(),
                    strBuff.append("The topic control configure ").append(entity.getTopicName())
                            .append(" already exists, please delete it first!")
                            .toString());
            strBuff.delete(0, strBuff.length());
            return result.isSuccess();
        }
        if (putConfig2Persistent(entity, strBuff, result)) {
            topicCtrlCache.put(entity.getTopicName(), entity);
        }
        return result.isSuccess();
    }

    @Override
    public boolean updTopicCtrlConf(TopicCtrlEntity entity,
                                    StringBuilder strBuff, ProcessResult result) {
        TopicCtrlEntity curEntity =
                topicCtrlCache.get(entity.getTopicName());
        if (curEntity == null) {
            result.setFailResult(DataOpErrCode.DERR_NOT_EXIST.getCode(),
                    strBuff.append("The topic control configure ").append(entity.getTopicName())
                            .append(" is not exists, please add it first!")
                            .toString());
            strBuff.delete(0, strBuff.length());
            return result.isSuccess();
        }
        if (curEntity.equals(entity)) {
            result.setFailResult(DataOpErrCode.DERR_UNCHANGED.getCode(),
                    strBuff.append("The topic control configure ").append(entity.getTopicName())
                            .append(" have not changed, please confirm it first!")
                            .toString());
            strBuff.delete(0, strBuff.length());
            return result.isSuccess();
        }
        if (putConfig2Persistent(entity, strBuff, result)) {
            topicCtrlCache.put(entity.getTopicName(), entity);
            result.setSuccResult(curEntity);
        }
        return result.isSuccess();
    }

    @Override
    public boolean delTopicCtrlConf(String topicName,
                                    StringBuilder strBuff,
                                    ProcessResult result) {
        TopicCtrlEntity curEntity =
                topicCtrlCache.get(topicName);
        if (curEntity == null) {
            result.setSuccResult(null);
            return result.isSuccess();
        }
        delConfigFromPersistent(topicName, strBuff);
        topicCtrlCache.remove(topicName);
        result.setSuccResult(curEntity);
        return result.isSuccess();
    }

    @Override
    public TopicCtrlEntity getTopicCtrlConf(String topicName) {
        return topicCtrlCache.get(topicName);
    }

    @Override
    public List<TopicCtrlEntity> getTopicCtrlConf(TopicCtrlEntity qryEntity) {
        List<TopicCtrlEntity> retEntities = new ArrayList<>();
        if (qryEntity == null) {
            retEntities.addAll(topicCtrlCache.values());
        } else {
            for (TopicCtrlEntity entity : topicCtrlCache.values()) {
                if (entity != null && entity.isMatched(qryEntity)) {
                    retEntities.add(entity);
                }
            }
        }
        return retEntities;
    }

    @Override
    public Map<String, TopicCtrlEntity> getTopicCtrlConf(Set<String> topicNameSet,
                                                         TopicCtrlEntity qryEntity) {
        Set<String> qryKeySet = new HashSet<>();
        Map<String, TopicCtrlEntity> retEntityMap = new HashMap<>();
        if (topicNameSet == null || topicNameSet.isEmpty()) {
            qryKeySet.addAll(topicCtrlCache.keySet());
        } else {
            qryKeySet.addAll(topicNameSet);
        }
        for (String topicName : qryKeySet) {
            TopicCtrlEntity entity = topicCtrlCache.get(topicName);
            if (entity == null || (qryEntity != null && !entity.isMatched(qryEntity))) {
                continue;
            }
            retEntityMap.put(topicName, entity);
        }
        return retEntityMap;
    }

    /**
     * Clear cached data
     */
    protected void clearCachedData() {
        topicCtrlCache.clear();
    }

    /**
     * Add or update a record
     *
     * @param entity  need added or updated entity
     */
    protected void addOrUpdCacheRecord(TopicCtrlEntity entity) {
        topicCtrlCache.put(entity.getTopicName(), entity);
    }

    /**
     * Put topic control configure information into persistent store
     *
     * @param entity   need add record
     * @param strBuff  the string buffer
     * @param result process result with old value
     * @return the process result
     */
    protected abstract boolean putConfig2Persistent(TopicCtrlEntity entity,
                                                    StringBuilder strBuff, ProcessResult result);

    /**
     * Delete topic control configure information from persistent storage
     *
     * @param recordKey  the record key
     * @param strBuff    the string buffer
     * @return the process result
     */
    protected abstract boolean delConfigFromPersistent(String recordKey, StringBuilder strBuff);
}
