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

package org.apache.tubemq.server.master.metamanage.metastore.dao.entity;

import java.io.Serializable;
import java.util.Objects;
import org.apache.tubemq.corebase.TBaseConstants;
import org.apache.tubemq.corebase.utils.TStringUtils;


/*
 * Topic property group, save topic related storage and configuration information.
 *
 */
public class TopicPropGroup implements Serializable {

    private int numTopicStores = TBaseConstants.META_VALUE_UNDEFINED;        //store num
    private int numPartitions = TBaseConstants.META_VALUE_UNDEFINED;        //partition num
    private int unflushThreshold = TBaseConstants.META_VALUE_UNDEFINED;     //flush threshold
    private int unflushInterval = TBaseConstants.META_VALUE_UNDEFINED;      //flush interval
    private int unflushDataHold = TBaseConstants.META_VALUE_UNDEFINED;      // flush dataSize
    private int memCacheMsgSizeInMB = TBaseConstants.META_VALUE_UNDEFINED;  // cache block size
    private int memCacheMsgCntInK = TBaseConstants.META_VALUE_UNDEFINED;    // cache max count
    private int memCacheFlushIntvl = TBaseConstants.META_VALUE_UNDEFINED;   // cache max interval
    private boolean acceptPublish = true;    //enable publish
    private boolean acceptSubscribe = true;  //enable subscribe
    private String deletePolicy = "";        // delete policy
    private int dataStoreType = TBaseConstants.META_VALUE_UNDEFINED;  // type
    private String dataPath = "";   //data path

    public TopicPropGroup() {

    }

    public TopicPropGroup(int numTopicStores, int numPartitions,
                          int unflushThreshold, int unflushInterval,
                          int unflushDataHold, int memCacheMsgSizeInMB,
                          int memCacheMsgCntInK, int memCacheFlushIntvl,
                          boolean acceptPublish, boolean acceptSubscribe,
                          String deletePolicy, int dataStoreType, String dataPath) {
        this.numTopicStores = numTopicStores;
        this.numPartitions = numPartitions;
        this.unflushThreshold = unflushThreshold;
        this.unflushInterval = unflushInterval;
        this.unflushDataHold = unflushDataHold;
        this.memCacheMsgSizeInMB = memCacheMsgSizeInMB;
        this.memCacheMsgCntInK = memCacheMsgCntInK;
        this.memCacheFlushIntvl = memCacheFlushIntvl;
        this.acceptPublish = acceptPublish;
        this.acceptSubscribe = acceptSubscribe;
        this.deletePolicy = deletePolicy;
        this.dataStoreType = dataStoreType;
        this.dataPath = dataPath;
    }

    public int getNumTopicStores() {
        return numTopicStores;
    }

    public void setNumTopicStores(int numTopicStores) {
        this.numTopicStores = numTopicStores;
    }

    public int getNumPartitions() {
        return numPartitions;
    }

    public void setNumPartitions(int numPartitions) {
        this.numPartitions = numPartitions;
    }

    public int getUnflushThreshold() {
        return unflushThreshold;
    }

    public void setUnflushThreshold(int unflushThreshold) {
        this.unflushThreshold = unflushThreshold;
    }

    public int getUnflushInterval() {
        return unflushInterval;
    }

    public void setUnflushInterval(int unflushInterval) {
        this.unflushInterval = unflushInterval;
    }

    public int getUnflushDataHold() {
        return unflushDataHold;
    }

    public void setUnflushDataHold(int unflushDataHold) {
        this.unflushDataHold = unflushDataHold;
    }

    public int getMemCacheMsgSizeInMB() {
        return memCacheMsgSizeInMB;
    }

    public void setMemCacheMsgSizeInMB(int memCacheMsgSizeInMB) {
        this.memCacheMsgSizeInMB = memCacheMsgSizeInMB;
    }

    public int getMemCacheMsgCntInK() {
        return memCacheMsgCntInK;
    }

    public void setMemCacheMsgCntInK(int memCacheMsgCntInK) {
        this.memCacheMsgCntInK = memCacheMsgCntInK;
    }

    public int getMemCacheFlushIntvl() {
        return memCacheFlushIntvl;
    }

    public void setMemCacheFlushIntvl(int memCacheFlushIntvl) {
        this.memCacheFlushIntvl = memCacheFlushIntvl;
    }

    public boolean isAcceptPublish() {
        return acceptPublish;
    }

    public void setAcceptPublish(boolean acceptPublish) {
        this.acceptPublish = acceptPublish;
    }

    public boolean isAcceptSubscribe() {
        return acceptSubscribe;
    }

    public void setAcceptSubscribe(boolean acceptSubscribe) {
        this.acceptSubscribe = acceptSubscribe;
    }

    public String getDeletePolicy() {
        return deletePolicy;
    }

    public void setDeletePolicy(String deletePolicy) {
        this.deletePolicy = deletePolicy;
    }

    public void setDataStoreInfo(int dataStoreType, String dataPath) {
        this.dataPath = dataPath;
        this.dataStoreType = dataStoreType;
    }

    public String getDataPath() {
        return dataPath;
    }

    public int getDataStoreType() {
        return dataStoreType;
    }

    /**
     * Check whether the specified query item value matches
     * Allowed query items:
     *   numTopicStores, numPartitions, unflushThreshold, unflushInterval, unflushDataHold,
     *   memCacheMsgSizeInMB, memCacheMsgCntInK, memCacheFlushIntvl, deletePolicy
     * @return true: matched, false: not match
     */
    public boolean isMatched(TopicPropGroup target) {
        if (target == null) {
            return true;
        }
        if ((target.getNumTopicStores() != TBaseConstants.META_VALUE_UNDEFINED
                && target.getNumTopicStores() != this.numTopicStores)
                || (target.getNumPartitions() != TBaseConstants.META_VALUE_UNDEFINED
                && target.getNumPartitions() != this.numPartitions)
                || (target.getUnflushThreshold() != TBaseConstants.META_VALUE_UNDEFINED
                && target.getUnflushThreshold() != this.unflushThreshold)
                || (target.getUnflushInterval() != TBaseConstants.META_VALUE_UNDEFINED
                && target.getUnflushInterval() != this.unflushInterval)
                || (target.getUnflushDataHold() != TBaseConstants.META_VALUE_UNDEFINED
                && target.getUnflushDataHold() != this.unflushDataHold)
                || (target.getMemCacheMsgSizeInMB() != TBaseConstants.META_VALUE_UNDEFINED
                && target.getMemCacheMsgSizeInMB() != this.memCacheMsgSizeInMB)
                || (target.getMemCacheMsgCntInK() != TBaseConstants.META_VALUE_UNDEFINED
                && target.getMemCacheMsgCntInK() != this.memCacheMsgCntInK)
                || (target.getMemCacheFlushIntvl() != TBaseConstants.META_VALUE_UNDEFINED
                && target.getMemCacheFlushIntvl() != this.memCacheFlushIntvl)
                || (TStringUtils.isNotBlank(target.getDeletePolicy())
                && !target.getDeletePolicy().equals(this.deletePolicy))) {
            return false;
        }
        return true;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof TopicPropGroup)) {
            return false;
        }
        TopicPropGroup that = (TopicPropGroup) o;
        return numTopicStores == that.numTopicStores &&
                numPartitions == that.numPartitions &&
                unflushThreshold == that.unflushThreshold &&
                unflushInterval == that.unflushInterval &&
                unflushDataHold == that.unflushDataHold &&
                memCacheMsgSizeInMB == that.memCacheMsgSizeInMB &&
                memCacheMsgCntInK == that.memCacheMsgCntInK &&
                memCacheFlushIntvl == that.memCacheFlushIntvl &&
                acceptPublish == that.acceptPublish &&
                acceptSubscribe == that.acceptSubscribe &&
                dataStoreType == that.dataStoreType &&
                Objects.equals(deletePolicy, that.deletePolicy) &&
                Objects.equals(dataPath, that.dataPath);
    }

    @Override
    public int hashCode() {
        return Objects.hash(numTopicStores, numPartitions, unflushThreshold, unflushInterval,
                unflushDataHold, memCacheMsgSizeInMB, memCacheMsgCntInK, memCacheFlushIntvl,
                acceptPublish, acceptSubscribe, deletePolicy, dataStoreType, dataPath);
    }
}
