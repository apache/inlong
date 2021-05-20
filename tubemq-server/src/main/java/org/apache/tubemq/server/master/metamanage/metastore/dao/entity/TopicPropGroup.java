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
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.tubemq.corebase.TBaseConstants;
import org.apache.tubemq.corebase.utils.TStringUtils;
import org.apache.tubemq.corebase.utils.Tuple2;
import org.apache.tubemq.server.common.TServerConstants;
import org.apache.tubemq.server.common.statusdef.CuPolType;


/*
 * Topic property group, save topic related storage and configuration information.
 *
 */
public class TopicPropGroup implements Serializable, Cloneable {

    private int numTopicStores = TBaseConstants.META_VALUE_UNDEFINED;        //store num
    private int numPartitions = TBaseConstants.META_VALUE_UNDEFINED;        //partition num
    private int unflushThreshold = TBaseConstants.META_VALUE_UNDEFINED;     //flush threshold
    private int unflushInterval = TBaseConstants.META_VALUE_UNDEFINED;      //flush interval
    private int unflushDataHold = TBaseConstants.META_VALUE_UNDEFINED;      // flush dataSize
    private int memCacheMsgSizeInMB = TBaseConstants.META_VALUE_UNDEFINED;  // cache block size
    private int memCacheMsgCntInK = TBaseConstants.META_VALUE_UNDEFINED;    // cache max count
    private int memCacheFlushIntvl = TBaseConstants.META_VALUE_UNDEFINED;   // cache max interval
    private Boolean acceptPublish = null;    //enable publish
    private Boolean acceptSubscribe = null;  //enable subscribe
    private int dataStoreType = TBaseConstants.META_VALUE_UNDEFINED;  // type
    private String dataPath = "";   //data path
    private String deletePolicy = "";        // delete policy
    // Retention period, unit ms
    private CuPolType fileCuPolicyType = CuPolType.CU_POL_DELETE;
    private long retPeriodInMs = TBaseConstants.META_VALUE_UNDEFINED;

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
        setDeletePolicy(deletePolicy);
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

    public Boolean getAcceptPublish() {
        return acceptPublish;
    }
    public void setAcceptPublish(Boolean acceptPublish) {
        this.acceptPublish = acceptPublish;
    }

    public boolean isAcceptSubscribe() {
        return acceptSubscribe;
    }

    public void setAcceptSubscribe(Boolean acceptSubscribe) {
        this.acceptSubscribe = acceptSubscribe;
    }

    public Boolean getAcceptSubscribe() {
        return acceptSubscribe;
    }

    public void setDeletePolicy(String deletePolicy) {
        if (TStringUtils.isNotBlank(deletePolicy)) {
            this.deletePolicy = deletePolicy;
            Tuple2<CuPolType, Long> parsedRet = parseDelPolicy(deletePolicy);
            this.fileCuPolicyType = parsedRet.getF0();
            this.retPeriodInMs = parsedRet.getF1();
        }
    }

    public String getDeletePolicy() {
        return deletePolicy;
    }

    public long getRetPeriodInMs() {
        return retPeriodInMs;
    }

    public CuPolType getFileCuPolicyType() {
        return fileCuPolicyType;
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
                || (target.getAcceptPublish() != null
                && target.getAcceptPublish() != this.acceptPublish)
                || (target.getAcceptSubscribe() != null
                && target.getAcceptSubscribe() != this.acceptSubscribe)
                || (TStringUtils.isNotBlank(target.getDeletePolicy())
                && !target.getDeletePolicy().equals(this.deletePolicy))) {
            return false;
        }
        return true;
    }

    /**
     * Serialize field to json format
     *
     * @param sBuilder
     * @return
     */
    StringBuilder toWebJsonStr(StringBuilder sBuilder, boolean isLongName) {
        if (isLongName) {
            sBuilder.append(",\"numTopicStores\":").append(numTopicStores)
                    .append(",\"numPartitions\":").append(numPartitions)
                    .append(",\"unflushThreshold\":").append(unflushThreshold)
                    .append(",\"unflushInterval\":").append(unflushInterval)
                    .append(",\"unflushDataHold\":").append(unflushDataHold)
                    .append(",\"memCacheMsgSizeInMB\":").append(memCacheMsgSizeInMB)
                    .append(",\"memCacheMsgCntInK\":").append(memCacheMsgCntInK)
                    .append(",\"memCacheFlushIntvl\":").append(memCacheFlushIntvl)
                    .append(",\"acceptPublish\":").append(acceptPublish)
                    .append(",\"acceptSubscribe\":").append(acceptSubscribe)
                    .append(",\"deletePolicy\":\"").append(deletePolicy).append("\"")
                    .append(",\"dataStoreType\":").append(dataStoreType)
                    .append(",\"dataPath\":\"").append(dataPath).append("\"");
        } else {
            sBuilder.append(",\"numStore\":").append(numTopicStores)
                    .append(",\"numPart\":").append(numPartitions)
                    .append(",\"unfDskMsgCnt\":").append(unflushThreshold)
                    .append(",\"unfDskInt\":").append(unflushInterval)
                    .append(",\"unfDskDataSz\":").append(unflushDataHold)
                    .append(",\"cacheInMB\":").append(memCacheMsgSizeInMB)
                    .append(",\"unfMemMsgCnt\":").append(memCacheMsgCntInK)
                    .append(",\"unfMemInt\":").append(memCacheFlushIntvl)
                    .append(",\"accPub\":").append(acceptPublish)
                    .append(",\"accSub\":").append(acceptSubscribe)
                    .append(",\"delPol\":\"").append(deletePolicy).append("\"")
                    .append(",\"dStType\":").append(dataStoreType)
                    .append(",\"dPath\":\"").append(dataPath).append("\"");
        }
        return sBuilder;
    }

    /**
     * fill fields with default value
     *
     * @return object
     */
    public TopicPropGroup fillDefaultValue() {
        this.numTopicStores = TServerConstants.TOPIC_STOREBLOCK_NUM_MIN;
        this.numPartitions = TServerConstants.TOPIC_PARTITION_NUM_MIN;
        this.unflushThreshold = TServerConstants.TOPIC_DSK_UNFLUSHTHRESHOLD_DEF;
        this.unflushInterval = TServerConstants.TOPIC_DSK_UNFLUSHINTERVAL_DEF;
        this.unflushDataHold = TServerConstants.TOPIC_DSK_UNFLUSHDATAHOLD_MIN;
        this.memCacheMsgSizeInMB = TServerConstants.TOPIC_CACHESIZE_MB_DEF;
        this.memCacheFlushIntvl = TServerConstants.TOPIC_CACHEINTVL_DEF;
        this.memCacheMsgCntInK = TServerConstants.TOPIC_CACHECNT_INK_DEF;
        this.acceptPublish = true;
        this.acceptSubscribe = true;
        this.deletePolicy = TServerConstants.TOPIC_POLICY_DEF;
        this.retPeriodInMs = TServerConstants.TOPIC_RET_PERIOD_IN_SEC_DEF;
        return this;
    }

    /**
     * check TopicPropGroup's partition or storeblock changed
     * @return if changed
     */
    public boolean isPartOrStoreChanged(TopicPropGroup other) {
        if (this.numPartitions != other.numPartitions
                || this.numTopicStores != other.numTopicStores) {
            return true;
        }
        return false;
    }

    /**
     * check if subclass fields is equals
     *
     * @param other  check object
     * @return if equals
     */
    public boolean isDataEquals(TopicPropGroup other) {
        return numTopicStores == other.numTopicStores
                && numPartitions == other.numPartitions
                && unflushThreshold == other.unflushThreshold
                && unflushInterval == other.unflushInterval
                && unflushDataHold == other.unflushDataHold
                && memCacheMsgSizeInMB == other.memCacheMsgSizeInMB
                && memCacheMsgCntInK == other.memCacheMsgCntInK
                && memCacheFlushIntvl == other.memCacheFlushIntvl
                && dataStoreType == other.dataStoreType
                && retPeriodInMs == other.retPeriodInMs
                && Objects.equals(acceptPublish, other.acceptPublish)
                && Objects.equals(acceptSubscribe, other.acceptSubscribe)
                && Objects.equals(dataPath, other.dataPath)
                && Objects.equals(deletePolicy, other.deletePolicy)
                && fileCuPolicyType == other.fileCuPolicyType;
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
        return isDataEquals(that);
    }

    @Override
    public int hashCode() {
        return Objects.hash(numTopicStores, numPartitions, unflushThreshold,
                unflushInterval, unflushDataHold, memCacheMsgSizeInMB, memCacheMsgCntInK,
                memCacheFlushIntvl, acceptPublish, acceptSubscribe, dataStoreType,
                dataPath, deletePolicy, fileCuPolicyType, retPeriodInMs);
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this);
    }

    @Override
    public TopicPropGroup clone() throws CloneNotSupportedException {
        return (TopicPropGroup) super.clone();
    }


    private Tuple2<CuPolType, Long> parseDelPolicy(String delPolicy) {
        long validDuration = 0;
        String[] tmpStrs = delPolicy.split(",");
        String validValStr = tmpStrs[1];
        String timeUnit = validValStr.substring(validValStr.length() - 1).toLowerCase();
        if (timeUnit.endsWith("s")) {
            validDuration = Long.parseLong(validValStr.substring(0, validValStr.length() - 1)) * 1000;
        } else if (timeUnit.endsWith("m")) {
            validDuration = Long.parseLong(validValStr.substring(0, validValStr.length() - 1)) * 60000;
        } else if (timeUnit.endsWith("h")) {
            validDuration = Long.parseLong(validValStr.substring(0, validValStr.length() - 1)) * 3600000;
        } else {
            validDuration = Long.parseLong(validValStr) * 3600000;
        }
        return new Tuple2<>(CuPolType.CU_POL_DELETE, validDuration);
    }

}
