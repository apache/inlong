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

package org.apache.tubemq.server.master.bdbstore.bdbentitys;

import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;
import java.io.Serializable;
import java.util.Date;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.tubemq.corebase.TBaseConstants;
import org.apache.tubemq.server.common.utils.WebParameterUtils;


/*
 * store the cluster default setting
 *
 */
@Entity
public class BdbClusterSettingEntity implements Serializable {

    private static final long serialVersionUID = -3259439355290322115L;

    @PrimaryKey
    private String recordKey = "";
    //broker tcp port
    private int brokerPort = TBaseConstants.META_VALUE_UNDEFINED;
    //broker tls port
    private int brokerTLSPort = TBaseConstants.META_VALUE_UNDEFINED;
    //broker web port
    private int brokerWebPort = TBaseConstants.META_VALUE_UNDEFINED;
    //store num
    private int numTopicStores = TBaseConstants.META_VALUE_UNDEFINED;
    //partition num
    private int numPartitions = TBaseConstants.META_VALUE_UNDEFINED;
    //flush disk threshold
    private int unflushDskThreshold = TBaseConstants.META_VALUE_UNDEFINED;
    //flush disk interval
    private int unflushDksInterval = TBaseConstants.META_VALUE_UNDEFINED;
    //flush memory cache threshold
    private int unflushMemThreshold = TBaseConstants.META_VALUE_UNDEFINED;
    //flush memory cache interval
    private int unflushMemInterval = TBaseConstants.META_VALUE_UNDEFINED;
    //flush memory cache count
    private int unflushMemCnt = TBaseConstants.META_VALUE_UNDEFINED;
    private boolean acceptPublish = true;   //enable publish
    private boolean acceptSubscribe = true; //enable subscribe
    private String deleteWhen = "";              //delete policy execute time
    private int qryPriorityId = TBaseConstants.META_VALUE_UNDEFINED;
    private int maxMsgSize = TBaseConstants.META_VALUE_UNDEFINED;
    private String attributes;               //extra attribute
    private String modifyUser;               //modify user
    private Date modifyDate;                 //modify date

    public BdbClusterSettingEntity() {
    }

    //Constructor
    public BdbClusterSettingEntity(String recordKey, int brokerPort, int brokerTLSPort,
                                   int brokerWebPort, int numTopicStores, int numPartitions,
                                   int unflushDskThreshold, int unflushDksInterval,
                                   int unflushMemThreshold, int unflushMemInterval,
                                   int unflushMemCnt, boolean acceptPublish,
                                   boolean acceptSubscribe, String deleteWhen,
                                   int qryPriorityId, int maxMsgSize, String attributes,
                                   String modifyUser, Date modifyDate) {
        this.recordKey = recordKey;
        this.brokerPort = brokerPort;
        this.brokerTLSPort = brokerTLSPort;
        this.brokerWebPort = brokerWebPort;
        this.numTopicStores = numTopicStores;
        this.numPartitions = numPartitions;
        this.unflushDskThreshold = unflushDskThreshold;
        this.unflushDksInterval = unflushDksInterval;
        this.unflushMemThreshold = unflushMemThreshold;
        this.unflushMemInterval = unflushMemInterval;
        this.unflushMemCnt = unflushMemCnt;
        this.acceptPublish = acceptPublish;
        this.acceptSubscribe = acceptSubscribe;
        this.deleteWhen = deleteWhen;
        this.qryPriorityId = qryPriorityId;
        this.maxMsgSize = maxMsgSize;
        this.attributes = attributes;
        this.modifyUser = modifyUser;
        this.modifyDate = modifyDate;
    }

    public void setRecordKey(String recordKey) {
        this.recordKey = recordKey;
    }

    public String getRecordKey() {
        return recordKey;
    }

    public int getBrokerPort() {
        return brokerPort;
    }

    public void setBrokerPort(int brokerPort) {
        this.brokerPort = brokerPort;
    }

    public int getBrokerTLSPort() {
        return brokerTLSPort;
    }

    public void setBrokerTLSPort(int brokerTLSPort) {
        this.brokerTLSPort = brokerTLSPort;
    }

    public int getBrokerWebPort() {
        return brokerWebPort;
    }

    public void setBrokerWebPort(int brokerWebPort) {
        this.brokerWebPort = brokerWebPort;
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

    public int getUnflushDskThreshold() {
        return unflushDskThreshold;
    }

    public void setUnflushDskThreshold(int unflushDskThreshold) {
        this.unflushDskThreshold = unflushDskThreshold;
    }

    public int getUnflushDksInterval() {
        return unflushDksInterval;
    }

    public void setUnflushDksInterval(int unflushDksInterval) {
        this.unflushDksInterval = unflushDksInterval;
    }

    public int getUnflushMemThreshold() {
        return unflushMemThreshold;
    }

    public void setUnflushMemThreshold(int unflushMemThreshold) {
        this.unflushMemThreshold = unflushMemThreshold;
    }

    public int getUnflushMemInterval() {
        return unflushMemInterval;
    }

    public void setUnflushMemInterval(int unflushMemInterval) {
        this.unflushMemInterval = unflushMemInterval;
    }

    public int getUnflushMemCnt() {
        return unflushMemCnt;
    }

    public void setUnflushMemCnt(int unflushMemCnt) {
        this.unflushMemCnt = unflushMemCnt;
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

    public String getDeleteWhen() {
        return deleteWhen;
    }

    public void setDeleteWhen(String deleteWhen) {
        this.deleteWhen = deleteWhen;
    }

    public int getQryPriorityId() {
        return qryPriorityId;
    }

    public void setQryPriorityId(int qryPriorityId) {
        this.qryPriorityId = qryPriorityId;
    }

    public int getMaxMsgSize() {
        return maxMsgSize;
    }

    public void setMaxMsgSize(int maxMsgSize) {
        this.maxMsgSize = maxMsgSize;
    }

    public String getAttributes() {
        return attributes;
    }

    public void setAttributes(String attributes) {
        this.attributes = attributes;
    }

    public String getModifyUser() {
        return modifyUser;
    }

    public void setModifyUser(String modifyUser) {
        this.modifyUser = modifyUser;
    }

    public Date getModifyDate() {
        return modifyDate;
    }

    public void setModifyDate(Date modifyDate) {
        this.modifyDate = modifyDate;
    }

    /**
     * Serialize field to json format
     *
     * @param sBuilder
     * @return
     */
    public StringBuilder toJsonString(final StringBuilder sBuilder) {
        return sBuilder.append("{\"type\":\"BdbClusterSettingEntity\",")
                .append("\"recordKey\":\"").append(recordKey).append("\"")
                .append(",\"brokerPort\":").append(brokerPort)
                .append(",\"brokerTLSPort\":").append(brokerTLSPort)
                .append(",\"brokerWebPort\":").append(brokerWebPort)
                .append(",\"numTopicStores\":").append(numTopicStores)
                .append(",\"numPartitions\":").append(numPartitions)
                .append(",\"unflushDskThreshold\":").append(unflushDskThreshold)
                .append(",\"unflushDksInterval\":").append(unflushDksInterval)
                .append(",\"unflushMemThreshold\":").append(unflushMemThreshold)
                .append(",\"unflushMemInterval\":").append(unflushMemInterval)
                .append(",\"unflushMemCnt\":").append(unflushMemCnt)
                .append(",\"acceptPublish\":").append(acceptPublish)
                .append(",\"acceptSubscribe\":").append(acceptSubscribe)
                .append(",\"deleteWhen\":\"").append(deleteWhen).append("\"")
                .append(",\"maxMsgSize\":").append(maxMsgSize)
                .append(",\"qryPriorityId\":").append(qryPriorityId)
                .append(",\"attributes\":\"").append(attributes).append("\"")
                .append(",\"modifyUser\":\"").append(modifyUser).append("\"")
                .append(",\"modifyDate\":\"")
                .append(WebParameterUtils.date2yyyyMMddHHmmss(modifyDate))
                .append("\"}");
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("recordKey", recordKey)
                .append("brokerPort", brokerPort)
                .append("brokerTLSPort", brokerTLSPort)
                .append("brokerWebPort", brokerWebPort)
                .append("numTopicStores", numTopicStores)
                .append("numPartitions", numPartitions)
                .append("unflushDskThreshold", unflushDskThreshold)
                .append("unflushDksInterval", unflushDksInterval)
                .append("unflushMemThreshold", unflushMemThreshold)
                .append("unflushMemInterval", unflushMemInterval)
                .append("unflushMemCnt", unflushMemCnt)
                .append("acceptPublish", acceptPublish)
                .append("acceptSubscribe", acceptSubscribe)
                .append("deleteWhen", deleteWhen)
                .append("maxMsgSize", maxMsgSize)
                .append("qryPriorityId", qryPriorityId)
                .append("attributes", attributes)
                .append("modifyUser", modifyUser)
                .append("modifyDate", modifyDate)
                .toString();
    }
}
