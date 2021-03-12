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

package org.apache.tubemq.server.master.metastore.dao.entity;

import java.util.Date;
import org.apache.tubemq.corebase.TBaseConstants;
import org.apache.tubemq.corebase.TokenConstants;
import org.apache.tubemq.server.common.statusdef.TopicStatus;


/*
 * store the topic configure setting
 *
 */
public class TopicConfEntity extends BaseEntity {

    private String recordKey = "";
    private String topicName = "";         //topic name
    private int brokerId = TBaseConstants.META_VALUE_UNDEFINED;     //broker id
    private TopicStatus topicStatus = TopicStatus.STATUS_TOPIC_OK;  // topic status
    private String brokerIp = "";          //broker ip
    private int brokerPort = TBaseConstants.META_VALUE_UNDEFINED;   //broker port
    private String brokerAddress = "";     //broker address
    private TopicPropGroup topicPropGroup = null;
    private int maxMsgSizeInM = TBaseConstants.META_VALUE_UNDEFINED;



    public TopicConfEntity() {
        super();
    }

    public TopicConfEntity(String topicName, int brokerId, TopicStatus topicStatus,
                           String brokerIp, int brokerPort, TopicPropGroup topicPropGroup,
                           int maxMsgSizeInM, String createUser, Date createDate) {
        super(createUser, createDate);
        setBrokerAndTopicInfo(brokerId, brokerIp, brokerPort, topicName);
        this.topicStatus = topicStatus;
        this.topicPropGroup = topicPropGroup;
        this.maxMsgSizeInM = maxMsgSizeInM;
    }

    public void setBrokerAndTopicInfo(int brokerId, String brokerIp,
                                      int brokerPort, String topicName) {
        StringBuilder sBuilder = new StringBuilder(TBaseConstants.BUILDER_DEFAULT_SIZE);
        this.recordKey = sBuilder.append(brokerId)
                .append(TokenConstants.ATTR_SEP).append(topicName).toString();
        this.brokerId = brokerId;
        this.brokerIp = brokerIp;
        this.brokerPort = brokerPort;
        this.topicName = topicName;
        if (this.brokerPort != TBaseConstants.META_VALUE_UNDEFINED) {
            sBuilder.delete(0, sBuilder.length());
            this.brokerAddress = sBuilder.append(brokerIp)
                    .append(TokenConstants.ATTR_SEP).append(brokerPort).toString();
        }
    }

    public String getRecordKey() {
        return recordKey;
    }

    public String getTopicName() {
        return topicName;
    }

    public void setTopicName(String topicName) {
        this.topicName = topicName;
    }

    public int getBrokerId() {
        return brokerId;
    }

    public String getBrokerIp() {
        return brokerIp;
    }

    public int getBrokerPort() {
        return brokerPort;
    }

    public String getBrokerAddress() {
        return brokerAddress;
    }

    public int getMaxMsgSizeInM() {
        return maxMsgSizeInM;
    }

    public void setMaxMsgSizeInM(int maxMsgSizeInM) {
        this.maxMsgSizeInM = maxMsgSizeInM;
    }

    public void setTopicStatusId(int topicStatusId) {
        this.topicStatus = TopicStatus.valueOf(topicStatusId);
    }

    public int getTopicStatusId() {
        return topicStatus.getCode();
    }

    public boolean isValidTopicStatus() {
        return this.topicStatus == TopicStatus.STATUS_TOPIC_OK;
    }

    public TopicPropGroup getTopicPropGroup() {
        return topicPropGroup;
    }

    public void setTopicPropGroup(TopicPropGroup topicPropGroup) {
        this.topicPropGroup = topicPropGroup;
    }
}
