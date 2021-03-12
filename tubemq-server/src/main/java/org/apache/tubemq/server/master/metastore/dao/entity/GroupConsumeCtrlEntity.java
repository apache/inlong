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
import org.apache.tubemq.server.common.statusdef.RuleStatus;


/*
 * store the group consume control setting
 *
 */
public class GroupConsumeCtrlEntity extends BaseEntity {
    private String recordKey = "";
    private String topicName = "";
    private String groupName = "";
    private RuleStatus resCheckStatus = RuleStatus.STATUS_UNDEFINE;
    private int allowedBrokerClientRate = TBaseConstants.META_VALUE_UNDEFINED;
    private RuleStatus filterConsumeStatus = RuleStatus.STATUS_UNDEFINE;
    private String filterCondStr = "";


    public GroupConsumeCtrlEntity() {
        super();
    }

    public GroupConsumeCtrlEntity(String topicName, String groupName,
                                  String createUser, Date createDate) {
        super(createUser, createDate);
        this.recordKey = new StringBuilder(TBaseConstants.BUILDER_DEFAULT_SIZE)
                .append(topicName).append(TokenConstants.ATTR_SEP)
                .append(groupName).toString();
        this.topicName = topicName;
        this.groupName = groupName;
    }

    public String getRecordKey() {
        return recordKey;
    }

    public void setTopicName(String topicName) {
        this.topicName = topicName;
    }

    public void setGroupName(String groupName) {
        this.groupName = groupName;
    }

    public String getTopicName() {
        return topicName;
    }

    public String getGroupName() {
        return groupName;
    }

    public boolean isEnableResCheck() {
        return resCheckStatus == RuleStatus.STATUS_ENABLE;
    }

    public void setResCheckStatus(boolean enableResChk) {
        if (enableResChk) {
            this.resCheckStatus = RuleStatus.STATUS_ENABLE;
        } else {
            this.resCheckStatus = RuleStatus.STATUS_DISABLE;
        }
    }

    public int getAllowedBrokerClientRate() {
        return allowedBrokerClientRate;
    }

    public void setAllowedBrokerClientRate(int allowedBrokerClientRate) {
        this.allowedBrokerClientRate = allowedBrokerClientRate;
    }

    public boolean isEnableFilterConsume() {
        return filterConsumeStatus == RuleStatus.STATUS_ENABLE;
    }

    public void setFilterConsumeStatus(boolean enableFilterConsume) {
        if (enableFilterConsume) {
            this.filterConsumeStatus = RuleStatus.STATUS_ENABLE;
        } else {
            this.filterConsumeStatus = RuleStatus.STATUS_DISABLE;
        }
    }

    public String getFilterCondStr() {
        return filterCondStr;
    }

    public void setFilterCondStr(String filterCondStr) {
        this.filterCondStr = filterCondStr;
    }
}
