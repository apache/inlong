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
import org.apache.tubemq.server.common.statusdef.EnableStatus;
import org.apache.tubemq.server.master.bdbstore.bdbentitys.BdbGroupFilterCondEntity;


/*
 * store the group filter consume control setting
 *
 */
public class GroupFilterCtrlEntity extends BaseEntity {
    private String recordKey = "";
    private String topicName = "";
    private String groupName = "";
    // filter consume setting
    private EnableStatus filterConsumeStatus = EnableStatus.STATUS_UNDEFINE;
    private String filterCondStr = "";


    public GroupFilterCtrlEntity() {
        super();
    }

    public GroupFilterCtrlEntity(String topicName, String groupName,
                                 EnableStatus filterConsumeStatus,
                                 String filterCondStr,  String createUser,
                                 Date createDate) {
        super(createUser, createDate);
        this.setTopicAndGroup(topicName, groupName);
        this.filterConsumeStatus = filterConsumeStatus;
        this.filterCondStr = filterCondStr;
    }

    public GroupFilterCtrlEntity(BdbGroupFilterCondEntity bdbEntity) {
        super(bdbEntity.getDataVerId(),
                bdbEntity.getCreateUser(), bdbEntity.getCreateDate());
        this.setTopicAndGroup(bdbEntity.getTopicName(),
                bdbEntity.getConsumerGroupName());
        if (bdbEntity.getControlStatus() == 2) {
            this.filterConsumeStatus = EnableStatus.STATUS_ENABLE;
        } else if (bdbEntity.getControlStatus() == -2) {
            this.filterConsumeStatus = EnableStatus.STATUS_UNDEFINE;
        } else {
            this.filterConsumeStatus = EnableStatus.STATUS_DISABLE;
        }
        this.filterCondStr = bdbEntity.getFilterCondStr();
        this.setAttributes(bdbEntity.getAttributes());
    }

    public BdbGroupFilterCondEntity buildBdbGroupFilterCondEntity() {
        BdbGroupFilterCondEntity bdbEntity =
                new BdbGroupFilterCondEntity(topicName, groupName,
                        filterConsumeStatus.getCode(), filterCondStr,
                        getAttributes(), getCreateUser(), getCreateDate());
        bdbEntity.setDataVerId(getDataVersionId());
        return bdbEntity;
    }

    public void setTopicAndGroup(String topicName, String groupName) {
        this.topicName = topicName;
        this.groupName = groupName;
        this.recordKey = new StringBuilder(TBaseConstants.BUILDER_DEFAULT_SIZE)
                .append(topicName).append(TokenConstants.ATTR_SEP)
                .append(groupName).toString();
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


    public boolean isEnableFilterConsume() {
        return filterConsumeStatus == EnableStatus.STATUS_ENABLE;
    }

    public void setFilterConsumeStatus(boolean enableFilterConsume) {
        if (enableFilterConsume) {
            this.filterConsumeStatus = EnableStatus.STATUS_ENABLE;
        } else {
            this.filterConsumeStatus = EnableStatus.STATUS_DISABLE;
        }
    }

    public String getFilterCondStr() {
        return filterCondStr;
    }

    public void setFilterCondStr(String filterCondStr) {
        this.filterCondStr = filterCondStr;
    }

}
