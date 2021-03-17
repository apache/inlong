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
import org.apache.tubemq.server.master.bdbstore.bdbentitys.BdbBlackGroupEntity;


/*
 * store group black list setting
 *
 */
public class GroupBlackListEntity extends BaseEntity {

    private String recordKey = "";
    private String topicName = "";
    private String groupName = "";
    private String reason = "";

    public GroupBlackListEntity() {
        super();
    }

    public GroupBlackListEntity(String topicName, String groupName,
                                String reason, String createUser, Date createDate) {
        super(createUser, createDate);
        this.setTopicAndGroup(topicName, groupName);
        this.reason = reason;
    }

    public GroupBlackListEntity(BdbBlackGroupEntity bdbEntity) {
        super(bdbEntity.getDataVerId(),
                bdbEntity.getCreateUser(), bdbEntity.getCreateDate());
        this.setTopicAndGroup(bdbEntity.getTopicName(),
                bdbEntity.getBlackGroupName());
        this.reason = bdbEntity.getReason();
        this.setAttributes(bdbEntity.getAttributes());
    }

    public BdbBlackGroupEntity buildBdbBlackListEntity() {
        BdbBlackGroupEntity bdbEntity =
                new BdbBlackGroupEntity(topicName, groupName,
                        getAttributes(), getCreateUser(), getCreateDate());
        bdbEntity.setDataVerId(getDataVersionId());
        bdbEntity.setReason(reason);
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

    public String getTopicName() {
        return topicName;
    }

    public String getGroupName() {
        return groupName;
    }

    public String getReason() {
        return reason;
    }

}
