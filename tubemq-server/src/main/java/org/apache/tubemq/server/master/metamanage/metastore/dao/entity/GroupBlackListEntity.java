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

import java.util.Date;
import java.util.Objects;
import org.apache.tubemq.corebase.utils.KeyBuilderUtils;
import org.apache.tubemq.corebase.utils.TStringUtils;
import org.apache.tubemq.server.master.bdbstore.bdbentitys.BdbBlackGroupEntity;


/*
 * store group black list setting
 *
 */
public class GroupBlackListEntity extends BaseEntity implements Cloneable {

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
        this.recordKey = KeyBuilderUtils.buildGroupTopicRecKey(groupName, topicName);
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

    /**
     * Check whether the specified query item value matches
     * Allowed query items:
     *   groupName, topicName
     * @return true: matched, false: not match
     */
    public boolean isMatched(GroupBlackListEntity target) {
        if (target == null) {
            return true;
        }
        if (!super.isMatched(target)) {
            return false;
        }
        if ((TStringUtils.isNotBlank(target.getTopicName())
                && !target.getTopicName().equals(this.topicName))
                || (TStringUtils.isNotBlank(target.getGroupName())
                && !target.getGroupName().equals(this.groupName))) {
            return false;
        }
        return true;
    }

    /**
     * Serialize field to json format
     *
     * @param sBuilder   build container
     * @param isLongName if return field key is long name
     * @return
     */
    @Override
    public StringBuilder toWebJsonStr(StringBuilder sBuilder, boolean isLongName) {
        if (isLongName) {
            sBuilder.append("{\"topicName\":\"").append(topicName).append("\"")
                    .append(",\"groupName\":\"").append(groupName).append("\"")
                    .append(",\"reason\":\"").append(reason).append("\"");
        } else {
            sBuilder.append("{\"topic\":\"").append(topicName).append("\"")
                    .append(",\"group\":\"").append(groupName).append("\"")
                    .append(",\"rsn\":\"").append(reason).append("\"");
        }
        super.toWebJsonStr(sBuilder, isLongName);
        sBuilder.append("}");
        return sBuilder;
    }

    /**
     * check if subclass fields is equals
     *
     * @param other  check object
     * @return if equals
     */
    public boolean isDataEquals(GroupBlackListEntity other) {
        return Objects.equals(recordKey, other.recordKey)
                && Objects.equals(topicName, other.topicName)
                && Objects.equals(groupName, other.groupName)
                && Objects.equals(reason, other.reason);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof GroupBlackListEntity)) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        GroupBlackListEntity that = (GroupBlackListEntity) o;
        return isDataEquals(that);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), recordKey, topicName, groupName, reason);
    }

    @Override
    public GroupBlackListEntity clone() {
        try {
            GroupBlackListEntity copy = (GroupBlackListEntity) super.clone();
            return copy;
        } catch (CloneNotSupportedException e) {
            return null;
        }
    }
}
