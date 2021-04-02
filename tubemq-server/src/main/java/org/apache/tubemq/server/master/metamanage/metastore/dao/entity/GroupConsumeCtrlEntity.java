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
import org.apache.tubemq.server.common.statusdef.EnableStatus;
import org.apache.tubemq.server.master.bdbstore.bdbentitys.BdbGroupFilterCondEntity;


/*
 * store the group consume control setting
 *
 */
public class GroupConsumeCtrlEntity extends BaseEntity implements Cloneable {
    private String recordKey = "";
    private String topicName = "";
    private String groupName = "";
    // filter consume setting
    private EnableStatus filterConsumeStatus = EnableStatus.STATUS_UNDEFINE;
    private String filterCondStr = "";


    public GroupConsumeCtrlEntity() {
        super();
    }

    public GroupConsumeCtrlEntity(String topicName, String groupName,
                                  EnableStatus filterConsumeStatus,
                                  String filterCondStr, String createUser,
                                  Date createDate) {
        super(createUser, createDate);
        this.setTopicAndGroup(topicName, groupName);
        this.filterConsumeStatus = filterConsumeStatus;
        this.filterCondStr = filterCondStr;
    }

    public GroupConsumeCtrlEntity(BdbGroupFilterCondEntity bdbEntity) {
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
        this.recordKey = KeyBuilderUtils.buildGroupTopicRecKey(groupName, topicName);
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

    public void setFilterConsumeStatus(EnableStatus filterConsumeStatus) {
        this.filterConsumeStatus = filterConsumeStatus;
    }

    public boolean isEnableFilterConsume() {
        return filterConsumeStatus == EnableStatus.STATUS_ENABLE;
    }

    public void setEnableFilterConsume(boolean enableFilterConsume) {
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

    public EnableStatus getFilterConsumeStatus() {
        return filterConsumeStatus;
    }

    /**
     * Check whether the specified query item value matches
     * Allowed query items:
     *   topicName, groupName, filterConsumeStatus
     * @return true: matched, false: not match
     */
    public boolean isMatched(GroupConsumeCtrlEntity target) {
        if (target == null) {
            return true;
        }
        if (!super.isMatched(target)) {
            return false;
        }
        if ((TStringUtils.isNotBlank(target.getTopicName())
                && !target.getTopicName().equals(this.topicName))
                || (TStringUtils.isNotBlank(target.getGroupName())
                && !target.getGroupName().equals(this.groupName))
                || (target.getFilterConsumeStatus() != EnableStatus.STATUS_UNDEFINE
                && target.getFilterConsumeStatus() != this.filterConsumeStatus)) {
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
            sBuilder.append("{\"recordKey\":\"").append(recordKey).append("\"")
                    .append(",\"topicName\":\"").append(topicName).append("\"")
                    .append(",\"groupName\":\"").append(groupName).append("\"")
                    .append(",\"filterEnable\":").append(filterConsumeStatus.isEnable())
                    .append(",\"filterConds\":\"").append(filterCondStr).append("\"");
        } else {
            sBuilder.append("{\"recKey\":\"").append(recordKey).append("\"")
                    .append(",\"topic\":\"").append(topicName).append("\"")
                    .append(",\"group\":\"").append(groupName).append("\"")
                    .append(",\"fltEn\":").append(filterConsumeStatus.isEnable())
                    .append(",\"fltRls\":\"").append(filterCondStr).append("\"");
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
    public boolean isDataEquals(GroupConsumeCtrlEntity other) {
        return recordKey.equals(other.recordKey)
                && Objects.equals(topicName, other.topicName)
                && Objects.equals(groupName, other.groupName)
                && filterConsumeStatus == other.filterConsumeStatus
                && Objects.equals(filterCondStr, other.filterCondStr);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof GroupConsumeCtrlEntity)) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        GroupConsumeCtrlEntity that = (GroupConsumeCtrlEntity) o;
        return isDataEquals(that);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), recordKey,
                topicName, groupName, filterConsumeStatus, filterCondStr);
    }

    @Override
    public GroupConsumeCtrlEntity clone() {
        try {
            GroupConsumeCtrlEntity copy = (GroupConsumeCtrlEntity) super.clone();
            copy.setFilterConsumeStatus(getFilterConsumeStatus());
            return copy;
        } catch (CloneNotSupportedException e) {
            return null;
        }
    }
}
