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
import org.apache.tubemq.corebase.TBaseConstants;
import org.apache.tubemq.corebase.utils.TStringUtils;
import org.apache.tubemq.server.common.statusdef.EnableStatus;
import org.apache.tubemq.server.master.bdbstore.bdbentitys.BdbGroupFlowCtrlEntity;

/*
 * store the group control setting
 *
 */
public class GroupBaseCtrlEntity extends BaseEntity {
    // group name
    private String groupName = "";
    // resource check control
    private EnableStatus resCheckStatus = EnableStatus.STATUS_UNDEFINE;
    private int allowedBrokerClientRate = TBaseConstants.META_VALUE_UNDEFINED;
    // consume priority id
    private int qryPriorityId = TBaseConstants.META_VALUE_UNDEFINED;
    // consume group flow control info
    private EnableStatus flowCtrlStatus = EnableStatus.STATUS_UNDEFINE;
    private int ruleCnt = 0;           // flow control rule count
    private String flowCtrlInfo = "";  // flow control info


    public GroupBaseCtrlEntity() {
        super();
    }

    public GroupBaseCtrlEntity(String groupName, boolean enableFlowCtrl,
                               int qryPriorityId, int ruleCnt,
                               String flowCtrlInfo, String createUser,
                               Date createDate) {
        super(createUser, createDate);
        this.groupName = groupName;
        if (enableFlowCtrl) {
            this.flowCtrlStatus = EnableStatus.STATUS_ENABLE;
        } else {
            this.flowCtrlStatus = EnableStatus.STATUS_DISABLE;
        }
        this.qryPriorityId = qryPriorityId;
        this.ruleCnt = ruleCnt;
        this.flowCtrlInfo = flowCtrlInfo;
    }

    public GroupBaseCtrlEntity(BdbGroupFlowCtrlEntity bdbEntity) {
        super(bdbEntity.getSerialId(),
                bdbEntity.getCreateUser(), bdbEntity.getCreateDate());
        this.groupName = bdbEntity.getGroupName();
        this.qryPriorityId = bdbEntity.getQryPriorityId();
        this.ruleCnt = bdbEntity.getRuleCnt();
        this.flowCtrlInfo = bdbEntity.getFlowCtrlInfo();
        if (bdbEntity.getStatusId() != 0) {
            this.flowCtrlStatus = EnableStatus.STATUS_ENABLE;
        } else {
            this.flowCtrlStatus = EnableStatus.STATUS_DISABLE;
        }
        setAttributes(bdbEntity.getAttributes());
    }

    public BdbGroupFlowCtrlEntity buildBdbGroupFlowCtrlEntity() {
        //Constructor
        int statusId = (this.flowCtrlStatus == EnableStatus.STATUS_ENABLE) ? 1 : 0;
        BdbGroupFlowCtrlEntity bdbEntity =
                new BdbGroupFlowCtrlEntity(getDataVersionId(), this.groupName,
                        this.flowCtrlInfo, statusId, this.ruleCnt, this.qryPriorityId,
                        getAttributes(), getCreateUser(), getCreateDate());
        bdbEntity.setResCheckStatus(resCheckStatus);
        bdbEntity.setAllowedBrokerClientRate(allowedBrokerClientRate);
        return bdbEntity;
    }

    public String getGroupName() {
        return groupName;
    }

    public void setGroupName(String groupName) {
        this.groupName = groupName;
    }

    public void setFlowCtrlRule(int ruleCnt, String flowCtrlInfo) {
        this.ruleCnt = ruleCnt;
        this.flowCtrlInfo = flowCtrlInfo;
    }

    public int getRuleCnt() {
        return ruleCnt;
    }

    public String getFlowCtrlInfo() {
        return flowCtrlInfo;
    }

    public void setFlowCtrlStatus(boolean enableFlowCtrl) {
        if (enableFlowCtrl) {
            this.flowCtrlStatus = EnableStatus.STATUS_ENABLE;
        } else {
            this.flowCtrlStatus = EnableStatus.STATUS_DISABLE;
        }
    }

    public int getQryPriorityId() {
        return qryPriorityId;
    }

    public void setQryPriorityId(int qryPriorityId) {
        this.qryPriorityId = qryPriorityId;
    }

    public boolean isFlowCtrlEnable() {
        return (this.flowCtrlStatus == EnableStatus.STATUS_ENABLE);
    }

    public boolean isEnableResCheck() {
        return resCheckStatus == EnableStatus.STATUS_ENABLE;
    }

    public void setResCheckStatus(boolean enableResChk, int allowedBrokerClientRate) {
        if (enableResChk) {
            this.resCheckStatus = EnableStatus.STATUS_ENABLE;
        } else {
            this.resCheckStatus = EnableStatus.STATUS_DISABLE;
        }
        this.allowedBrokerClientRate = allowedBrokerClientRate;
    }

    public int getAllowedBrokerClientRate() {
        return allowedBrokerClientRate;
    }

    public void setAllowedBrokerClientRate(int allowedBrokerClientRate) {
        this.allowedBrokerClientRate = allowedBrokerClientRate;
    }

    public EnableStatus getResCheckStatus() {
        return resCheckStatus;
    }

    public void setResCheckStatus(EnableStatus resCheckStatus) {
        this.resCheckStatus = resCheckStatus;
    }

    public EnableStatus getFlowCtrlStatus() {
        return flowCtrlStatus;
    }

    public void setFlowCtrlStatus(EnableStatus flowCtrlStatus) {
        this.flowCtrlStatus = flowCtrlStatus;
    }

    /**
     * Check whether the specified query item value matches
     * Allowed query items:
     *   groupName, qryPriorityId, resCheckStatus,
     *   flowCtrlStatus, allowedBrokerClientRate
     * @return true: matched, false: not match
     */
    public boolean isMatched(GroupBaseCtrlEntity target) {
        if (target == null) {
            return true;
        }
        if (!super.isMatched(target)) {
            return false;
        }
        if ((target.getQryPriorityId() != TBaseConstants.META_VALUE_UNDEFINED
                && target.getQryPriorityId() != this.qryPriorityId)
                || (TStringUtils.isNotBlank(target.getGroupName())
                && !target.getGroupName().equals(this.groupName))
                || (target.getResCheckStatus() != EnableStatus.STATUS_UNDEFINE
                && target.getResCheckStatus() != this.resCheckStatus)
                || (target.getFlowCtrlStatus() != EnableStatus.STATUS_UNDEFINE
                && target.getFlowCtrlStatus() != this.flowCtrlStatus)
                || (target.getAllowedBrokerClientRate() != TBaseConstants.META_VALUE_UNDEFINED
                && target.getAllowedBrokerClientRate() != this.allowedBrokerClientRate)) {
            return false;
        }
        return true;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof GroupBaseCtrlEntity)) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        GroupBaseCtrlEntity that = (GroupBaseCtrlEntity) o;
        return allowedBrokerClientRate == that.allowedBrokerClientRate &&
                qryPriorityId == that.qryPriorityId &&
                ruleCnt == that.ruleCnt &&
                groupName.equals(that.groupName) &&
                resCheckStatus == that.resCheckStatus &&
                flowCtrlStatus == that.flowCtrlStatus &&
                Objects.equals(flowCtrlInfo, that.flowCtrlInfo);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), groupName, resCheckStatus,
                allowedBrokerClientRate, qryPriorityId, flowCtrlStatus,
                ruleCnt, flowCtrlInfo);
    }
}
