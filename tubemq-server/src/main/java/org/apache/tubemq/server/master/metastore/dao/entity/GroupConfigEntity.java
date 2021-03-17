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
import org.apache.tubemq.server.common.statusdef.EnableStatus;
import org.apache.tubemq.server.master.bdbstore.bdbentitys.BdbGroupFlowCtrlEntity;

/*
 * store the group control setting
 *
 */
public class GroupConfigEntity extends BaseEntity {
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


    public GroupConfigEntity() {
        super();
    }

    public GroupConfigEntity(String groupName, boolean enableFlowCtrl,
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

    public GroupConfigEntity(BdbGroupFlowCtrlEntity bdbEntity) {
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

}
