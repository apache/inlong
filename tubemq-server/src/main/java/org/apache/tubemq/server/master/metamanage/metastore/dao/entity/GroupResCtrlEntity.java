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
import org.apache.tubemq.server.common.TServerConstants;
import org.apache.tubemq.server.common.statusdef.EnableStatus;
import org.apache.tubemq.server.master.bdbstore.bdbentitys.BdbGroupFlowCtrlEntity;

/*
 * store the group resource control setting
 *
 */
public class GroupResCtrlEntity extends BaseEntity implements Cloneable {
    // group name
    private String groupName = "";
    // Enable consume control, global control
    private EnableStatus consumeEnable = EnableStatus.STATUS_UNDEFINE;
    private String disableReason = "";
    // resource check control
    private EnableStatus resCheckStatus = EnableStatus.STATUS_UNDEFINE;
    private int allowedBrokerClientRate = TBaseConstants.META_VALUE_UNDEFINED;
    // consume priority id
    private int qryPriorityId = TBaseConstants.META_VALUE_UNDEFINED;
    // consume group flow control info
    private EnableStatus flowCtrlStatus = EnableStatus.STATUS_UNDEFINE;
    private int ruleCnt = 0;           // flow control rule count
    private String flowCtrlInfo = "";  // flow control info


    public GroupResCtrlEntity() {
        super();
    }

    public GroupResCtrlEntity(BaseEntity opEntity, String groupName) {
        super(opEntity);
        this.groupName = groupName;
    }

    public GroupResCtrlEntity(String groupName,
                              boolean enableConsume, String disableRsn,
                              boolean enableFlowCtrl,
                              int qryPriorityId, int ruleCnt,
                              String flowCtrlInfo, String createUser,
                              Date createDate) {
        super(createUser, createDate);
        this.groupName = groupName;
        if (enableConsume) {
            this.consumeEnable = EnableStatus.STATUS_ENABLE;
        } else {
            this.consumeEnable = EnableStatus.STATUS_DISABLE;
        }
        this.disableReason = disableRsn;
        if (enableFlowCtrl) {
            this.flowCtrlStatus = EnableStatus.STATUS_ENABLE;
        } else {
            this.flowCtrlStatus = EnableStatus.STATUS_DISABLE;
        }
        this.qryPriorityId = qryPriorityId;
        this.ruleCnt = ruleCnt;
        this.flowCtrlInfo = flowCtrlInfo;
    }

    public GroupResCtrlEntity(BdbGroupFlowCtrlEntity bdbEntity) {
        super(bdbEntity.getSerialId(),
                bdbEntity.getCreateUser(), bdbEntity.getCreateDate());
        this.groupName = bdbEntity.getGroupName();
        this.consumeEnable = bdbEntity.getConsumeEnable();
        this.disableReason = bdbEntity.getDisableConsumeReason();
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
                new BdbGroupFlowCtrlEntity(getDataVerId(), this.groupName,
                        this.flowCtrlInfo, statusId, this.ruleCnt, this.qryPriorityId,
                        getAttributes(), getCreateUser(), getCreateDate());
        bdbEntity.setConsumeEnable(consumeEnable);
        bdbEntity.setDisableConsumeReason(disableReason);
        bdbEntity.setResCheckStatus(resCheckStatus);
        bdbEntity.setAllowedBrokerClientRate(allowedBrokerClientRate);
        return bdbEntity;
    }

    /**
     * fill fields with default value
     *
     * @return object
     */
    public GroupResCtrlEntity fillDefaultValue() {
        this.consumeEnable = EnableStatus.STATUS_ENABLE;
        this.disableReason = "";
        this.resCheckStatus = EnableStatus.STATUS_DISABLE;
        this.allowedBrokerClientRate = 0;
        this.qryPriorityId = TServerConstants.QRY_PRIORITY_DEF_VALUE;
        this.flowCtrlStatus = EnableStatus.STATUS_DISABLE;
        this.flowCtrlInfo = TServerConstants.BLANK_FILTER_ITEM_STR;
        return this;
    }

    public GroupResCtrlEntity setGroupName(String groupName) {
        this.groupName = groupName;
        return this;
    }

    public String getGroupName() {
        return groupName;
    }

    public int getRuleCnt() {
        return ruleCnt;
    }

    public String getFlowCtrlInfo() {
        return flowCtrlInfo;
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

    public EnableStatus getConsumeEnable() {
        return consumeEnable;
    }

    public String getDisableReason() {
        return disableReason;
    }

    public void setDisableReason(String disableReason) {
        this.disableReason = disableReason;
    }

    private void setResCheckStatus(boolean enableResChk) {
        if (enableResChk) {
            this.resCheckStatus = EnableStatus.STATUS_ENABLE;
        } else {
            this.resCheckStatus = EnableStatus.STATUS_DISABLE;
        }
    }

    private void setFlowCtrlStatus(boolean enableFlowCtrl) {
        if (enableFlowCtrl) {
            this.flowCtrlStatus = EnableStatus.STATUS_ENABLE;
        } else {
            this.flowCtrlStatus = EnableStatus.STATUS_DISABLE;
        }
    }

    private void setFlowCtrlRule(int ruleCnt, String flowCtrlInfo) {
        this.ruleCnt = ruleCnt;
        this.flowCtrlInfo = flowCtrlInfo;
    }

    private void setConsumeEnable(boolean enableConsume) {
        if (enableConsume) {
            this.consumeEnable = EnableStatus.STATUS_ENABLE;
        } else {
            this.consumeEnable = EnableStatus.STATUS_DISABLE;
        }
    }

    /**
     * update subclass field values
     *
     * @return if changed
     */
    public boolean updModifyInfo(long dataVerId, Boolean enableConsume, String disableRsn,
                                 Boolean resChkEnable, int allowedB2CRate,
                                 int qryPriorityId, Boolean flowCtrlEnable,
                                 int flowRuleCnt, String flowCtrlRuleInfo) {
        boolean changed = false;
        // check and set dataVerId info
        if (dataVerId != TBaseConstants.META_VALUE_UNDEFINED
                && this.getDataVerId() != dataVerId) {
            changed = true;
            this.setDataVersionId(dataVerId);
        }
        // check and set resCheckStatus info
        if (enableConsume != null
                && this.consumeEnable.isEnable() != enableConsume) {
            changed = true;
            setConsumeEnable(enableConsume);
        }
        // check and set disableReason info
        if (TStringUtils.isNotBlank(disableRsn)
                && !disableRsn.equals(disableReason)) {
            changed = true;
            this.disableReason = disableRsn;
        }
        // check and set resCheckStatus info
        if (resChkEnable != null
                && this.resCheckStatus.isEnable() != resChkEnable) {
            changed = true;
            setResCheckStatus(resChkEnable);
        }
        // check and set allowed broker client rate
        if (allowedB2CRate != TBaseConstants.META_VALUE_UNDEFINED
                && this.allowedBrokerClientRate != allowedB2CRate) {
            changed = true;
            this.allowedBrokerClientRate = allowedB2CRate;
        }
        // check and set qry priority id
        if (qryPriorityId != TBaseConstants.META_VALUE_UNDEFINED
                && this.qryPriorityId != qryPriorityId) {
            changed = true;
            this.qryPriorityId = qryPriorityId;
        }
        // check and set flowCtrl info
        if (flowCtrlEnable != null
                && this.flowCtrlStatus.isEnable() != flowCtrlEnable) {
            changed = true;
            setFlowCtrlStatus(flowCtrlEnable);
        }
        // check and set flowCtrlInfo info
        if (TStringUtils.isNotBlank(flowCtrlRuleInfo)
                && !flowCtrlRuleInfo.equals(flowCtrlInfo)) {
            changed = true;
            setFlowCtrlRule(flowRuleCnt, flowCtrlRuleInfo);
        }
        if (changed) {
            updSerialId();
        }
        return changed;
    }

    /**
     * Check whether the specified query item value matches
     * Allowed query items:
     *   groupName, qryPriorityId, resCheckStatus,
     *   flowCtrlStatus, allowedBrokerClientRate
     * @return true: matched, false: not match
     */
    public boolean isMatched(GroupResCtrlEntity target) {
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
                || (target.getConsumeEnable() != EnableStatus.STATUS_UNDEFINE
                && target.getConsumeEnable() != this.consumeEnable)
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

    /**
     * Serialize field to json format
     *
     * @param sBuilder   build container
     * @param isLongName if return field key is long name
     * @param fullFormat if return full format json
     * @return
     */
    public StringBuilder toWebJsonStr(StringBuilder sBuilder,
                                      boolean isLongName,
                                      boolean fullFormat) {
        if (isLongName) {
            sBuilder.append("{\"groupName\":\"").append(groupName).append("\"")
                    .append(",\"consumeEnable\":").append(consumeEnable.isEnable())
                    .append(",\"reason\":\"").append(disableReason).append("\"")
                    .append(",\"resCheckEnable\":").append(resCheckStatus.isEnable())
                    .append(",\"alwdBrokerClientRate\":").append(allowedBrokerClientRate)
                    .append(",\"qryPriorityId\":").append(qryPriorityId)
                    .append(",\"flowCtrlEnable\":").append(flowCtrlStatus.isEnable())
                    .append(",\"flowCtrlRuleCount\":").append(ruleCnt)
                    .append(",\"flowCtrlInfo\":").append(flowCtrlInfo);
        } else {
            sBuilder.append("{\"group\":\"").append(groupName).append("\"")
                    .append(",\"csmEn\":").append(consumeEnable.isEnable())
                    .append(",\"rsn\":\"").append(disableReason).append("\"")
                    .append(",\"resChkEn\":").append(resCheckStatus.isEnable())
                    .append(",\"abcr\":").append(allowedBrokerClientRate)
                    .append(",\"qryPriId\":").append(qryPriorityId)
                    .append(",\"fCtrlEn\":").append(flowCtrlStatus.isEnable())
                    .append(",\"fCtrlCnt\":").append(ruleCnt)
                    .append(",\"fCtrlInfo\":").append(flowCtrlInfo);
        }
        super.toWebJsonStr(sBuilder, isLongName);
        if (fullFormat) {
            sBuilder.append("}");
        }
        return sBuilder;
    }

    /**
     * check if subclass fields is equals
     *
     * @param other  check object
     * @return if equals
     */
    public boolean isDataEquals(GroupResCtrlEntity other) {
        return allowedBrokerClientRate == other.allowedBrokerClientRate
                && qryPriorityId == other.qryPriorityId
                && ruleCnt == other.ruleCnt
                && groupName.equals(other.groupName)
                && consumeEnable == other.consumeEnable
                && Objects.equals(disableReason, other.disableReason)
                && resCheckStatus == other.resCheckStatus
                && flowCtrlStatus == other.flowCtrlStatus
                && Objects.equals(flowCtrlInfo, other.flowCtrlInfo);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof GroupResCtrlEntity)) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        GroupResCtrlEntity that = (GroupResCtrlEntity) o;
        return isDataEquals(that);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), groupName, consumeEnable,
                disableReason, resCheckStatus, allowedBrokerClientRate,
                qryPriorityId, flowCtrlStatus, ruleCnt, flowCtrlInfo);
    }

    @Override
    public GroupResCtrlEntity clone() {
        GroupResCtrlEntity copy = (GroupResCtrlEntity) super.clone();
        copy.setConsumeEnable(getConsumeEnable().isEnable());
        copy.setFlowCtrlStatus(getFlowCtrlStatus());
        copy.setResCheckStatus(getResCheckStatus());
        return copy;
    }

}
