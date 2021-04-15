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
import org.apache.tubemq.corebase.utils.SettingValidUtils;
import org.apache.tubemq.corebase.utils.TStringUtils;
import org.apache.tubemq.server.common.TServerConstants;
import org.apache.tubemq.server.common.statusdef.EnableStatus;
import org.apache.tubemq.server.master.bdbstore.bdbentitys.BdbClusterSettingEntity;
import org.apache.tubemq.server.master.metamanage.metastore.TStoreConstants;



/*
 * store the cluster default setting
 *
 */
public class ClusterSettingEntity extends BaseEntity implements Cloneable {

    private String recordKey =
            TStoreConstants.TOKEN_DEFAULT_CLUSTER_SETTING;
    // broker tcp port
    private int brokerPort = TBaseConstants.META_VALUE_UNDEFINED;
    // broker tls port
    private int brokerTLSPort = TBaseConstants.META_VALUE_UNDEFINED;
    // broker web port
    private int brokerWebPort = TBaseConstants.META_VALUE_UNDEFINED;
    private TopicPropGroup clsDefTopicProps = new TopicPropGroup();
    private int maxMsgSizeInB = TBaseConstants.META_VALUE_UNDEFINED;
    private int maxMsgSizeInMB = TBaseConstants.META_VALUE_UNDEFINED;
    private int qryPriorityId = TBaseConstants.META_VALUE_UNDEFINED;
    private EnableStatus gloFlowCtrlStatus = EnableStatus.STATUS_UNDEFINE;
    // flow control rule count
    private int gloFlowCtrlRuleCnt = TBaseConstants.META_VALUE_UNDEFINED;
    // flow control info
    private String gloFlowCtrlRuleInfo = "";



    public ClusterSettingEntity() {
        super();
    }

    public ClusterSettingEntity(long dataVerId, String createUser, Date createDate) {
        super(dataVerId, createUser, createDate);
    }

    // Constructor by BdbClusterSettingEntity
    public ClusterSettingEntity(BdbClusterSettingEntity bdbEntity) {
        super(bdbEntity.getConfigId(), bdbEntity.getModifyUser(), bdbEntity.getModifyDate());
        this.brokerPort = bdbEntity.getBrokerPort();
        this.brokerTLSPort = bdbEntity.getBrokerTLSPort();
        this.brokerWebPort = bdbEntity.getBrokerWebPort();
        this.clsDefTopicProps =
                new TopicPropGroup(bdbEntity.getNumTopicStores(), bdbEntity.getNumPartitions(),
                        bdbEntity.getUnflushThreshold(), bdbEntity.getUnflushInterval(),
                        bdbEntity.getUnflushDataHold(), bdbEntity.getMemCacheMsgSizeInMB(),
                        bdbEntity.getMemCacheMsgCntInK(), bdbEntity.getMemCacheFlushIntvl(),
                        bdbEntity.isAcceptPublish(), bdbEntity.isAcceptSubscribe(),
                        bdbEntity.getDeletePolicy(), bdbEntity.getDefDataType(),
                        bdbEntity.getDefDataPath());
        this.maxMsgSizeInB = bdbEntity.getMaxMsgSizeInB();
        this.maxMsgSizeInMB =
                this.maxMsgSizeInB / TBaseConstants.META_MB_UNIT_SIZE;
        this.qryPriorityId = bdbEntity.getQryPriorityId();
        setEnableFlowCtrl(bdbEntity.getEnableGloFlowCtrl());
        setGloFlowCtrlInfo(bdbEntity.getGloFlowCtrlCnt(), bdbEntity.getGloFlowCtrlInfo());
        setAttributes(bdbEntity.getAttributes());
    }

    // build bdb object from current info
    public BdbClusterSettingEntity buildBdbClsDefSettingEntity() {
        BdbClusterSettingEntity bdbEntity =
                new BdbClusterSettingEntity(recordKey, getDataVersionId(), brokerPort,
                        brokerTLSPort, brokerWebPort, clsDefTopicProps.getNumTopicStores(),
                        clsDefTopicProps.getNumPartitions(), clsDefTopicProps.getUnflushThreshold(),
                        clsDefTopicProps.getUnflushInterval(), clsDefTopicProps.getUnflushDataHold(),
                        clsDefTopicProps.getMemCacheMsgCntInK(), clsDefTopicProps.getMemCacheFlushIntvl(),
                        clsDefTopicProps.getMemCacheMsgSizeInMB(), clsDefTopicProps.isAcceptPublish(),
                        clsDefTopicProps.isAcceptSubscribe(), clsDefTopicProps.getDeletePolicy(),
                        this.qryPriorityId, this.maxMsgSizeInB, getAttributes(),
                        getModifyUser(), getModifyDate());
        bdbEntity.setDefDataPath(clsDefTopicProps.getDataPath());
        bdbEntity.setDefDataType(clsDefTopicProps.getDataStoreType());
        bdbEntity.setEnableGloFlowCtrl(enableFlowCtrl());
        bdbEntity.setGloFlowCtrlCnt(gloFlowCtrlRuleCnt);
        bdbEntity.setGloFlowCtrlInfo(gloFlowCtrlRuleInfo);
        return bdbEntity;
    }

    /**
     * fill fields with default value
     *
     * @return object
     */
    public ClusterSettingEntity fillDefaultValue() {
        this.brokerPort = TBaseConstants.META_DEFAULT_BROKER_PORT;
        this.brokerTLSPort = TBaseConstants.META_DEFAULT_BROKER_TLS_PORT;
        this.brokerWebPort = TBaseConstants.META_DEFAULT_BROKER_WEB_PORT;
        this.maxMsgSizeInMB = TBaseConstants.META_MIN_ALLOWED_MESSAGE_SIZE_MB;
        this.maxMsgSizeInB =
                SettingValidUtils.validAndXfeMaxMsgSizeFromMBtoB(this.maxMsgSizeInMB);
        this.qryPriorityId = TServerConstants.QRY_PRIORITY_DEF_VALUE;
        this.gloFlowCtrlStatus = EnableStatus.STATUS_DISABLE;
        this.gloFlowCtrlRuleCnt = 0;
        this.gloFlowCtrlRuleInfo = TServerConstants.BLANK_FLOWCTRL_RULES;
        this.clsDefTopicProps.fillDefaultValue();
        return this;
    }

    /**
     * update subclass field values
     *
     * @return if changed
     */
    public boolean updModifyInfo(int newBrokerPort, int newBrokerTLSPort,
                                 int newBrokerWebPort, int newMaxMsgSizeMB,
                                 int newQryPriorityId, Boolean newFlowCtrlEnable,
                                 int flowRuleCnt, String newFlowCtrlRuleInfo,
                                 TopicPropGroup newDefTopicProps) {
        boolean changed = false;
        if (newBrokerPort != TBaseConstants.META_VALUE_UNDEFINED
                && this.brokerPort != newBrokerPort) {
            changed = true;
            this.brokerPort = newBrokerPort;
        }
        if (newBrokerTLSPort != TBaseConstants.META_VALUE_UNDEFINED
                && this.brokerTLSPort != newBrokerTLSPort) {
            changed = true;
            this.brokerTLSPort = newBrokerTLSPort;
        }
        if (newBrokerWebPort != TBaseConstants.META_VALUE_UNDEFINED
                && this.brokerWebPort != newBrokerWebPort) {
            changed = true;
            this.brokerWebPort = newBrokerWebPort;
        }
        // check and set modified field
        if (newMaxMsgSizeMB != TBaseConstants.META_VALUE_UNDEFINED) {
            int newMaxMsgSizeB =
                    SettingValidUtils.validAndXfeMaxMsgSizeFromMBtoB(newMaxMsgSizeMB);
            if (this.maxMsgSizeInB != newMaxMsgSizeB) {
                changed = true;
                this.maxMsgSizeInB = newMaxMsgSizeB;
                this.maxMsgSizeInMB = newMaxMsgSizeMB;
            }
        }
        // check and set qry priority id
        if (newQryPriorityId != TBaseConstants.META_VALUE_UNDEFINED
                && this.qryPriorityId != newQryPriorityId) {
            changed = true;
            this.qryPriorityId = newQryPriorityId;
        }
        // check and set flowCtrl info
        if (newFlowCtrlEnable != null
                && this.gloFlowCtrlStatus.isEnable() != newFlowCtrlEnable) {
            changed = true;
            setEnableFlowCtrl(newFlowCtrlEnable);
        }
        if (TStringUtils.isNotBlank(newFlowCtrlRuleInfo)
                && !newFlowCtrlRuleInfo.equals(gloFlowCtrlRuleInfo)) {
            changed = true;
            setGloFlowCtrlInfo(flowRuleCnt, newFlowCtrlRuleInfo);
        }
        // check and set clsDefTopicProps info
        if (newDefTopicProps != null
                && !newDefTopicProps.isDataEquals(clsDefTopicProps)) {
            changed = true;
            clsDefTopicProps = newDefTopicProps;
        }
        if (changed) {
            updSerialId();
        }
        return changed;
    }

    public String getRecordKey() {
        return recordKey;
    }

    public int getBrokerPort() {
        return brokerPort;
    }

    public int getBrokerTLSPort() {
        return brokerTLSPort;
    }

    public int getBrokerWebPort() {
        return brokerWebPort;
    }

    public int getMaxMsgSizeInB() {
        return maxMsgSizeInB;
    }

    public int getMaxMsgSizeInMB() {
        return maxMsgSizeInMB;
    }

    public int getQryPriorityId() {
        return qryPriorityId;
    }

    public EnableStatus getGloFlowCtrlStatus() {
        return gloFlowCtrlStatus;
    }

    public int getGloFlowCtrlRuleCnt() {
        return gloFlowCtrlRuleCnt;
    }

    public String getGloFlowCtrlRuleInfo() {
        return gloFlowCtrlRuleInfo;
    }

    public boolean enableFlowCtrl() {
        return gloFlowCtrlStatus == EnableStatus.STATUS_ENABLE;
    }

    public TopicPropGroup getClsDefTopicProps() {
        return clsDefTopicProps;
    }

    /**
     * check if subclass fields is equals
     *
     * @param other  check object
     * @return if equals
     */
    public boolean isDataEquals(ClusterSettingEntity other) {
        return brokerPort == other.brokerPort
                && brokerTLSPort == other.brokerTLSPort
                && brokerWebPort == other.brokerWebPort
                && maxMsgSizeInB == other.maxMsgSizeInB
                && qryPriorityId == other.qryPriorityId
                && gloFlowCtrlRuleCnt == other.gloFlowCtrlRuleCnt
                && recordKey.equals(other.recordKey)
                && Objects.equals(clsDefTopicProps, other.clsDefTopicProps)
                && gloFlowCtrlStatus == other.gloFlowCtrlStatus
                && Objects.equals(gloFlowCtrlRuleInfo, other.gloFlowCtrlRuleInfo);
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
            sBuilder.append("{\"brokerPort\":").append(brokerPort)
                    .append(",\"brokerTLSPort\":").append(brokerTLSPort)
                    .append(",\"brokerWebPort\":").append(brokerWebPort)
                    .append(",\"maxMsgSizeInMB\":").append(maxMsgSizeInMB)
                    .append(",\"qryPriorityId\":").append(qryPriorityId)
                    .append(",\"flowCtrlEnable\":").append(gloFlowCtrlStatus.isEnable())
                    .append(",\"flowCtrlRuleCount\":").append(gloFlowCtrlRuleCnt)
                    .append(",\"flowCtrlInfo\":").append(gloFlowCtrlRuleInfo);
        } else {
            sBuilder.append("{\"bPort\":").append(brokerPort)
                    .append(",\"bTlsPort\":").append(brokerTLSPort)
                    .append(",\"bWebPort\":").append(brokerWebPort)
                    .append(",\"mxMsgInMB\":").append(maxMsgSizeInMB)
                    .append(",\"qryPriId\":").append(qryPriorityId)
                    .append(",\"fCtrlEn\":").append(gloFlowCtrlStatus.isEnable())
                    .append(",\"fCtrlCnt\":").append(gloFlowCtrlRuleCnt)
                    .append(",\"fCtrlInfo\":").append(gloFlowCtrlRuleInfo);
        }
        clsDefTopicProps.toWebJsonStr(sBuilder, isLongName);
        super.toWebJsonStr(sBuilder, isLongName);
        if (fullFormat) {
            sBuilder.append("}");
        }
        return sBuilder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ClusterSettingEntity)) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        ClusterSettingEntity entity = (ClusterSettingEntity) o;
        return isDataEquals(entity);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), recordKey, brokerPort, brokerTLSPort,
                brokerWebPort, clsDefTopicProps, maxMsgSizeInB, qryPriorityId,
                gloFlowCtrlStatus, gloFlowCtrlRuleCnt, gloFlowCtrlRuleInfo);
    }

    @Override
    public ClusterSettingEntity clone() {
        try {
            ClusterSettingEntity copy = (ClusterSettingEntity) super.clone();
            copy.setGloFlowCtrlStatus(getGloFlowCtrlStatus());
            copy.setClsDefTopicProps(getClsDefTopicProps().clone());
            return copy;
        } catch (CloneNotSupportedException e) {
            return null;
        }
    }

    private void setGloFlowCtrlStatus(EnableStatus gloFlowCtrlStatus) {
        this.gloFlowCtrlStatus = gloFlowCtrlStatus;
    }

    private void setGloFlowCtrlInfo(int flowCtrlCnt, String flowCtrlInfo) {
        this.gloFlowCtrlRuleCnt = flowCtrlCnt;
        this.gloFlowCtrlRuleInfo = flowCtrlInfo;
    }

    private void setEnableFlowCtrl(boolean enableFlowCtrl) {
        if (enableFlowCtrl) {
            this.gloFlowCtrlStatus = EnableStatus.STATUS_ENABLE;
        } else {
            this.gloFlowCtrlStatus = EnableStatus.STATUS_DISABLE;
        }
    }

    private void setClsDefTopicProps(TopicPropGroup clsDefTopicProps) {
        this.clsDefTopicProps = clsDefTopicProps;
    }
}
