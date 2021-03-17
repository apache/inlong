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

import org.apache.tubemq.corebase.TBaseConstants;
import org.apache.tubemq.server.common.statusdef.EnableStatus;
import org.apache.tubemq.server.master.bdbstore.bdbentitys.BdbClusterSettingEntity;
import org.apache.tubemq.server.master.metastore.TStoreConstants;


/*
 * store the cluster default setting
 *
 */
public class ClusterSettingEntity extends BaseEntity {

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
    private int qryPriorityId = TBaseConstants.META_VALUE_UNDEFINED;
    private EnableStatus gloFlowCtrlStatus = EnableStatus.STATUS_UNDEFINE;
    // flow control rule count
    private int gloFlowCtrlRuleCnt = TBaseConstants.META_VALUE_UNDEFINED;
    // flow control info
    private String gloFlowCtrlRuleInfo = "";



    public ClusterSettingEntity() {
        super();
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
        this.qryPriorityId = bdbEntity.getQryPriorityId();
        setGloFlowCtrlInfo(bdbEntity.getEnableGloFlowCtrl(),
                bdbEntity.getGloFlowCtrlCnt(), bdbEntity.getGloFlowCtrlInfo());
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

    public void setGloFlowCtrlInfo(Boolean enableFlowCtrl,
                                   int flowCtrlCnt, String flowCtrlInfo) {
        if (enableFlowCtrl != null) {
            if (enableFlowCtrl) {
                this.gloFlowCtrlStatus = EnableStatus.STATUS_ENABLE;
            } else {
                this.gloFlowCtrlStatus = EnableStatus.STATUS_DISABLE;
            }
        }
        this.gloFlowCtrlRuleCnt = flowCtrlCnt;
        this.gloFlowCtrlRuleInfo = flowCtrlInfo;
    }

    public String getRecordKey() {
        return recordKey;
    }

    public int getBrokerPort() {
        return brokerPort;
    }

    public void setBrokerPort(int brokerPort) {
        this.brokerPort = brokerPort;
    }

    public int getBrokerTLSPort() {
        return brokerTLSPort;
    }

    public void setBrokerTLSPort(int brokerTLSPort) {
        this.brokerTLSPort = brokerTLSPort;
    }

    public int getBrokerWebPort() {
        return brokerWebPort;
    }

    public void setBrokerWebPort(int brokerWebPort) {
        this.brokerWebPort = brokerWebPort;
    }

    public int getMaxMsgSizeInB() {
        return maxMsgSizeInB;
    }

    public void setMaxMsgSizeInB(int maxMsgSizeInB) {
        this.maxMsgSizeInB = maxMsgSizeInB;
    }

    public int getQryPriorityId() {
        return qryPriorityId;
    }

    public void setQryPriorityId(int qryPriorityId) {
        this.qryPriorityId = qryPriorityId;
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

    public void setEnableFlowCtrl(boolean enableFlowCtrl) {
        if (enableFlowCtrl) {
            this.gloFlowCtrlStatus = EnableStatus.STATUS_ENABLE;
        } else {
            this.gloFlowCtrlStatus = EnableStatus.STATUS_DISABLE;
        }
    }

    public TopicPropGroup getClsDefTopicProps() {
        return clsDefTopicProps;
    }

    public void setClsDefTopicProps(TopicPropGroup clsDefTopicProps) {
        this.clsDefTopicProps = clsDefTopicProps;
    }

}
