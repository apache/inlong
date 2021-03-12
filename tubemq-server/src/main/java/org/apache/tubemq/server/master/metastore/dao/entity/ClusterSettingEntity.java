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
import org.apache.tubemq.server.common.TServerConstants;
import org.apache.tubemq.server.common.statusdef.RuleStatus;



/*
 * store the cluster default setting
 *
 */
public class ClusterSettingEntity extends BaseEntity {

    private String recordKey =
            TServerConstants.TOKEN_DEFAULT_CLUSTER_SETTING;
    //broker tcp port
    private int brokerPort = TBaseConstants.META_VALUE_UNDEFINED;
    //broker tls port
    private int brokerTLSPort = TBaseConstants.META_VALUE_UNDEFINED;
    //broker web port
    private int brokerWebPort = TBaseConstants.META_VALUE_UNDEFINED;
    private TopicPropGroup clsDefTopicPropGroup = new TopicPropGroup();
    private int maxMsgSizeInB = TBaseConstants.META_VALUE_UNDEFINED;
    private int qryPriorityId = TBaseConstants.META_VALUE_UNDEFINED;
    private RuleStatus flowCtrlStatus = RuleStatus.STATUS_UNDEFINE;
    private int flowCtrlRuleCnt = 0;                //flow control rule count
    private String flowCtrlRuleInfo = "";  // flow control info

    public ClusterSettingEntity() {
        super();
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

    public void setFlowCtrlRuleCnt(Boolean enableFlowCtrl,
                                   int flowCtrlCnt, String flowCtrlInfo) {
        if (enableFlowCtrl != null) {
            if (enableFlowCtrl) {
                this.flowCtrlStatus = RuleStatus.STATUS_ENABLE;
            } else {
                this.flowCtrlStatus = RuleStatus.STATUS_DISABLE;
            }
        }
        this.flowCtrlRuleCnt = flowCtrlCnt;
        this.flowCtrlRuleInfo = flowCtrlInfo;
    }

    public int getFlowCtrlRuleCnt() {
        return flowCtrlRuleCnt;
    }

    public String getFlowCtrlRuleInfo() {
        return flowCtrlRuleInfo;
    }

    public boolean enableFlowCtrl() {
        return flowCtrlStatus == RuleStatus.STATUS_ENABLE;
    }

    public void setEnableFlowCtrl(boolean enableFlowCtrl) {
        if (enableFlowCtrl) {
            this.flowCtrlStatus = RuleStatus.STATUS_ENABLE;
        } else {
            this.flowCtrlStatus = RuleStatus.STATUS_DISABLE;
        }
    }

    public TopicPropGroup getClsDefTopicPropGroup() {
        return clsDefTopicPropGroup;
    }

    public void setClsDefTopicPropGroup(TopicPropGroup clsDefTopicPropGroup) {
        this.clsDefTopicPropGroup = clsDefTopicPropGroup;
    }
}
