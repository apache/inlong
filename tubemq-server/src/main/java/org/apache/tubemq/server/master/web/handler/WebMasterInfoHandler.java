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

package org.apache.tubemq.server.master.web.handler;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import javax.servlet.http.HttpServletRequest;
import org.apache.tubemq.corebase.TBaseConstants;
import org.apache.tubemq.corebase.cluster.BrokerInfo;
import org.apache.tubemq.corebase.cluster.TopicInfo;
import org.apache.tubemq.corebase.utils.SettingValidUtils;
import org.apache.tubemq.corebase.utils.TStringUtils;
import org.apache.tubemq.corebase.utils.Tuple2;
import org.apache.tubemq.corebase.utils.Tuple3;
import org.apache.tubemq.server.common.TServerConstants;
import org.apache.tubemq.server.common.fielddef.WebFieldDef;
import org.apache.tubemq.server.common.utils.ProcessResult;
import org.apache.tubemq.server.common.utils.WebParameterUtils;
import org.apache.tubemq.server.master.TMaster;
import org.apache.tubemq.server.master.bdbstore.bdbentitys.BdbBrokerConfEntity;
import org.apache.tubemq.server.master.bdbstore.bdbentitys.BdbTopicAuthControlEntity;
import org.apache.tubemq.server.master.bdbstore.bdbentitys.BdbTopicConfEntity;
import org.apache.tubemq.server.master.metamanage.metastore.dao.entity.ClusterSettingEntity;
import org.apache.tubemq.server.master.metamanage.metastore.dao.entity.TopicPropGroup;
import org.apache.tubemq.server.master.nodemanage.nodebroker.TopicPSInfoManager;
import org.apache.tubemq.server.master.web.model.ClusterGroupVO;
import org.apache.tubemq.server.master.web.model.ClusterNodeVO;




public class WebMasterInfoHandler extends AbstractWebHandler {

    /**
     * Constructor
     *
     * @param master tube master
     */
    public WebMasterInfoHandler(TMaster master) {
        super(master);
    }

    @Override
    public void registerWebApiMethod() {
        // register query method
        registerQueryWebMethod("admin_query_master_group_info",
                "getGroupAddressStrInfo");
        registerQueryWebMethod("admin_query_cluster_default_setting",
                "adminQueryClusterDefSetting");
        registerQueryWebMethod("admin_query_cluster_topic_view",
                "adminQueryClusterTopicView");

        // register modify method
        registerModifyWebMethod("admin_transfer_current_master",
                "transferCurrentMaster");
        // register modify method
        registerModifyWebMethod("admin_set_cluster_default_setting",
                "adminSetClusterDefSetting");
    }

    /**
     * Get master group info
     *
     * @param req  HttpServletRequest
     * @return
     */
    public StringBuilder getGroupAddressStrInfo(HttpServletRequest req) {
        StringBuilder strBuffer = new StringBuilder(512);
        ClusterGroupVO clusterGroupVO = metaDataManager.getGroupAddressStrInfo();
        if (clusterGroupVO == null) {
            WebParameterUtils.buildFailResultWithBlankData(
                    500, "GetBrokerGroup info error", strBuffer);
        } else {
            strBuffer.append("{\"result\":true,\"errCode\":0,\"errMsg\":\"Ok\",\"groupName\":\"")
                    .append(clusterGroupVO.getGroupName()).append("\",\"isPrimaryNodeActive\":")
                    .append(clusterGroupVO.isPrimaryNodeActive()).append(",\"data\":[");
            int count = 0;
            List<ClusterNodeVO> nodeList = clusterGroupVO.getNodeData();
            if (nodeList != null) {
                for (ClusterNodeVO node : nodeList) {
                    if (node == null) {
                        continue;
                    }
                    if (count++ > 0) {
                        strBuffer.append(",");
                    }
                    strBuffer.append("{\"index\":").append(count).append(",\"name\":\"").append(node.getNodeName())
                            .append("\",\"hostName\":\"").append(node.getHostName())
                            .append("\",\"port\":\"").append(node.getPort())
                            .append("\",\"statusInfo\":{").append("\"nodeStatus\":\"")
                            .append(node.getNodeStatus()).append("\",\"joinTime\":\"")
                            .append(node.getJoinTime()).append("\"}}");
                }
            }
            strBuffer.append("],\"count\":").append(count).append(",\"groupStatus\":\"")
                    .append(clusterGroupVO.getGroupStatus()).append("\"}");
        }
        return strBuffer;
    }

    /**
     * transfer current master to another node
     *
     * @param req  HttpServletRequest
     * @return
     */
    public StringBuilder transferCurrentMaster(HttpServletRequest req) {
        ProcessResult result = new ProcessResult();
        StringBuilder sBuilder = new StringBuilder(512);
        // valid operation authorize info
        if (!WebParameterUtils.validReqAuthorizeInfo(req,
                WebFieldDef.ADMINAUTHTOKEN, true, master, result)) {
            WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
            return sBuilder;
        }
        try {
            brokerConfManager.transferMaster();
            WebParameterUtils.buildSuccessResult(sBuilder,
                    "TransferMaster method called, please wait 20 seconds!");
        } catch (Exception e2) {
            WebParameterUtils.buildFailResult(sBuilder, e2.getMessage());
        }
        return sBuilder;
    }

    /**
     * Query cluster default setting
     *
     * @param req
     * @return
     * @throws Exception
     */
    public StringBuilder adminQueryClusterDefSetting(HttpServletRequest req) {
        StringBuilder sBuilder = new StringBuilder(512);
        ClusterSettingEntity defClusterSetting =
                metaDataManager.getClusterDefSetting();
        WebParameterUtils.buildSuccessWithDataRetBegin(sBuilder);
        if (defClusterSetting != null) {
            defClusterSetting.toWebJsonStr(sBuilder, true);
        }
        WebParameterUtils.buildSuccessWithDataRetEnd(sBuilder, 1);
        return sBuilder;
    }

    /**
     * Add or modify cluster default setting
     *
     * @param req
     * @return
     */
    public StringBuilder adminSetClusterDefSetting(HttpServletRequest req) {
        ProcessResult result = new ProcessResult();
        StringBuilder sBuilder = new StringBuilder(512);
        // valid operation authorize info
        if (!WebParameterUtils.validReqAuthorizeInfo(req,
                WebFieldDef.ADMINAUTHTOKEN, true, master, result)) {
            WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
            return sBuilder;
        }
        // check and get operation info
        if (!WebParameterUtils.getAUDBaseInfo(req, false, result)) {
            WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
            return sBuilder;
        }
        Tuple3<Long, String, Date> inOpTupleInfo =
                (Tuple3<Long, String, Date>) result.getRetData();
        // check max message size
        if (!WebParameterUtils.getIntParamValue(req,
                WebFieldDef.MAXMSGSIZEINMB, false,
                TBaseConstants.META_VALUE_UNDEFINED,
                TBaseConstants.META_MIN_ALLOWED_MESSAGE_SIZE_MB,
                TBaseConstants.META_MAX_ALLOWED_MESSAGE_SIZE_MB,
                result)) {
            WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
            return sBuilder;
        }
        int inMaxMsgSize = (int) result.getRetData();
        // get broker port info
        if (!WebParameterUtils.getIntParamValue(req, WebFieldDef.BROKERPORT,
                false, TBaseConstants.META_VALUE_UNDEFINED, 1, result)) {
            WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
            return sBuilder;
        }
        int inBrokerPort = (int) result.getRetData();
        // get broker tls port info
        if (!WebParameterUtils.getIntParamValue(req, WebFieldDef.BROKERTLSPORT,
                false, TBaseConstants.META_VALUE_UNDEFINED, 1, result)) {
            WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
            return sBuilder;
        }
        int inBrokerTlsPort = (int) result.getRetData();
        // get broker web port info
        if (!WebParameterUtils.getIntParamValue(req, WebFieldDef.BROKERWEBPORT,
                false, TBaseConstants.META_VALUE_UNDEFINED, 1, result)) {
            WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
            return sBuilder;
        }
        int inBrokerWebPort = (int) result.getRetData();
        // get and valid TopicPropGroup info
        if (!WebParameterUtils.getTopicPropInfo(req, null, result)) {
            WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
            return sBuilder;
        }
        TopicPropGroup defTopicProps = (TopicPropGroup) result.getRetData();
        // get and valid qryPriorityId info
        if (!WebParameterUtils.getQryPriorityIdParameter(req,
                false, TBaseConstants.META_VALUE_UNDEFINED, 101, result)) {
            WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
            return sBuilder;
        }
        int inQryPriorityId = (int) result.retData1;
        // get flowCtrlEnable info
        if (!WebParameterUtils.getBooleanParamValue(req,
                WebFieldDef.FLOWCTRLENABLE, false, null, result)) {
            WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
            return sBuilder;
        }
        Boolean flowCtrlEnable = (Boolean) result.retData1;
        // get and flow control rule info
        int flowRuleCnt = WebParameterUtils.getAndCheckFlowRules(req, null, result);
        if (!result.success) {
            WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
            return sBuilder;
        }
        String flowCtrlInfo = (String) result.retData1;

        // add or modify record
        ClusterSettingEntity newConf = null;
        ClusterSettingEntity curConf = metaDataManager.getClusterDefSetting();
        if (curConf == null) {
            newConf = new ClusterSettingEntity(
                    inOpTupleInfo.getF0(), inOpTupleInfo.getF1(), inOpTupleInfo.getF2());
            // check and process max message size
            if (inMaxMsgSize == TBaseConstants.META_VALUE_UNDEFINED) {
                inMaxMsgSize = TBaseConstants.META_MIN_ALLOWED_MESSAGE_SIZE_MB;
            }
            newConf.setMaxMsgSizeInB(
                    SettingValidUtils.validAndXfeMaxMsgSizeFromMBtoB(inMaxMsgSize));
            // check and process broker ports
            if (inBrokerPort == TBaseConstants.META_VALUE_UNDEFINED) {
                inBrokerPort = TBaseConstants.META_DEFAULT_BROKER_PORT;
            }
            newConf.setBrokerPort(inBrokerPort);
            if (inBrokerTlsPort == TBaseConstants.META_VALUE_UNDEFINED) {
                inBrokerTlsPort = TBaseConstants.META_DEFAULT_BROKER_TLS_PORT;
            }
            newConf.setBrokerTLSPort(inBrokerTlsPort);
            if (inBrokerWebPort == TBaseConstants.META_VALUE_UNDEFINED) {
                inBrokerWebPort = TBaseConstants.META_DEFAULT_BROKER_WEB_PORT;
            }
            newConf.setBrokerWebPort(inBrokerWebPort);
            if (inQryPriorityId == TBaseConstants.META_VALUE_UNDEFINED) {
                inQryPriorityId = TServerConstants.QRY_PRIORITY_DEF_VALUE;
            }
            newConf.setQryPriorityId(inQryPriorityId);
            newConf.setClsDefTopicProps(defTopicProps);
            if (flowCtrlEnable == null) {
                flowCtrlEnable = false;
            }
            if (flowCtrlInfo == null) {
                flowCtrlInfo = TServerConstants.BLANK_FLOWCTRL_RULES;
            }
            newConf.setGloFlowCtrlInfo(flowCtrlEnable, flowRuleCnt, flowCtrlInfo);
            if (!metaDataManager.confAddClusterDefSetting(newConf, sBuilder, result)) {
                WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
                return sBuilder;
            }
        } else {
            boolean dataChanged = false;
            newConf = curConf.clone();
            newConf.setModifyInfo(inOpTupleInfo.getF0(),
                    false, inOpTupleInfo.getF1(), inOpTupleInfo.getF2());
            if (inMaxMsgSize != TBaseConstants.META_VALUE_UNDEFINED) {
                inMaxMsgSize = SettingValidUtils.validAndXfeMaxMsgSizeFromMBtoB(inMaxMsgSize);
                if (newConf.getMaxMsgSizeInB() != inMaxMsgSize) {
                    dataChanged = true;
                    newConf.setMaxMsgSizeInB(inMaxMsgSize);
                }
            }
            if (inBrokerPort != TBaseConstants.META_VALUE_UNDEFINED
                    && newConf.getBrokerPort() != inBrokerPort) {
                dataChanged = true;
                newConf.setBrokerPort(inBrokerPort);
            }
            if (inBrokerTlsPort != TBaseConstants.META_VALUE_UNDEFINED
                    && newConf.getBrokerTLSPort() != inBrokerTlsPort) {
                dataChanged = true;
                newConf.setBrokerTLSPort(inBrokerTlsPort);
            }
            if (inBrokerWebPort != TBaseConstants.META_VALUE_UNDEFINED
                    && newConf.getBrokerWebPort() != inBrokerWebPort) {
                dataChanged = true;
                newConf.setBrokerWebPort(inBrokerWebPort);
            }
            // check and set qry priority id
            if (inQryPriorityId != TBaseConstants.META_VALUE_UNDEFINED
                    && newConf.getQryPriorityId() != inQryPriorityId) {
                dataChanged = true;
                newConf.setQryPriorityId(inQryPriorityId);
            }
            // check and set flowCtrl info
            if (flowCtrlEnable != null
                    && flowCtrlEnable != newConf.getGloFlowCtrlStatus().isEnable()) {
                dataChanged = true;
                newConf.setEnableFlowCtrl(flowCtrlEnable);
            }
            if (TStringUtils.isNotBlank(flowCtrlInfo)
                    && !flowCtrlInfo.equals(newConf.getGloFlowCtrlRuleInfo())) {
                dataChanged = true;
                newConf.setGloFlowCtrlInfo(newConf.getGloFlowCtrlStatus().isEnable(),
                        flowRuleCnt, flowCtrlInfo);
            }
            // check if changed
            if (!dataChanged) {
                WebParameterUtils.buildSuccessResult(sBuilder, "No data is changed!");
                return sBuilder;
            }
            if (!metaDataManager.confModClusterDefSetting(newConf, sBuilder, result)) {
                WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
                return sBuilder;
            }
        }
        curConf = metaDataManager.getClusterDefSetting();
        WebParameterUtils.buildSuccWithData(curConf.getDataVersionId(), sBuilder);
        return sBuilder;
    }


    /**
     * Query cluster topic overall view
     *
     * @param req
     * @return
     */
    public StringBuilder adminQueryClusterTopicView(HttpServletRequest req) {
        ProcessResult result = new ProcessResult();
        StringBuilder sBuilder = new StringBuilder(512);
        // check and get brokerId field
        if (!WebParameterUtils.getIntParamValue(req,
                WebFieldDef.COMPSBROKERID, false, result)) {
            WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
            return sBuilder;
        }
        Set<Integer> brokerIds = (Set<Integer>) result.retData1;
        // check and get topicName field
        if (!WebParameterUtils.getStringParamValue(req,
                WebFieldDef.COMPSTOPICNAME, false, null, result)) {
            WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
            return sBuilder;
        }
        Set<String> topicNameSet = (Set<String>) result.retData1;
        // query topic configure info
        ConcurrentHashMap<String, List<BdbTopicConfEntity>> topicConfigMap =
                brokerConfManager.getBdbTopicEntityMap(null);
        TopicPSInfoManager topicPSInfoManager = master.getTopicPSInfoManager();
        int totalCount = 0;
        int brokerCount = 0;
        int totalCfgNumPartCount = 0;
        int totalRunNumPartCount = 0;
        boolean isSrvAcceptPublish = false;
        boolean isSrvAcceptSubscribe = false;
        boolean isAcceptPublish = false;
        boolean isAcceptSubscribe = false;
        boolean enableAuthControl = false;
        sBuilder.append("{\"result\":true,\"errCode\":0,\"errMsg\":\"OK\",\"data\":[");
        for (Map.Entry<String, List<BdbTopicConfEntity>> entry : topicConfigMap.entrySet()) {
            if (!topicNameSet.isEmpty() && !topicNameSet.contains(entry.getKey())) {
                continue;
            }
            if (totalCount++ > 0) {
                sBuilder.append(",");
            }
            brokerCount = 0;
            totalCfgNumPartCount = 0;
            totalRunNumPartCount = 0;
            isSrvAcceptPublish = false;
            isSrvAcceptSubscribe = false;
            enableAuthControl = false;
            isAcceptPublish = false;
            isAcceptSubscribe = false;
            for (BdbTopicConfEntity entity : entry.getValue()) {
                if ((!brokerIds.isEmpty()) && (!brokerIds.contains(entity.getBrokerId()))) {
                    continue;
                }
                brokerCount++;
                totalCfgNumPartCount += entity.getNumPartitions() * entity.getNumTopicStores();
                BdbBrokerConfEntity brokerConfEntity =
                        brokerConfManager.getBrokerDefaultConfigStoreInfo(entity.getBrokerId());
                if (brokerConfEntity != null) {
                    Tuple2<Boolean, Boolean> pubSubStatus =
                            WebParameterUtils.getPubSubStatusByManageStatus(
                                    brokerConfEntity.getManageStatus());
                    isAcceptPublish = pubSubStatus.getF0();
                    isAcceptSubscribe = pubSubStatus.getF1();
                }
                BrokerInfo broker =
                        new BrokerInfo(entity.getBrokerId(),
                                entity.getBrokerIp(), entity.getBrokerPort());
                TopicInfo topicInfo = topicPSInfoManager.getTopicInfo(
                        entity.getTopicName(), broker);
                if (topicInfo != null) {
                    if (isAcceptPublish && topicInfo.isAcceptPublish()) {
                        isSrvAcceptPublish = true;
                    }
                    if (isAcceptSubscribe && topicInfo.isAcceptSubscribe()) {
                        isSrvAcceptSubscribe = true;
                    }
                    totalRunNumPartCount +=
                            topicInfo.getPartitionNum() * topicInfo.getTopicStoreNum();
                }
            }
            BdbTopicAuthControlEntity authEntity =
                    brokerConfManager.getBdbEnableAuthControlByTopicName(entry.getKey());
            if (authEntity != null) {
                enableAuthControl = authEntity.isEnableAuthControl();
            }
            sBuilder.append("{\"topicName\":\"").append(entry.getKey())
                    .append("\",\"totalCfgBrokerCnt\":").append(brokerCount)
                    .append(",\"totalCfgNumPart\":").append(totalCfgNumPartCount)
                    .append(",\"totalRunNumPartCount\":").append(totalRunNumPartCount)
                    .append(",\"isSrvAcceptPublish\":").append(isSrvAcceptPublish)
                    .append(",\"isSrvAcceptSubscribe\":").append(isSrvAcceptSubscribe)
                    .append(",\"enableAuthControl\":").append(enableAuthControl)
                    .append("}");
        }
        sBuilder.append("],\"dataCount\":").append(totalCount).append("}");
        return sBuilder;
    }


}
