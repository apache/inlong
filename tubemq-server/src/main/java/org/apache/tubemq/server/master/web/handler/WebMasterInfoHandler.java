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

import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.servlet.http.HttpServletRequest;
import org.apache.tubemq.corebase.TBaseConstants;
import org.apache.tubemq.corebase.cluster.BrokerInfo;
import org.apache.tubemq.corebase.cluster.TopicInfo;
import org.apache.tubemq.corebase.utils.Tuple2;
import org.apache.tubemq.server.common.TServerConstants;
import org.apache.tubemq.server.common.fielddef.WebFieldDef;
import org.apache.tubemq.server.common.utils.ProcessResult;
import org.apache.tubemq.server.common.utils.WebParameterUtils;
import org.apache.tubemq.server.master.TMaster;
import org.apache.tubemq.server.master.metamanage.DataOpErrCode;
import org.apache.tubemq.server.master.metamanage.metastore.dao.entity.BaseEntity;
import org.apache.tubemq.server.master.metamanage.metastore.dao.entity.BrokerConfEntity;
import org.apache.tubemq.server.master.metamanage.metastore.dao.entity.ClusterSettingEntity;
import org.apache.tubemq.server.master.metamanage.metastore.dao.entity.TopicCtrlEntity;
import org.apache.tubemq.server.master.metamanage.metastore.dao.entity.TopicDeployEntity;
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
        StringBuilder sBuffer = new StringBuilder(512);
        // valid operation authorize info
        if (!WebParameterUtils.validReqAuthorizeInfo(req,
                WebFieldDef.ADMINAUTHTOKEN, true, master, sBuffer, result)) {
            WebParameterUtils.buildFailResult(sBuffer, result.errInfo);
            return sBuffer;
        }
        try {
            metaDataManager.transferMaster();
            WebParameterUtils.buildSuccessResult(sBuffer,
                    "TransferMaster method called, please wait 20 seconds!");
        } catch (Exception e2) {
            WebParameterUtils.buildFailResult(sBuffer, e2.getMessage());
        }
        return sBuffer;
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
                metaDataManager.getClusterDefSetting(true);
        WebParameterUtils.buildSuccessWithDataRetBegin(sBuilder);
        if (defClusterSetting != null) {
            defClusterSetting.toWebJsonStr(sBuilder, true, true);
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
        StringBuilder sBuffer = new StringBuilder(512);
        // valid operation authorize info
        if (!WebParameterUtils.validReqAuthorizeInfo(req,
                WebFieldDef.ADMINAUTHTOKEN, true, master, sBuffer, result)) {
            WebParameterUtils.buildFailResult(sBuffer, result.errInfo);
            return sBuffer;
        }
        // check and get operation info
        if (!WebParameterUtils.getAUDBaseInfo(req, false, null, sBuffer, result)) {
            WebParameterUtils.buildFailResult(sBuffer, result.errInfo);
            return sBuffer;
        }
        BaseEntity opEntity = (BaseEntity) result.getRetData();
        // check max message size
        if (!WebParameterUtils.getIntParamValue(req,
                WebFieldDef.MAXMSGSIZEINMB, false,
                TBaseConstants.META_VALUE_UNDEFINED,
                TBaseConstants.META_MIN_ALLOWED_MESSAGE_SIZE_MB,
                TBaseConstants.META_MAX_ALLOWED_MESSAGE_SIZE_MB, sBuffer, result)) {
            WebParameterUtils.buildFailResult(sBuffer, result.errInfo);
            return sBuffer;
        }
        int inMaxMsgSizeMB = (int) result.getRetData();
        // get broker port info
        if (!WebParameterUtils.getIntParamValue(req, WebFieldDef.BROKERPORT,
                false, TBaseConstants.META_VALUE_UNDEFINED, 1, sBuffer, result)) {
            WebParameterUtils.buildFailResult(sBuffer, result.errInfo);
            return sBuffer;
        }
        int inBrokerPort = (int) result.getRetData();
        // get broker tls port info
        if (!WebParameterUtils.getIntParamValue(req, WebFieldDef.BROKERTLSPORT,
                false, TBaseConstants.META_VALUE_UNDEFINED, 1, sBuffer, result)) {
            WebParameterUtils.buildFailResult(sBuffer, result.errInfo);
            return sBuffer;
        }
        int inBrokerTlsPort = (int) result.getRetData();
        // get broker web port info
        if (!WebParameterUtils.getIntParamValue(req, WebFieldDef.BROKERWEBPORT,
                false, TBaseConstants.META_VALUE_UNDEFINED, 1, sBuffer, result)) {
            WebParameterUtils.buildFailResult(sBuffer, result.errInfo);
            return sBuffer;
        }
        int inBrokerWebPort = (int) result.getRetData();
        // get and valid TopicPropGroup info
        TopicPropGroup defTopicProps = new TopicPropGroup();
        defTopicProps.fillDefaultValue();
        if (!WebParameterUtils.getTopicPropInfo(req, defTopicProps, sBuffer, result)) {
            WebParameterUtils.buildFailResult(sBuffer, result.errInfo);
            return sBuffer;
        }
        defTopicProps = (TopicPropGroup) result.getRetData();
        // get and valid qryPriorityId info
        if (!WebParameterUtils.getQryPriorityIdParameter(req,
                false, TBaseConstants.META_VALUE_UNDEFINED,
                TServerConstants.QRY_PRIORITY_MIN_VALUE, sBuffer, result)) {
            WebParameterUtils.buildFailResult(sBuffer, result.errInfo);
            return sBuffer;
        }
        int inQryPriorityId = (int) result.retData1;
        // get flowCtrlEnable info
        if (!WebParameterUtils.getBooleanParamValue(req,
                WebFieldDef.FLOWCTRLENABLE, false, null, sBuffer, result)) {
            WebParameterUtils.buildFailResult(sBuffer, result.errInfo);
            return sBuffer;
        }
        Boolean flowCtrlEnable = (Boolean) result.retData1;
        // get and flow control rule info
        int flowRuleCnt = WebParameterUtils.getAndCheckFlowRules(req, null, sBuffer, result);
        if (!result.success) {
            WebParameterUtils.buildFailResult(sBuffer, result.errInfo);
            return sBuffer;
        }
        String flowCtrlInfo = (String) result.retData1;
        // add or modify record
        ClusterSettingEntity newConf = null;
        ClusterSettingEntity curConf = metaDataManager.getClusterDefSetting(true);
        if (curConf == null) {
            if (!metaDataManager.addClusterDefSetting(opEntity, inBrokerPort,
                    inBrokerTlsPort, inBrokerWebPort, inMaxMsgSizeMB,
                    inQryPriorityId, flowCtrlEnable, flowRuleCnt, flowCtrlInfo,
                    defTopicProps, sBuffer, result)) {
                WebParameterUtils.buildFailResult(sBuffer, result.errInfo);
                return sBuffer;
            }
        } else {
            if (!metaDataManager.modClusterDefSetting(opEntity, inBrokerPort,
                    inBrokerTlsPort, inBrokerWebPort, inMaxMsgSizeMB,
                    inQryPriorityId, flowCtrlEnable, flowRuleCnt, flowCtrlInfo,
                    defTopicProps, sBuffer, result)) {
                WebParameterUtils.buildFailResult(sBuffer, result.errInfo);
                return sBuffer;
            }
        }
        curConf = metaDataManager.getClusterDefSetting(true);
        if (curConf == null) {
            WebParameterUtils.buildFailResultWithBlankData(
                    DataOpErrCode.DERR_UPD_NOT_EXIST.getCode(),
                    DataOpErrCode.DERR_UPD_NOT_EXIST.getDescription(), sBuffer);
            return sBuffer;
        }
        // build return result
        WebParameterUtils.buildSuccessWithDataRetBegin(sBuffer);
        curConf.toWebJsonStr(sBuffer, true, true);
        WebParameterUtils.buildSuccessWithDataRetEnd(sBuffer, 1);
        return sBuffer;
    }

    /**
     * Query cluster topic overall view
     *
     * @param req
     * @return
     */
    public StringBuilder adminQueryClusterTopicView(HttpServletRequest req) {
        ProcessResult result = new ProcessResult();
        StringBuilder sBuffer = new StringBuilder(512);
        // check and get brokerId field
        if (!WebParameterUtils.getIntParamValue(req,
                WebFieldDef.COMPSBROKERID, false, sBuffer, result)) {
            WebParameterUtils.buildFailResult(sBuffer, result.errInfo);
            return sBuffer;
        }
        Set<Integer> brokerIds = (Set<Integer>) result.retData1;
        // check and get topicName field
        if (!WebParameterUtils.getStringParamValue(req,
                WebFieldDef.COMPSTOPICNAME, false, null, sBuffer, result)) {
            WebParameterUtils.buildFailResult(sBuffer, result.errInfo);
            return sBuffer;
        }
        Set<String> topicNameSet = (Set<String>) result.retData1;
        // query topic configure info
        Map<String, List<TopicDeployEntity>> topicConfMap =
                metaDataManager.getTopicConfMapByTopicAndBrokerIds(topicNameSet, brokerIds);
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
        WebParameterUtils.buildSuccessWithDataRetBegin(sBuffer);
        for (Map.Entry<String, List<TopicDeployEntity>> entry : topicConfMap.entrySet()) {
            if (totalCount++ > 0) {
                sBuffer.append(",");
            }
            brokerCount = 0;
            totalCfgNumPartCount = 0;
            totalRunNumPartCount = 0;
            isSrvAcceptPublish = false;
            isSrvAcceptSubscribe = false;
            enableAuthControl = false;
            isAcceptPublish = false;
            isAcceptSubscribe = false;
            for (TopicDeployEntity entity : entry.getValue()) {
                brokerCount++;
                TopicPropGroup topicProps = entity.getTopicProps();
                totalCfgNumPartCount +=
                        topicProps.getNumPartitions() * topicProps.getNumTopicStores();
                BrokerConfEntity brokerConfEntity =
                        metaDataManager.getBrokerConfByBrokerId(entity.getBrokerId());
                if (brokerConfEntity != null) {
                    Tuple2<Boolean, Boolean> pubSubStatus =
                            WebParameterUtils.getPubSubStatusByManageStatus(
                                    brokerConfEntity.getManageStatus().getCode());
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
            TopicCtrlEntity authEntity =
                    metaDataManager.getTopicCtrlByTopicName(entry.getKey());
            if (authEntity != null) {
                enableAuthControl = authEntity.isAuthCtrlEnable();
            }
            sBuffer.append("{\"topicName\":\"").append(entry.getKey())
                    .append("\",\"totalCfgBrokerCnt\":").append(brokerCount)
                    .append(",\"totalCfgNumPart\":").append(totalCfgNumPartCount)
                    .append(",\"totalRunNumPartCount\":").append(totalRunNumPartCount)
                    .append(",\"isSrvAcceptPublish\":").append(isSrvAcceptPublish)
                    .append(",\"isSrvAcceptSubscribe\":").append(isSrvAcceptSubscribe)
                    .append(",\"enableAuthControl\":").append(enableAuthControl)
                    .append("}");
        }
        WebParameterUtils.buildSuccessWithDataRetEnd(sBuffer, totalCount);
        return sBuffer;
    }

}
