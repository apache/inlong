/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.inlong.tubemq.server.master.web.handler;

import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.servlet.http.HttpServletRequest;
import org.apache.inlong.tubemq.corebase.TBaseConstants;
import org.apache.inlong.tubemq.corebase.cluster.TopicInfo;
import org.apache.inlong.tubemq.corebase.rv.ProcessResult;
import org.apache.inlong.tubemq.corebase.utils.Tuple2;
import org.apache.inlong.tubemq.server.common.TServerConstants;
import org.apache.inlong.tubemq.server.common.fielddef.WebFieldDef;
import org.apache.inlong.tubemq.server.common.utils.WebParameterUtils;
import org.apache.inlong.tubemq.server.master.TMaster;
import org.apache.inlong.tubemq.server.master.metamanage.metastore.dao.entity.BaseEntity;
import org.apache.inlong.tubemq.server.master.metamanage.metastore.dao.entity.BrokerConfEntity;
import org.apache.inlong.tubemq.server.master.metamanage.metastore.dao.entity.ClusterSettingEntity;
import org.apache.inlong.tubemq.server.master.metamanage.metastore.dao.entity.TopicCtrlEntity;
import org.apache.inlong.tubemq.server.master.metamanage.metastore.dao.entity.TopicDeployEntity;
import org.apache.inlong.tubemq.server.master.metamanage.metastore.dao.entity.TopicPropGroup;
import org.apache.inlong.tubemq.server.master.nodemanage.nodebroker.BrokerRunManager;
import org.apache.inlong.tubemq.server.master.web.model.ClusterGroupVO;
import org.apache.inlong.tubemq.server.master.web.model.ClusterNodeVO;

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
        registerQueryWebMethod("admin_query_cluster_topic_view",
                "adminQueryClusterTopicView");
        registerQueryWebMethod("admin_query_cluster_default_setting",
                "adminQueryClusterDefSetting");

        // register modify method
        registerModifyWebMethod("admin_transfer_current_master",
                "transferCurrentMaster");
        registerModifyWebMethod("admin_set_cluster_default_setting",
                "adminSetClusterDefSetting");
        registerModifyWebMethod("admin_update_cluster_default_setting",
                "adminUpdClusterDefSetting");

        // Deprecated methods begin
        // query method
        registerQueryWebMethod("admin_query_def_flow_control_rule",
                "adminQueryDefFlowCtrlRule");
        // register modify method
        registerModifyWebMethod("admin_set_def_flow_control_rule",
                "adminSetDefFlowControlRule");
        registerModifyWebMethod("admin_rmv_def_flow_control_rule",
                "adminDelDefFlowControlRule");
        registerModifyWebMethod("admin_upd_def_flow_control_rule",
                "adminModDefFlowCtrlRule");

        // Deprecated methods end
    }

    /**
     * Get master group info
     *
     * @param req       Http Servlet Request
     * @param sBuffer   string buffer
     * @param result    process result
     * @return    process result
     */
    public StringBuilder getGroupAddressStrInfo(HttpServletRequest req,
                                                StringBuilder sBuffer,
                                                ProcessResult result) {
        ClusterGroupVO clusterGroupVO = metaDataManager.getGroupAddressStrInfo();
        if (clusterGroupVO == null) {
            WebParameterUtils.buildFailResultWithBlankData(
                    500, "GetBrokerGroup info error", sBuffer);
        } else {
            sBuffer.append("{\"result\":true,\"errCode\":0,\"errMsg\":\"Ok\",\"groupName\":\"")
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
                        sBuffer.append(",");
                    }
                    sBuffer.append("{\"index\":").append(count)
                            .append(",\"name\":\"").append(node.getNodeName())
                            .append("\",\"hostName\":\"").append(node.getHostName())
                            .append("\",\"port\":\"").append(node.getPort())
                            .append("\",\"statusInfo\":{").append("\"nodeStatus\":\"")
                            .append(node.getNodeStatus()).append("\",\"joinTime\":\"")
                            .append(node.getJoinTime()).append("\"}}");
                }
            }
            sBuffer.append("],\"count\":").append(count).append(",\"groupStatus\":\"")
                    .append(clusterGroupVO.getGroupStatus()).append("\"}");
        }
        return sBuffer;
    }

    /**
     * transfer current master to another node
     *
     * @param req       Http Servlet Request
     * @param sBuffer   string buffer
     * @param result    process result
     * @return    process result
     */
    public StringBuilder transferCurrentMaster(HttpServletRequest req,
                                               StringBuilder sBuffer,
                                               ProcessResult result) {
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
     * @param req       Http Servlet Request
     * @param sBuffer   string buffer
     * @param result    process result
     * @return    process result
     */
    public StringBuilder adminQueryClusterDefSetting(HttpServletRequest req,
                                                     StringBuilder sBuffer,
                                                     ProcessResult result) {
        return buildRetInfo(sBuffer, true);
    }

    /**
     * query default flow control rule
     *
     * @param req       Http Servlet Request
     * @param sBuffer   string buffer
     * @param result    process result
     * @return    process result
     */
    public StringBuilder adminQueryDefFlowCtrlRule(HttpServletRequest req,
                                                   StringBuilder sBuffer,
                                                   ProcessResult result) {
        return buildRetInfo(sBuffer, false);
    }

    /**
     * Add cluster default setting
     *
     * @param req       Http Servlet Request
     * @param sBuffer   string buffer
     * @param result    process result
     * @return    process result
     */
    public StringBuilder adminSetClusterDefSetting(HttpServletRequest req,
                                                   StringBuilder sBuffer,
                                                   ProcessResult result) {
        return innAddOrUpdDefFlowControlRule(req, sBuffer, result, true, true);
    }

    /**
     * Modify cluster default setting
     *
     * @param req       Http Servlet Request
     * @param sBuffer   string buffer
     * @param result    process result
     * @return    process result
     */
    public StringBuilder adminUpdClusterDefSetting(HttpServletRequest req,
                                                   StringBuilder sBuffer,
                                                   ProcessResult result) {
        return innAddOrUpdDefFlowControlRule(req, sBuffer, result, false, true);
    }

    /**
     * add default flow control rule
     *
     * @param req       Http Servlet Request
     * @param sBuffer   string buffer
     * @param result    process result
     * @return    process result
     */
    public StringBuilder adminSetDefFlowControlRule(HttpServletRequest req,
                                                    StringBuilder sBuffer,
                                                    ProcessResult result) {
        return innAddOrUpdDefFlowControlRule(req, sBuffer, result, true, false);
    }

    /**
     * update default flow control rule
     *
     * @param req       Http Servlet Request
     * @param sBuffer   string buffer
     * @param result    process result
     * @return    process result
     */
    public StringBuilder adminModDefFlowCtrlRule(HttpServletRequest req,
                                                 StringBuilder sBuffer,
                                                 ProcessResult result) {
        return innAddOrUpdDefFlowControlRule(req, sBuffer, result, false, false);
    }

    /**
     * Query cluster topic overall view
     *
     * @param req       Http Servlet Request
     * @param sBuffer   string buffer
     * @param result    process result
     * @return    process result
     */
    public StringBuilder adminQueryClusterTopicView(HttpServletRequest req,
                                                    StringBuilder sBuffer,
                                                    ProcessResult result) {
        // check and get brokerId field
        if (!WebParameterUtils.getIntParamValue(req,
                WebFieldDef.COMPSBROKERID, false, sBuffer, result)) {
            WebParameterUtils.buildFailResult(sBuffer, result.getErrMsg());
            return sBuffer;
        }
        Set<Integer> brokerIds = (Set<Integer>) result.getRetData();
        // check and get topicName field
        if (!WebParameterUtils.getStringParamValue(req,
                WebFieldDef.COMPSTOPICNAME, false, null, sBuffer, result)) {
            WebParameterUtils.buildFailResult(sBuffer, result.getErrMsg());
            return sBuffer;
        }
        Set<String> topicNameSet = (Set<String>) result.getRetData();
        // query topic configure info
        Map<String, List<TopicDeployEntity>> topicConfMap =
                metaDataManager.getTopicConfMapByTopicAndBrokerIds(topicNameSet, brokerIds);
        BrokerRunManager brokerRunManager = master.getBrokerRunManager();
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
                TopicInfo topicInfo =
                        brokerRunManager.getPubBrokerTopicInfo(entity.getBrokerId(), entity.getTopicName());
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

    /**
     * delete flow control rule
     *
     * @param req       Http Servlet Request
     * @param sBuffer   string buffer
     * @param result    process result
     * @return    process result
     */
    public StringBuilder adminDelDefFlowControlRule(HttpServletRequest req,
                                                    StringBuilder sBuffer,
                                                    ProcessResult result) {
        // check and get operation info
        if (!WebParameterUtils.getAUDBaseInfo(req, false, null, sBuffer, result)) {
            WebParameterUtils.buildFailResult(sBuffer, result.getErrMsg());
            return sBuffer;
        }
        BaseEntity opEntity = (BaseEntity) result.getRetData();
        // add or modify record
        if (!metaDataManager.addOrUpdClusterDefSetting(opEntity,
                TBaseConstants.META_VALUE_UNDEFINED, TBaseConstants.META_VALUE_UNDEFINED,
                TBaseConstants.META_VALUE_UNDEFINED, TBaseConstants.META_VALUE_UNDEFINED,
                TBaseConstants.META_VALUE_UNDEFINED, Boolean.FALSE, 0,
                TServerConstants.BLANK_FLOWCTRL_RULES, null, sBuffer, result)) {
            WebParameterUtils.buildFailResult(sBuffer, result.getErrMsg());
            return sBuffer;
        }
        return buildRetInfo(sBuffer, false);
    }

    /**
     * add default flow control rule
     *
     * @param req       Http Servlet Request
     * @param sBuffer   string buffer
     * @param result    process result
     * @param isAddOp   whether add operation
     * @param isNewVer  whether new version return
     * @return       process result
     */
    private StringBuilder innAddOrUpdDefFlowControlRule(HttpServletRequest req,
                                                        StringBuilder sBuffer,
                                                        ProcessResult result,
                                                        boolean isAddOp,
                                                        boolean isNewVer) {
        // check and get operation info
        if (!WebParameterUtils.getAUDBaseInfo(req, isAddOp, null, sBuffer, result)) {
            WebParameterUtils.buildFailResult(sBuffer, result.getErrMsg());
            return sBuffer;
        }
        final BaseEntity opEntity = (BaseEntity) result.getRetData();
        // check max message size
        if (!WebParameterUtils.getIntParamValue(req,
                WebFieldDef.MAXMSGSIZEINMB, false,
                TBaseConstants.META_VALUE_UNDEFINED,
                TBaseConstants.META_MIN_ALLOWED_MESSAGE_SIZE_MB,
                TBaseConstants.META_MAX_ALLOWED_MESSAGE_SIZE_MB, sBuffer, result)) {
            WebParameterUtils.buildFailResult(sBuffer, result.getErrMsg());
            return sBuffer;
        }
        final int maxMsgSizeMB = (int) result.getRetData();
        // get broker port info
        if (!WebParameterUtils.getIntParamValue(req, WebFieldDef.BROKERPORT,
                false, TBaseConstants.META_VALUE_UNDEFINED, 1, sBuffer, result)) {
            WebParameterUtils.buildFailResult(sBuffer, result.getErrMsg());
            return sBuffer;
        }
        final int inBrokerPort = (int) result.getRetData();
        // get broker tls port info
        if (!WebParameterUtils.getIntParamValue(req, WebFieldDef.BROKERTLSPORT,
                false, TBaseConstants.META_VALUE_UNDEFINED, 1, sBuffer, result)) {
            WebParameterUtils.buildFailResult(sBuffer, result.getErrMsg());
            return sBuffer;
        }
        final int inBrokerTlsPort = (int) result.getRetData();
        // get broker web port info
        if (!WebParameterUtils.getIntParamValue(req, WebFieldDef.BROKERWEBPORT,
                false, TBaseConstants.META_VALUE_UNDEFINED, 1, sBuffer, result)) {
            WebParameterUtils.buildFailResult(sBuffer, result.getErrMsg());
            return sBuffer;
        }
        final int inBrokerWebPort = (int) result.getRetData();
        // get and valid TopicPropGroup info
        TopicPropGroup defProps = null;
        if (isAddOp) {
            defProps = new TopicPropGroup();
            defProps.fillDefaultValue();
        }
        if (!WebParameterUtils.getTopicPropInfo(req, defProps, sBuffer, result)) {
            WebParameterUtils.buildFailResult(sBuffer, result.getErrMsg());
            return sBuffer;
        }
        final TopicPropGroup inTopicProps = (TopicPropGroup) result.getRetData();
        // get and valid qryPriorityId info
        if (!WebParameterUtils.getQryPriorityIdParameter(req,
                false, TBaseConstants.META_VALUE_UNDEFINED,
                TServerConstants.QRY_PRIORITY_MIN_VALUE, sBuffer, result)) {
            WebParameterUtils.buildFailResult(sBuffer, result.getErrMsg());
            return sBuffer;
        }
        int inQryPriorityId = (int) result.getRetData();
        // get flowCtrlEnable info
        if (isNewVer) {
            if (!WebParameterUtils.getBooleanParamValue(req,
                    WebFieldDef.FLOWCTRLENABLE, false, null, sBuffer, result)) {
                WebParameterUtils.buildFailResult(sBuffer, result.getErrMsg());
                return sBuffer;
            }
        } else {
            if (!WebParameterUtils.getFlowCtrlStatusParamValue(req,
                    false, null, sBuffer, result)) {
                WebParameterUtils.buildFailResult(sBuffer, result.getErrMsg());
                return sBuffer;
            }
        }
        Boolean flowCtrlEnable = (Boolean) result.getRetData();
        // get and flow control rule info
        int flowRuleCnt = WebParameterUtils.getAndCheckFlowRules(req,
                (isAddOp ? TServerConstants.BLANK_FLOWCTRL_RULES : null), sBuffer, result);
        if (!result.isSuccess()) {
            WebParameterUtils.buildFailResult(sBuffer, result.getErrMsg());
            return sBuffer;
        }
        String flowCtrlInfo = (String) result.getRetData();
        // add or modify record
        if (!metaDataManager.addOrUpdClusterDefSetting(opEntity, inBrokerPort,
                inBrokerTlsPort, inBrokerWebPort, maxMsgSizeMB, inQryPriorityId,
                flowCtrlEnable, flowRuleCnt, flowCtrlInfo, inTopicProps, sBuffer, result)) {
            WebParameterUtils.buildFailResult(sBuffer, result.getErrMsg());
            return sBuffer;
        }
        return buildRetInfo(sBuffer, isNewVer);
    }

    private StringBuilder buildRetInfo(StringBuilder sBuffer, boolean isNewVer) {
        int totalCnt = 0;
        ClusterSettingEntity curConf =
                metaDataManager.getClusterDefSetting(true);
        WebParameterUtils.buildSuccessWithDataRetBegin(sBuffer);
        if (curConf != null) {
            totalCnt++;
            if (isNewVer) {
                curConf.toWebJsonStr(sBuffer, true, true);
            } else {
                curConf.toOldVerFlowCtrlWebJsonStr(sBuffer, true);
            }
        }
        WebParameterUtils.buildSuccessWithDataRetEnd(sBuffer, totalCnt);
        return sBuffer;
    }
}
