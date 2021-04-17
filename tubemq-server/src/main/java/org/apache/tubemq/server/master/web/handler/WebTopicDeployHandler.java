/*
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.servlet.http.HttpServletRequest;

import org.apache.tubemq.corebase.TBaseConstants;
import org.apache.tubemq.corebase.cluster.BrokerInfo;
import org.apache.tubemq.corebase.cluster.TopicInfo;
import org.apache.tubemq.corebase.utils.Tuple2;
import org.apache.tubemq.server.common.fielddef.WebFieldDef;
import org.apache.tubemq.server.common.statusdef.ManageStatus;
import org.apache.tubemq.server.common.statusdef.TopicStatus;
import org.apache.tubemq.server.common.utils.ProcessResult;
import org.apache.tubemq.server.common.utils.WebParameterUtils;
import org.apache.tubemq.server.master.TMaster;
import org.apache.tubemq.server.master.metamanage.DataOpErrCode;
import org.apache.tubemq.server.master.metamanage.metastore.dao.entity.BaseEntity;
import org.apache.tubemq.server.master.metamanage.metastore.dao.entity.BrokerConfEntity;
import org.apache.tubemq.server.master.metamanage.metastore.dao.entity.ClusterSettingEntity;
import org.apache.tubemq.server.master.metamanage.metastore.dao.entity.GroupConsumeCtrlEntity;
import org.apache.tubemq.server.master.metamanage.metastore.dao.entity.TopicCtrlEntity;
import org.apache.tubemq.server.master.metamanage.metastore.dao.entity.TopicDeployEntity;
import org.apache.tubemq.server.master.metamanage.metastore.dao.entity.TopicPropGroup;
import org.apache.tubemq.server.master.nodemanage.nodebroker.BrokerSyncStatusInfo;
import org.apache.tubemq.server.master.nodemanage.nodebroker.TopicPSInfoManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public class WebTopicDeployHandler extends AbstractWebHandler {

    private static final Logger logger =
            LoggerFactory.getLogger(WebTopicDeployHandler.class);

    /**
     * Constructor
     *
     * @param master tube master
     */
    public WebTopicDeployHandler(TMaster master) {
        super(master);
    }



    @Override
    public void registerWebApiMethod() {
        // register query method
        registerQueryWebMethod("admin_query_topic_info",
                "adminQueryTopicCfgEntityAndRunInfo");
        registerQueryWebMethod("admin_query_broker_topic_config_info",
                "adminQueryBrokerTopicCfgAndRunInfo");
        registerQueryWebMethod("admin_query_topicName",
                "adminQuerySimpleTopicName");
        registerQueryWebMethod("admin_query_brokerId",
                "adminQuerySimpleBrokerId");
        // register modify method
        registerModifyWebMethod("admin_add_new_topic_record",
                "adminAddTopicEntityInfo");
        registerModifyWebMethod("admin_bath_add_new_topic_record",
                "adminBatchAddTopicEntityInfo");
        registerModifyWebMethod("admin_modify_topic_info",
                "adminModifyTopicEntityInfo");
        registerModifyWebMethod("admin_delete_topic_info",
                "adminDeleteTopicEntityInfo");
        registerModifyWebMethod("admin_redo_deleted_topic_info",
                "adminRedoDeleteTopicEntityInfo");
        registerModifyWebMethod("admin_remove_topic_info",
                "adminRemoveTopicEntityInfo");
    }

    /**
     * Query topic info
     *
     * @param req
     * @return
     */
    public StringBuilder adminQueryTopicCfgEntityAndRunInfo(HttpServletRequest req) {
        ProcessResult result = new ProcessResult();
        StringBuilder sBuffer = new StringBuilder(512);
        TopicDeployEntity qryEntity = new TopicDeployEntity();
        // get queried operation info, for createUser, modifyUser, dataVersionId
        if (!WebParameterUtils.getQueriedOperateInfo(req, qryEntity, result)) {
            WebParameterUtils.buildFailResult(sBuffer, result.errInfo);
            return sBuffer;
        }
        // check and get topicName field
        if (!WebParameterUtils.getStringParamValue(req,
                WebFieldDef.COMPSTOPICNAME, false, null, result)) {
            WebParameterUtils.buildFailResult(sBuffer, result.errInfo);
            return sBuffer;
        }
        Set<String> topicNameSet = (Set<String>) result.retData1;
        // check and get brokerId field
        if (!WebParameterUtils.getIntParamValue(req,
                WebFieldDef.COMPSBROKERID, false, result)) {
            WebParameterUtils.buildFailResult(sBuffer, result.errInfo);
            return sBuffer;
        }
        Set<Integer> brokerIdSet = (Set<Integer>) result.retData1;
        // get brokerPort field
        if (!WebParameterUtils.getIntParamValue(req, WebFieldDef.BROKERPORT,
                false, TBaseConstants.META_VALUE_UNDEFINED, 1, result)) {
            WebParameterUtils.buildFailResult(sBuffer, result.errInfo);
            return sBuffer;
        }
        int brokerPort = (int) result.getRetData();
        // get and valid topicProps info
        if (!WebParameterUtils.getTopicPropInfo(req,
                null, result)) {
            WebParameterUtils.buildFailResult(sBuffer, result.errInfo);
            return sBuffer;
        }
        TopicPropGroup topicProps = (TopicPropGroup) result.getRetData();
        // get and valid TopicStatusId info
        if (!WebParameterUtils.getTopicStatusParamValue(req,
                false, TopicStatus.STATUS_TOPIC_UNDEFINED, result)) {
            WebParameterUtils.buildFailResult(sBuffer, result.errInfo);
            return sBuffer;
        }
        TopicStatus topicStatus = (TopicStatus) result.getRetData();
        qryEntity.updModifyInfo(qryEntity.getDataVerId(),
                TBaseConstants.META_VALUE_UNDEFINED,
                brokerPort, null, topicStatus, topicProps);
        Map<String, List<TopicDeployEntity>> topicDeployInfoMap =
                metaDataManager.getTopicConfEntityMap(topicNameSet, brokerIdSet, qryEntity);
        // build query result
        return buildQueryResult(sBuffer, true, topicDeployInfoMap);
    }

    private StringBuilder buildQueryResult(StringBuilder sBuffer,
                                           boolean withAuthInfo,
                                           Map<String, List<TopicDeployEntity>> topicDeployInfoMap) {
        // build query result
        int totalCnt = 0;
        int totalCfgNumPartCount = 0;
        int totalRunNumPartCount = 0;
        boolean isSrvAcceptPublish = false;
        boolean isSrvAcceptSubscribe = false;
        boolean isAcceptPublish = false;
        boolean isAcceptSubscribe = false;
        ManageStatus manageStatus;
        Tuple2<Boolean, Boolean> pubSubStatus;
        TopicPSInfoManager topicPSInfoManager = master.getTopicPSInfoManager();
        WebParameterUtils.buildSuccessWithDataRetBegin(sBuffer);
        for (Map.Entry<String, List<TopicDeployEntity>> entry : topicDeployInfoMap.entrySet()) {
            totalCfgNumPartCount = 0;
            totalRunNumPartCount = 0;
            isSrvAcceptPublish = false;
            isSrvAcceptSubscribe = false;
            isAcceptPublish = false;
            isAcceptSubscribe = false;
            TopicCtrlEntity ctrlEntity =
                    metaDataManager.getTopicCtrlByTopicName(entry.getKey());
            ctrlEntity.toWebJsonStr(sBuffer, true, false);
            sBuffer.append(",\"deployInfo\":[");
            int brokerCount = 0;
            for (TopicDeployEntity entity : entry.getValue()) {
                if (brokerCount++ > 0) {
                    sBuffer.append(",");
                }
                totalCfgNumPartCount += entity.getNumPartitions() * entity.getNumTopicStores();
                entity.toWebJsonStr(sBuffer, true, false);
                sBuffer.append("\",\"runInfo\":{");
                BrokerConfEntity brokerConfEntity =
                        metaDataManager.getBrokerConfByBrokerId(entity.getBrokerId());
                String strManageStatus = "-";
                if (brokerConfEntity != null) {
                    manageStatus = brokerConfEntity.getManageStatus();
                    strManageStatus = manageStatus.getDescription();
                    pubSubStatus = manageStatus.getPubSubStatus();
                    isAcceptPublish = pubSubStatus.getF0();
                    isAcceptSubscribe = pubSubStatus.getF1();
                }
                BrokerInfo broker = new BrokerInfo(entity.getBrokerId(),
                        entity.getBrokerIp(), entity.getBrokerPort());
                TopicInfo topicInfo =
                        topicPSInfoManager.getTopicInfo(entity.getTopicName(), broker);
                if (topicInfo == null) {
                    sBuffer.append("\"acceptPublish\":\"-\"").append(",\"acceptSubscribe\":\"-\"")
                            .append(",\"numPartitions\":\"-\"").append(",\"brokerManageStatus\":\"-\"");
                } else {
                    if (isAcceptPublish) {
                        sBuffer.append("\"acceptPublish\":").append(topicInfo.isAcceptPublish());
                        if (topicInfo.isAcceptPublish()) {
                            isSrvAcceptPublish = true;
                        }
                    } else {
                        sBuffer.append("\"acceptPublish\":false");
                    }
                    if (isAcceptSubscribe) {
                        sBuffer.append(",\"acceptSubscribe\":").append(topicInfo.isAcceptSubscribe());
                        if (topicInfo.isAcceptSubscribe()) {
                            isSrvAcceptSubscribe = true;
                        }
                    } else {
                        sBuffer.append(",\"acceptSubscribe\":false");
                    }
                    totalRunNumPartCount += topicInfo.getPartitionNum() * topicInfo.getTopicStoreNum();
                    sBuffer.append(",\"numPartitions\":").append(topicInfo.getPartitionNum())
                            .append(",\"numTopicStores\":").append(topicInfo.getTopicStoreNum())
                            .append(",\"brokerManageStatus\":\"").append(strManageStatus).append("\"");
                }
                sBuffer.append("}}");
            }
            sBuffer.append("],\"infoCount\":").append(brokerCount)
                    .append(",\"totalCfgNumPart\":").append(totalCfgNumPartCount)
                    .append(",\"isSrvAcceptPublish\":").append(isSrvAcceptPublish)
                    .append(",\"isSrvAcceptSubscribe\":").append(isSrvAcceptSubscribe)
                    .append(",\"totalRunNumPartCount\":").append(totalRunNumPartCount);
            if (withAuthInfo) {
                sBuffer.append(",\"authConsumeGroup\":[");
                List<GroupConsumeCtrlEntity> groupCtrlInfoLst =
                        metaDataManager.getConsumeCtrlByTopic(entry.getKey());
                int countJ = 0;
                for (GroupConsumeCtrlEntity groupEntity : groupCtrlInfoLst) {
                    if (countJ++ > 0) {
                        sBuffer.append(",");
                    }
                    groupEntity.toWebJsonStr(sBuffer, true, false);
                }
                sBuffer.append("],\"groupCount\":").append(countJ);
            }
            sBuffer.append("}");
        }
        WebParameterUtils.buildSuccessWithDataRetEnd(sBuffer, totalCnt);
        return sBuffer;
    }

    /**
     * Query broker topic config info
     *
     * @param req
     * @return
     */
    public StringBuilder adminQueryBrokerTopicCfgAndRunInfo(HttpServletRequest req) {
        ProcessResult result = new ProcessResult();
        StringBuilder sBuffer = new StringBuilder(512);
        if (!WebParameterUtils.getStringParamValue(req,
                WebFieldDef.COMPSTOPICNAME, false, null, result)) {
            WebParameterUtils.buildFailResult(sBuffer, result.errInfo);
            return sBuffer;
        }
        Set<String> topicNameSet = (Set<String>) result.retData1;
        // check and get brokerId field
        if (!WebParameterUtils.getIntParamValue(req,
                WebFieldDef.COMPSBROKERID, false, result)) {
            WebParameterUtils.buildFailResult(sBuffer, result.errInfo);
            return sBuffer;
        }
        Set<Integer> brokerIdSet = (Set<Integer>) result.retData1;
        Map<Integer, List<TopicDeployEntity>> queryResult =
                metaDataManager.getTopicDeployInfoMap(topicNameSet, brokerIdSet);
        // build query result
        int dataCount = 0;
        int totalStoreNum = 0;
        int totalNumPartCount = 0;
        WebParameterUtils.buildSuccessWithDataRetBegin(sBuffer);
        for (Map.Entry<Integer, List<TopicDeployEntity>> entry : queryResult.entrySet()) {
            if (entry.getKey() == null || entry.getValue() == null) {
                continue;
            }
            BrokerConfEntity brokerConf =
                    metaDataManager.getBrokerConfByBrokerId(entry.getKey());
            if (brokerConf == null) {
                continue;
            }
            BrokerSyncStatusInfo brokerSyncInfo =
                    metaDataManager.getBrokerRunSyncStatusInfo(entry.getKey());
            List<TopicDeployEntity> topicDeployInfo = entry.getValue();
            // build return info item
            if (dataCount++ > 0) {
                sBuffer.append(",");
            }
            totalStoreNum = 0;
            totalNumPartCount = 0;
            sBuffer.append("{\"brokerId\":").append(brokerConf.getBrokerId())
                    .append(",\"brokerIp\":\"").append(brokerConf.getBrokerIp())
                    .append("\",\"brokerPort\":").append(brokerConf.getBrokerPort())
                    .append(",\"runInfo\":{");
            String strManageStatus =
                    brokerConf.getManageStatus().getDescription();
            Tuple2<Boolean, Boolean> pubSubStatus =
                    brokerConf.getManageStatus().getPubSubStatus();
            if (brokerSyncInfo == null) {
                sBuffer.append("\"acceptPublish\":\"-\"")
                        .append(",\"acceptSubscribe\":\"-\"")
                        .append(",\"totalPartitionNum\":\"-\"")
                        .append(",\"totalTopicStoreNum\":\"-\"")
                        .append(",\"brokerManageStatus\":\"-\"");
            } else {
                if (pubSubStatus.getF0()) {
                    sBuffer.append("\"acceptPublish\":")
                            .append(brokerSyncInfo.isAcceptPublish());
                } else {
                    sBuffer.append("\"acceptPublish\":false");
                }
                if (pubSubStatus.getF1()) {
                    sBuffer.append(",\"acceptSubscribe\":")
                            .append(brokerSyncInfo.isAcceptSubscribe());
                } else {
                    sBuffer.append(",\"acceptSubscribe\":false");
                }
                for (TopicDeployEntity topicEntity : topicDeployInfo) {
                    if (topicEntity == null) {
                        continue;
                    }
                    totalStoreNum +=
                            topicEntity.getNumTopicStores();
                    totalNumPartCount +=
                            topicEntity.getNumTopicStores() * topicEntity.getNumPartitions();
                }
                sBuffer.append(",\"totalPartitionNum\":").append(totalNumPartCount)
                        .append(",\"totalTopicStoreNum\":").append(totalStoreNum)
                        .append(",\"brokerManageStatus\":\"")
                        .append(strManageStatus).append("\"");
            }
            sBuffer.append("}}");
        }
        WebParameterUtils.buildSuccessWithDataRetEnd(sBuffer, dataCount);
        return sBuffer;
    }

    /**
     * Query broker's topic-name set info
     *
     * @param req
     * @return
     */
    public StringBuilder adminQuerySimpleTopicName(HttpServletRequest req) {
        ProcessResult result = new ProcessResult();
        StringBuilder sBuilder = new StringBuilder(512);
        if (!WebParameterUtils.getIntParamValue(req,
                WebFieldDef.COMPSBROKERID, false, result)) {
            WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
            return sBuilder;
        }
        Set<Integer> brokerIds = (Set<Integer>) result.retData1;
        Map<Integer, Set<String>> brokerTopicConfigMap =
                metaDataManager.getBrokerTopicConfigInfo(brokerIds);
        // build query result
        int dataCount = 0;
        WebParameterUtils.buildSuccessWithDataRetBegin(sBuilder);
        for (Map.Entry<Integer, Set<String>> entry : brokerTopicConfigMap.entrySet()) {
            if (dataCount++ > 0) {
                sBuilder.append(",");
            }
            sBuilder.append("{\"brokerId\":").append(entry.getKey()).append(",\"topicName\":[");
            int topicCnt = 0;
            Set<String> topicSet = entry.getValue();
            for (String topic : topicSet) {
                if (topicCnt++ > 0) {
                    sBuilder.append(",");
                }
                sBuilder.append("\"").append(topic).append("\"");
            }
            sBuilder.append("],\"topicCount\":").append(topicCnt).append("}");
        }
        WebParameterUtils.buildSuccessWithDataRetEnd(sBuilder, dataCount);
        return sBuilder;
    }

    /**
     * Query topic's broker id set
     *
     * @param req
     * @return
     */
    public StringBuilder adminQuerySimpleBrokerId(HttpServletRequest req) {
        ProcessResult result = new ProcessResult();
        StringBuilder sBuilder = new StringBuilder(512);
        if (!WebParameterUtils.getStringParamValue(req,
                WebFieldDef.COMPSTOPICNAME, false, null, result)) {
            WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
            return sBuilder;
        }
        Set<String> topicNameSet = (Set<String>) result.retData1;
        if (!WebParameterUtils.getBooleanParamValue(req,
                WebFieldDef.WITHIP, false, false, result)) {
            WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
            return sBuilder;
        }
        boolean withIp = (Boolean) result.retData1;
        Map<String, Map<Integer, String>> topicBrokerConfigMap =
                metaDataManager.getTopicBrokerConfigInfo(topicNameSet);
        // build query result
        int dataCount = 0;
        WebParameterUtils.buildSuccessWithDataRetBegin(sBuilder);
        for (Map.Entry<String, Map<Integer, String>> entry : topicBrokerConfigMap.entrySet()) {
            if (dataCount++ > 0) {
                sBuilder.append(",");
            }
            sBuilder.append("{\"topicName\":\"").append(entry.getKey()).append("\",\"brokerInfo\":[");
            int topicCnt = 0;
            Map<Integer, String> brokerMap = entry.getValue();
            if (withIp) {
                for (Map.Entry<Integer, String> entry1 : brokerMap.entrySet()) {
                    if (topicCnt++ > 0) {
                        sBuilder.append(",");
                    }
                    sBuilder.append("{\"brokerId\":").append(entry1.getKey())
                            .append(",\"brokerIp\":\"").append(entry1.getValue()).append("\"}");
                }
            } else {
                for (Map.Entry<Integer, String> entry1 : brokerMap.entrySet()) {
                    if (topicCnt++ > 0) {
                        sBuilder.append(",");
                    }
                    sBuilder.append(entry1.getKey());
                }
            }
            sBuilder.append("],\"brokerCnt\":").append(topicCnt).append("}");
        }
        WebParameterUtils.buildSuccessWithDataRetEnd(sBuilder, dataCount);
        return sBuilder;
    }

    /**
     * Add new topic record
     *
     * @param req
     * @return
     */
    public StringBuilder adminAddTopicEntityInfo(HttpServletRequest req) {
        ProcessResult result = new ProcessResult();
        StringBuilder sBuilder = new StringBuilder(512);
        // valid operation authorize info
        if (!WebParameterUtils.validReqAuthorizeInfo(req,
                WebFieldDef.ADMINAUTHTOKEN, true, master, result)) {
            WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
            return sBuilder;
        }
        // check and get operation info
        if (!WebParameterUtils.getAUDBaseInfo(req, true, result)) {
            WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
            return sBuilder;
        }
        BaseEntity opEntity = (BaseEntity) result.getRetData();
        // check and get topicName info
        if (!WebParameterUtils.getStringParamValue(req,
                WebFieldDef.COMPSTOPICNAME, true, null, result)) {
            WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
            return sBuilder;
        }
        Set<String> topicNameSet = (Set<String>) result.retData1;
        // check and get brokerId info
        if (!WebParameterUtils.getIntParamValue(req,
                WebFieldDef.COMPSBROKERID, true, result)) {
            WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
            return sBuilder;
        }
        Set<Integer> brokerIdSet = (Set<Integer>) result.retData1;
        // get and valid TopicPropGroup info
        if (!WebParameterUtils.getTopicPropInfo(req,
                null, result)) {
            WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
            return sBuilder;
        }
        TopicPropGroup topicPropInfo = (TopicPropGroup) result.getRetData();
        /* check max message size
        if (!WebParameterUtils.getIntParamValue(req,
                WebFieldDef.MAXMSGSIZEINMB, false,
                TBaseConstants.META_MIN_ALLOWED_MESSAGE_SIZE_MB,
                TBaseConstants.META_MIN_ALLOWED_MESSAGE_SIZE_MB,
                TBaseConstants.META_MAX_ALLOWED_MESSAGE_SIZE_MB,
                result)) {
            WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
            return sBuilder;
        }
        int inMaxMsgSizeMB = (int) result.getRetData();
         */
        List<TopicProcessResult> retInfo =
                metaDataManager.addTopicDeployInfo(opEntity, brokerIdSet,
                        topicNameSet, topicPropInfo, sBuilder, result);
        return buildRetInfo(retInfo, sBuilder);
    }

    /**
     * Add new topic record in batch
     *
     * @param req
     * @return
     */
    public StringBuilder adminBatchAddTopicEntityInfo(HttpServletRequest req) {
        ProcessResult result = new ProcessResult();
        StringBuilder sBuilder = new StringBuilder(512);
        // valid operation authorize info
        if (!WebParameterUtils.validReqAuthorizeInfo(req,
                WebFieldDef.ADMINAUTHTOKEN, true, master, result)) {
            WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
            return sBuilder;
        }
        // check and get operation info
        if (!WebParameterUtils.getAUDBaseInfo(req, true, result)) {
            WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
            return sBuilder;
        }
        BaseEntity defOpEntity = (BaseEntity) result.getRetData();
        // check and get add record map
        if (!getTopicDeployJsonSetInfo(req, true,
                defOpEntity, null, sBuilder, result)) {
            WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
            return sBuilder;
        }
        Map<String, TopicDeployEntity> addRecordMap =
                (Map<String, TopicDeployEntity>) result.getRetData();
        List<TopicProcessResult> retInfo = new ArrayList<>();
        for (TopicDeployEntity topicDeployInfo : addRecordMap.values()) {
            retInfo.add(metaDataManager.addTopicDeployInfo(topicDeployInfo, sBuilder, result));
        }
        return buildRetInfo(retInfo, sBuilder);
    }

    /**
     * Modify topic info
     *
     * @param req
     * @return
     * @throws Exception
     */
    // #lizard forgives
    public StringBuilder adminModifyTopicEntityInfo(HttpServletRequest req) {
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
        BaseEntity opEntity = (BaseEntity) result.getRetData();
        // check and get topicName info
        if (!WebParameterUtils.getStringParamValue(req,
                WebFieldDef.COMPSTOPICNAME, true, null, result)) {
            WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
            return sBuilder;
        }
        Set<String> topicNameSet = (Set<String>) result.retData1;
        // check and get brokerId info
        if (!WebParameterUtils.getIntParamValue(req,
                WebFieldDef.COMPSBROKERID, true, result)) {
            WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
            return sBuilder;
        }
        Set<Integer> brokerIdSet = (Set<Integer>) result.retData1;
        // get and valid TopicPropGroup info
        if (!WebParameterUtils.getTopicPropInfo(req, null, result)) {
            WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
            return sBuilder;
        }
        TopicPropGroup topicProps = (TopicPropGroup) result.getRetData();
        // modify records
        List<TopicProcessResult> retInfo =
                metaDataManager.modTopicConfig(opEntity, brokerIdSet,
                        topicNameSet, topicProps, sBuilder, result);
        return buildRetInfo(retInfo, sBuilder);
    }

    /**
     * Delete topic info
     *
     * @param req
     * @return
     * @throws Exception
     */
    public StringBuilder adminDeleteTopicEntityInfo(HttpServletRequest req) {
        return innModifyTopicStatusEntityInfo(req, TopicStatus.STATUS_TOPIC_SOFT_DELETE);
    }

    /**
     * Remove topic info
     *
     * @param req
     * @return
     * @throws Exception
     */
    public StringBuilder adminRemoveTopicEntityInfo(HttpServletRequest req) {
        return innModifyTopicStatusEntityInfo(req, TopicStatus.STATUS_TOPIC_SOFT_REMOVE);
    }

    /**
     * Redo delete topic info
     *
     * @param req
     * @return
     * @throws Exception
     */
    // #lizard forgives
    public StringBuilder adminRedoDeleteTopicEntityInfo(HttpServletRequest req) {
        ProcessResult result = new ProcessResult();
        StringBuilder sBuffer = new StringBuilder(512);
        // valid operation authorize info
        if (!WebParameterUtils.validReqAuthorizeInfo(req,
                WebFieldDef.ADMINAUTHTOKEN, true, master, result)) {
            WebParameterUtils.buildFailResult(sBuffer, result.errInfo);
            return sBuffer;
        }
        // check and get operation info
        if (!WebParameterUtils.getAUDBaseInfo(req, false, result)) {
            WebParameterUtils.buildFailResult(sBuffer, result.errInfo);
            return sBuffer;
        }
        BaseEntity opEntity = (BaseEntity) result.getRetData();
        // check and get topicName info
        if (!WebParameterUtils.getStringParamValue(req,
                WebFieldDef.COMPSTOPICNAME, true, null, result)) {
            WebParameterUtils.buildFailResult(sBuffer, result.errInfo);
            return sBuffer;
        }
        Set<String> topicNameSet = (Set<String>) result.retData1;
        // check and get brokerId info
        if (!WebParameterUtils.getIntParamValue(req,
                WebFieldDef.COMPSBROKERID, true, result)) {
            WebParameterUtils.buildFailResult(sBuffer, result.errInfo);
            return sBuffer;
        }
        Set<Integer> brokerIdSet = (Set<Integer>) result.retData1;
        // modify records
        List<TopicProcessResult> retInfo =
                metaDataManager.modRedoDelTopicConf(opEntity, brokerIdSet,
                        topicNameSet, sBuffer, result);
        return buildRetInfo(retInfo, sBuffer);
    }



    private boolean getTopicDeployJsonSetInfo(HttpServletRequest req, boolean isAdd,
                                              BaseEntity defOpEntity,
                                              List<Map<String, String>> defValue,
                                              StringBuilder sBuilder,
                                              ProcessResult result) {
        if (!WebParameterUtils.getJsonArrayParamValue(req,
                WebFieldDef.TOPICJSONSET, true, defValue, result)) {
            return result.success;
        }
        List<Map<String, String>> deployJsonArray =
                (List<Map<String, String>>) result.retData1;
        // check and get cluster default setting info
        ClusterSettingEntity defClusterSetting =
                metaDataManager.getClusterDefSetting(false);
        TopicDeployEntity itemConf;
        Map<String, TopicDeployEntity> addRecordMap = new HashMap<>();
        // check and get topic deployment configure
        for (int j = 0; j < deployJsonArray.size(); j++) {
            Map<String, String> confMap = deployJsonArray.get(j);
            // check and get operation info
            if (!WebParameterUtils.getAUDBaseInfo(confMap, isAdd, defOpEntity, result)) {
                return result.isSuccess();
            }
            BaseEntity itemOpEntity = (BaseEntity) result.getRetData();
            // get topicName configure info
            if (!WebParameterUtils.getStringParamValue(confMap,
                    WebFieldDef.TOPICNAME, true, "", result)) {
                return result.success;
            }
            String topicName = (String) result.retData1;
            // get broker configure info
            if (!getBrokerConfInfo(confMap, sBuilder, result)) {
                return result.isSuccess();
            }
            BrokerConfEntity brokerConf =
                    (BrokerConfEntity) result.getRetData();
            // get and valid TopicPropGroup info
            if (!WebParameterUtils.getTopicPropInfo(confMap,
                    defClusterSetting.getClsDefTopicProps(), result)) {
                return result.isSuccess();
            }
            TopicPropGroup topicPropInfo = (TopicPropGroup) result.getRetData();
            /* check max message size
            if (!WebParameterUtils.getIntParamValue(confMap,
                    WebFieldDef.MAXMSGSIZEINMB, false,
                    TBaseConstants.META_MIN_ALLOWED_MESSAGE_SIZE_MB,
                    TBaseConstants.META_MIN_ALLOWED_MESSAGE_SIZE_MB,
                    TBaseConstants.META_MAX_ALLOWED_MESSAGE_SIZE_MB,
                    result)) {
                return result.isSuccess();
            }
            int inMaxMsgSizeMB = (int) result.getRetData();
             */
            // get topicNameId field
            int topicNameId = TBaseConstants.META_VALUE_UNDEFINED;
            TopicCtrlEntity topicCtrlEntity =
                    metaDataManager.getTopicCtrlByTopicName(topicName);
            if (topicCtrlEntity != null) {
                topicNameId = topicCtrlEntity.getTopicId();
            }
            itemConf = new TopicDeployEntity(itemOpEntity, brokerConf.getBrokerId(),
                    brokerConf.getBrokerIp(), brokerConf.getBrokerPort(), topicName);
            itemConf.updModifyInfo(itemOpEntity.getDataVerId(), topicNameId,
                    TBaseConstants.META_VALUE_UNDEFINED, null,
                    TopicStatus.STATUS_TOPIC_OK, topicPropInfo);
            addRecordMap.put(itemConf.getRecordKey(), itemConf);
        }
        // check result
        if (addRecordMap.isEmpty()) {
            result.setFailResult(sBuilder
                    .append("Not found record in ")
                    .append(WebFieldDef.TOPICJSONSET.name)
                    .append(" parameter!").toString());
            sBuilder.delete(0, sBuilder.length());
            return result.isSuccess();
        }
        result.setSuccResult(addRecordMap);
        return result.isSuccess();
    }

    private boolean getBrokerConfInfo(Map<String, String> keyValueMap,
                                      StringBuilder sBuilder, ProcessResult result) {
        // get brokerId
        if (!WebParameterUtils.getIntParamValue(keyValueMap,
                WebFieldDef.BROKERID, true, 0, 0, result)) {
            return result.success;
        }
        int brokerId = (int) result.getRetData();
        BrokerConfEntity curEntity =
                metaDataManager.getBrokerConfByBrokerId(brokerId);
        if (curEntity == null) {
            result.setFailResult(DataOpErrCode.DERR_NOT_EXIST.getCode(),
                    sBuilder.append("Not found broker configure by ")
                            .append(WebFieldDef.BROKERID.name).append(" = ").append(brokerId)
                            .append(", please create the broker's configure first!").toString());
            return result.isSuccess();
        }
        result.setSuccResult(curEntity);
        return result.isSuccess();
    }

    private StringBuilder buildRetInfo(List<TopicProcessResult> retInfo,
                                       StringBuilder sBuilder) {
        int totalCnt = 0;
        WebParameterUtils.buildSuccessWithDataRetBegin(sBuilder);
        for (TopicProcessResult entry : retInfo) {
            if (totalCnt++ > 0) {
                sBuilder.append(",");
            }
            sBuilder.append("{\"brokerId\":").append(entry.getBrokerId())
                    .append(",\"topicName\":\"").append(entry.getTopicName()).append("\"")
                    .append(",\"success\":").append(entry.isSuccess())
                    .append(",\"errCode\":").append(entry.getErrCode())
                    .append(",\"errInfo\":\"").append(entry.getErrInfo()).append("\"}");
        }
        WebParameterUtils.buildSuccessWithDataRetEnd(sBuilder, totalCnt);
        return sBuilder;
    }


    /**
     * Internal method to perform deletion and removal of topic
     *
     * @param req
     * @param topicStatus
     * @return
     */
    // #lizard forgives
    private StringBuilder innModifyTopicStatusEntityInfo(HttpServletRequest req,
                                                         TopicStatus topicStatus) {
        ProcessResult result = new ProcessResult();
        StringBuilder sBuffer = new StringBuilder(512);
        // valid operation authorize info
        if (!WebParameterUtils.validReqAuthorizeInfo(req,
                WebFieldDef.ADMINAUTHTOKEN, true, master, result)) {
            WebParameterUtils.buildFailResult(sBuffer, result.errInfo);
            return sBuffer;
        }
        // check and get operation info
        if (!WebParameterUtils.getAUDBaseInfo(req, false, result)) {
            WebParameterUtils.buildFailResult(sBuffer, result.errInfo);
            return sBuffer;
        }
        BaseEntity opEntity = (BaseEntity) result.getRetData();
        // check and get topicName info
        if (!WebParameterUtils.getStringParamValue(req,
                WebFieldDef.COMPSTOPICNAME, true, null, result)) {
            WebParameterUtils.buildFailResult(sBuffer, result.errInfo);
            return sBuffer;
        }
        Set<String> topicNameSet = (Set<String>) result.retData1;
        // check and get brokerId info
        if (!WebParameterUtils.getIntParamValue(req,
                WebFieldDef.COMPSBROKERID, true, result)) {
            WebParameterUtils.buildFailResult(sBuffer, result.errInfo);
            return sBuffer;
        }
        Set<Integer> brokerIdSet = (Set<Integer>) result.retData1;
        // modify records
        List<TopicProcessResult> retInfo =
                metaDataManager.modDelOrRmvTopicConf(opEntity, brokerIdSet,
                        topicNameSet, topicStatus, sBuffer, result);
        return buildRetInfo(retInfo, sBuffer);
    }

}
