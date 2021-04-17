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
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.servlet.http.HttpServletRequest;

import org.apache.tubemq.corebase.TBaseConstants;
import org.apache.tubemq.corebase.TokenConstants;
import org.apache.tubemq.corebase.utils.KeyBuilderUtils;
import org.apache.tubemq.corebase.utils.TStringUtils;
import org.apache.tubemq.server.common.TServerConstants;
import org.apache.tubemq.server.common.fielddef.WebFieldDef;
import org.apache.tubemq.server.common.utils.ProcessResult;
import org.apache.tubemq.server.common.utils.WebParameterUtils;
import org.apache.tubemq.server.master.TMaster;
import org.apache.tubemq.server.master.metamanage.DataOpErrCode;
import org.apache.tubemq.server.master.metamanage.metastore.dao.entity.BaseEntity;
import org.apache.tubemq.server.master.metamanage.metastore.dao.entity.GroupConsumeCtrlEntity;
import org.apache.tubemq.server.master.nodemanage.nodeconsumer.ConsumerBandInfo;
import org.apache.tubemq.server.master.nodemanage.nodeconsumer.ConsumerInfoHolder;
import org.apache.tubemq.server.master.nodemanage.nodeconsumer.NodeRebInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;




public class WebGroupConsumeCtrlHandler extends AbstractWebHandler {

    private static final Logger logger =
            LoggerFactory.getLogger(WebGroupConsumeCtrlHandler.class);


    public WebGroupConsumeCtrlHandler(TMaster master) {
        super(master);
    }

    @Override
    public void registerWebApiMethod() {
        // register query method
        registerQueryWebMethod("admin_query_black_consumer_group_info",
                "adminQueryBlackGroupInfo");
        registerQueryWebMethod("admin_query_allowed_consumer_group_info",
                "adminQueryConsumerGroupInfo");
        registerQueryWebMethod("admin_query_group_filtercond_info",
                "adminQueryGroupFilterCondInfo");
        registerQueryWebMethod("admin_query_consume_group_setting",
                "adminQueryConsumeGroupSetting");
        // register modify method
        registerModifyWebMethod("admin_add_black_consumergroup_info",
                "adminAddBlackGroupInfo");
        registerModifyWebMethod("admin_bath_add_black_consumergroup_info",
                "adminBatchAddBlackGroupInfo");
        registerModifyWebMethod("admin_delete_black_consumergroup_info",
                "adminDeleteBlackGroupInfo");
        registerModifyWebMethod("admin_add_authorized_consumergroup_info",
                "adminAddConsumerGroupInfo");
        registerModifyWebMethod("admin_delete_allowed_consumer_group_info",
                "adminDeleteConsumerGroupInfo");
        registerModifyWebMethod("admin_bath_add_authorized_consumergroup_info",
                "adminBatchAddConsumerGroupInfo");
        registerModifyWebMethod("admin_add_group_filtercond_info",
                "adminAddGroupFilterCondInfo");
        registerModifyWebMethod("admin_bath_add_group_filtercond_info",
                "adminBatchAddGroupFilterCondInfo");
        registerModifyWebMethod("admin_mod_group_filtercond_info",
                "adminModGroupFilterCondInfo");
        registerModifyWebMethod("admin_bath_mod_group_filtercond_info",
                "adminBatchModGroupFilterCondInfo");
        registerModifyWebMethod("admin_del_group_filtercond_info",
                "adminDeleteGroupFilterCondInfo");
        registerModifyWebMethod("admin_add_consume_group_setting",
                "adminAddConsumeGroupSettingInfo");
        registerModifyWebMethod("admin_bath_add_consume_group_setting",
                "adminBatchAddConsumeGroupSetting");
        registerModifyWebMethod("admin_upd_consume_group_setting",
                "adminUpdConsumeGroupSetting");
        registerModifyWebMethod("admin_del_consume_group_setting",
                "adminDeleteConsumeGroupSetting");
        registerModifyWebMethod("admin_rebalance_group_allocate",
                "adminRebalanceGroupAllocateInfo");
    }


    /**
     * query group consume control info
     *
     * @param req
     * @return
     */
    public StringBuilder adminQueryGroupConsumeCtrlInfo(HttpServletRequest req) {
        ProcessResult result = new ProcessResult();
        StringBuilder sBuilder = new StringBuilder(512);
        // build query entity
        GroupConsumeCtrlEntity qryEntity = new GroupConsumeCtrlEntity();
        // get queried operation info, for createUser, modifyUser, dataVersionId
        if (!WebParameterUtils.getQueriedOperateInfo(req, qryEntity, result)) {
            WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
            return sBuilder;
        }
        // get group list
        if (!WebParameterUtils.getStringParamValue(req,
                WebFieldDef.COMPSGROUPNAME, false, null, result)) {
            WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
            return sBuilder;
        }
        Set<String> groupSet = (Set<String>) result.retData1;
        // check and get topicName field
        if (!WebParameterUtils.getStringParamValue(req,
                WebFieldDef.COMPSTOPICNAME, false, null, result)) {
            WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
            return sBuilder;
        }
        Set<String> topicNameSet = (Set<String>) result.retData1;
        // get consumeEnable info
        if (!WebParameterUtils.getBooleanParamValue(req,
                WebFieldDef.CONSUMEENABLE, false, null, result)) {
            WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
            return sBuilder;
        }
        Boolean consumeEnable = (Boolean) result.retData1;
        // get filterEnable info
        if (!WebParameterUtils.getBooleanParamValue(req,
                WebFieldDef.FILTERENABLE, false, null, result)) {
            WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
            return sBuilder;
        }
        Boolean filterEnable = (Boolean) result.retData1;
        // get filterConds info
        if (!WebParameterUtils.getFilterCondSet(req, false, true, result)) {
            WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
            return sBuilder;
        }
        Set<String> filterCondSet = (Set<String>) result.retData1;
        qryEntity.updModifyInfo(qryEntity.getDataVerId(),
                consumeEnable, null, filterEnable, null);
        Map<String, List<GroupConsumeCtrlEntity>> qryResultSet =
                metaDataManager.getGroupConsumeCtrlConf(groupSet, topicNameSet);
        // build return result
        int totalCnt = 0;
        WebParameterUtils.buildSuccessWithDataRetBegin(sBuilder);
        for (List<GroupConsumeCtrlEntity> consumeCtrlEntityList : qryResultSet.values()) {
            if (consumeCtrlEntityList == null || consumeCtrlEntityList.isEmpty()) {
                continue;
            }
            for (GroupConsumeCtrlEntity entity : consumeCtrlEntityList) {
                if (entity == null
                        || !entity.isMatched(qryEntity)
                        || !isFilterItemAllIncluded(filterCondSet, entity.getFilterCondStr())) {
                    continue;
                }
                if (totalCnt++ > 0) {
                    sBuilder.append(",");
                }
                entity.toWebJsonStr(sBuilder, true, true);
            }
        }
        WebParameterUtils.buildSuccessWithDataRetEnd(sBuilder, totalCnt);
        return sBuilder;
    }

    /**
     * add group consume control info
     *
     * @param req
     * @return
     */
    public StringBuilder adminAddGroupConsumeCtrlInfo(HttpServletRequest req) {
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
        BaseEntity opInfoEntity = (BaseEntity) result.getRetData();
        // check and get topicName field
        if (!WebParameterUtils.getAndValidTopicNameInfo(req,
                metaDataManager, true, null, result)) {
            WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
            return sBuilder;
        }
        Set<String> topicNameSet = (Set<String>) result.retData1;
        // get groupName field
        if (!WebParameterUtils.getStringParamValue(req,
                WebFieldDef.COMPSGROUPNAME, true, null, result)) {
            WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
            return sBuilder;
        }
        Set<String> groupNameSet = (Set<String>) result.retData1;
        // get consumeEnable info
        if (!WebParameterUtils.getBooleanParamValue(req,
                WebFieldDef.CONSUMEENABLE, false, true, result)) {
            WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
            return sBuilder;
        }
        Boolean consumeEnable = (Boolean) result.retData1;
        // get disableReason list
        if (!WebParameterUtils.getStringParamValue(req,
                WebFieldDef.REASON, false, "", result)) {
            WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
            return sBuilder;
        }
        String disableRsn = (String) result.retData1;
        // get filterEnable info
        if (!WebParameterUtils.getBooleanParamValue(req,
                WebFieldDef.FILTERENABLE, false, false, result)) {
            WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
            return sBuilder;
        }
        Boolean filterEnable = (Boolean) result.retData1;
        // get filterConds info
        if (!WebParameterUtils.getFilterCondString(req, false, true, result)) {
            WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
            return sBuilder;
        }
        String filterCondStr = (String) result.retData1;
        // add group resource record
        GroupProcessResult csmProcessResult;
        List<GroupProcessResult> retInfo = new ArrayList<>();
        for (String groupName : groupNameSet) {
            for (String topicName : topicNameSet) {
                csmProcessResult =
                        metaDataManager.addGroupConsumeCtrlInfo(opInfoEntity, groupName,
                                topicName, consumeEnable, disableRsn,
                                filterEnable, filterCondStr, sBuilder, result);
                retInfo.add(csmProcessResult);
            }
        }
        buildRetInfo(retInfo, sBuilder);
        return sBuilder;
    }

    /**
     * Batch add group consume control info
     *
     * @param req
     * @return
     */
    public StringBuilder adminBatchAddGroupConsumeCtrlInfo(HttpServletRequest req) {
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
        // check and get groupCsmJsonSet data
        if (!getGroupConsumeJsonSetInfo(req, true,
                defOpEntity, null, sBuilder, result)) {
            WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
            return sBuilder;
        }
        Map<String, GroupProcessResult> batchAddInfoMap =
                (Map<String, GroupProcessResult>) result.getRetData();
        // add group resource record
        GroupConsumeCtrlEntity addEntity;
        GroupProcessResult addResult;
        List<GroupProcessResult> retInfo = new ArrayList<>();
        for (GroupProcessResult addInfo : batchAddInfoMap.values()) {
            if (!addInfo.isSuccess()) {
                retInfo.add(addInfo);
                continue;
            }
            addEntity = (GroupConsumeCtrlEntity) addInfo.getRetData();
            addResult = metaDataManager.addGroupConsumeCtrlInfo(addEntity, sBuilder, result);
            retInfo.add(addResult);
        }
        buildRetInfo(retInfo, sBuilder);
        return sBuilder;
    }

    /**
     * modify group consume control info
     *
     * @param req
     * @return
     */
    public StringBuilder adminModGroupConsumeCtrlInfo(HttpServletRequest req) {
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
        // check and get topicName field
        if (!WebParameterUtils.getAndValidTopicNameInfo(req,
                metaDataManager, true, null, result)) {
            WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
            return sBuilder;
        }
        Set<String> topicNameSet = (Set<String>) result.retData1;
        // get groupName field
        if (!WebParameterUtils.getStringParamValue(req,
                WebFieldDef.COMPSGROUPNAME, true, null, result)) {
            WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
            return sBuilder;
        }
        Set<String> groupNameSet = (Set<String>) result.retData1;
        // get consumeEnable info
        if (!WebParameterUtils.getBooleanParamValue(req,
                WebFieldDef.CONSUMEENABLE, false, null, result)) {
            WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
            return sBuilder;
        }
        Boolean consumeEnable = (Boolean) result.retData1;
        // get disableReason list
        if (!WebParameterUtils.getStringParamValue(req,
                WebFieldDef.REASON, false, null, result)) {
            WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
            return sBuilder;
        }
        String disableRsn = (String) result.retData1;
        // get filterEnable info
        if (!WebParameterUtils.getBooleanParamValue(req,
                WebFieldDef.FILTERENABLE, false, null, result)) {
            WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
            return sBuilder;
        }
        Boolean filterEnable = (Boolean) result.retData1;
        // get filterConds info
        if (!WebParameterUtils.getFilterCondString(req, false, false, result)) {
            WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
            return sBuilder;
        }
        String filterCondStr = (String) result.retData1;
        // modify group resource record
        GroupProcessResult csmProcessResult;
        List<GroupProcessResult> retInfo = new ArrayList<>();
        for (String groupName : groupNameSet) {
            for (String topicName : topicNameSet) {
                csmProcessResult =
                        metaDataManager.modGroupConsumeCtrlInfo(opEntity, groupName,
                                topicName, consumeEnable, disableRsn,
                                filterEnable, filterCondStr, sBuilder, result);
                retInfo.add(csmProcessResult);
            }
        }
        buildRetInfo(retInfo, sBuilder);
        return sBuilder;
    }

    /**
     * Modify group consume control info in batch
     *
     * @param req
     * @return
     */
    public StringBuilder adminBatchModGroupFilterCondInfo(HttpServletRequest req) {
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
        // check and get groupCsmJsonSet data
        if (!getGroupConsumeJsonSetInfo(req, true,
                opEntity, null, sBuilder, result)) {
            WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
            return sBuilder;
        }
        Map<String, GroupProcessResult> batchModInfoMap =
                (Map<String, GroupProcessResult>) result.getRetData();
        // add group resource record
        GroupConsumeCtrlEntity modEntity;
        GroupProcessResult modResult;
        List<GroupProcessResult> retInfo = new ArrayList<>();
        for (GroupProcessResult addInfo : batchModInfoMap.values()) {
            if (!addInfo.isSuccess()) {
                retInfo.add(addInfo);
                continue;
            }
            modEntity = (GroupConsumeCtrlEntity) addInfo.getRetData();
            modResult = metaDataManager.modGroupConsumeCtrlInfo(modEntity, sBuilder, result);
            retInfo.add(modResult);
        }
        buildRetInfo(retInfo, sBuilder);
        return sBuilder;
    }

    /**
     * Delete group consume configure info
     *
     * @param req
     * @return
     */
    public StringBuilder adminDeleteGroupFilterCondInfo(HttpServletRequest req) {
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
        // check and get topicName field
        if (!WebParameterUtils.getStringParamValue(req,
                WebFieldDef.COMPSTOPICNAME, true, null, result)) {
            WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
            return sBuilder;
        }
        Set<String> topicNameSet = (Set<String>) result.retData1;
        // get groupName field
        if (!WebParameterUtils.getStringParamValue(req,
                WebFieldDef.COMPSGROUPNAME, false, null, result)) {
            WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
            return sBuilder;
        }
        Set<String> groupNameSet = (Set<String>) result.retData1;
        // execute delete operation
        List<GroupProcessResult> retInfo =
                metaDataManager.delGroupConsumeCtrlConf(opEntity.getModifyUser(),
                        groupNameSet, topicNameSet, sBuilder, result);
        buildRetInfo(retInfo, sBuilder);
        return sBuilder;
    }


    private StringBuilder buildRetInfo(List<GroupProcessResult> retInfo,
                                       StringBuilder sBuilder) {
        int totalCnt = 0;
        WebParameterUtils.buildSuccessWithDataRetBegin(sBuilder);
        for (GroupProcessResult result : retInfo) {
            if (totalCnt++ > 0) {
                sBuilder.append(",");
            }
            sBuilder.append("{\"groupName\":\"").append(result.getGroupName()).append("\"")
                    .append("{\"topicName\":\"").append(result.getTopicName()).append("\"")
                    .append(",\"success\":").append(result.isSuccess())
                    .append(",\"errCode\":").append(result.getErrCode())
                    .append(",\"errInfo\":\"").append(result.getErrInfo()).append("\"}");
        }
        WebParameterUtils.buildSuccessWithDataRetEnd(sBuilder, totalCnt);
        return sBuilder;
    }



    /**
     * Re-balance group allocation info
     *
     * @param req
     * @return
     * @throws Exception
     */
    public StringBuilder adminRebalanceGroupAllocateInfo(HttpServletRequest req) {
        StringBuilder sBuilder = new StringBuilder(512);
        try {
            WebParameterUtils.reqAuthorizeCheck(master, brokerConfManager,
                    req.getParameter("confModAuthToken"));
            String groupName =
                WebParameterUtils.validGroupParameter("groupName",
                    req.getParameter("groupName"),
                    TBaseConstants.META_MAX_GROUPNAME_LENGTH,
                    true, "");
            String modifyUser =
                    WebParameterUtils.validStringParameter("modifyUser",
                            req.getParameter("modifyUser"),
                            TBaseConstants.META_MAX_USERNAME_LENGTH,
                            true, "");
            Date modifyDate =
                    WebParameterUtils.validDateParameter("modifyDate",
                            req.getParameter("modifyDate"),
                            TBaseConstants.META_MAX_DATEVALUE_LENGTH,
                            false, new Date());
            int reJoinWait =
                    WebParameterUtils.validIntDataParameter("reJoinWait",
                            req.getParameter("reJoinWait"),
                            false, 0, 0);
            Set<String> batchOpConsumerIds = new HashSet<>();
            String inputConsumerId = req.getParameter("consumerId");
            if (TStringUtils.isNotBlank(inputConsumerId)) {
                inputConsumerId = inputConsumerId.trim();
                String[] strInputConsumerIds =
                        inputConsumerId.split(TokenConstants.ARRAY_SEP);
                for (int i = 0; i < strInputConsumerIds.length; i++) {
                    if (TStringUtils.isBlank(strInputConsumerIds[i])) {
                        continue;
                    }
                    String consumerId = strInputConsumerIds[i].trim();
                    if (consumerId.length() > TServerConstants.CFG_CONSUMER_CLIENTID_MAX_LENGTH) {
                        throw new Exception(sBuilder.append("The max length of ")
                                .append(consumerId)
                                .append(" in consumerId parameter over ")
                                .append(TServerConstants.CFG_CONSUMER_CLIENTID_MAX_LENGTH)
                                .append(" characters").toString());
                    }
                    if (!consumerId.matches(TBaseConstants.META_TMP_CONSUMERID_VALUE)) {
                        throw new Exception(sBuilder.append("The value of ").append(consumerId)
                                .append("in consumerId parameter must begin with a letter, " +
                                        "can only contain characters,numbers,dot,scores,and underscores").toString());
                    }
                    if (!batchOpConsumerIds.contains(consumerId)) {
                        batchOpConsumerIds.add(consumerId);
                    }
                }
            }
            if (batchOpConsumerIds.isEmpty()) {
                throw new Exception("Null value of required consumerId parameter");
            }
            ConsumerInfoHolder consumerInfoHolder =
                    master.getConsumerHolder();
            ConsumerBandInfo consumerBandInfo =
                    consumerInfoHolder.getConsumerBandInfo(groupName);
            if (consumerBandInfo == null) {
                return sBuilder.append("{\"result\":false,\"errCode\":400,\"errMsg\":\"The group(")
                        .append(groupName).append(") not online! \"}");
            } else {
                Map<String, NodeRebInfo> nodeRebInfoMap = consumerBandInfo.getRebalanceMap();
                for (String consumerId : batchOpConsumerIds) {
                    if (nodeRebInfoMap.containsKey(consumerId)) {
                        return sBuilder
                                .append("{\"result\":false,\"errCode\":400,\"errMsg\":\"Duplicated set for consumerId(")
                                .append(consumerId).append(") in group(")
                                .append(groupName).append(")! \"}");
                    }
                }
                logger.info(sBuilder.append("[Re-balance] Add rebalance consumer: group=")
                        .append(groupName).append(", consumerIds=")
                        .append(batchOpConsumerIds.toString())
                        .append(", reJoinWait=").append(reJoinWait)
                        .append(", creator=").append(modifyUser).toString());
                sBuilder.delete(0, sBuilder.length());
                consumerInfoHolder.addRebConsumerInfo(groupName, batchOpConsumerIds, reJoinWait);
                sBuilder.append("{\"result\":true,\"errCode\":0,\"errMsg\":\"OK\"}");
            }
        } catch (Exception e) {
            sBuilder.delete(0, sBuilder.length());
            sBuilder.append("{\"result\":false,\"errCode\":400,\"errMsg\":\"")
                    .append(e.getMessage()).append("\"}");
        }
        return sBuilder;
    }



    private boolean getGroupConsumeJsonSetInfo(HttpServletRequest req, boolean isAddOp,
                                               BaseEntity defOpEntity,
                                               List<Map<String, String>> defValue,
                                               StringBuilder sBuilder,
                                               ProcessResult result) {
        if (!WebParameterUtils.getJsonArrayParamValue(req,
                WebFieldDef.GROUPCSMJSONSET, true, defValue, result)) {
            return result.success;
        }
        List<Map<String, String>> filterJsonArray =
                (List<Map<String, String>>) result.retData1;
        Set<String> configuredTopicSet =
                metaDataManager.getTotalConfiguredTopicNames();
        HashMap<String, GroupProcessResult> csmProcessMap =
                new HashMap<>();
        for (int j = 0; j < filterJsonArray.size(); j++) {
            Map<String, String> groupObject = filterJsonArray.get(j);
            if (!WebParameterUtils.getStringParamValue(groupObject,
                    WebFieldDef.GROUPNAME, true, "", result)) {
                return result.success;
            }
            String groupName = (String) result.retData1;
            if (!WebParameterUtils.getStringParamValue(groupObject,
                    WebFieldDef.TOPICNAME, true, "", result)) {
                return result.success;
            }
            String topicName = (String) result.retData1;
            if (!configuredTopicSet.contains(topicName)) {
                result.setFailResult(sBuilder
                        .append(WebFieldDef.TOPICNAME.name)
                        .append(" value ").append(topicName)
                        .append(" is not configure, please configure first!").toString());
                sBuilder.delete(0, sBuilder.length());
                return result.success;
            }
            // get consumeEnable info
            if (!WebParameterUtils.getBooleanParamValue(groupObject,
                    WebFieldDef.CONSUMEENABLE, false, (isAddOp ? true : null), result)) {
                return result.isSuccess();
            }
            Boolean consumeEnable = (Boolean) result.retData1;
            // get disableReason list
            if (!WebParameterUtils.getStringParamValue(groupObject,
                    WebFieldDef.REASON, false, "", result)) {
                return result.isSuccess();
            }
            String disableRsn = (String) result.retData1;
            // get filterEnable info
            if (!WebParameterUtils.getBooleanParamValue(groupObject,
                    WebFieldDef.FILTERENABLE, false, (isAddOp ? false : null), result)) {
                return result.isSuccess();
            }
            Boolean filterEnable = (Boolean) result.retData1;
            // get filterConds info
            if (!WebParameterUtils.getFilterCondString(groupObject,
                    false, isAddOp, result)) {
                return result.isSuccess();
            }
            String filterCondStr = (String) result.retData1;
            // record object
            if (isAddOp) {
                // add new record
                GroupConsumeCtrlEntity entity =
                        new GroupConsumeCtrlEntity(defOpEntity, groupName, topicName);
                entity.updModifyInfo(defOpEntity.getDataVerId(),
                        consumeEnable, disableRsn, filterEnable, filterCondStr);
                result.setSuccResult(entity);
                csmProcessMap.put(entity.getRecordKey(),
                        new GroupProcessResult(groupName, topicName, result));
            } else {
                // modify current record
                GroupConsumeCtrlEntity curEntity =
                        metaDataManager.getGroupConsumeCtrlConf(groupName, topicName);
                if (curEntity == null) {
                    result.setFailResult(DataOpErrCode.DERR_NOT_EXIST.getCode(),
                            DataOpErrCode.DERR_NOT_EXIST.getDescription());
                    csmProcessMap.put(KeyBuilderUtils.buildGroupTopicRecKey(groupName, topicName),
                            new GroupProcessResult(groupName, topicName, result));
                    continue;
                }
                GroupConsumeCtrlEntity newEntity = curEntity.clone();
                newEntity.updBaseModifyInfo(defOpEntity);
                if (newEntity.updModifyInfo(defOpEntity.getDataVerId(),
                        consumeEnable, disableRsn, filterEnable, filterCondStr)) {
                    result.setSuccResult(newEntity);
                } else {
                    result.setFailResult(DataOpErrCode.DERR_UNCHANGED.getCode(),
                            DataOpErrCode.DERR_UNCHANGED.getDescription());
                }
                csmProcessMap.put(newEntity.getRecordKey(),
                        new GroupProcessResult(groupName, topicName, result));
            }
        }
        // check result
        if (csmProcessMap.isEmpty()) {
            result.setFailResult(sBuilder
                    .append("Not found record in ")
                    .append(WebFieldDef.GROUPCSMJSONSET.name)
                    .append(" parameter!").toString());
            sBuilder.delete(0, sBuilder.length());
            return result.isSuccess();
        }
        result.setSuccResult(csmProcessMap);
        return result.isSuccess();
    }

    private boolean isFilterItemAllIncluded(Set<String> filterCondSet, String filterConsStr) {
        if (filterCondSet == null || filterCondSet.isEmpty()) {
            return true;
        }
        if (filterConsStr == null
                || (filterConsStr.length() == 2
                && filterConsStr.equals(TServerConstants.BLANK_FILTER_ITEM_STR))) {
            return false;
        }
        boolean allInc = true;
        for (String filterCond : filterCondSet) {
            if (!filterConsStr.contains(filterCond)) {
                allInc = false;
                break;
            }
        }
        return allInc;
    }
}
