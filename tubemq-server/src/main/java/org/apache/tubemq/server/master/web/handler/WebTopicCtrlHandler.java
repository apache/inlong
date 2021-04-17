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
import org.apache.tubemq.server.common.fielddef.WebFieldDef;
import org.apache.tubemq.server.common.utils.ProcessResult;
import org.apache.tubemq.server.common.utils.WebParameterUtils;
import org.apache.tubemq.server.master.TMaster;
import org.apache.tubemq.server.master.metamanage.metastore.dao.entity.BaseEntity;
import org.apache.tubemq.server.master.metamanage.metastore.dao.entity.ClusterSettingEntity;
import org.apache.tubemq.server.master.metamanage.metastore.dao.entity.TopicCtrlEntity;




public class WebTopicCtrlHandler extends AbstractWebHandler {

    /**
     * Constructor
     *
     * @param master tube master
     */
    public WebTopicCtrlHandler(TMaster master) {
        super(master);
    }



    @Override
    public void registerWebApiMethod() {
        // register query method
        registerQueryWebMethod("admin_query_topic_control_info",
                "adminQueryTopicCtrlInfo");

        // register modify method
        registerModifyWebMethod("admin_add_topic_control_info",
                "adminAddTopicCtrlInfo");
        registerModifyWebMethod("admin_batch_add_topic_control_info",
                "adminBatchAddTopicCtrlInfo");
        registerModifyWebMethod("admin_modify_topic_control_info",
                "adminModTopicCtrlInfo");
        registerModifyWebMethod("admin_batch_modify_topic_control_info",
                "adminBatchModTopicCtrlInfo");
        registerModifyWebMethod("admin_delete_topic_control_info",
                "adminDeleteTopicCtrlInfo");
    }

    /**
     * Query topic control info
     *
     * @param req
     * @return
     */
    public StringBuilder adminQueryTopicCtrlInfo(HttpServletRequest req) {
        ProcessResult result = new ProcessResult();
        StringBuilder sBuffer = new StringBuilder(512);
        TopicCtrlEntity qryEntity = new TopicCtrlEntity();
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
        // query matched records
        Map<String, TopicCtrlEntity> topicCtrlMap =
                metaDataManager.getTopicCtrlConf(topicNameSet, qryEntity);
        // build query result
        int totalCnt = 0;
        WebParameterUtils.buildSuccessWithDataRetBegin(sBuffer);
        for (TopicCtrlEntity entity : topicCtrlMap.values()) {
            if (entity == null) {
                continue;
            }
            if (totalCnt++ > 0) {
                sBuffer.append(",");
            }
            entity.toWebJsonStr(sBuffer, true, true);
        }
        WebParameterUtils.buildSuccessWithDataRetEnd(sBuffer, totalCnt);
        return sBuffer;
    }

    /**
     * Add new topic control record
     *
     * @param req
     * @return
     */
    public StringBuilder adminAddTopicCtrlInfo(HttpServletRequest req) {
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
        // check and get topicName field
        if (!WebParameterUtils.getStringParamValue(req,
                WebFieldDef.COMPSTOPICNAME, true, null, result)) {
            WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
            return sBuilder;
        }
        Set<String> topicNameSet = (Set<String>) result.retData1;
        // get topicNameId info
        int topicNameId = TBaseConstants.META_VALUE_UNDEFINED;
        if (topicNameSet.size() == 1) {
            if (!WebParameterUtils.getIntParamValue(req, WebFieldDef.TOPICNAMEID,
                    false, TBaseConstants.META_VALUE_UNDEFINED, 0, result)) {
                WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
                return sBuilder;
            }
            topicNameId = (int) result.getRetData();
        }
        // get authCtrlStatus info
        if (!WebParameterUtils.getBooleanParamValue(req,
                WebFieldDef.AUTHCTRLENABLE, false, false, result)) {
            WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
            return sBuilder;
        }
        Boolean enableTopicAuth = (Boolean) result.retData1;
        // check and get max message size
        ClusterSettingEntity defClusterSetting =
                metaDataManager.getClusterDefSetting(false);
        int maxMsgSizeMB = defClusterSetting.getMaxMsgSizeInMB();
        // check max message size
        if (!WebParameterUtils.getIntParamValue(req,
                WebFieldDef.MAXMSGSIZEINMB, false, maxMsgSizeMB,
                TBaseConstants.META_MIN_ALLOWED_MESSAGE_SIZE_MB,
                TBaseConstants.META_MAX_ALLOWED_MESSAGE_SIZE_MB, result)) {
            WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
            return sBuilder;
        }
        maxMsgSizeMB = (int) result.getRetData();
        // add records
        List<TopicProcessResult> retInfo =
                metaDataManager.addOrUpdTopicCtrlConf(opEntity, topicNameSet,
                        topicNameId, enableTopicAuth, maxMsgSizeMB, sBuilder, result);
        return buildRetInfo(retInfo, sBuilder);
    }

    /**
     * Add new topic control record in batch
     *
     * @param req
     * @return
     */
    public StringBuilder adminBatchAddTopicCtrlInfo(HttpServletRequest req) {
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
        if (!getTopicCtrlJsonSetInfo(req, true,
                defOpEntity, null, sBuilder, result)) {
            WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
            return sBuilder;
        }
        Map<String, TopicCtrlEntity> addRecordMap =
                (Map<String, TopicCtrlEntity>) result.getRetData();
        List<TopicProcessResult> retInfo = new ArrayList<>();
        for (TopicCtrlEntity topicCtrlInfo : addRecordMap.values()) {
            retInfo.add(metaDataManager.addOrUpdTopicCtrlConf(topicCtrlInfo, sBuilder, result));
        }
        return buildRetInfo(retInfo, sBuilder);
    }

    /**
     * Modify topic control info
     *
     * @param req
     * @return
     */
    // #lizard forgives
    public StringBuilder adminModTopicCtrlInfo(HttpServletRequest req) {
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
        BaseEntity opInfoEntity = (BaseEntity) result.getRetData();
        // check and get topicName info
        if (!WebParameterUtils.getStringParamValue(req,
                WebFieldDef.COMPSTOPICNAME, true, null, result)) {
            WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
            return sBuilder;
        }
        Set<String> topicNameSet = (Set<String>) result.retData1;
        // get topicNameId info
        int topicNameId = TBaseConstants.META_VALUE_UNDEFINED;
        if (topicNameSet.size() == 1) {
            if (!WebParameterUtils.getIntParamValue(req, WebFieldDef.TOPICNAMEID,
                    false, TBaseConstants.META_VALUE_UNDEFINED, 0, result)) {
                WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
                return sBuilder;
            }
            topicNameId = (int) result.getRetData();
        }
        // get authCtrlStatus info
        if (!WebParameterUtils.getBooleanParamValue(req,
                WebFieldDef.AUTHCTRLENABLE, false, false, result)) {
            WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
            return sBuilder;
        }
        Boolean enableTopicAuth = (Boolean) result.retData1;
        // check and get max message size
        ClusterSettingEntity defClusterSetting =
                metaDataManager.getClusterDefSetting(false);
        int maxMsgSizeMB = defClusterSetting.getMaxMsgSizeInMB();
        // check max message size
        if (!WebParameterUtils.getIntParamValue(req,
                WebFieldDef.MAXMSGSIZEINMB, false, maxMsgSizeMB,
                TBaseConstants.META_MIN_ALLOWED_MESSAGE_SIZE_MB,
                TBaseConstants.META_MAX_ALLOWED_MESSAGE_SIZE_MB, result)) {
            WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
            return sBuilder;
        }
        maxMsgSizeMB = (int) result.getRetData();
        // modify records
        List<TopicProcessResult> retInfo =
                metaDataManager.addOrUpdTopicCtrlConf(opInfoEntity, topicNameSet,
                        topicNameId, enableTopicAuth, maxMsgSizeMB, sBuilder, result);
        return buildRetInfo(retInfo, sBuilder);
    }

    /**
     * Modify new topic control record in batch
     *
     * @param req
     * @return
     */
    public StringBuilder adminBatchModTopicCtrlInfo(HttpServletRequest req) {
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
        BaseEntity defOpEntity = (BaseEntity) result.getRetData();
        // check and get modify record map
        if (!getTopicCtrlJsonSetInfo(req, false,
                defOpEntity, null, sBuilder, result)) {
            WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
            return sBuilder;
        }
        Map<String, TopicCtrlEntity> modRecordMap =
                (Map<String, TopicCtrlEntity>) result.getRetData();
        List<TopicProcessResult> retInfo = new ArrayList<>();
        for (TopicCtrlEntity topicCtrlInfo : modRecordMap.values()) {
            retInfo.add(metaDataManager.addOrUpdTopicCtrlConf(topicCtrlInfo, sBuilder, result));
        }
        return buildRetInfo(retInfo, sBuilder);
    }

    /**
     * Delete topic control info
     *
     * @param req
     * @return
     */
    public StringBuilder adminDeleteTopicCtrlInfo(HttpServletRequest req) {
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
        // delete records
        List<TopicProcessResult> retInfo = new ArrayList<>();
        for (String topicName : topicNameSet) {
            metaDataManager.delTopicCtrlConf(opEntity.getModifyUser(), topicName, sBuffer, result);
            retInfo.add(new TopicProcessResult(
                    TBaseConstants.META_VALUE_UNDEFINED, topicName, result));
        }
        return buildRetInfo(retInfo, sBuffer);
    }

    private boolean getTopicCtrlJsonSetInfo(HttpServletRequest req, boolean isAddOp,
                                            BaseEntity defOpEntity,
                                            List<Map<String, String>> defValue,
                                            StringBuilder sBuilder,
                                            ProcessResult result) {
        if (!WebParameterUtils.getJsonArrayParamValue(req,
                WebFieldDef.TOPICCTRLSET, true, defValue, result)) {
            return result.success;
        }
        List<Map<String, String>> ctrlJsonArray =
                (List<Map<String, String>>) result.retData1;
        // get default max message size
        ClusterSettingEntity defClusterSetting =
                metaDataManager.getClusterDefSetting(false);
        int defMaxMsgSizeMB = defClusterSetting.getMaxMsgSizeInMB();
        // check and get topic control configure
        TopicCtrlEntity itemConf;
        HashMap<String, TopicCtrlEntity> addRecordMap = new HashMap<>();
        for (int j = 0; j < ctrlJsonArray.size(); j++) {
            Map<String, String> confMap = ctrlJsonArray.get(j);
            // check and get operation info
            if (!WebParameterUtils.getAUDBaseInfo(confMap, isAddOp, defOpEntity, result)) {
                return result.isSuccess();
            }
            BaseEntity itemOpEntity = (BaseEntity) result.getRetData();
            // get topicName configure info
            if (!WebParameterUtils.getStringParamValue(confMap,
                    WebFieldDef.TOPICNAME, true, "", result)) {
                return result.success;
            }
            String topicName = (String) result.retData1;
            // check max message size
            if (!WebParameterUtils.getIntParamValue(confMap,
                    WebFieldDef.MAXMSGSIZEINMB, false, defMaxMsgSizeMB,
                    TBaseConstants.META_MIN_ALLOWED_MESSAGE_SIZE_MB,
                    TBaseConstants.META_MAX_ALLOWED_MESSAGE_SIZE_MB, result)) {
                return result.isSuccess();
            }
            int itemMaxMsgSizeMB = (int) result.getRetData();
            // get topicNameId field
            if (!WebParameterUtils.getIntParamValue(req, WebFieldDef.TOPICNAMEID,
                    false, TBaseConstants.META_VALUE_UNDEFINED, 0, result)) {
                return result.isSuccess();
            }
            int itemTopicNameId = (int) result.getRetData();
            // get authCtrlStatus info
            if (!WebParameterUtils.getBooleanParamValue(req,
                    WebFieldDef.AUTHCTRLENABLE, false, false, result)) {
                return result.isSuccess();
            }
            Boolean enableTopicAuth = (Boolean) result.retData1;
            itemConf = new TopicCtrlEntity(itemOpEntity, topicName);
            itemConf.updModifyInfo(itemOpEntity.getDataVerId(),
                    itemTopicNameId, itemMaxMsgSizeMB, enableTopicAuth);
            addRecordMap.put(itemConf.getTopicName(), itemConf);
        }
        // check result
        if (addRecordMap.isEmpty()) {
            result.setFailResult(sBuilder
                    .append("Not found record info in ")
                    .append(WebFieldDef.TOPICCTRLSET.name)
                    .append(" parameter!").toString());
            sBuilder.delete(0, sBuilder.length());
            return result.isSuccess();
        }
        result.setSuccResult(addRecordMap);
        return result.isSuccess();
    }

    private StringBuilder buildRetInfo(List<TopicProcessResult> retInfo,
                                       StringBuilder sBuilder) {
        int totalCnt = 0;
        WebParameterUtils.buildSuccessWithDataRetBegin(sBuilder);
        for (TopicProcessResult entry : retInfo) {
            if (entry == null) {
                continue;
            }
            if (totalCnt++ > 0) {
                sBuilder.append(",");
            }
            sBuilder.append("{\"topicName\":\"").append(entry.getTopicName()).append("\"")
                    .append(",\"success\":").append(entry.isSuccess())
                    .append(",\"errCode\":").append(entry.getErrCode())
                    .append(",\"errInfo\":\"").append(entry.getErrInfo()).append("\"}");
        }
        WebParameterUtils.buildSuccessWithDataRetEnd(sBuilder, totalCnt);
        return sBuilder;
    }

}
