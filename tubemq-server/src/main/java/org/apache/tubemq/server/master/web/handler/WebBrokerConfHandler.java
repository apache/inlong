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

import static java.lang.Math.abs;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.servlet.http.HttpServletRequest;
import org.apache.tubemq.corebase.TBaseConstants;
import org.apache.tubemq.corebase.utils.AddressUtils;
import org.apache.tubemq.corebase.utils.Tuple2;
import org.apache.tubemq.server.common.TServerConstants;
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
import org.apache.tubemq.server.master.metamanage.metastore.dao.entity.TopicDeployEntity;
import org.apache.tubemq.server.master.metamanage.metastore.dao.entity.TopicPropGroup;



/**
 * <p>
 * The class to handle the default config of broker, including:
 * - Add config
 * - Update config
 * - Delete config
 * And manage the broker status.
 * <p>
 * Please note that one IP could only host one broker, and brokerId must be unique
 */
public class WebBrokerConfHandler extends AbstractWebHandler {

    /**
     * Constructor
     *
     * @param master tube master
     */
    public WebBrokerConfHandler(TMaster master) {
        super(master);
    }

    @Override
    public void registerWebApiMethod() {
        // register query method
        registerQueryWebMethod("admin_query_broker_run_status",
                "adminQueryBrokerRunStatusInfo");
        registerQueryWebMethod("admin_query_broker_configure",
                "adminQueryBrokerDefConfEntityInfo");
        // register modify method
        registerModifyWebMethod("admin_add_broker_configure",
                "adminAddBrokerDefConfEntityInfo");
        registerModifyWebMethod("admin_bath_add_broker_configure",
                "adminBatchAddBrokerDefConfEntityInfo");
        registerModifyWebMethod("admin_online_broker_configure",
                "adminOnlineBrokerConf");
        registerModifyWebMethod("admin_update_broker_configure",
                "adminUpdateBrokerConf");
        registerModifyWebMethod("admin_reload_broker_configure",
                "adminReloadBrokerConf");
        registerModifyWebMethod("admin_set_broker_read_or_write",
                "adminSetReadOrWriteBrokerConf");
        registerModifyWebMethod("admin_release_broker_autoforbidden_status",
                "adminRelBrokerAutoForbiddenStatus");
        registerModifyWebMethod("admin_offline_broker_configure",
                "adminOfflineBrokerConf");
        registerModifyWebMethod("admin_delete_broker_configure",
                "adminDeleteBrokerConfEntityInfo");
    }

    /**
     * Query broker config
     *
     * @param req
     * @return
     */
    public StringBuilder adminQueryBrokerDefConfEntityInfo(HttpServletRequest req) {
        ProcessResult result = new ProcessResult();
        StringBuilder sBuffer = new StringBuilder(512);
        BrokerConfEntity qryEntity = new BrokerConfEntity();
        // get queried operation info, for createUser, modifyUser, dataVersionId
        if (!WebParameterUtils.getQueriedOperateInfo(req, qryEntity, result)) {
            WebParameterUtils.buildFailResult(sBuffer, result.errInfo);
            return sBuffer;
        }
        // check and get brokerId field
        if (!WebParameterUtils.getIntParamValue(req,
                WebFieldDef.COMPSBROKERID, false, result)) {
            WebParameterUtils.buildFailResult(sBuffer, result.errInfo);
            return sBuffer;
        }
        Set<Integer> brokerIds = (Set<Integer>) result.retData1;
        // get brokerIp info
        if (!WebParameterUtils.getStringParamValue(req,
                WebFieldDef.COMPBROKERIP, false, null, result)) {
            WebParameterUtils.buildFailResult(sBuffer, result.errInfo);
            return sBuffer;
        }
        Set<String> brokerIpSet = (Set<String>) result.retData1;
        // get brokerPort field
        if (!WebParameterUtils.getIntParamValue(req, WebFieldDef.BROKERPORT,
                false, TBaseConstants.META_VALUE_UNDEFINED, 1, result)) {
            WebParameterUtils.buildFailResult(sBuffer, result.errInfo);
            return sBuffer;
        }
        int brokerPort = (int) result.getRetData();
        // get brokerTlsPort field
        if (!WebParameterUtils.getIntParamValue(req, WebFieldDef.BROKERTLSPORT,
                false, TBaseConstants.META_VALUE_UNDEFINED, 1, result)) {
            WebParameterUtils.buildFailResult(sBuffer, result.errInfo);
            return sBuffer;
        }
        int brokerTlsPort = (int) result.getRetData();
        // get brokerWebPort field
        if (!WebParameterUtils.getIntParamValue(req, WebFieldDef.BROKERWEBPORT,
                false, TBaseConstants.META_VALUE_UNDEFINED, 1, result)) {
            WebParameterUtils.buildFailResult(sBuffer, result.errInfo);
            return sBuffer;
        }
        int brokerWebPort = (int) result.getRetData();
        // get regionId field
        if (!WebParameterUtils.getIntParamValue(req, WebFieldDef.REGIONID,
                false, TBaseConstants.META_VALUE_UNDEFINED,
                TServerConstants.BROKER_REGION_ID_MIN, result)) {
            WebParameterUtils.buildFailResult(sBuffer, result.errInfo);
            return sBuffer;
        }
        int regionId = (int) result.getRetData();
        // get groupId field
        if (!WebParameterUtils.getIntParamValue(req, WebFieldDef.GROUPID,
                false, TBaseConstants.META_VALUE_UNDEFINED,
                TServerConstants.BROKER_GROUP_ID_MIN, result)) {
            WebParameterUtils.buildFailResult(sBuffer, result.errInfo);
            return sBuffer;
        }
        int groupId = (int) result.getRetData();
        // get and valid TopicPropGroup info
        if (!WebParameterUtils.getTopicPropInfo(req,
                null, result)) {
            WebParameterUtils.buildFailResult(sBuffer, result.errInfo);
            return sBuffer;
        }
        TopicPropGroup brokerProps = (TopicPropGroup) result.getRetData();
        // get and valid TopicStatusId info
        if (!WebParameterUtils.getTopicStatusParamValue(req,
                false, TopicStatus.STATUS_TOPIC_UNDEFINED, result)) {
            WebParameterUtils.buildFailResult(sBuffer, result.errInfo);
            return sBuffer;
        }
        TopicStatus topicStatus = (TopicStatus) result.getRetData();
        // get topic info
        if (!WebParameterUtils.getStringParamValue(req,
                WebFieldDef.COMPSTOPICNAME, false, null, result)) {
            WebParameterUtils.buildFailResult(sBuffer, result.errInfo);
            return sBuffer;
        }
        Set<String> topicNameSet = (Set<String>) result.retData1;
        // get isInclude info
        if (!WebParameterUtils.getBooleanParamValue(req,
                WebFieldDef.ISINCLUDE, false, true, result)) {
            WebParameterUtils.buildFailResult(sBuffer, result.errInfo);
            return sBuffer;
        }
        Boolean isInclude = (Boolean) result.retData1;
        // get withTopic info
        if (!WebParameterUtils.getBooleanParamValue(req,
                WebFieldDef.WITHTOPIC, false, false, result)) {
            WebParameterUtils.buildFailResult(sBuffer, result.errInfo);
            return sBuffer;
        }
        Boolean withTopic = (Boolean) result.retData1;
        // fill query entity fields
        qryEntity.updModifyInfo(qryEntity.getDataVerId(), brokerPort, brokerTlsPort,
                brokerWebPort, regionId, groupId, null, brokerProps);
        Map<Integer, BrokerConfEntity> qryResult =
                metaDataManager.getBrokerConfInfo(brokerIds, brokerIpSet, qryEntity);
        // build query result
        int totalCnt = 0;
        WebParameterUtils.buildSuccessWithDataRetBegin(sBuffer);
        for (BrokerConfEntity entity : qryResult.values()) {
            Map<String, TopicDeployEntity> topicConfEntityMap =
                    metaDataManager.getBrokerTopicConfEntitySet(entity.getBrokerId());
            if (!isValidRecord(topicNameSet, isInclude, topicStatus, topicConfEntityMap)) {
                continue;
            }
            if (totalCnt++ > 0) {
                sBuffer.append(",");
            }
            entity.toWebJsonStr(sBuffer, true, false);
            sBuffer = addTopicInfo(withTopic, sBuffer, topicConfEntityMap);
            sBuffer.append("}");
        }
        WebParameterUtils.buildSuccessWithDataRetEnd(sBuffer, totalCnt);
        return sBuffer;
    }

    /**
     * Add broker configure
     *
     * @param req
     * @return
     */
    public StringBuilder adminAddBrokerConfInfo(HttpServletRequest req) {
        return innAddOrUpdBrokerConfInfo(req, true);
    }

    /**
     * update broker configure
     *
     * @param req
     * @return
     */
    public StringBuilder adminUpdateBrokerConfInfo(HttpServletRequest req) {
        return innAddOrUpdBrokerConfInfo(req, false);
    }

    /**
     * Add default config to brokers in batch
     *
     * @param req
     * @return
     */
    public StringBuilder adminBatchAddBrokerConfInfo(HttpServletRequest req) {
        return innBatchAddOrUpdBrokerConfInfo(req, true);
    }


    private StringBuilder innAddOrUpdBrokerConfInfo(HttpServletRequest req,
                                                    boolean isAddOp) {
        ProcessResult result = new ProcessResult();
        StringBuilder sBuffer = new StringBuilder(512);
        // valid operation authorize info
        if (!WebParameterUtils.validReqAuthorizeInfo(req,
                WebFieldDef.ADMINAUTHTOKEN, true, master, result)) {
            WebParameterUtils.buildFailResult(sBuffer, result.errInfo);
            return sBuffer;
        }
        // check and get operation info
        if (!WebParameterUtils.getAUDBaseInfo(req, isAddOp, result)) {
            WebParameterUtils.buildFailResult(sBuffer, result.errInfo);
            return sBuffer;
        }
        BaseEntity opEntity = (BaseEntity) result.getRetData();
        // check and get cluster default setting info
        ClusterSettingEntity defClusterSetting =
                metaDataManager.getClusterDefSetting(false);
        // get brokerPort field
        if (!WebParameterUtils.getIntParamValue(req, WebFieldDef.BROKERPORT,
                false, (isAddOp ? defClusterSetting.getBrokerPort()
                        : TBaseConstants.META_VALUE_UNDEFINED), 1, result)) {
            WebParameterUtils.buildFailResult(sBuffer, result.errInfo);
            return sBuffer;
        }
        int brokerPort = (int) result.getRetData();
        // get brokerTlsPort field
        if (!WebParameterUtils.getIntParamValue(req, WebFieldDef.BROKERTLSPORT,
                false, (isAddOp ? defClusterSetting.getBrokerTLSPort()
                        : TBaseConstants.META_VALUE_UNDEFINED), 1, result)) {
            WebParameterUtils.buildFailResult(sBuffer, result.errInfo);
            return sBuffer;
        }
        int brokerTlsPort = (int) result.getRetData();
        // get brokerWebPort field
        if (!WebParameterUtils.getIntParamValue(req, WebFieldDef.BROKERWEBPORT,
                false, (isAddOp ? defClusterSetting.getBrokerWebPort()
                        : TBaseConstants.META_VALUE_UNDEFINED), 1, result)) {
            WebParameterUtils.buildFailResult(sBuffer, result.errInfo);
            return sBuffer;
        }
        int brokerWebPort = (int) result.getRetData();
        // get regionId field
        if (!WebParameterUtils.getIntParamValue(req, WebFieldDef.REGIONID,
                false, (isAddOp ? TServerConstants.BROKER_REGION_ID_DEF
                        : TBaseConstants.META_VALUE_UNDEFINED),
                TServerConstants.BROKER_REGION_ID_MIN, result)) {
            WebParameterUtils.buildFailResult(sBuffer, result.errInfo);
            return sBuffer;
        }
        int regionId = (int) result.getRetData();
        // get groupId field
        if (!WebParameterUtils.getIntParamValue(req, WebFieldDef.GROUPID,
                false, (isAddOp ? TServerConstants.BROKER_GROUP_ID_DEF
                        : TBaseConstants.META_VALUE_UNDEFINED),
                TServerConstants.BROKER_GROUP_ID_MIN, result)) {
            WebParameterUtils.buildFailResult(sBuffer, result.errInfo);
            return sBuffer;
        }
        int groupId = (int) result.getRetData();
        // get and valid TopicPropGroup info
        if (!WebParameterUtils.getTopicPropInfo(req,
                (isAddOp ? defClusterSetting.getClsDefTopicProps() : null), result)) {
            WebParameterUtils.buildFailResult(sBuffer, result.errInfo);
            return sBuffer;
        }
        TopicPropGroup brokerProps = (TopicPropGroup) result.getRetData();
        // add record and process result
        List<BrokerProcessResult> retInfo = new ArrayList<>();
        if (isAddOp) {
            // get brokerIp and brokerId field
            if (!getBrokerIpAndIdParamValue(req, sBuffer, result)) {
                WebParameterUtils.buildFailResult(sBuffer, result.errInfo);
                return sBuffer;
            }
            Tuple2<Integer, String> brokerIdAndIpTuple =
                    (Tuple2<Integer, String>) result.getRetData();
            retInfo.add(metaDataManager.addOrUpdBrokerConfig(isAddOp, opEntity,
                    brokerIdAndIpTuple.getF0(), brokerIdAndIpTuple.getF1(), brokerPort,
                    brokerTlsPort, brokerWebPort, regionId, groupId,
                    ManageStatus.STATUS_MANAGE_APPLY, brokerProps, sBuffer, result));
        } else {
            // check and get brokerId field
            if (!WebParameterUtils.getIntParamValue(req,
                    WebFieldDef.COMPSBROKERID, true, result)) {
                WebParameterUtils.buildFailResult(sBuffer, result.errInfo);
                return sBuffer;
            }
            Set<Integer> brokerIdSet = (Set<Integer>) result.retData1;
            for (Integer brokerId : brokerIdSet) {
                retInfo.add(metaDataManager.addOrUpdBrokerConfig(isAddOp, opEntity,
                        brokerId, "", brokerPort, brokerTlsPort, brokerWebPort,
                        regionId, groupId, null, brokerProps, sBuffer, result));
            }
        }
        return buildRetInfo(retInfo, sBuffer);
    }

    /**
     * Add default config to brokers in batch
     *
     * @param req
     * @return
     */
    private StringBuilder innBatchAddOrUpdBrokerConfInfo(HttpServletRequest req,
                                                         boolean isAddOp) {
        ProcessResult result = new ProcessResult();
        StringBuilder sBuffer = new StringBuilder(512);
        // valid operation authorize info
        if (!WebParameterUtils.validReqAuthorizeInfo(req,
                WebFieldDef.ADMINAUTHTOKEN, true, master, result)) {
            WebParameterUtils.buildFailResult(sBuffer, result.errInfo);
            return sBuffer;
        }
        // check and get operation info
        if (!WebParameterUtils.getAUDBaseInfo(req, isAddOp, result)) {
            WebParameterUtils.buildFailResult(sBuffer, result.errInfo);
            return sBuffer;
        }
        BaseEntity defOpEntity = (BaseEntity) result.getRetData();
        // check and get brokerJsonSet info
        if (!getBrokerJsonSetInfo(req, isAddOp, defOpEntity, sBuffer, result)) {
            WebParameterUtils.buildFailResult(sBuffer, result.errInfo);
            return sBuffer;
        }
        Map<Integer, BrokerConfEntity> addedRecordMap =
                (HashMap<Integer, BrokerConfEntity>) result.getRetData();
        // add record and process result
        List<BrokerProcessResult> retInfo = new ArrayList<>();
        for (BrokerConfEntity brokerEntity : addedRecordMap.values()) {
            retInfo.add(metaDataManager.addOrUpdBrokerConfig(
                    isAddOp, brokerEntity, sBuffer, result));
        }
        return buildRetInfo(retInfo, sBuffer);
    }


    /**
     * Delete broker config
     *
     * @param req
     * @return
     */
    public StringBuilder adminDeleteBrokerConfEntityInfo(HttpServletRequest req) {
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
        BaseEntity opInfoEntity = (BaseEntity) result.getRetData();
        // get isReservedData info
        if (!WebParameterUtils.getBooleanParamValue(req,
                WebFieldDef.ISRESERVEDDATA, false, false, result)) {
            WebParameterUtils.buildFailResult(sBuffer, result.errInfo);
            return sBuffer;
        }
        Boolean isReservedData = (Boolean) result.retData1;
        // check and get brokerId field
        if (!WebParameterUtils.getIntParamValue(req,
                WebFieldDef.COMPSBROKERID, true, result)) {
            WebParameterUtils.buildFailResult(sBuffer, result.errInfo);
            return sBuffer;
        }
        Set<Integer> brokerIds = (Set<Integer>) result.retData1;
        if (brokerIds.isEmpty()) {
            WebParameterUtils.buildFailResult(sBuffer,
                    "Illegal value: Null value of brokerId parameter");
            return sBuffer;
        }
        Map<Integer, BrokerConfEntity> qryResult =
                metaDataManager.getBrokerConfInfo(brokerIds, null, null);
        if (qryResult.isEmpty()) {
            WebParameterUtils.buildFailResult(sBuffer,
                    "Illegal value: not found broker configure by brokerId value");
            return sBuffer;
        }
        // check broker configure status
        List<BrokerProcessResult> retInfo = new ArrayList<>();
        Map<Integer, BrokerConfEntity> needDelMap = new HashMap<>();
        Map<String, TopicDeployEntity> topicConfigMap;
        for (BrokerConfEntity entity : qryResult.values()) {
            if (entity == null) {
                continue;
            }
            topicConfigMap = metaDataManager.getBrokerTopicConfEntitySet(entity.getBrokerId());
            if (topicConfigMap == null || topicConfigMap.isEmpty()) {
                needDelMap.put(entity.getBrokerId(), entity);
                continue;
            }
            if (WebParameterUtils.checkBrokerInOfflining(entity.getBrokerId(),
                    entity.getManageStatus().getCode(), metaDataManager)) {
                result.setFailResult(DataOpErrCode.DERR_ILLEGAL_STATUS.getCode(),
                        sBuffer.append("Illegal value: the broker is processing offline event by brokerId=")
                                .append(entity.getBrokerId()).append(", please wait and try later!").toString());
                retInfo.add(new BrokerProcessResult(
                        entity.getBrokerId(), entity.getBrokerIp(), result));
                continue;
            }
            boolean isMatched = true;
            if (isReservedData) {
                for (Map.Entry<String, TopicDeployEntity> entry : topicConfigMap.entrySet()) {
                    if (entry.getValue() == null) {
                        continue;
                    }
                    if (entry.getValue().getTopicProps().isAcceptPublish()
                            || entry.getValue().getTopicProps().isAcceptSubscribe()) {
                        result.setFailResult(DataOpErrCode.DERR_ILLEGAL_STATUS.getCode(),
                                sBuffer.append("The topic ").append(entry.getKey())
                                        .append("'s acceptPublish and acceptSubscribe parameters")
                                        .append(" must be false in broker=")
                                        .append(entity.getBrokerId())
                                        .append(" before broker delete by reserve data method!").toString());
                        retInfo.add(new BrokerProcessResult(
                                entity.getBrokerId(), entity.getBrokerIp(), result));
                        isMatched = false;
                        break;
                    }
                }
            } else {
                result.setFailResult(DataOpErrCode.DERR_ILLEGAL_STATUS.getCode(),
                        sBuffer.append("Topic configure of broker by brokerId=")
                                .append(entity.getBrokerId())
                                .append(" not deleted, please delete broker's topic configure first!").toString());
                retInfo.add(new BrokerProcessResult(
                        entity.getBrokerId(), entity.getBrokerIp(), result));
                isMatched = false;
            }
            if (isMatched) {
                needDelMap.put(entity.getBrokerId(), entity);
            }
        }
        if (needDelMap.isEmpty()) {
            return buildRetInfo(retInfo, sBuffer);
        }
        // do delete operation
        for (BrokerConfEntity entry : needDelMap.values()) {
            if (entry == null) {
                continue;
            }
            if (isReservedData) {
                Map<String, TopicDeployEntity> brokerTopicConfMap =
                        metaDataManager.getBrokerTopicConfEntitySet(entry.getBrokerId());
                if (brokerTopicConfMap != null) {
                    metaDataManager.delBrokerTopicConfig(opInfoEntity.getModifyUser(),
                            entry.getBrokerId(), sBuffer, result);
                }
            }
            metaDataManager.confDelBrokerConfig(opInfoEntity.getModifyUser(),
                    entry.getBrokerId(), sBuffer, result);
            retInfo.add(new BrokerProcessResult(
                    entry.getBrokerId(), entry.getBrokerIp(), result));
        }
        return buildRetInfo(retInfo, sBuffer);

    }

    /**
     * Check if the record is valid
     *
     * @param qryTopicSet
     * @param topicStatus
     * @param isInclude
     * @param topicConfEntityMap
     * @return
     */
    private boolean isValidRecord(Set<String> qryTopicSet, Boolean isInclude,
                                  TopicStatus topicStatus,
                                  Map<String, TopicDeployEntity> topicConfEntityMap) {
        if ((topicConfEntityMap == null) || (topicConfEntityMap.isEmpty())) {
            if ((qryTopicSet.isEmpty() || !isInclude)
                    && topicStatus == TopicStatus.STATUS_TOPIC_UNDEFINED) {
                return true;
            }
            return false;
        }
        // first search topic if match require
        if (!qryTopicSet.isEmpty()) {
            boolean matched = false;
            Set<String> curTopics = topicConfEntityMap.keySet();
            if (isInclude) {
                for (String topic : qryTopicSet) {
                    if (curTopics.contains(topic)) {
                        matched = true;
                        break;
                    }
                }
            } else {
                matched = true;
                for (String topic : qryTopicSet) {
                    if (curTopics.contains(topic)) {
                        matched = false;
                        break;
                    }
                }
            }
            if (!matched) {
                return false;
            }
        }
        // second check topic status if match
        for (TopicDeployEntity topicConfEntity : topicConfEntityMap.values()) {
            if (topicConfEntity.getDeployStatus() == topicStatus) {
                return true;
            }
        }
        return false;
    }

    /**
     * Private method to add topic info
     *
     * @param withTopic
     * @param sBuilder
     * @param topicConfEntityMap
     * @return
     */
    private StringBuilder addTopicInfo(Boolean withTopic, StringBuilder sBuilder,
                                       Map<String, TopicDeployEntity> topicConfEntityMap) {
        if (withTopic) {
            sBuilder.append(",\"topicSet\":[");
            int topicCount = 0;
            if (topicConfEntityMap != null) {
                for (TopicDeployEntity topicEntity : topicConfEntityMap.values()) {
                    if (topicCount++ > 0) {
                        sBuilder.append(",");
                    }
                    topicEntity.toWebJsonStr(sBuilder, true, true);
                }
            }
            sBuilder.append("]");
        }
        return sBuilder;
    }

    private boolean getBrokerJsonSetInfo(HttpServletRequest req, boolean isAddOp,
                                         BaseEntity defOpEntity, StringBuilder sBuffer,
                                         ProcessResult result) {
        if (!WebParameterUtils.getJsonArrayParamValue(req,
                WebFieldDef.BROKERJSONSET, true, null, result)) {
            return result.success;
        }
        List<Map<String, String>> brokerJsonArray =
                (List<Map<String, String>>) result.retData1;
        // check and get cluster default setting info
        ClusterSettingEntity defClusterSetting =
                metaDataManager.getClusterDefSetting(false);
        // check and get broker configure
        HashMap<Integer, BrokerConfEntity> addedRecordMap = new HashMap<>();
        for (int j = 0; j < brokerJsonArray.size(); j++) {
            Map<String, String> brokerObject = brokerJsonArray.get(j);
            // check and get operation info
            if (!WebParameterUtils.getAUDBaseInfo(brokerObject,
                    isAddOp, defOpEntity, result)) {
                return result.isSuccess();
            }
            BaseEntity itemOpEntity = (BaseEntity) result.getRetData();
            // get brokerIp and brokerId field
            if (!getBrokerIpAndIdParamValue(brokerObject, sBuffer, result)) {
                return result.isSuccess();
            }
            Tuple2<Integer, String> brokerIdAndIpTuple =
                    (Tuple2<Integer, String>) result.getRetData();
            // get brokerPort field
            if (!WebParameterUtils.getIntParamValue(brokerObject, WebFieldDef.BROKERPORT,
                    false, (isAddOp ? defClusterSetting.getBrokerPort()
                            : TBaseConstants.META_VALUE_UNDEFINED), 1, result)) {
                return result.isSuccess();
            }
            int brokerPort = (int) result.getRetData();
            // get brokerTlsPort field
            if (!WebParameterUtils.getIntParamValue(brokerObject, WebFieldDef.BROKERTLSPORT,
                    false, (isAddOp ? defClusterSetting.getBrokerTLSPort()
                            : TBaseConstants.META_VALUE_UNDEFINED), 1, result)) {
                return result.isSuccess();
            }
            int brokerTlsPort = (int) result.getRetData();
            // get brokerWebPort field
            if (!WebParameterUtils.getIntParamValue(brokerObject, WebFieldDef.BROKERWEBPORT,
                    false, (isAddOp ? defClusterSetting.getBrokerWebPort()
                            : TBaseConstants.META_VALUE_UNDEFINED), 1, result)) {
                return result.isSuccess();
            }
            int brokerWebPort = (int) result.getRetData();
            // get regionId field
            if (!WebParameterUtils.getIntParamValue(brokerObject, WebFieldDef.REGIONID,
                    false, (isAddOp ? TServerConstants.BROKER_REGION_ID_DEF
                            : TBaseConstants.META_VALUE_UNDEFINED),
                    TServerConstants.BROKER_REGION_ID_MIN, result)) {
                return result.isSuccess();
            }
            int regionId = (int) result.getRetData();
            // get groupId field
            if (!WebParameterUtils.getIntParamValue(brokerObject, WebFieldDef.GROUPID,
                    false, (isAddOp ? TServerConstants.BROKER_GROUP_ID_DEF
                            : TBaseConstants.META_VALUE_UNDEFINED),
                    TServerConstants.BROKER_GROUP_ID_MIN, result)) {
                return result.isSuccess();
            }
            int groupId = (int) result.getRetData();
            // get and valid TopicPropGroup info
            if (!WebParameterUtils.getTopicPropInfo(brokerObject,
                    (isAddOp ? defClusterSetting.getClsDefTopicProps() : null), result)) {
                return result.isSuccess();
            }
            TopicPropGroup brokerProps = (TopicPropGroup) result.getRetData();
            // manageStatusId
            ManageStatus manageStatus = ManageStatus.STATUS_MANAGE_APPLY;
            BrokerConfEntity entity =
                    new BrokerConfEntity(itemOpEntity,
                            brokerIdAndIpTuple.getF0(), brokerIdAndIpTuple.getF1());
            entity.updModifyInfo(itemOpEntity.getDataVerId(), brokerPort, brokerTlsPort,
                    brokerWebPort, regionId, groupId, manageStatus, brokerProps);
            addedRecordMap.put(entity.getBrokerId(), entity);
        }
        // check result
        if (addedRecordMap.isEmpty()) {
            result.setFailResult(sBuffer
                    .append("Not found record in ")
                    .append(WebFieldDef.BROKERJSONSET.name)
                    .append(" parameter!").toString());
            sBuffer.delete(0, sBuffer.length());
            return result.isSuccess();
        }
        result.setSuccResult(addedRecordMap);
        return result.isSuccess();
    }

    private StringBuilder buildRetInfo(List<BrokerProcessResult> retInfo,
                                       StringBuilder sBuilder) {
        int totalCnt = 0;
        WebParameterUtils.buildSuccessWithDataRetBegin(sBuilder);
        for (BrokerProcessResult entry : retInfo) {
            if (totalCnt++ > 0) {
                sBuilder.append(",");
            }
            sBuilder.append("{\"brokerId\":").append(entry.getBrokerId())
                    .append("{\"brokerIp\":\"").append(entry.getBrokerIp()).append("\"")
                    .append(",\"success\":").append(entry.isSuccess())
                    .append(",\"errCode\":").append(entry.getErrCode())
                    .append(",\"errInfo\":\"").append(entry.getErrInfo()).append("\"}");
        }
        WebParameterUtils.buildSuccessWithDataRetEnd(sBuilder, totalCnt);
        return sBuilder;
    }

    private boolean getBrokerIpAndIdParamValue(HttpServletRequest req,
                                               StringBuilder sBuffer,
                                               ProcessResult result) {
        // get brokerIp
        if (!WebParameterUtils.getStringParamValue(req,
                WebFieldDef.BROKERIP, true, null, result)) {
            return result.success;
        }
        String brokerIp = (String) result.retData1;
        // get brokerId
        if (!WebParameterUtils.getIntParamValue(req,
                WebFieldDef.BROKERID, true, 0, 0, result)) {
            return result.success;
        }
        int brokerId = (int) result.getRetData();
        return validBrokerIdAndIpValues(brokerId, brokerIp, sBuffer, result);
    }

    private boolean getBrokerIpAndIdParamValue(Map<String, String> keyValueMap,
                                               StringBuilder sBuffer,
                                               ProcessResult result) {
        // get brokerIp
        if (!WebParameterUtils.getStringParamValue(keyValueMap,
                WebFieldDef.BROKERIP, true, null, result)) {
            return result.success;
        }
        String brokerIp = (String) result.retData1;
        // get brokerId
        if (!WebParameterUtils.getIntParamValue(keyValueMap,
                WebFieldDef.BROKERID, true, 0, 0, result)) {
            return result.success;
        }
        int brokerId = (int) result.getRetData();
        return validBrokerIdAndIpValues(brokerId, brokerIp, sBuffer, result);
    }


    private boolean validBrokerIdAndIpValues(int brokerId, String brokerIp,
                                             StringBuilder sBuffer,
                                             ProcessResult result) {
        if (brokerId <= 0) {
            try {
                brokerId = abs(AddressUtils.ipToInt(brokerIp));
            } catch (Exception e) {
                result.setFailResult(DataOpErrCode.DERR_ILLEGAL_VALUE.getCode(),
                        sBuffer.append("Get ").append(WebFieldDef.BROKERID.name)
                                .append(" by ").append(WebFieldDef.BROKERIP.name)
                                .append(" error !, exception is :")
                                .append(e.toString()).toString());
                sBuffer.delete(0, sBuffer.length());
                return result.isSuccess();
            }
        }
        BrokerConfEntity curEntity = metaDataManager.getBrokerConfByBrokerIp(brokerIp);
        if (curEntity != null) {
            result.setFailResult(DataOpErrCode.DERR_EXISTED.getCode(),
                    sBuffer.append("Duplicated broker configure record, query by ")
                            .append(WebFieldDef.BROKERIP.name)
                            .append(" : ").append(brokerIp).toString());
            sBuffer.delete(0, sBuffer.length());
            return result.isSuccess();
        }
        curEntity = metaDataManager.getBrokerConfByBrokerId(brokerId);
        if (curEntity != null) {
            result.setFailResult(DataOpErrCode.DERR_EXISTED.getCode(),
                    sBuffer.append("Duplicated broker configure record, query by ")
                            .append(WebFieldDef.BROKERID.name).append(" : ")
                            .append(brokerId).toString());
            sBuffer.delete(0, sBuffer.length());
            return result.isSuccess();
        }
        result.setSuccResult(new Tuple2<>(brokerId, brokerIp));
        return result.isSuccess();
    }

}
