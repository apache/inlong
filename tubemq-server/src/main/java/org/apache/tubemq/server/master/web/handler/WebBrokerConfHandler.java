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
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.servlet.http.HttpServletRequest;
import org.apache.tubemq.corebase.TBaseConstants;
import org.apache.tubemq.corebase.utils.AddressUtils;
import org.apache.tubemq.corebase.utils.Tuple2;
import org.apache.tubemq.corebase.utils.Tuple3;
import org.apache.tubemq.server.common.TServerConstants;
import org.apache.tubemq.server.common.fielddef.WebFieldDef;
import org.apache.tubemq.server.common.statusdef.ManageStatus;
import org.apache.tubemq.server.common.statusdef.TopicStatus;
import org.apache.tubemq.server.common.utils.ProcessResult;
import org.apache.tubemq.server.common.utils.WebParameterUtils;
import org.apache.tubemq.server.master.TMaster;
import org.apache.tubemq.server.master.metamanage.DataOpErrCode;
import org.apache.tubemq.server.master.metamanage.metastore.dao.entity.BrokerConfEntity;
import org.apache.tubemq.server.master.metamanage.metastore.dao.entity.ClusterSettingEntity;
import org.apache.tubemq.server.master.metamanage.metastore.dao.entity.TopicDeployConfEntity;
import org.apache.tubemq.server.master.metamanage.metastore.dao.entity.TopicPropGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


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

    private static final Logger logger =
            LoggerFactory.getLogger(WebBrokerConfHandler.class);

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
        StringBuilder sBuilder = new StringBuilder(512);
        BrokerConfEntity qryEntity = new BrokerConfEntity();
        // get queried operation info, for createUser, modifyUser, dataVersionId
        if (!WebParameterUtils.getQueriedOperateInfo(req, qryEntity, result)) {
            WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
            return sBuilder;
        }
        // check and get brokerId field
        if (!WebParameterUtils.getIntParamValue(req,
                WebFieldDef.COMPSBROKERID, false, result)) {
            WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
            return sBuilder;
        }
        Set<Integer> brokerIds = (Set<Integer>) result.retData1;
        // get brokerIp info
        if (!WebParameterUtils.getStringParamValue(req,
                WebFieldDef.COMPBROKERIP, false, null, result)) {
            WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
            return sBuilder;
        }
        Set<String> brokerIpSet = (Set<String>) result.retData1;
        // get brokerPort field
        if (!WebParameterUtils.getIntParamValue(req, WebFieldDef.BROKERPORT,
                false, TBaseConstants.META_VALUE_UNDEFINED, 1, result)) {
            WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
            return sBuilder;
        }
        int brokerPort = (int) result.getRetData();
        // get brokerTlsPort field
        if (!WebParameterUtils.getIntParamValue(req, WebFieldDef.BROKERTLSPORT,
                false, TBaseConstants.META_VALUE_UNDEFINED, 1, result)) {
            WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
            return sBuilder;
        }
        int brokerTlsPort = (int) result.getRetData();
        // get brokerWebPort field
        if (!WebParameterUtils.getIntParamValue(req, WebFieldDef.BROKERWEBPORT,
                false, TBaseConstants.META_VALUE_UNDEFINED, 1, result)) {
            WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
            return sBuilder;
        }
        int brokerWebPort = (int) result.getRetData();
        // get regionId field
        if (!WebParameterUtils.getIntParamValue(req, WebFieldDef.REGIONID,
                false, TBaseConstants.META_VALUE_UNDEFINED,
                TServerConstants.BROKER_REGION_ID_MIN, result)) {
            WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
            return sBuilder;
        }
        int regionId = (int) result.getRetData();
        // get groupId field
        if (!WebParameterUtils.getIntParamValue(req, WebFieldDef.GROUPID,
                false, TBaseConstants.META_VALUE_UNDEFINED,
                TServerConstants.BROKER_GROUP_ID_MIN, result)) {
            WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
            return sBuilder;
        }
        int groupId = (int) result.getRetData();
        // get and valid TopicPropGroup info
        if (!WebParameterUtils.getTopicPropInfo(req,
                null, result)) {
            WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
            return sBuilder;
        }
        TopicPropGroup brokerProps = (TopicPropGroup) result.getRetData();
        // get and valid TopicStatusId info
        if (!WebParameterUtils.getTopicStatusParamValue(req,
                false, TopicStatus.STATUS_TOPIC_UNDEFINED, result)) {
            WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
            return sBuilder;
        }
        TopicStatus topicStatus = (TopicStatus) result.getRetData();
        // get topic info
        if (!WebParameterUtils.getStringParamValue(req,
                WebFieldDef.COMPSTOPICNAME, false, null, result)) {
            WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
            return sBuilder;
        }
        Set<String> topicNameSet = (Set<String>) result.retData1;
        // get isInclude info
        if (!WebParameterUtils.getBooleanParamValue(req,
                WebFieldDef.ISINCLUDE, false, true, result)) {
            WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
            return sBuilder;
        }
        Boolean isInclude = (Boolean) result.retData1;
        // get withTopic info
        if (!WebParameterUtils.getBooleanParamValue(req,
                WebFieldDef.WITHTOPIC, false, false, result)) {
            WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
            return sBuilder;
        }
        Boolean withTopic = (Boolean) result.retData1;
        // fill query entity fields
        qryEntity.updModifyInfo(brokerPort, brokerTlsPort, brokerWebPort,
                regionId, groupId, null, brokerProps);
        Map<Integer, BrokerConfEntity> qryResult =
                metaDataManager.getBrokerConfInfo(brokerIds, brokerIpSet, qryEntity);
        // build query result
        int totalCnt = 0;
        WebParameterUtils.buildSuccessWithDataRetBegin(sBuilder);
        for (BrokerConfEntity entity : qryResult.values()) {
            Map<String, TopicDeployConfEntity> topicConfEntityMap =
                    metaDataManager.getBrokerTopicConfEntitySet(entity.getBrokerId());
            if (!isValidRecord(topicNameSet, isInclude, topicStatus, topicConfEntityMap)) {
                continue;
            }
            if (totalCnt++ > 0) {
                sBuilder.append(",");
            }
            entity.toWebJsonStr(sBuilder, true, false);
            sBuilder = addTopicInfo(withTopic, sBuilder, topicConfEntityMap);
            sBuilder.append("}");
        }
        WebParameterUtils.buildSuccessWithDataRetEnd(sBuilder, totalCnt);
        return sBuilder;
    }


    /**
     * Add default config to a broker
     *
     * @param req
     * @return
     */
    public StringBuilder adminAddBrokerDefConfEntityInfo(HttpServletRequest req) {
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
        Tuple3<Long, String, Date> opTupleInfo =
                (Tuple3<Long, String, Date>) result.getRetData();
        // check and get cluster default setting info
        ClusterSettingEntity defClusterSetting =
                metaDataManager.getClusterDefSetting();
        if (defClusterSetting == null) {
            if (!metaDataManager.addClusterDefSetting(sBuilder, result)) {
                WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
                return sBuilder;
            }
            defClusterSetting = metaDataManager.getClusterDefSetting();
        }
        // get brokerIp and brokerId field
        if (!getBrokerIpAndIdParamValue(req, sBuilder, result)) {
            WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
            return sBuilder;
        }
        Tuple2<Integer, String> brokerIdAndIpTuple =
                (Tuple2<Integer, String>) result.getRetData();
        // get brokerPort field
        if (!WebParameterUtils.getIntParamValue(req, WebFieldDef.BROKERPORT,
                false, defClusterSetting.getBrokerPort(), 1, result)) {
            WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
            return sBuilder;
        }
        int brokerPort = (int) result.getRetData();
        // get brokerTlsPort field
        if (!WebParameterUtils.getIntParamValue(req, WebFieldDef.BROKERTLSPORT,
                false, defClusterSetting.getBrokerTLSPort(), 1, result)) {
            WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
            return sBuilder;
        }
        int brokerTlsPort = (int) result.getRetData();
        // get brokerWebPort field
        if (!WebParameterUtils.getIntParamValue(req, WebFieldDef.BROKERWEBPORT,
                false, defClusterSetting.getBrokerWebPort(), 1, result)) {
            WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
            return sBuilder;
        }
        int brokerWebPort = (int) result.getRetData();
        // get regionId field
        if (!WebParameterUtils.getIntParamValue(req, WebFieldDef.REGIONID,
                false, TServerConstants.BROKER_REGION_ID_DEF,
                TServerConstants.BROKER_REGION_ID_MIN, result)) {
            WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
            return sBuilder;
        }
        int regionId = (int) result.getRetData();
        // get groupId field
        if (!WebParameterUtils.getIntParamValue(req, WebFieldDef.GROUPID,
                false, TServerConstants.BROKER_GROUP_ID_DEF,
                TServerConstants.BROKER_GROUP_ID_MIN, result)) {
            WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
            return sBuilder;
        }
        int groupId = (int) result.getRetData();
        // get and valid TopicPropGroup info
        if (!WebParameterUtils.getTopicPropInfo(req,
                defClusterSetting.getClsDefTopicProps(), result)) {
            WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
            return sBuilder;
        }
        TopicPropGroup brokerProps = (TopicPropGroup) result.getRetData();
        // manageStatusId
        ManageStatus manageStatus = ManageStatus.STATUS_MANAGE_APPLY;
        // add record and process result
        List<BrokerProcessResult> retInfo = new ArrayList<>();
        BrokerProcessResult processResult =
                metaDataManager.addBrokerConfig(opTupleInfo.getF0(),
                        opTupleInfo.getF1(), opTupleInfo.getF2(), brokerIdAndIpTuple.getF0(),
                        brokerIdAndIpTuple.getF1(), brokerPort, brokerTlsPort, brokerWebPort,
                        regionId, groupId, manageStatus, brokerProps, sBuilder, result);
        retInfo.add(processResult);
        return buildRetInfo(retInfo, sBuilder);
    }

    /**
     * Add default config to brokers in batch
     *
     * @param req
     * @return
     * @throws Exception
     */
    public StringBuilder adminBatchAddBrokerDefConfEntityInfo(HttpServletRequest req) {
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
        Tuple3<Long, String, Date> opTupleInfo =
                (Tuple3<Long, String, Date>) result.getRetData();
        // check and get brokerJsonSet info
        if (!getBrokerJsonSetInfo(req, true, true,
                opTupleInfo.getF0(), opTupleInfo.getF1(), opTupleInfo.getF2(),
                null, sBuilder, result)) {
            WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
            return sBuilder;
        }
        Map<Integer, BrokerConfEntity> addedRecordMap =
                (HashMap<Integer, BrokerConfEntity>) result.getRetData();
        // add record and process result
        List<BrokerProcessResult> retInfo = new ArrayList<>();
        for (BrokerConfEntity brokerEntity : addedRecordMap.values()) {
            BrokerProcessResult processResult =
                    metaDataManager.addBrokerConfig(brokerEntity, sBuilder, result);
            retInfo.add(processResult);
        }
        return buildRetInfo(retInfo, sBuilder);
    }

    /**
     * Update broker default config.
     * The current record will be checked firstly.
     * The update will be performed only when there are changes.
     *
     * @param req
     * @return
     * @throws Throwable
     */
    public StringBuilder adminUpdateBrokerConf(HttpServletRequest req) {
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
        Tuple3<Long, String, Date> opTupleInfo =
                (Tuple3<Long, String, Date>) result.getRetData();
        // check and get brokerId field
        if (!WebParameterUtils.getIntParamValue(req,
                WebFieldDef.COMPSBROKERID, true, result)) {
            WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
            return sBuilder;
        }
        Set<Integer> brokerIdSet = (Set<Integer>) result.retData1;
        // get brokerPort field
        if (!WebParameterUtils.getIntParamValue(req, WebFieldDef.BROKERPORT,
                false, TBaseConstants.META_VALUE_UNDEFINED, 1, result)) {
            WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
            return sBuilder;
        }
        int brokerPort = (int) result.getRetData();
        // get brokerTlsPort field
        if (!WebParameterUtils.getIntParamValue(req, WebFieldDef.BROKERTLSPORT,
                false, TBaseConstants.META_VALUE_UNDEFINED, 1, result)) {
            WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
            return sBuilder;
        }
        int brokerTlsPort = (int) result.getRetData();
        // get brokerWebPort field
        if (!WebParameterUtils.getIntParamValue(req, WebFieldDef.BROKERWEBPORT,
                false, TBaseConstants.META_VALUE_UNDEFINED, 1, result)) {
            WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
            return sBuilder;
        }
        int brokerWebPort = (int) result.getRetData();
        // get regionId field
        if (!WebParameterUtils.getIntParamValue(req, WebFieldDef.REGIONID,
                false, TBaseConstants.META_VALUE_UNDEFINED,
                TServerConstants.BROKER_REGION_ID_MIN, result)) {
            WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
            return sBuilder;
        }
        int regionId = (int) result.getRetData();
        // get groupId field
        if (!WebParameterUtils.getIntParamValue(req, WebFieldDef.GROUPID,
                false, TBaseConstants.META_VALUE_UNDEFINED,
                TServerConstants.BROKER_GROUP_ID_MIN, result)) {
            WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
            return sBuilder;
        }
        int groupId = (int) result.getRetData();
        // get and valid TopicPropGroup info
        if (!WebParameterUtils.getTopicPropInfo(req, null, result)) {
            WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
            return sBuilder;
        }
        TopicPropGroup brokerProps = (TopicPropGroup) result.getRetData();
        // manageStatusId
        ManageStatus manageStatus = ManageStatus.STATUS_MANAGE_APPLY;
        // add record and process result
        List<BrokerProcessResult> retInfo = new ArrayList<>();
        BrokerConfEntity newEntity;
        for (Integer brokerId : brokerIdSet) {
            BrokerConfEntity curEntity =
                    metaDataManager.getBrokerConfByBrokerId(brokerId);
            if (curEntity == null) {
                result.setFailResult(DataOpErrCode.DERR_NOT_EXIST.getCode(),
                        DataOpErrCode.DERR_NOT_EXIST.getDescription());
                retInfo.add(new BrokerProcessResult(brokerId, "", result));
                continue;
            }
            newEntity = curEntity.clone();
            newEntity.updBaseModifyInfo(opTupleInfo.getF0(), null, null,
                    opTupleInfo.getF1(), opTupleInfo.getF2(), null);
            if (!newEntity.updModifyInfo(brokerPort, brokerTlsPort,
                    brokerWebPort, regionId, groupId, null, brokerProps)) {
                result.setFailResult(DataOpErrCode.DERR_SUCCESS_UNCHANGED.getCode(),
                        DataOpErrCode.DERR_SUCCESS_UNCHANGED.getDescription());
                retInfo.add(new BrokerProcessResult(brokerId, curEntity.getBrokerIp(), result));
                continue;
            }
            metaDataManager.modBrokerConfig(newEntity, sBuilder, result);
            retInfo.add(new BrokerProcessResult(brokerId, curEntity.getBrokerIp(), result));
        }
        return buildRetInfo(retInfo, sBuilder);
    }

    /**
     * Delete broker config
     *
     * @param req
     * @return
     */
    public StringBuilder adminDeleteBrokerConfEntityInfo(HttpServletRequest req) {
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
        Tuple3<Long, String, Date> opTupleInfo =
                (Tuple3<Long, String, Date>) result.getRetData();
        // get isReservedData info
        if (!WebParameterUtils.getBooleanParamValue(req,
                WebFieldDef.ISRESERVEDDATA, false, false, result)) {
            WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
            return sBuilder;
        }
        Boolean isReservedData = (Boolean) result.retData1;
        // check and get brokerId field
        if (!WebParameterUtils.getIntParamValue(req,
                WebFieldDef.COMPSBROKERID, true, result)) {
            WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
            return sBuilder;
        }
        Set<Integer> brokerIds = (Set<Integer>) result.retData1;
        if (brokerIds.isEmpty()) {
            WebParameterUtils.buildFailResult(sBuilder,
                    "Illegal value: Null value of brokerId parameter");
            return sBuilder;
        }
        Map<Integer, BrokerConfEntity> qryResult =
                metaDataManager.getBrokerConfInfo(brokerIds, null, null);
        if (qryResult.isEmpty()) {
            WebParameterUtils.buildFailResult(sBuilder,
                    "Illegal value: not found broker configure by brokerId value");
            return sBuilder;
        }
        // check broker configure status
        List<BrokerProcessResult> retInfo = new ArrayList<>();
        Map<Integer, BrokerConfEntity> needDelMap = new HashMap<>();
        Map<String, TopicDeployConfEntity> topicConfigMap;
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
                        sBuilder.append("Illegal value: the broker is processing offline event by brokerId=")
                                .append(entity.getBrokerId()).append(", please wait and try later!").toString());
                retInfo.add(new BrokerProcessResult(
                        entity.getBrokerId(), entity.getBrokerIp(), result));
                continue;
            }
            boolean isMatched = true;
            if (isReservedData) {
                for (Map.Entry<String, TopicDeployConfEntity> entry : topicConfigMap.entrySet()) {
                    if (entry.getValue() == null) {
                        continue;
                    }
                    if (entry.getValue().getTopicProps().isAcceptPublish()
                            || entry.getValue().getTopicProps().isAcceptSubscribe()) {
                        result.setFailResult(DataOpErrCode.DERR_ILLEGAL_STATUS.getCode(),
                                sBuilder.append("The topic ").append(entry.getKey())
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
                        sBuilder.append("Topic configure of broker by brokerId=")
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
            return buildRetInfo(retInfo, sBuilder);
        }
        // do delete operation
        for (BrokerConfEntity entry : needDelMap.values()) {
            if (entry == null) {
                continue;
            }
            if (isReservedData) {
                Map<String, TopicDeployConfEntity> brokerTopicConfMap =
                        metaDataManager.getBrokerTopicConfEntitySet(entry.getBrokerId());
                if (brokerTopicConfMap != null) {
                    metaDataManager.delBrokerTopicConfig(opTupleInfo.getF1(),
                            entry.getBrokerId(), sBuilder, result);
                }
            }
            metaDataManager.confDelBrokerConfig(
                    opTupleInfo.getF1(), entry.getBrokerId(), sBuilder, result);
            retInfo.add(new BrokerProcessResult(
                    entry.getBrokerId(), entry.getBrokerIp(), result));
        }
        return buildRetInfo(retInfo, sBuilder);

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
                                  Map<String, TopicDeployConfEntity> topicConfEntityMap) {
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
        for (TopicDeployConfEntity topicConfEntity : topicConfEntityMap.values()) {
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
                                       Map<String, TopicDeployConfEntity> topicConfEntityMap) {
        if (withTopic) {
            sBuilder.append(",\"topicSet\":[");
            int topicCount = 0;
            if (topicConfEntityMap != null) {
                for (TopicDeployConfEntity topicEntity : topicConfEntityMap.values()) {
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

    private boolean getBrokerJsonSetInfo(HttpServletRequest req, boolean required,
                                         boolean isCreate, long dataVerId,
                                         String operator, Date operateDate,
                                         List<Map<String, String>> defValue,
                                         StringBuilder sBuilder,
                                         ProcessResult result) {
        if (!WebParameterUtils.getJsonArrayParamValue(req,
                WebFieldDef.BROKERJSONSET, required, defValue, result)) {
            return result.success;
        }
        List<Map<String, String>> brokerJsonArray =
                (List<Map<String, String>>) result.retData1;
        // check and get cluster default setting info
        ClusterSettingEntity defClusterSetting =
                metaDataManager.getClusterDefSetting();
        if (defClusterSetting == null) {
            if (!metaDataManager.addClusterDefSetting(sBuilder, result)) {
                return result.isSuccess();
            }
            defClusterSetting = metaDataManager.getClusterDefSetting();
        }
        // check and get broker configure
        HashMap<Integer, BrokerConfEntity> addedRecordMap = new HashMap<>();
        for (int j = 0; j < brokerJsonArray.size(); j++) {
            Map<String, String> brokerObject = brokerJsonArray.get(j);
            // check and get operation info
            long itemDataVerId = dataVerId;
            String itemCreator = operator;
            Date itemCreateDate = operateDate;
            if (!WebParameterUtils.getAUDBaseInfo(
                    brokerObject, true, result)) {
                return result.isSuccess();
            }
            Tuple3<Long, String, Date> opTupleInfo =
                    (Tuple3<Long, String, Date>) result.getRetData();
            if (opTupleInfo.getF0() != TBaseConstants.META_VALUE_UNDEFINED) {
                itemDataVerId = opTupleInfo.getF0();
            }
            if (opTupleInfo.getF1() != null) {
                itemCreator = opTupleInfo.getF1();
            }
            if (opTupleInfo.getF2() != null) {
                itemCreateDate = opTupleInfo.getF2();
            }
            // get brokerIp and brokerId field
            if (!getBrokerIpAndIdParamValue(brokerObject, sBuilder, result)) {
                return result.isSuccess();
            }
            Tuple2<Integer, String> brokerIdAndIpTuple =
                    (Tuple2<Integer, String>) result.getRetData();
            // get brokerPort field
            if (!WebParameterUtils.getIntParamValue(brokerObject, WebFieldDef.BROKERPORT,
                    false, defClusterSetting.getBrokerPort(), 1, result)) {
                return result.isSuccess();
            }
            int brokerPort = (int) result.getRetData();
            // get brokerTlsPort field
            if (!WebParameterUtils.getIntParamValue(brokerObject, WebFieldDef.BROKERTLSPORT,
                    false, defClusterSetting.getBrokerTLSPort(), 1, result)) {
                return result.isSuccess();
            }
            int brokerTlsPort = (int) result.getRetData();
            // get brokerWebPort field
            if (!WebParameterUtils.getIntParamValue(brokerObject, WebFieldDef.BROKERWEBPORT,
                    false, defClusterSetting.getBrokerWebPort(), 1, result)) {
                return result.isSuccess();
            }
            int brokerWebPort = (int) result.getRetData();
            // get regionId field
            if (!WebParameterUtils.getIntParamValue(brokerObject, WebFieldDef.REGIONID,
                    false, TServerConstants.BROKER_REGION_ID_DEF,
                    TServerConstants.BROKER_REGION_ID_MIN, result)) {
                return result.isSuccess();
            }
            int regionId = (int) result.getRetData();
            // get groupId field
            if (!WebParameterUtils.getIntParamValue(brokerObject, WebFieldDef.GROUPID,
                    false, TServerConstants.BROKER_GROUP_ID_DEF,
                    TServerConstants.BROKER_GROUP_ID_MIN, result)) {
                return result.isSuccess();
            }
            int groupId = (int) result.getRetData();
            // get and valid TopicPropGroup info
            if (!WebParameterUtils.getTopicPropInfo(brokerObject,
                    defClusterSetting.getClsDefTopicProps(), result)) {
                return result.isSuccess();
            }
            TopicPropGroup brokerProps = (TopicPropGroup) result.getRetData();
            // manageStatusId
            ManageStatus manageStatus = ManageStatus.STATUS_MANAGE_APPLY;
            BrokerConfEntity entity =
                    new BrokerConfEntity(itemDataVerId, itemCreator, itemCreateDate);
            entity.setBrokerIdAndIp(brokerIdAndIpTuple.getF0(), brokerIdAndIpTuple.getF1());
            entity.updModifyInfo(brokerPort, brokerTlsPort, brokerWebPort,
                    regionId, groupId, manageStatus, brokerProps);
            addedRecordMap.put(entity.getBrokerId(), entity);
        }
        // check result
        if (addedRecordMap.isEmpty()) {
            if (isCreate) {
                result.setFailResult(sBuilder
                        .append("Not found record in ")
                        .append(WebFieldDef.BROKERJSONSET.name)
                        .append(" parameter!").toString());
                sBuilder.delete(0, sBuilder.length());
                return result.isSuccess();
            }
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
                                               StringBuilder sBuilder,
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
        if (brokerId <= 0) {
            try {
                brokerId = abs(AddressUtils.ipToInt(brokerIp));
            } catch (Exception e) {
                result.setFailResult(DataOpErrCode.DERR_ILLEGAL_VALUE.getCode(),
                        sBuilder.append("Get ").append(WebFieldDef.BROKERID.name)
                                .append(" by ").append(WebFieldDef.BROKERIP.name)
                                .append(" error !, exception is :")
                                .append(e.toString()).toString());
                return result.isSuccess();
            }
        }
        BrokerConfEntity curEntity = metaDataManager.getBrokerConfByBrokerIp(brokerIp);
        if (curEntity != null) {
            result.setFailResult(DataOpErrCode.DERR_EXISTED.getCode(),
                    sBuilder.append("Duplicated broker configure record, ")
                            .append("query by ").append(WebFieldDef.BROKERIP.name)
                            .append(" : ").append(brokerIp).toString());
            return result.isSuccess();
        }
        curEntity = metaDataManager.getBrokerConfByBrokerId(brokerId);
        if (curEntity != null) {
            result.setFailResult(DataOpErrCode.DERR_EXISTED.getCode(),
                    sBuilder.append("Duplicated broker configure record, ")
                            .append("query by ").append(WebFieldDef.BROKERID.name)
                            .append(" : ").append(brokerId).toString());
            return result.isSuccess();
        }
        result.setSuccResult(new Tuple2<>(brokerId, brokerIp));
        return result.isSuccess();
    }

    private boolean getBrokerIpAndIdParamValue(Map<String, String> keyValueMap,
                                               StringBuilder sBuilder,
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
        if (brokerId <= 0) {
            try {
                brokerId = abs(AddressUtils.ipToInt(brokerIp));
            } catch (Exception e) {
                result.setFailResult(DataOpErrCode.DERR_ILLEGAL_VALUE.getCode(),
                        sBuilder.append("Get ").append(WebFieldDef.BROKERID.name)
                                .append(" by ").append(WebFieldDef.BROKERIP.name)
                                .append(" error !, exception is :")
                                .append(e.toString()).toString());
                return result.isSuccess();
            }
        }
        BrokerConfEntity curEntity = metaDataManager.getBrokerConfByBrokerIp(brokerIp);
        if (curEntity != null) {
            result.setFailResult(DataOpErrCode.DERR_EXISTED.getCode(),
                    sBuilder.append("Duplicated broker configure record, ")
                            .append("query by ").append(WebFieldDef.BROKERIP.name)
                            .append(" : ").append(brokerIp).toString());
            return result.isSuccess();
        }
        curEntity = metaDataManager.getBrokerConfByBrokerId(brokerId);
        if (curEntity != null) {
            result.setFailResult(DataOpErrCode.DERR_EXISTED.getCode(),
                    sBuilder.append("Duplicated broker configure record, ")
                            .append("query by ").append(WebFieldDef.BROKERID.name)
                            .append(" : ").append(brokerId).toString());
            return result.isSuccess();
        }
        result.setSuccResult(new Tuple2<>(brokerId, brokerIp));
        return result.isSuccess();
    }





}
