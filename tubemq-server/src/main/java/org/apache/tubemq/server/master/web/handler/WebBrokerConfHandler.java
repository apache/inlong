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
        registerQueryWebMethod("admin_query_broker_configure",
                "adminQueryBrokerConfInfo");
        // register modify method
        registerModifyWebMethod("admin_add_broker_configure",
                "adminAddBrokerConfInfo");
        registerModifyWebMethod("admin_batch_add_broker_configure",
                "adminBatchAddBrokerConfInfo");
        registerModifyWebMethod("admin_update_broker_configure",
                "adminUpdateBrokerConfInfo");
        registerModifyWebMethod("admin_batch_update_broker_configure",
                "adminBatchUpdBrokerConfInfo");
        registerModifyWebMethod("admin_delete_broker_configure",
                "adminDeleteBrokerConfEntityInfo");
    }

    /**
     * Query broker config
     *
     * @param req
     * @return
     */
    public StringBuilder adminQueryBrokerConfInfo(HttpServletRequest req) {
        ProcessResult result = new ProcessResult();
        StringBuilder sBuffer = new StringBuilder(512);
        BrokerConfEntity qryEntity = new BrokerConfEntity();
        // get queried operation info, for createUser, modifyUser, dataVersionId
        if (!WebParameterUtils.getQueriedOperateInfo(req, qryEntity, sBuffer, result)) {
            WebParameterUtils.buildFailResult(sBuffer, result.errInfo);
            return sBuffer;
        }
        // check and get brokerId field
        if (!WebParameterUtils.getIntParamValue(req,
                WebFieldDef.COMPSBROKERID, false, sBuffer, result)) {
            WebParameterUtils.buildFailResult(sBuffer, result.errInfo);
            return sBuffer;
        }
        Set<Integer> brokerIds = (Set<Integer>) result.getRetData();
        // get brokerIp info
        if (!WebParameterUtils.getStringParamValue(req,
                WebFieldDef.COMPBROKERIP, false, null, sBuffer, result)) {
            WebParameterUtils.buildFailResult(sBuffer, result.errInfo);
            return sBuffer;
        }
        Set<String> brokerIpSet = (Set<String>) result.getRetData();
        // get brokerPort field
        if (!WebParameterUtils.getIntParamValue(req, WebFieldDef.BROKERPORT,
                false, TBaseConstants.META_VALUE_UNDEFINED, 1, sBuffer, result)) {
            WebParameterUtils.buildFailResult(sBuffer, result.errInfo);
            return sBuffer;
        }
        int brokerPort = (int) result.getRetData();
        // get brokerTlsPort field
        if (!WebParameterUtils.getIntParamValue(req, WebFieldDef.BROKERTLSPORT,
                false, TBaseConstants.META_VALUE_UNDEFINED, 1, sBuffer, result)) {
            WebParameterUtils.buildFailResult(sBuffer, result.errInfo);
            return sBuffer;
        }
        int brokerTlsPort = (int) result.getRetData();
        // get brokerWebPort field
        if (!WebParameterUtils.getIntParamValue(req, WebFieldDef.BROKERWEBPORT,
                false, TBaseConstants.META_VALUE_UNDEFINED, 1, sBuffer, result)) {
            WebParameterUtils.buildFailResult(sBuffer, result.errInfo);
            return sBuffer;
        }
        int brokerWebPort = (int) result.getRetData();
        // get regionId field
        if (!WebParameterUtils.getIntParamValue(req, WebFieldDef.REGIONID,
                false, TBaseConstants.META_VALUE_UNDEFINED,
                TServerConstants.BROKER_REGION_ID_MIN, sBuffer, result)) {
            WebParameterUtils.buildFailResult(sBuffer, result.errInfo);
            return sBuffer;
        }
        int regionId = (int) result.getRetData();
        // get groupId field
        if (!WebParameterUtils.getIntParamValue(req, WebFieldDef.GROUPID,
                false, TBaseConstants.META_VALUE_UNDEFINED,
                TServerConstants.BROKER_GROUP_ID_MIN, sBuffer, result)) {
            WebParameterUtils.buildFailResult(sBuffer, result.errInfo);
            return sBuffer;
        }
        int groupId = (int) result.getRetData();
        // get and valid TopicPropGroup info
        if (!WebParameterUtils.getTopicPropInfo(req,
                null, sBuffer, result)) {
            WebParameterUtils.buildFailResult(sBuffer, result.errInfo);
            return sBuffer;
        }
        TopicPropGroup brokerProps = (TopicPropGroup) result.getRetData();
        // get and valid TopicStatusId info
        if (!WebParameterUtils.getTopicStatusParamValue(req,
                false, TopicStatus.STATUS_TOPIC_UNDEFINED, sBuffer, result)) {
            WebParameterUtils.buildFailResult(sBuffer, result.errInfo);
            return sBuffer;
        }
        TopicStatus topicStatus = (TopicStatus) result.getRetData();
        // get and valid broker manage status info
        if (!getManageStatusParamValue(false, req, sBuffer, result)) {
            WebParameterUtils.buildFailResult(sBuffer, result.errInfo);
            return sBuffer;
        }
        ManageStatus mngStatus = (ManageStatus) result.getRetData();
        // get topic info
        if (!WebParameterUtils.getStringParamValue(req,
                WebFieldDef.COMPSTOPICNAME, false, null, sBuffer, result)) {
            WebParameterUtils.buildFailResult(sBuffer, result.errInfo);
            return sBuffer;
        }
        Set<String> topicNameSet = (Set<String>) result.getRetData();
        // get isInclude info
        if (!WebParameterUtils.getBooleanParamValue(req,
                WebFieldDef.ISINCLUDE, false, true, sBuffer, result)) {
            WebParameterUtils.buildFailResult(sBuffer, result.errInfo);
            return sBuffer;
        }
        Boolean isInclude = (Boolean) result.getRetData();
        // get withTopic info
        if (!WebParameterUtils.getBooleanParamValue(req,
                WebFieldDef.WITHTOPIC, false, false, sBuffer, result)) {
            WebParameterUtils.buildFailResult(sBuffer, result.errInfo);
            return sBuffer;
        }
        Boolean withTopic = (Boolean) result.getRetData();
        // fill query entity fields
        qryEntity.updModifyInfo(qryEntity.getDataVerId(), brokerPort, brokerTlsPort,
                brokerWebPort, regionId, groupId, mngStatus, brokerProps);
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
     * Add broker config to brokers in batch
     *
     * @param req
     * @return
     */
    public StringBuilder adminBatchAddBrokerConfInfo(HttpServletRequest req) {
        return innBatchAddOrUpdBrokerConfInfo(req, true);
    }

    /**
     * Update broker configure in batch
     *
     * @param req
     * @return
     */
    public StringBuilder adminBatchUpdBrokerConfInfo(HttpServletRequest req) {
        return innBatchAddOrUpdBrokerConfInfo(req, false);
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
        // get isReservedData info
        if (!WebParameterUtils.getBooleanParamValue(req,
                WebFieldDef.ISRESERVEDDATA, false, false, sBuffer, result)) {
            WebParameterUtils.buildFailResult(sBuffer, result.errInfo);
            return sBuffer;
        }
        Boolean isReservedData = (Boolean) result.getRetData();
        // check and get brokerId field
        if (!WebParameterUtils.getIntParamValue(req,
                WebFieldDef.COMPSBROKERID, true, sBuffer, result)) {
            WebParameterUtils.buildFailResult(sBuffer, result.errInfo);
            return sBuffer;
        }
        Set<Integer> brokerIds = (Set<Integer>) result.getRetData();
        List<BrokerProcessResult> retInfo =
                metaDataManager.delBrokerConfInfo(opEntity.getModifyUser(),
                        isReservedData, brokerIds, sBuffer, result);
        return buildRetInfo(retInfo, sBuffer);
    }

    private StringBuilder innAddOrUpdBrokerConfInfo(HttpServletRequest req,
                                                    boolean isAddOp) {
        ProcessResult result = new ProcessResult();
        StringBuilder sBuffer = new StringBuilder(512);
        // valid operation authorize info
        if (!WebParameterUtils.validReqAuthorizeInfo(req,
                WebFieldDef.ADMINAUTHTOKEN, true, master, sBuffer, result)) {
            WebParameterUtils.buildFailResult(sBuffer, result.errInfo);
            return sBuffer;
        }
        // check and get operation info
        if (!WebParameterUtils.getAUDBaseInfo(req, isAddOp, null, sBuffer, result)) {
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
                        : TBaseConstants.META_VALUE_UNDEFINED), 1, sBuffer, result)) {
            WebParameterUtils.buildFailResult(sBuffer, result.errInfo);
            return sBuffer;
        }
        int brokerPort = (int) result.getRetData();
        // get brokerTlsPort field
        if (!WebParameterUtils.getIntParamValue(req, WebFieldDef.BROKERTLSPORT,
                false, (isAddOp ? defClusterSetting.getBrokerTLSPort()
                        : TBaseConstants.META_VALUE_UNDEFINED), 1, sBuffer, result)) {
            WebParameterUtils.buildFailResult(sBuffer, result.errInfo);
            return sBuffer;
        }
        int brokerTlsPort = (int) result.getRetData();
        // get brokerWebPort field
        if (!WebParameterUtils.getIntParamValue(req, WebFieldDef.BROKERWEBPORT,
                false, (isAddOp ? defClusterSetting.getBrokerWebPort()
                        : TBaseConstants.META_VALUE_UNDEFINED), 1, sBuffer, result)) {
            WebParameterUtils.buildFailResult(sBuffer, result.errInfo);
            return sBuffer;
        }
        int brokerWebPort = (int) result.getRetData();
        // get regionId field
        if (!WebParameterUtils.getIntParamValue(req, WebFieldDef.REGIONID,
                false, (isAddOp ? TServerConstants.BROKER_REGION_ID_DEF
                        : TBaseConstants.META_VALUE_UNDEFINED),
                TServerConstants.BROKER_REGION_ID_MIN, sBuffer, result)) {
            WebParameterUtils.buildFailResult(sBuffer, result.errInfo);
            return sBuffer;
        }
        int regionId = (int) result.getRetData();
        // get groupId field
        if (!WebParameterUtils.getIntParamValue(req, WebFieldDef.GROUPID,
                false, (isAddOp ? TServerConstants.BROKER_GROUP_ID_DEF
                        : TBaseConstants.META_VALUE_UNDEFINED),
                TServerConstants.BROKER_GROUP_ID_MIN, sBuffer, result)) {
            WebParameterUtils.buildFailResult(sBuffer, result.errInfo);
            return sBuffer;
        }
        int groupId = (int) result.getRetData();
        // get and valid TopicPropGroup info
        if (!WebParameterUtils.getTopicPropInfo(req,
                (isAddOp ? defClusterSetting.getClsDefTopicProps() : null), sBuffer, result)) {
            WebParameterUtils.buildFailResult(sBuffer, result.errInfo);
            return sBuffer;
        }
        TopicPropGroup brokerProps = (TopicPropGroup) result.getRetData();
        // get and valid broker manage status info
        if (!getManageStatusParamValue(isAddOp, req, sBuffer, result)) {
            WebParameterUtils.buildFailResult(sBuffer, result.errInfo);
            return sBuffer;
        }
        ManageStatus mngStatus = (ManageStatus) result.getRetData();
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
                    mngStatus, brokerProps, sBuffer, result));
        } else {
            // check and get brokerId field
            if (!WebParameterUtils.getIntParamValue(req,
                    WebFieldDef.COMPSBROKERID, true, sBuffer, result)) {
                WebParameterUtils.buildFailResult(sBuffer, result.errInfo);
                return sBuffer;
            }
            Set<Integer> brokerIdSet = (Set<Integer>) result.getRetData();
            for (Integer brokerId : brokerIdSet) {
                retInfo.add(metaDataManager.addOrUpdBrokerConfig(isAddOp, opEntity,
                        brokerId, "", brokerPort, brokerTlsPort, brokerWebPort,
                        regionId, groupId, mngStatus, brokerProps, sBuffer, result));
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
                WebFieldDef.ADMINAUTHTOKEN, true, master, sBuffer, result)) {
            WebParameterUtils.buildFailResult(sBuffer, result.errInfo);
            return sBuffer;
        }
        // check and get operation info
        if (!WebParameterUtils.getAUDBaseInfo(req, isAddOp, null, sBuffer, result)) {
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
                (List<Map<String, String>>) result.getRetData();
        // check and get cluster default setting info
        ClusterSettingEntity defClusterSetting =
                metaDataManager.getClusterDefSetting(false);
        // check and get broker configure
        BrokerConfEntity itemEntity;
        HashMap<Integer, BrokerConfEntity> addedRecordMap = new HashMap<>();
        for (int j = 0; j < brokerJsonArray.size(); j++) {
            Map<String, String> brokerObject = brokerJsonArray.get(j);
            // check and get operation info
            if (!WebParameterUtils.getAUDBaseInfo(brokerObject,
                    isAddOp, defOpEntity, sBuffer, result)) {
                return result.isSuccess();
            }
            BaseEntity itemOpEntity = (BaseEntity) result.getRetData();
            // get brokerPort field
            if (!WebParameterUtils.getIntParamValue(brokerObject, WebFieldDef.BROKERPORT,
                    false, (isAddOp ? defClusterSetting.getBrokerPort()
                            : TBaseConstants.META_VALUE_UNDEFINED), 1, sBuffer, result)) {
                return result.isSuccess();
            }
            int brokerPort = (int) result.getRetData();
            // get brokerTlsPort field
            if (!WebParameterUtils.getIntParamValue(brokerObject, WebFieldDef.BROKERTLSPORT,
                    false, (isAddOp ? defClusterSetting.getBrokerTLSPort()
                            : TBaseConstants.META_VALUE_UNDEFINED), 1, sBuffer, result)) {
                return result.isSuccess();
            }
            int brokerTlsPort = (int) result.getRetData();
            // get brokerWebPort field
            if (!WebParameterUtils.getIntParamValue(brokerObject, WebFieldDef.BROKERWEBPORT,
                    false, (isAddOp ? defClusterSetting.getBrokerWebPort()
                            : TBaseConstants.META_VALUE_UNDEFINED), 1, sBuffer, result)) {
                return result.isSuccess();
            }
            int brokerWebPort = (int) result.getRetData();
            // get regionId field
            if (!WebParameterUtils.getIntParamValue(brokerObject, WebFieldDef.REGIONID,
                    false, (isAddOp ? TServerConstants.BROKER_REGION_ID_DEF
                            : TBaseConstants.META_VALUE_UNDEFINED),
                    TServerConstants.BROKER_REGION_ID_MIN, sBuffer, result)) {
                return result.isSuccess();
            }
            int regionId = (int) result.getRetData();
            // get groupId field
            if (!WebParameterUtils.getIntParamValue(brokerObject, WebFieldDef.GROUPID,
                    false, (isAddOp ? TServerConstants.BROKER_GROUP_ID_DEF
                            : TBaseConstants.META_VALUE_UNDEFINED),
                    TServerConstants.BROKER_GROUP_ID_MIN, sBuffer, result)) {
                return result.isSuccess();
            }
            int groupId = (int) result.getRetData();
            // get and valid TopicPropGroup info
            if (!WebParameterUtils.getTopicPropInfo(brokerObject,
                    (isAddOp ? defClusterSetting.getClsDefTopicProps() : null), sBuffer, result)) {
                return result.isSuccess();
            }
            TopicPropGroup brokerProps = (TopicPropGroup) result.getRetData();
            // get and valid broker manage status info
            if (!getManageStatusParamValue(isAddOp, brokerObject, sBuffer, result)) {
                return result.isSuccess();
            }
            ManageStatus mngStatus = (ManageStatus) result.getRetData();
            if (isAddOp) {
                // get brokerIp and brokerId field
                if (!getBrokerIpAndIdParamValue(brokerObject, sBuffer, result)) {
                    return result.isSuccess();
                }
                Tuple2<Integer, String> brokerIdAndIpTuple =
                        (Tuple2<Integer, String>) result.getRetData();
                // buid new record
                itemEntity = new BrokerConfEntity(itemOpEntity,
                        brokerIdAndIpTuple.getF0(), brokerIdAndIpTuple.getF1());
                itemEntity.updModifyInfo(itemOpEntity.getDataVerId(), brokerPort, brokerTlsPort,
                        brokerWebPort, regionId, groupId, mngStatus, brokerProps);
                addedRecordMap.put(itemEntity.getBrokerId(), itemEntity);
            } else {
                // check and get brokerId field
                if (!WebParameterUtils.getIntParamValue(brokerObject,
                        WebFieldDef.BROKERID, true, sBuffer, result)) {
                    return result.isSuccess();
                }
                Integer brokerId = (Integer) result.getRetData();
                itemEntity = new BrokerConfEntity(itemOpEntity, brokerId, "");
                itemEntity.updModifyInfo(itemOpEntity.getDataVerId(), brokerPort, brokerTlsPort,
                        brokerWebPort, regionId, groupId, mngStatus, brokerProps);
                addedRecordMap.put(itemEntity.getBrokerId(), itemEntity);
            }
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

    private <T> boolean getBrokerIpAndIdParamValue(T paramCntr,
                                                   StringBuilder sBuffer,
                                                   ProcessResult result) {
        // get brokerIp
        if (!WebParameterUtils.getStringParamValue(paramCntr,
                WebFieldDef.BROKERIP, true, null, sBuffer, result)) {
            return result.success;
        }
        String brokerIp = (String) result.getRetData();
        // get brokerId
        if (!WebParameterUtils.getIntParamValue(paramCntr,
                WebFieldDef.BROKERID, true, 0, 0, sBuffer, result)) {
            return result.success;
        }
        int brokerId = (int) result.getRetData();
        // valid brokerIp and brokerId
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

    private <T> boolean getManageStatusParamValue(boolean isAddOp, T paramCntr,
                                                  StringBuilder sBuffer,
                                                  ProcessResult result) {
        // get manage status id value
        if (!WebParameterUtils.getIntParamValue(paramCntr,
                WebFieldDef.MANAGESTATUS, false,
                (isAddOp ? ManageStatus.STATUS_MANAGE_APPLY.getCode()
                        : ManageStatus.STATUS_MANAGE_UNDEFINED.getCode()),
                ManageStatus.STATUS_MANAGE_APPLY.getCode(),
                ManageStatus.STATUS_MANAGE_OFFLINE.getCode(), sBuffer, result)) {
            return result.success;
        }
        int manageStatusId = (int) result.getRetData();
        try {
            ManageStatus mngStatus = ManageStatus.valueOf(manageStatusId);
            result.setSuccResult(mngStatus);
        } catch (Throwable e) {
            result.setFailResult(DataOpErrCode.DERR_ILLEGAL_VALUE.getCode(),
                    sBuffer.append("Illegal ").append(WebFieldDef.MANAGESTATUS.name)
                            .append(" parameter value :").append(e.getMessage()).toString());
            sBuffer.delete(0, sBuffer.length());
        }
        return result.isSuccess();
    }

}
