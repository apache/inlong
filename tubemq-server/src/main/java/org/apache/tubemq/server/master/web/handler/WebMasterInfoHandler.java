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
import javax.servlet.http.HttpServletRequest;
import org.apache.tubemq.corebase.TBaseConstants;
import org.apache.tubemq.corebase.utils.SettingValidUtils;
import org.apache.tubemq.server.common.fielddef.WebFieldDef;
import org.apache.tubemq.server.common.utils.ProcessResult;
import org.apache.tubemq.server.common.utils.WebParameterUtils;
import org.apache.tubemq.server.master.TMaster;
import org.apache.tubemq.server.master.bdbstore.bdbentitys.BdbClusterSettingEntity;
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
        ClusterGroupVO clusterGroupVO = brokerConfManager.getGroupAddressStrInfo();
        if (clusterGroupVO == null) {
            strBuffer.append("{\"result\":false,\"errCode\":500,\"errMsg\":\"GetBrokerGroup info error\",\"data\":[]}");
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
        BdbClusterSettingEntity defClusterSetting =
                brokerConfManager.getBdbClusterSetting();
        sBuilder.append("{\"result\":true,\"errCode\":0,\"errMsg\":\"Ok\",\"data\":[");
        if (defClusterSetting != null) {
            defClusterSetting.toJsonString(sBuilder);
        }
        sBuilder.append("]}");
        return sBuilder;
    }

    /**
     * Add or modify cluster default setting
     *
     * @param req
     * @return
     */
    public StringBuilder adminSetClusterDefSetting(HttpServletRequest req) {
        boolean dataChanged = false;
        ProcessResult result = new ProcessResult();
        StringBuilder sBuilder = new StringBuilder(512);
        // valid operation authorize info
        if (!WebParameterUtils.validReqAuthorizeInfo(req,
                WebFieldDef.ADMINAUTHTOKEN, true, master, result)) {
            WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
            return sBuilder;
        }
        // check modify user field
        if (!WebParameterUtils.getStringParamValue(req,
                WebFieldDef.MODIFYUSER, true, null, result)) {
            WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
            return sBuilder;
        }
        String modifyUser = (String) result.retData1;
        // check max message size
        if (!WebParameterUtils.getIntParamValue(req,
                WebFieldDef.MAXMSGSIZE, false,
                TBaseConstants.META_VALUE_UNDEFINED,
                TBaseConstants.META_MIN_ALLOWED_MESSAGE_SIZE_MB,
                TBaseConstants.META_MAX_ALLOWED_MESSAGE_SIZE_MB,
                result)) {
            WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
            return sBuilder;
        }
        int maxMsgSizeInMB = (int) result.retData1;
        if (maxMsgSizeInMB != TBaseConstants.META_VALUE_UNDEFINED) {
            dataChanged = true;
        }
        // check and get modify date
        if (!WebParameterUtils.getDateParameter(req,
                WebFieldDef.MODIFYDATE, false, new Date(), result)) {
            WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
            return sBuilder;
        }
        Date modifyDate = (Date) result.retData1;
        if (!dataChanged) {
            WebParameterUtils.buildSuccessResult(sBuilder, "No data is changed!");
            return sBuilder;
        }
        // add or modify cluster setting info
        BdbClusterSettingEntity defClusterSetting =
                brokerConfManager.getBdbClusterSetting();
        if (defClusterSetting == null) {
            defClusterSetting = new BdbClusterSettingEntity();
        }
        defClusterSetting.setModifyInfo(modifyUser, modifyDate);
        if (maxMsgSizeInMB != TBaseConstants.META_VALUE_UNDEFINED) {
            defClusterSetting.setMaxMsgSizeInB(
                    SettingValidUtils.validAndXfeMaxMsgSizeFromMBtoB(maxMsgSizeInMB));
        }
        try {
            brokerConfManager.confSetBdbClusterDefSetting(defClusterSetting);
            WebParameterUtils.buildSuccessResult(sBuilder);
        } catch (Exception e) {
            WebParameterUtils.buildFailResult(sBuilder, e.getMessage());
        }
        return sBuilder;
    }




}
