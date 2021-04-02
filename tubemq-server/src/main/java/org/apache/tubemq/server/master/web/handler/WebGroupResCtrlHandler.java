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

import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import javax.servlet.http.HttpServletRequest;
import org.apache.tubemq.corebase.TBaseConstants;
import org.apache.tubemq.corebase.utils.Tuple3;
import org.apache.tubemq.server.common.TServerConstants;
import org.apache.tubemq.server.common.fielddef.WebFieldDef;
import org.apache.tubemq.server.common.utils.ProcessResult;
import org.apache.tubemq.server.common.utils.WebParameterUtils;
import org.apache.tubemq.server.master.TMaster;
import org.apache.tubemq.server.master.metamanage.DataOpErrCode;
import org.apache.tubemq.server.master.metamanage.metastore.dao.entity.GroupResCtrlEntity;


public class WebGroupResCtrlHandler extends AbstractWebHandler {

    private static final Set<String> rsvGroupNameSet =
            new HashSet<>(Arrays.asList(TServerConstants.TOKEN_DEFAULT_FLOW_CONTROL));


    public WebGroupResCtrlHandler(TMaster master) {
        super(master);
    }

    @Override
    public void registerWebApiMethod() {
        // register query method
        registerQueryWebMethod("admin_query_group_resctrl_info",
                "adminQueryGroupResCtrlConf");
        // register modify method
        registerModifyWebMethod("admin_add_group_resctrl_info",
                "adminAddGroupResCtrlConf");
        registerModifyWebMethod("admin_upd_group_resctrl_info",
                "adminModGroupResCtrlConf");
        registerModifyWebMethod("admin_rmv_group_resctrl_info",
                "adminDelGroupResCtrlConf");

        // register query method
        registerQueryWebMethod("admin_query_def_flow_control_rule",
                "adminBlankProcessFun");
        registerQueryWebMethod("admin_query_group_flow_control_rule",
                "adminBlankProcessFun");
        // register modify method
        registerModifyWebMethod("admin_set_def_flow_control_rule",
                "adminBlankProcessFun");
        registerModifyWebMethod("admin_set_group_flow_control_rule",
                "adminBlankProcessFun");
        registerModifyWebMethod("admin_rmv_def_flow_control_rule",
                "adminBlankProcessFun");
        registerModifyWebMethod("admin_rmv_group_flow_control_rule",
                "adminBlankProcessFun");
        registerModifyWebMethod("admin_upd_def_flow_control_rule",
                "adminBlankProcessFun");
        registerModifyWebMethod("admin_upd_group_flow_control_rule",
                "adminBlankProcessFun");
    }

    /**
     * query group resource control info
     *
     * @param req
     * @return
     */
    public StringBuilder adminQueryGroupResCtrlConf(HttpServletRequest req) {
        ProcessResult result = new ProcessResult();
        StringBuilder sBuilder = new StringBuilder(512);
        // get createUser query key
        if (!WebParameterUtils.getStringParamValue(req,
                WebFieldDef.CREATEUSER, false, null, result)) {
            WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
            return sBuilder;
        }
        String inCreateUser = (String) result.getRetData();
        // get modifyUser query key
        if (!WebParameterUtils.getStringParamValue(req,
                WebFieldDef.MODIFYUSER, false, null, result)) {
            WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
            return sBuilder;
        }
        String inModifyUser = (String) result.getRetData();
        // get group list
        if (!WebParameterUtils.getStringParamValue(req,
                WebFieldDef.COMPSGROUPNAME, false, null, result)) {
            WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
            return sBuilder;
        }
        Set<String> inGroupSet = (Set<String>) result.retData1;
        // get resCheckStatus info
        if (!WebParameterUtils.getBooleanParamValue(req,
                WebFieldDef.RESCHECKENABLE, false, null, result)) {
            WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
            return sBuilder;
        }
        Boolean resCheckEnable = (Boolean) result.retData1;
        // get and valid qryPriorityId info
        if (!WebParameterUtils.getQryPriorityIdParameter(req,
                false, TBaseConstants.META_VALUE_UNDEFINED,
                TServerConstants.QRY_PRIORITY_MIN_VALUE, result)) {
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
        // build query entity
        GroupResCtrlEntity entity = new GroupResCtrlEntity();
        entity.updBaseModifyInfo(TBaseConstants.META_VALUE_UNDEFINED,
                inCreateUser, null, inModifyUser, null, null);
        entity.updModifyInfo(resCheckEnable, TBaseConstants.META_VALUE_UNDEFINED,
                inQryPriorityId, flowCtrlEnable, TBaseConstants.META_VALUE_UNDEFINED, null);
        Map<String, GroupResCtrlEntity> groupResCtrlEntityMap =
                metaDataManager.confGetGroupResCtrlConf(entity);
        // Fill in the key to be queried
        if (inGroupSet.isEmpty()) {
            inGroupSet.addAll(groupResCtrlEntityMap.keySet());
        }
        // build return result
        int totalCnt = 0;
        WebParameterUtils.buildSuccessWithDataRetBegin(sBuilder);
        for (String groupName : inGroupSet) {
            entity = groupResCtrlEntityMap.get(groupName);
            if (entity == null) {
                continue;
            }
            if (totalCnt++ > 0) {
                sBuilder.append(",");
            }
            sBuilder = entity.toWebJsonStr(sBuilder, true);
        }
        WebParameterUtils.buildSuccessWithDataRetEnd(sBuilder, totalCnt);
        return sBuilder;
    }

    /**
     * add group resource control info
     *
     * @param req
     * @return
     */
    private StringBuilder adminAddGroupResCtrlConf(HttpServletRequest req) {
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
        // get group list
        if (!WebParameterUtils.getStringParamValue(req,
                WebFieldDef.COMPSGROUPNAME, true, null, result)) {
            WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
            return sBuilder;
        }
        Set<String> batchGroupNames = (Set<String>) result.retData1;
        // get resCheckStatus info
        if (!WebParameterUtils.getBooleanParamValue(req,
                WebFieldDef.RESCHECKENABLE, false, false, result)) {
            WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
            return sBuilder;
        }
        Boolean resCheckEnable = (Boolean) result.retData1;
        // get and valid allowedBrokerClientRate info
        if (!WebParameterUtils.getQryPriorityIdParameter(req,
                false, TServerConstants.GROUP_BROKER_CLIENT_RATE_MIN,
                TServerConstants.GROUP_BROKER_CLIENT_RATE_MIN, result)) {
            WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
            return sBuilder;
        }
        int allowedBClientRate = (int) result.retData1;
        // get and valid qryPriorityId info
        if (!WebParameterUtils.getQryPriorityIdParameter(req,
                false, TServerConstants.QRY_PRIORITY_DEF_VALUE,
                TServerConstants.QRY_PRIORITY_MIN_VALUE, result)) {
            WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
            return sBuilder;
        }
        int qryPriorityId = (int) result.retData1;
        // get flowCtrlEnable info
        if (!WebParameterUtils.getBooleanParamValue(req,
                WebFieldDef.FLOWCTRLENABLE, false, false, result)) {
            WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
            return sBuilder;
        }
        Boolean flowCtrlEnable = (Boolean) result.retData1;
        // get and flow control rule info
        int flowRuleCnt = WebParameterUtils.getAndCheckFlowRules(req,
                TServerConstants.BLANK_FLOWCTRL_RULES, result);
        if (!result.success) {
            WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
            return sBuilder;
        }
        String flowCtrlInfo = (String) result.retData1;
        // add group resource record
        Map<String, Tuple3<Boolean, Integer, String>> retInfo =
                new HashMap<>(batchGroupNames.size());
        for (String groupName : batchGroupNames) {
            GroupResCtrlEntity entity =
                    new GroupResCtrlEntity(opTupleInfo.getF0(),
                            opTupleInfo.getF1(), opTupleInfo.getF2());
            entity.setGroupName(groupName);
            entity.updModifyInfo(resCheckEnable, allowedBClientRate,
                    qryPriorityId, flowCtrlEnable, flowRuleCnt, flowCtrlInfo);
            metaDataManager.confAddGroupResCtrlConf(entity, sBuilder, result);
            retInfo.put(entity.getGroupName(), new Tuple3<>(
                    result.isSuccess(), result.getErrCode(), result.getErrInfo()));
        }
        return buildRetInfo(retInfo, sBuilder);
    }

    /**
     * update group resource control info
     *
     * @param req
     * @return
     */
    private StringBuilder adminModGroupResCtrlConf(HttpServletRequest req) {
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
        // get group list
        if (!WebParameterUtils.getStringParamValue(req,
                WebFieldDef.COMPSGROUPNAME, true, null, result)) {
            WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
            return sBuilder;
        }
        Set<String> batchGroupNames = (Set<String>) result.retData1;
        // get resCheckStatus info
        if (!WebParameterUtils.getBooleanParamValue(req,
                WebFieldDef.RESCHECKENABLE, false, null, result)) {
            WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
            return sBuilder;
        }
        Boolean resCheckEnable = (Boolean) result.retData1;
        // get and valid allowedBrokerClientRate info
        if (!WebParameterUtils.getQryPriorityIdParameter(req,
                false, TBaseConstants.META_VALUE_UNDEFINED,
                TServerConstants.GROUP_BROKER_CLIENT_RATE_MIN, result)) {
            WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
            return sBuilder;
        }
        int allowedBClientRate = (int) result.retData1;
        // get and valid qryPriorityId info
        if (!WebParameterUtils.getQryPriorityIdParameter(req,
                false, TBaseConstants.META_VALUE_UNDEFINED,
                TServerConstants.QRY_PRIORITY_MIN_VALUE, result)) {
            WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
            return sBuilder;
        }
        int qryPriorityId = (int) result.retData1;
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
        // modify group resource record
        GroupResCtrlEntity curConf;
        GroupResCtrlEntity newConf;
        Map<String, Tuple3<Boolean, Integer, String>> retInfo =
                new HashMap<>(batchGroupNames.size());
        for (String groupName : batchGroupNames) {
            GroupResCtrlEntity newEntity = null;
            curConf = metaDataManager.confGetGroupResCtrlConf(groupName);
            if (curConf == null) {
                retInfo.put(groupName, new Tuple3<>(false,
                        DataOpErrCode.DERR_NOT_EXIST.getCode(),
                        DataOpErrCode.DERR_NOT_EXIST.getDescription()));
                continue;
            }
            newConf = curConf.clone();
            newConf.setModifyInfo(opTupleInfo.getF0(),
                    opTupleInfo.getF1(), opTupleInfo.getF2());
            newConf.updModifyInfo(resCheckEnable, allowedBClientRate,
                    qryPriorityId, flowCtrlEnable, flowRuleCnt, flowCtrlInfo);
            metaDataManager.confAddGroupResCtrlConf(newConf, sBuilder, result);
            retInfo.put(newConf.getGroupName(), new Tuple3<>(
                    result.isSuccess(), result.getErrCode(), result.getErrInfo()));
        }
        return buildRetInfo(retInfo, sBuilder);
    }

    /**
     * delete group resource control rule
     *
     * @param req
     * @return
     */
    private StringBuilder adminDelGroupResCtrlConf(HttpServletRequest req) {
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
        // get group list
        if (!WebParameterUtils.getStringParamValue(req,
                WebFieldDef.COMPSGROUPNAME, true, null, result)) {
            WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
            return sBuilder;
        }
        Set<String> batchGroupNames = (Set<String>) result.retData1;
        metaDataManager.confDelGroupResCtrlConf(
                opTupleInfo.getF1(), batchGroupNames, sBuilder, result);
        Map<String, Tuple3<Boolean, Integer, String>> retInfo =
                (Map<String, Tuple3<Boolean, Integer, String>>) result.retData1;
        return buildRetInfo(retInfo, sBuilder);
    }

    private StringBuilder buildRetInfo(Map<String, Tuple3<Boolean, Integer, String>> retInfo,
                                       StringBuilder sBuilder) {
        int totalCnt = 0;
        WebParameterUtils.buildSuccessWithDataRetBegin(sBuilder);
        for (Map.Entry<String, Tuple3<Boolean, Integer, String>> entry : retInfo.entrySet()) {
            if (totalCnt++ > 0) {
                sBuilder.append(",");
            }
            sBuilder.append("{\"groupName\":\"").append(entry.getKey()).append("\"")
                    .append(",\"success\":").append(entry.getValue().getF0())
                    .append(",\"errCode\":").append(entry.getValue().getF1())
                    .append(",\"errInfo\":\"").append(entry.getValue().getF2()).append("\"}");
        }
        WebParameterUtils.buildSuccessWithDataRetEnd(sBuilder, totalCnt);
        return sBuilder;
    }

    /**
     * blank process function
     *
     * @param req
     * @return
     */
    public StringBuilder adminBlankProcessFun(HttpServletRequest req) {
        ProcessResult result = new ProcessResult();
        StringBuilder sBuilder = new StringBuilder(512);
        WebParameterUtils.buildFailResult(sBuilder,
                "Expired method, please check the latest api documentation!");
        return sBuilder;
    }

}
