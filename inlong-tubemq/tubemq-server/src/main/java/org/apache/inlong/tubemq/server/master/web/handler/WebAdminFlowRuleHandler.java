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

import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.servlet.http.HttpServletRequest;
import org.apache.inlong.tubemq.corebase.TBaseConstants;
import org.apache.inlong.tubemq.corebase.policies.FlowCtrlItem;
import org.apache.inlong.tubemq.corebase.policies.FlowCtrlRuleHandler;
import org.apache.inlong.tubemq.corebase.utils.TStringUtils;
import org.apache.inlong.tubemq.server.common.TServerConstants;
import org.apache.inlong.tubemq.server.common.fielddef.WebFieldDef;
import org.apache.inlong.tubemq.server.common.utils.ProcessResult;
import org.apache.inlong.tubemq.server.common.utils.WebParameterUtils;
import org.apache.inlong.tubemq.server.master.TMaster;
import org.apache.inlong.tubemq.server.master.bdbstore.bdbentitys.BdbGroupFlowCtrlEntity;



public class WebAdminFlowRuleHandler extends AbstractWebHandler {

    private static final String blankFlowCtrlRules = "[]";
    private static final List<Integer> allowedPriorityVal = Arrays.asList(1, 2, 3);
    private static final Set<String> rsvGroupNameSet =
            new HashSet<>(Arrays.asList(TServerConstants.TOKEN_DEFAULT_FLOW_CONTROL));


    public WebAdminFlowRuleHandler(TMaster master) {
        super(master);
    }

    @Override
    public void registerWebApiMethod() {
        // register query method
        registerQueryWebMethod("admin_query_def_flow_control_rule",
                "adminQueryDefGroupFlowCtrlRule");
        registerQueryWebMethod("admin_query_group_flow_control_rule",
                "adminQuerySpecGroupFlowCtrlRule");
        // register modify method
        registerModifyWebMethod("admin_set_def_flow_control_rule",
                "adminSetDefGroupFlowCtrlRule");
        registerModifyWebMethod("admin_set_group_flow_control_rule",
                "adminSetSpecGroupFlowCtrlRule");
        registerModifyWebMethod("admin_rmv_def_flow_control_rule",
                "adminDelDefGroupFlowCtrlRuleStatus");
        registerModifyWebMethod("admin_rmv_group_flow_control_rule",
                "adminDelSpecGroupFlowCtrlRuleStatus");
        registerModifyWebMethod("admin_upd_def_flow_control_rule",
                "adminModDefGroupFlowCtrlRuleStatus");
        registerModifyWebMethod("admin_upd_group_flow_control_rule",
                "adminModSpecGroupFlowCtrlRuleStatus");
    }

    public StringBuilder adminQueryDefGroupFlowCtrlRule(HttpServletRequest req) {
        return innQueryGroupFlowCtrlRule(req, true);
    }

    public StringBuilder adminQuerySpecGroupFlowCtrlRule(HttpServletRequest req) {
        return innQueryGroupFlowCtrlRule(req, false);
    }

    public StringBuilder adminSetDefGroupFlowCtrlRule(HttpServletRequest req) {
        return innSetFlowControlRule(req, true);
    }

    public StringBuilder adminSetSpecGroupFlowCtrlRule(HttpServletRequest req) {
        return innSetFlowControlRule(req, false);
    }

    public StringBuilder adminDelDefGroupFlowCtrlRuleStatus(HttpServletRequest req) {
        return innDelGroupFlowCtrlRuleStatus(req, true);
    }

    public StringBuilder adminDelSpecGroupFlowCtrlRuleStatus(HttpServletRequest req) {
        return innDelGroupFlowCtrlRuleStatus(req, false);
    }

    public StringBuilder adminModDefGroupFlowCtrlRuleStatus(HttpServletRequest req) {
        return innModGroupFlowCtrlRuleStatus(req, true);
    }

    public StringBuilder adminModSpecGroupFlowCtrlRuleStatus(HttpServletRequest req) {
        return innModGroupFlowCtrlRuleStatus(req, false);
    }

    /**
     * add flow control rule
     *
     * @param req
     * @param do4DefFlowCtrl
     * @return
     */
    private StringBuilder innSetFlowControlRule(HttpServletRequest req,
                                                boolean do4DefFlowCtrl) {
        ProcessResult result = new ProcessResult();
        StringBuilder sBuilder = new StringBuilder(512);
        // valid operation authorize info
        if (!WebParameterUtils.validReqAuthorizeInfo(req,
                WebFieldDef.ADMINAUTHTOKEN, true, master, result)) {
            WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
            return sBuilder;
        }
        // get createUser info
        if (!WebParameterUtils.getStringParamValue(req,
                WebFieldDef.CREATEUSER, true, null, result)) {
            WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
            return sBuilder;
        }
        String createUser = (String) result.retData1;
        // check and get create date
        if (!WebParameterUtils.getDateParameter(req,
                WebFieldDef.CREATEDATE, false, new Date(), result)) {
            WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
            return sBuilder;
        }
        Date createDate = (Date) result.retData1;
        // get rule required status info
        if (!WebParameterUtils.getIntParamValue(req,
                WebFieldDef.STATUSID, false, 0, 0, result)) {
            WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
            return sBuilder;
        }
        int statusId = (int) result.retData1;
        // get and valid priority info
        if (!getQryPriorityIdWithCheck(req, false, 301, 101, result)) {
            WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
            return sBuilder;
        }
        int qryPriorityId = (int) result.retData1;
        // get group name info
        if (!getGroupNameWithCheck(req, true, do4DefFlowCtrl, result)) {
            WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
            return sBuilder;
        }
        Set<String> batchGroupNames = (Set<String>) result.retData1;
        // get and flow control rule info
        int ruleCnt = getAndCheckFlowRules(req, blankFlowCtrlRules, result);
        if (!result.success) {
            WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
            return sBuilder;
        }
        String flowCtrlInfo = (String) result.retData1;
        try {
            // add flow control to bdb
            for (String groupName : batchGroupNames) {
                if (groupName.equals(TServerConstants.TOKEN_DEFAULT_FLOW_CONTROL)) {
                    brokerConfManager.confAddBdbGroupFlowCtrl(
                            new BdbGroupFlowCtrlEntity(flowCtrlInfo,
                                    statusId, ruleCnt, qryPriorityId, "",
                                    false, createUser, createDate));
                } else {
                    brokerConfManager.confAddBdbGroupFlowCtrl(
                            new BdbGroupFlowCtrlEntity(groupName,
                                    flowCtrlInfo, statusId, ruleCnt, qryPriorityId, "",
                                    false, createUser, createDate));
                }
            }
            WebParameterUtils.buildSuccessResult(sBuilder);
        } catch (Exception e) {
            WebParameterUtils.buildFailResult(sBuilder, e.getMessage());
        }
        return sBuilder;
    }

    /**
     * delete flow control rule
     *
     * @param req
     * @param do4DefFlowCtrl
     * @return
     */
    private StringBuilder innDelGroupFlowCtrlRuleStatus(HttpServletRequest req,
                                                        boolean do4DefFlowCtrl) {
        ProcessResult result = new ProcessResult();
        StringBuilder sBuilder = new StringBuilder(512);
        // valid operation authorize info
        if (!WebParameterUtils.validReqAuthorizeInfo(req,
                WebFieldDef.ADMINAUTHTOKEN, true, master, result)) {
            WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
            return sBuilder;
        }
        // get modifyUser info
        if (!WebParameterUtils.getStringParamValue(req,
                WebFieldDef.CREATEUSER, true, null, result)) {
            WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
            return sBuilder;
        }
        String modifyUser = (String) result.retData1;
        // check and get modifyDate date
        if (!WebParameterUtils.getDateParameter(req,
                WebFieldDef.CREATEDATE, false, new Date(), result)) {
            WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
            return sBuilder;
        }
        Date modifyDate = (Date) result.retData1;
        // get group name info
        if (!getGroupNameWithCheck(req, true, do4DefFlowCtrl, result)) {
            WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
            return sBuilder;
        }
        Set<String> batchGroupNames = (Set<String>) result.retData1;
        try {
            brokerConfManager.confDeleteBdbGroupFlowCtrl(batchGroupNames);
            WebParameterUtils.buildSuccessResult(sBuilder);
        } catch (Exception e) {
            WebParameterUtils.buildFailResult(sBuilder, e.getMessage());
        }
        return sBuilder;
    }

    /**
     * modify flow control rule
     *
     * @param req
     * @param do4DefFlowCtrl
     * @return
     */
    private StringBuilder innModGroupFlowCtrlRuleStatus(HttpServletRequest req,
                                                        boolean do4DefFlowCtrl) {
        // #lizard forgives
        ProcessResult result = new ProcessResult();
        StringBuilder sBuilder = new StringBuilder(512);
        // valid operation authorize info
        if (!WebParameterUtils.validReqAuthorizeInfo(req,
                WebFieldDef.ADMINAUTHTOKEN, true, master, result)) {
            WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
            return sBuilder;
        }
        // get modifyUser info
        if (!WebParameterUtils.getStringParamValue(req,
                WebFieldDef.CREATEUSER, true, null, result)) {
            WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
            return sBuilder;
        }
        String modifyUser = (String) result.retData1;
        // check and get modifyDate date
        if (!WebParameterUtils.getDateParameter(req,
                WebFieldDef.CREATEDATE, false, new Date(), result)) {
            WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
            return sBuilder;
        }
        Date modifyDate = (Date) result.retData1;
        // get group name info
        if (!getGroupNameWithCheck(req, true, do4DefFlowCtrl, result)) {
            WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
            return sBuilder;
        }
        Set<String> batchGroupNames = (Set<String>) result.retData1;
        // get rule required status info
        if (!WebParameterUtils.getIntParamValue(req,
                WebFieldDef.STATUSID, false,
                TBaseConstants.META_VALUE_UNDEFINED, 0, result)) {
            WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
            return sBuilder;
        }
        int statusId = (int) result.retData1;
        // get and flow control rule info
        int ruleCnt = getAndCheckFlowRules(req, null, result);
        if (!result.success) {
            WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
            return sBuilder;
        }
        String newFlowCtrlInfo = (String) result.retData1;
        // get and valid priority info
        if (!getQryPriorityIdWithCheck(req, false,
                TBaseConstants.META_VALUE_UNDEFINED, 101, result)) {
            WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
            return sBuilder;
        }
        int qryPriorityId = (int) result.retData1;
        try {
            boolean foundChange;
            for (String groupName : batchGroupNames) {
                // check if record changed
                BdbGroupFlowCtrlEntity oldEntity =
                        brokerConfManager.getBdbGroupFlowCtrl(groupName);
                if (oldEntity != null) {
                    foundChange = false;
                    BdbGroupFlowCtrlEntity newGroupFlowCtrlEntity =
                            new BdbGroupFlowCtrlEntity(oldEntity.getGroupName(),
                                    oldEntity.getFlowCtrlInfo(), oldEntity.getStatusId(),
                                    oldEntity.getRuleCnt(), oldEntity.getAttributes(),
                                    oldEntity.getSsdTranslateId(), oldEntity.isNeedSSDProc(),
                                    oldEntity.getCreateUser(), oldEntity.getCreateDate());
                    if (statusId != TBaseConstants.META_VALUE_UNDEFINED
                            && statusId != oldEntity.getStatusId()) {
                        foundChange = true;
                        newGroupFlowCtrlEntity.setStatusId(statusId);
                    }
                    if (qryPriorityId != TBaseConstants.META_VALUE_UNDEFINED
                            && qryPriorityId != oldEntity.getQryPriorityId()) {
                        foundChange = true;
                        newGroupFlowCtrlEntity.setQryPriorityId(qryPriorityId);
                    }
                    if (TStringUtils.isNotBlank(newFlowCtrlInfo)
                            && !newFlowCtrlInfo.equals(oldEntity.getFlowCtrlInfo())) {
                        foundChange = true;
                        newGroupFlowCtrlEntity.setFlowCtrlInfo(ruleCnt, newFlowCtrlInfo);
                    }
                    // update record if found change
                    if (foundChange) {
                        try {
                            newGroupFlowCtrlEntity.setModifyInfo(modifyUser, modifyDate);
                            brokerConfManager.confUpdateBdbGroupFlowCtrl(newGroupFlowCtrlEntity);
                        } catch (Throwable ee) {
                            //
                        }
                    }
                }
            }
            WebParameterUtils.buildSuccessResult(sBuilder);
        } catch (Exception e) {
            WebParameterUtils.buildFailResult(sBuilder, e.getMessage());
        }
        return sBuilder;
    }

    /**
     * query flow control rule
     *
     * @param req
     * @param do4DefFlowCtrl
     * @return
     */
    private StringBuilder innQueryGroupFlowCtrlRule(HttpServletRequest req,
                                                    boolean do4DefFlowCtrl) {
        ProcessResult result = new ProcessResult();
        BdbGroupFlowCtrlEntity bdbGroupFlowCtrlEntity = new BdbGroupFlowCtrlEntity();
        // get modifyUser info
        WebParameterUtils.getStringParamValue(req,
                WebFieldDef.CREATEUSER, false, null, result);
        bdbGroupFlowCtrlEntity.setCreateUser((String) result.retData1);
        // get status id info
        WebParameterUtils.getIntParamValue(req, WebFieldDef.STATUSID, false,
                TBaseConstants.META_VALUE_UNDEFINED, 0, result);
        bdbGroupFlowCtrlEntity.setStatusId((int) result.retData1);
        // get and valid priority info
        getQryPriorityIdWithCheck(req, false,
                TBaseConstants.META_VALUE_UNDEFINED, 101, result);
        bdbGroupFlowCtrlEntity.setQryPriorityId((int) result.retData1);
        getGroupNameWithCheck(req, false, do4DefFlowCtrl, result);
        Set<String> batchGroupNames = (Set<String>) result.retData1;
        // query group flow ctrl infos
        List<BdbGroupFlowCtrlEntity> webGroupFlowCtrlEntities =
                brokerConfManager.confGetBdbGroupFlowCtrl(bdbGroupFlowCtrlEntity);
        int totalCnt = 0;
        boolean found = false;
        StringBuilder sBuilder = new StringBuilder(512);
        WebParameterUtils.buildSuccessWithDataRetBegin(sBuilder);
        if (do4DefFlowCtrl) {
            for (BdbGroupFlowCtrlEntity entity : webGroupFlowCtrlEntities) {
                if (entity.getGroupName().equals(
                        TServerConstants.TOKEN_DEFAULT_FLOW_CONTROL)) {
                    if (totalCnt++ > 0) {
                        sBuilder.append(",");
                    }
                    sBuilder = entity.toJsonString(sBuilder);
                    break;
                }
            }
        } else {
            for (BdbGroupFlowCtrlEntity entity : webGroupFlowCtrlEntities) {
                if (entity.getGroupName().equals(
                        TServerConstants.TOKEN_DEFAULT_FLOW_CONTROL)) {
                    continue;
                }
                found = false;
                if (batchGroupNames.isEmpty()) {
                    found = true;
                } else {
                    for (String tmpGroupName : batchGroupNames) {
                        if (entity.getGroupName().equals(tmpGroupName)) {
                            found = true;
                            break;
                        }
                    }
                }
                if (found) {
                    if (totalCnt++ > 0) {
                        sBuilder.append(",");
                    }
                    sBuilder = entity.toJsonString(sBuilder);
                }
            }
        }
        WebParameterUtils.buildSuccessWithDataRetEnd(sBuilder, totalCnt);
        return sBuilder;
    }

    // translate rule info to json format string
    private int getAndCheckFlowRules(HttpServletRequest req,
                                     String defValue,
                                     ProcessResult result) {
        int ruleCnt = 0;
        StringBuilder strBuffer = new StringBuilder(512);
        // get parameter value
        String paramValue = req.getParameter(WebFieldDef.FLOWCTRLSET.name);
        if (paramValue == null) {
            paramValue = req.getParameter(WebFieldDef.FLOWCTRLSET.shortName);
        }
        if (TStringUtils.isBlank(paramValue)) {
            result.setSuccResult(defValue);
            return ruleCnt;
        }
        strBuffer.append("[");
        paramValue = paramValue.trim();
        List<Integer> ruleTypes = Arrays.asList(0, 1, 2, 3);
        FlowCtrlRuleHandler flowCtrlRuleHandler =
                new FlowCtrlRuleHandler(true);
        Map<Integer, List<FlowCtrlItem>> flowCtrlItemMap;
        try {
            flowCtrlItemMap =
                    flowCtrlRuleHandler.parseFlowCtrlInfo(paramValue);
        } catch (Throwable e) {
            result.setFailResult(new StringBuilder(512)
                    .append("Parse parameter ").append(WebFieldDef.FLOWCTRLSET.name)
                    .append(" failure: '").append(e.toString()).toString());
            return 0;
        }
        for (Integer typeId : ruleTypes) {
            if (typeId != null) {
                int rules = 0;
                List<FlowCtrlItem> flowCtrlItems = flowCtrlItemMap.get(typeId);
                if (flowCtrlItems != null) {
                    if (ruleCnt++ > 0) {
                        strBuffer.append(",");
                    }
                    strBuffer.append("{\"type\":").append(typeId.intValue()).append(",\"rule\":[");
                    for (FlowCtrlItem flowCtrlItem : flowCtrlItems) {
                        if (flowCtrlItem != null) {
                            if (rules++ > 0) {
                                strBuffer.append(",");
                            }
                            strBuffer = flowCtrlItem.toJsonString(strBuffer);
                        }
                    }
                    strBuffer.append("]}");
                }
            }
        }
        strBuffer.append("]");
        result.setSuccResult(strBuffer.toString());
        return ruleCnt;
    }

    private boolean getGroupNameWithCheck(HttpServletRequest req, boolean required,
                                          boolean do4DefFlowCtrl, ProcessResult result) {
        if (do4DefFlowCtrl) {
            result.setSuccResult(rsvGroupNameSet);
            return true;
        }
        // get group list
        if (!WebParameterUtils.getStringParamValue(req,
                WebFieldDef.COMPSGROUPNAME, required, null, result)) {
            return result.success;
        }
        Set<String> inGroupSet = (Set<String>) result.retData1;
        for (String rsvGroup : rsvGroupNameSet) {
            if (inGroupSet.contains(rsvGroup)) {
                result.setFailResult(new StringBuilder(512)
                        .append("Illegal value in ").append(WebFieldDef.COMPSGROUPNAME.name)
                        .append(" parameter: '").append(rsvGroup)
                        .append("' is a system reserved value!").toString());
                return false;
            }
        }
        return true;
    }

    private boolean getQryPriorityIdWithCheck(HttpServletRequest req, boolean required,
                                              int defValue, int minValue,
                                              ProcessResult result) {
        if (!WebParameterUtils.getIntParamValue(req,
                WebFieldDef.QRYPRIORITYID, required,
                defValue, minValue, result)) {
            return result.success;
        }
        int qryPriorityId = (int) result.retData1;
        if (qryPriorityId == defValue) {
            return result.success;
        }
        if (qryPriorityId > 303 || qryPriorityId < 101) {
            result.setFailResult(new StringBuilder(512)
                    .append("Illegal value in ").append(WebFieldDef.QRYPRIORITYID.name)
                    .append(" parameter: ").append(WebFieldDef.QRYPRIORITYID.name)
                    .append(" value must be greater than or equal")
                    .append(" to 101 and less than or equal to 303!").toString());
            return false;
        }
        if (!allowedPriorityVal.contains(qryPriorityId % 100)) {
            result.setFailResult(new StringBuilder(512)
                    .append("Illegal value in ").append(WebFieldDef.QRYPRIORITYID.name)
                    .append(" parameter: the units of ").append(WebFieldDef.QRYPRIORITYID.name)
                    .append(" must in ").append(allowedPriorityVal).toString());
            return false;
        }
        if (!allowedPriorityVal.contains(qryPriorityId / 100)) {
            result.setFailResult(new StringBuilder(512)
                    .append("Illegal value in ").append(WebFieldDef.QRYPRIORITYID.name)
                    .append(" parameter: the hundreds of ").append(WebFieldDef.QRYPRIORITYID.name)
                    .append(" must in ").append(allowedPriorityVal).toString());
            return false;
        }
        return true;
    }

}
