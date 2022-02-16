/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.inlong.manager.service.core.impl;

import com.google.common.collect.Sets;
import org.apache.inlong.manager.common.enums.BizConstant;
import org.apache.inlong.manager.common.enums.BizErrorCodeEnum;
import org.apache.inlong.manager.common.enums.EntityStatus;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.pojo.business.BusinessInfo;
import org.apache.inlong.manager.common.pojo.datastream.StreamBriefResponse;
import org.apache.inlong.manager.common.util.Preconditions;
import org.apache.inlong.manager.common.workflow.bussiness.NewBusinessWorkflowForm;
import org.apache.inlong.manager.service.core.BusinessService;
import org.apache.inlong.manager.service.core.DataStreamService;
import org.apache.inlong.manager.service.workflow.ProcessName;
import org.apache.inlong.manager.common.pojo.workflow.WorkflowResult;
import org.apache.inlong.manager.service.workflow.WorkflowService;
import org.apache.inlong.manager.common.workflow.bussiness.UpdateBusinessWorkflowForm;
import org.apache.inlong.manager.common.workflow.bussiness.UpdateBusinessWorkflowForm.OperateType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Set;

/**
 * Operation related to business access process
 */
@Service
public class BusinessProcessOperation {

    private static final Logger LOGGER = LoggerFactory.getLogger(BusinessProcessOperation.class);
    @Autowired
    private BusinessService businessService;
    @Autowired
    private WorkflowService workflowService;
    @Autowired
    private DataStreamService streamService;

    /**
     * Allocate resource application groups for access services and initiate an approval process
     *
     * @param groupId Business group id
     * @param operator Operator name
     * @return Process information
     */
    public WorkflowResult startProcess(String groupId, String operator) {
        LOGGER.info("begin to start approve process, groupId = {}, operator = {}", groupId, operator);
        final EntityStatus nextEntityStatus = EntityStatus.BIZ_WAIT_SUBMIT;
        BusinessInfo businessInfo = validateBusiness(groupId, EntityStatus.ALLOW_START_WORKFLOW_STATUS,
                nextEntityStatus);

        // Modify business status
        businessInfo.setStatus(nextEntityStatus.getCode());
        businessService.update(businessInfo, operator);

        // Initiate the approval process
        NewBusinessWorkflowForm form = genNewBusinessWorkflowForm(businessInfo);
        return workflowService.start(ProcessName.NEW_BUSINESS_WORKFLOW, operator, form);
    }

    /**
     * Suspend resource application group which is started up successfully, stop dataSource collecting task
     * and sort task related to application group asynchronously, persist the application state if necessary
     *
     * @param groupId
     * @param operator
     * @return Process information
     */
    public WorkflowResult suspendProcess(String groupId, String operator) {
        LOGGER.info("begin to suspend process, groupId = {}, operator = {}", groupId, operator);
        final EntityStatus nextEntityStatus = EntityStatus.BIZ_SUSPEND;
        BusinessInfo businessInfo = validateBusiness(groupId,
                Sets.newHashSet(EntityStatus.BIZ_CONFIG_SUCCESSFUL.getCode()),
                nextEntityStatus);

        businessInfo.setStatus(nextEntityStatus.getCode());
        businessService.update(businessInfo, operator);
        UpdateBusinessWorkflowForm form = genUpdateBusinessWorkflowForm(businessInfo, OperateType.SUSPEND);
        return workflowService.start(ProcessName.SUSPEND_BUSINESS_WORKFLOW, operator, form);
    }

    /**
     * Restart resource application group which is suspended successfully, starting from the last persist snapshot
     *
     * @param groupId
     * @param operator
     * @return Process information
     */
    public WorkflowResult restartProcess(String groupId, String operator) {
        LOGGER.info("begin to restart process, groupId = {}, operator = {}", groupId, operator);
        final EntityStatus nextEntityStatus = EntityStatus.BIZ_RESTART;
        BusinessInfo businessInfo = validateBusiness(groupId, Sets.newHashSet(EntityStatus.BIZ_SUSPEND.getCode()),
                nextEntityStatus);
        businessInfo.setStatus(nextEntityStatus.getCode());
        businessService.update(businessInfo, operator);
        UpdateBusinessWorkflowForm form = genUpdateBusinessWorkflowForm(businessInfo, OperateType.RESTART);
        return workflowService.start(ProcessName.RESTART_BUSINESS_WORKFLOW, operator, form);
    }

    /**
     * Delete resource application group logically and delete related resource
     *
     * @param groupId
     * @param operator
     * @return
     */
    public boolean deleteProcess(String groupId, String operator) {
        LOGGER.info("begin to delete process, groupId = {}, operator = {}", groupId, operator);
        BusinessInfo businessInfo = businessService.get(groupId);
        UpdateBusinessWorkflowForm form = genUpdateBusinessWorkflowForm(businessInfo, OperateType.DELETE);
        try {
            workflowService.start(ProcessName.DELETE_BUSINESS_WORKFLOW, operator, form);
        } catch (Exception ex) {
            LOGGER.error("exception while delete process, groupId = {}, operator = {}", groupId, operator, ex);
            throw ex;
        }
        boolean result = businessService.delete(groupId, operator);
        return result;
    }

    /**
     * Generate the form of [New Business Workflow]
     */
    public NewBusinessWorkflowForm genNewBusinessWorkflowForm(BusinessInfo businessInfo) {
        NewBusinessWorkflowForm form = new NewBusinessWorkflowForm();
        form.setBusinessInfo(businessInfo);

        // Query all data streams under the groupId and the storage information of each data stream
        List<StreamBriefResponse> infoList = streamService.getBriefList(businessInfo.getInlongGroupId());
        form.setStreamInfoList(infoList);

        return form;
    }

    private UpdateBusinessWorkflowForm genUpdateBusinessWorkflowForm(BusinessInfo businessInfo,
            OperateType operateType) {
        UpdateBusinessWorkflowForm updateBusinessWorkflowForm = new UpdateBusinessWorkflowForm();
        updateBusinessWorkflowForm.setBusinessInfo(businessInfo);
        updateBusinessWorkflowForm.setOperateType(operateType);
        return updateBusinessWorkflowForm;
    }

    private BusinessInfo validateBusiness(String groupId, Set<Integer> allowedStatus,
            EntityStatus nextBusinessStatus) {
        Preconditions.checkNotNull(groupId, BizConstant.GROUP_ID_IS_EMPTY);

        // Check whether the current status of the business allows the process to be re-initiated
        BusinessInfo businessInfo = businessService.get(groupId);
        if (businessInfo == null) {
            LOGGER.error("business not found by groupId={}", groupId);
            throw new BusinessException(BizErrorCodeEnum.BUSINESS_NOT_FOUND);
        }
        Preconditions.checkTrue(allowedStatus.contains(businessInfo.getStatus()),
                String.format("current status was not allowed to %s workflow", nextBusinessStatus.getDescription()));
        return businessInfo;
    }
}
