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

import java.util.Date;
import java.util.List;
import org.apache.inlong.manager.common.enums.BizConstant;
import org.apache.inlong.manager.common.enums.BizErrorCodeEnum;
import org.apache.inlong.manager.common.enums.EntityStatus;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.pojo.business.BusinessInfo;
import org.apache.inlong.manager.common.pojo.datastream.DataStreamSummaryInfo;
import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.dao.entity.BusinessEntity;
import org.apache.inlong.manager.dao.mapper.BusinessEntityMapper;
import org.apache.inlong.manager.service.core.DataStreamService;
import org.apache.inlong.manager.service.core.StorageService;
import org.apache.inlong.manager.service.workflow.ProcessName;
import org.apache.inlong.manager.service.workflow.WorkflowResult;
import org.apache.inlong.manager.service.workflow.WorkflowService;
import org.apache.inlong.manager.service.workflow.business.NewBusinessWorkflowForm;
import org.apache.inlong.manager.common.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * Operation related to business access process
 */
@Service
public class BusinessProcessOperation {

    private static final Logger LOGGER = LoggerFactory.getLogger(BusinessProcessOperation.class);

    @Autowired
    private BusinessEntityMapper businessMapper;
    @Autowired
    private WorkflowService workflowService;
    @Autowired
    private DataStreamService streamService;
    @Autowired
    private StorageService storageService;

    /**
     * Allocate resource application groups for access services and initiate an approval process
     *
     * @param groupId Business group id
     * @param operator Operator name
     * @return Process information
     */
    public WorkflowResult startProcess(String groupId, String operator) {
        LOGGER.info("begin to start approve process, groupId={}", groupId);
        Preconditions.checkNotNull(groupId, BizConstant.GROUP_ID_IS_EMPTY);

        // Check whether the current status of the business allows the process to be re-initiated
        BusinessEntity entity = businessMapper.selectByIdentifier(groupId);
        if (entity == null) {
            LOGGER.error("business not found by groupId={}", groupId);
            throw new BusinessException(BizErrorCodeEnum.BUSINESS_NOT_FOUND);
        }
        Preconditions.checkTrue(EntityStatus.ALLOW_START_WORKFLOW_STATUS.contains(entity.getStatus()),
                "current status was not allowed to start workflow");

        // Modify business status and other information
        entity.setModifier(operator);
        entity.setModifyTime(new Date());
        entity.setStatus(EntityStatus.BIZ_WAIT_APPROVAL.getCode());
        int success = businessMapper.updateByIdentifierSelective(entity);
        Preconditions.checkTrue(success == 1, "failed to update business during assign and start process");

        // Initiate the approval process
        BusinessInfo businessInfo = CommonBeanUtils.copyProperties(entity, BusinessInfo::new);
        NewBusinessWorkflowForm form = genNewBusinessWorkflowForm(businessInfo);
        return workflowService.start(ProcessName.NEW_BUSINESS_WORKFLOW, operator, form);
    }

    /**
     * Generate the form of [New Business Workflow]
     */
    public NewBusinessWorkflowForm genNewBusinessWorkflowForm(BusinessInfo businessInfo) {
        NewBusinessWorkflowForm form = new NewBusinessWorkflowForm();
        form.setBusinessInfo(businessInfo);

        // Query all data streams under the groupId and the storage information of each data stream
        List<DataStreamSummaryInfo> infoList = streamService.getSummaryList(businessInfo.getInlongGroupId());
        form.setStreamInfoList(infoList);

        return form;
    }

}
