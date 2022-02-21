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
import org.apache.inlong.manager.common.enums.Constant;
import org.apache.inlong.manager.common.enums.EntityStatus;
import org.apache.inlong.manager.common.enums.ErrorCodeEnum;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.pojo.group.InlongGroupRequest;
import org.apache.inlong.manager.common.pojo.stream.StreamBriefResponse;
import org.apache.inlong.manager.common.pojo.workflow.WorkflowResult;
import org.apache.inlong.manager.common.pojo.workflow.form.NewGroupProcessForm;
import org.apache.inlong.manager.common.pojo.workflow.form.UpdateGroupProcessForm;
import org.apache.inlong.manager.common.pojo.workflow.form.UpdateGroupProcessForm.OperateType;
import org.apache.inlong.manager.common.util.Preconditions;
import org.apache.inlong.manager.service.core.InlongGroupService;
import org.apache.inlong.manager.service.core.InlongStreamService;
import org.apache.inlong.manager.service.workflow.ProcessName;
import org.apache.inlong.manager.service.workflow.WorkflowService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Set;

/**
 * Operation related to inlong group process
 */
@Service
public class InlongGroupProcessOperation {

    private static final Logger LOGGER = LoggerFactory.getLogger(InlongGroupProcessOperation.class);
    @Autowired
    private InlongGroupService groupService;
    @Autowired
    private WorkflowService workflowService;
    @Autowired
    private InlongStreamService streamService;

    /**
     * Allocate resource application groups for access services and initiate an approval process
     *
     * @param groupId Inlong group id
     * @param operator Operator name
     * @return WorkflowProcess information
     */
    public WorkflowResult startProcess(String groupId, String operator) {
        LOGGER.info("begin to start approve process, groupId = {}, operator = {}", groupId, operator);
        final EntityStatus nextStatus = EntityStatus.GROUP_WAIT_APPROVAL;
        InlongGroupRequest groupInfo = validateGroup(groupId, EntityStatus.ALLOW_START_WORKFLOW_STATUS, nextStatus);

        // Modify inlong group status
        groupInfo.setStatus(nextStatus.getCode());
        groupService.update(groupInfo, operator);

        // Initiate the approval process
        NewGroupProcessForm form = genNewGroupProcessForm(groupInfo);
        return workflowService.start(ProcessName.NEW_GROUP_PROCESS, operator, form);
    }

    /**
     * Suspend resource application group which is started up successfully, stop dataSource collecting task
     * and sort task related to application group asynchronously, persist the application status if necessary
     *
     * @return WorkflowProcess information
     */
    public WorkflowResult suspendProcess(String groupId, String operator) {
        LOGGER.info("begin to suspend process, groupId = {}, operator = {}", groupId, operator);
        final EntityStatus nextEntityStatus = EntityStatus.GROUP_SUSPEND;
        InlongGroupRequest groupInfo = validateGroup(groupId,
                Sets.newHashSet(EntityStatus.GROUP_CONFIG_SUCCESSFUL.getCode()),
                nextEntityStatus);

        groupInfo.setStatus(nextEntityStatus.getCode());
        groupService.update(groupInfo, operator);
        UpdateGroupProcessForm form = genUpdateGroupProcessForm(groupInfo, OperateType.SUSPEND);
        return workflowService.start(ProcessName.SUSPEND_GROUP_PROCESS, operator, form);
    }

    /**
     * Restart resource application group which is suspended successfully, starting from the last persist snapshot
     *
     * @return WorkflowProcess information
     */
    public WorkflowResult restartProcess(String groupId, String operator) {
        LOGGER.info("begin to restart process, groupId = {}, operator = {}", groupId, operator);
        EntityStatus nextStatus = EntityStatus.GROUP_RESTART;
        InlongGroupRequest groupInfo = validateGroup(groupId, Sets.newHashSet(EntityStatus.GROUP_SUSPEND.getCode()),
                nextStatus);
        groupInfo.setStatus(nextStatus.getCode());
        groupService.update(groupInfo, operator);
        UpdateGroupProcessForm form = genUpdateGroupProcessForm(groupInfo, OperateType.RESTART);
        return workflowService.start(ProcessName.RESTART_GROUP_PROCESS, operator, form);
    }

    /**
     * Delete resource application group logically and delete related resource
     */
    public boolean deleteProcess(String groupId, String operator) {
        LOGGER.info("begin to delete process, groupId = {}, operator = {}", groupId, operator);
        InlongGroupRequest groupInfo = groupService.get(groupId);
        UpdateGroupProcessForm form = genUpdateGroupProcessForm(groupInfo, OperateType.DELETE);
        try {
            workflowService.start(ProcessName.DELETE_GROUP_PROCESS, operator, form);
        } catch (Exception ex) {
            LOGGER.error("exception while delete process, groupId = {}, operator = {}", groupId, operator, ex);
            throw ex;
        }
        return groupService.delete(groupId, operator);
    }

    /**
     * Generate the form of [New Group Workflow]
     */
    public NewGroupProcessForm genNewGroupProcessForm(InlongGroupRequest groupInfo) {
        NewGroupProcessForm form = new NewGroupProcessForm();
        form.setGroupInfo(groupInfo);
        // Query all inlong streams under the groupId and the sink information of each inlong stream
        List<StreamBriefResponse> infoList = streamService.getBriefList(groupInfo.getInlongGroupId());
        form.setStreamInfoList(infoList);
        return form;
    }

    private UpdateGroupProcessForm genUpdateGroupProcessForm(InlongGroupRequest groupInfo,
            OperateType operateType) {
        UpdateGroupProcessForm updateForm = new UpdateGroupProcessForm();
        updateForm.setGroupInfo(groupInfo);
        updateForm.setOperateType(operateType);
        return updateForm;
    }

    private InlongGroupRequest validateGroup(String groupId, Set<Integer> allowedStatus, EntityStatus nextStatus) {
        Preconditions.checkNotNull(groupId, Constant.GROUP_ID_IS_EMPTY);

        // Check whether the current status of the inlong group allows the process to be re-initiated
        InlongGroupRequest groupInfo = groupService.get(groupId);
        if (groupInfo == null) {
            LOGGER.error("inlong group not found by groupId={}", groupId);
            throw new BusinessException(ErrorCodeEnum.GROUP_NOT_FOUND);
        }
        Preconditions.checkTrue(allowedStatus.contains(groupInfo.getStatus()),
                String.format("current status was not allowed to %s workflow", nextStatus.getDescription()));
        return groupInfo;
    }

}
