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

import org.apache.inlong.manager.common.consts.InlongConstants;
import org.apache.inlong.manager.common.enums.ErrorCodeEnum;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.common.util.Preconditions;
import org.apache.inlong.manager.dao.entity.WorkflowApproverEntity;
import org.apache.inlong.manager.dao.mapper.WorkflowApproverEntityMapper;
import org.apache.inlong.manager.pojo.workflow.ApproverRequest;
import org.apache.inlong.manager.pojo.workflow.ApproverResponse;
import org.apache.inlong.manager.service.core.WorkflowApproverService;
import org.apache.inlong.manager.workflow.core.ProcessDefinitionService;
import org.apache.inlong.manager.workflow.definition.UserTask;
import org.apache.inlong.manager.workflow.definition.WorkflowProcess;
import org.apache.inlong.manager.workflow.definition.WorkflowTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Operation of workflow approvers
 */
@Service
public class WorkflowApproverServiceImpl implements WorkflowApproverService {

    private static final Logger LOGGER = LoggerFactory.getLogger(WorkflowApproverServiceImpl.class);

    @Autowired
    private WorkflowApproverEntityMapper workflowApproverMapper;
    @Autowired
    private ProcessDefinitionService processDefinitionService;

    @Override
    public Integer save(ApproverRequest request, String operator) {
        LOGGER.info("begin to save approver: {} by user: {}", request, operator);
        WorkflowProcess process = processDefinitionService.getByName(request.getProcessName());
        Preconditions.checkNotNull(process, "process not exit with name: " + request.getProcessName());
        WorkflowTask task = process.getTaskByName(request.getTaskName());
        Preconditions.checkNotNull(task, "task not exit with name: " + request.getTaskName());
        Preconditions.checkTrue(task instanceof UserTask, "task should be UserTask");

        List<WorkflowApproverEntity> exist = workflowApproverMapper.selectByQuery(request);
        Preconditions.checkEmpty(exist, "workflow approver already exits");

        WorkflowApproverEntity entity = CommonBeanUtils.copyProperties(request, WorkflowApproverEntity::new);
        entity.setCreator(operator);
        entity.setModifier(operator);
        workflowApproverMapper.insert(entity);

        LOGGER.info("success to save approver: {} by user: {}", request, operator);
        return entity.getId();
    }

    @Override
    public ApproverResponse get(Integer id) {
        Preconditions.checkNotNull(id, "approver id cannot be null");
        WorkflowApproverEntity approverEntity = workflowApproverMapper.selectById(id);
        if (approverEntity == null) {
            LOGGER.error("workflow approver not found by id={}", id);
            throw new BusinessException(ErrorCodeEnum.WORKFLOW_APPROVER_NOT_FOUND);
        }
        return CommonBeanUtils.copyProperties(approverEntity, ApproverResponse::new);
    }

    @Override
    public List<String> getApprovers(String processName, String taskName) {
        ApproverRequest approverRequest = ApproverRequest.builder()
                .processName(processName)
                .taskName(taskName)
                .build();
        List<WorkflowApproverEntity> approverEntities = workflowApproverMapper.selectByQuery(approverRequest);
        Set<String> resultSet = new HashSet<>();
        approverEntities.forEach(entity ->
                resultSet.addAll(Arrays.asList(entity.getApprovers().split(InlongConstants.COMMA))));

        return new ArrayList<>(resultSet);
    }

    @Override
    public List<ApproverResponse> listByCondition(ApproverRequest request) {
        List<WorkflowApproverEntity> entityList = workflowApproverMapper.selectByQuery(request);
        return CommonBeanUtils.copyListProperties(entityList, ApproverResponse::new);
    }

    @Override
    public Integer update(ApproverRequest request, String operator) {
        Preconditions.checkNotNull(request, "approver request cannot be null");
        Integer id = request.getId();
        Preconditions.checkNotNull(id, "approver id cannot be null");

        WorkflowApproverEntity entity = workflowApproverMapper.selectById(id);
        Preconditions.checkNotNull(entity, "not exist with id:" + id);
        String errMsg = String.format("approver has already updated with id=%s, process=%s, task=%s, curVersion=%s",
                id, request.getProcessName(), request.getTaskName(), request.getVersion());
        if (!Objects.equals(entity.getVersion(), request.getVersion())) {
            LOGGER.error(errMsg);
            throw new BusinessException(ErrorCodeEnum.CONFIG_EXPIRED);
        }
        entity.setModifier(operator);
        entity.setApprovers(request.getApprovers());
        workflowApproverMapper.updateById(entity);

        LOGGER.info("success to update workflow approver for request: {} by user: {}", request, operator);
        return id;
    }

    @Override
    public void delete(Integer id, String operator) {
        WorkflowApproverEntity entity = workflowApproverMapper.selectById(id);
        Preconditions.checkNotNull(entity, "not exist with id:" + id);
        int success = this.workflowApproverMapper.deleteByPrimaryKey(id, operator);
        Preconditions.checkTrue(success == 1, "delete failed");
    }

}
