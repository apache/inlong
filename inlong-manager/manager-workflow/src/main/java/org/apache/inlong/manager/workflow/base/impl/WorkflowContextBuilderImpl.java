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

package org.apache.inlong.manager.workflow.base.impl;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.apache.inlong.manager.common.util.JsonUtils;
import org.apache.inlong.manager.common.util.Preconditions;
import org.apache.inlong.manager.dao.entity.WorkflowProcessEntity;
import org.apache.inlong.manager.dao.entity.WorkflowTaskEntity;
import org.apache.inlong.manager.dao.mapper.WorkflowProcessEntityMapper;
import org.apache.inlong.manager.dao.mapper.WorkflowTaskEntityMapper;
import org.apache.inlong.manager.workflow.WorkflowAction;
import org.apache.inlong.manager.workflow.WorkflowContext;
import org.apache.inlong.manager.workflow.base.ProcessDefinitionRepository;
import org.apache.inlong.manager.workflow.base.WorkflowContextBuilder;
import org.apache.inlong.manager.workflow.definition.ProcessForm;
import org.apache.inlong.manager.workflow.definition.TaskForm;
import org.apache.inlong.manager.workflow.definition.WorkflowProcess;
import org.apache.inlong.manager.workflow.definition.WorkflowTask;
import org.apache.inlong.manager.workflow.util.WorkflowFormParserUtils;

import java.util.List;
import java.util.Map;

/**
 * Workflow context builder
 */
public class WorkflowContextBuilderImpl implements WorkflowContextBuilder {

    private final ProcessDefinitionRepository definitionRepository;
    private final WorkflowProcessEntityMapper processEntityMapper;
    private final WorkflowTaskEntityMapper taskEntityMapper;

    public WorkflowContextBuilderImpl(ProcessDefinitionRepository definitionRepository,
            WorkflowProcessEntityMapper processEntityMapper,
            WorkflowTaskEntityMapper taskEntityMapper) {
        this.definitionRepository = definitionRepository;
        this.processEntityMapper = processEntityMapper;
        this.taskEntityMapper = taskEntityMapper;
    }

    @Override
    public WorkflowContext buildContextForProcess(String name, String applicant, ProcessForm form) {
        WorkflowProcess process = definitionRepository.get(name);
        WorkflowContext context = new WorkflowContext();
        context.setProcess(process);
        context.setProcessForm(form);
        context.setApplicant(applicant);

        return context;
    }

    @Override
    public WorkflowContext buildContextForProcess(Integer processId) {
        WorkflowProcessEntity processEntity = processEntityMapper.selectById(processId);
        Preconditions.checkNotNull(processEntity, "process not exist with id: " + processId);
        WorkflowProcess process = definitionRepository.get(processEntity.getName());

        return new WorkflowContext()
                .setApplicant(processEntity.getApplicant())
                .setProcess(process)
                .setProcessForm(WorkflowFormParserUtils.parseProcessForm(processEntity.getFormData(), process))
                .setProcessEntity(processEntity);
    }

    @Override
    public WorkflowContext buildContextForTask(Integer taskId, WorkflowAction action, String remark, String operator) {
        return buildContextForTask(taskId, action, null, null, remark, operator);
    }

    @Override
    public WorkflowContext buildContextForTask(Integer taskId, WorkflowAction action, TaskForm taskForm, String remark,
            String operator) {
        return buildContextForTask(taskId, action, taskForm, null, remark, operator);
    }

    @Override
    public WorkflowContext buildContextForTask(Integer taskId, WorkflowAction action, List<String> transferToUsers,
            String remark, String operator) {
        return buildContextForTask(taskId, action, null, transferToUsers, remark, operator);
    }

    @Override
    public WorkflowContext buildContextForTask(Integer taskId, WorkflowAction action) {
        WorkflowTaskEntity taskEntity = taskEntityMapper.selectById(taskId);
        WorkflowProcess process = definitionRepository.get(taskEntity.getProcessName());
        TaskForm taskForm = WorkflowFormParserUtils.parseTaskForm(taskEntity, process);
        List<String> transferToUsers = getTransferToUsers(taskEntity.getExtParams());
        return buildContextForTask(taskId, action, taskForm, transferToUsers, taskEntity.getRemark(),
                taskEntity.getOperator());
    }

    private WorkflowContext buildContextForTask(Integer taskId, WorkflowAction action, TaskForm taskForm,
            List<String> transferToUsers, String remark, String operator) {
        WorkflowTaskEntity taskEntity = taskEntityMapper.selectById(taskId);
        Preconditions.checkNotNull(taskEntity, "task not exist with id: " + taskId);

        WorkflowProcessEntity processEntity = processEntityMapper.selectById(taskEntity.getProcessId());
        WorkflowProcess process = definitionRepository.get(processEntity.getName());
        ProcessForm processForm = WorkflowFormParserUtils.parseProcessForm(processEntity.getFormData(), process);
        WorkflowTask task = process.getTaskByName(taskEntity.getName());

        return new WorkflowContext().setProcess(process)
                .setApplicant(processEntity.getApplicant())
                .setProcessForm(processForm)
                .setProcessEntity(processEntity)
                .setCurrentElement(task)
                .setActionContext(
                        new WorkflowContext.ActionContext()
                                .setAction(action)
                                .setTaskEntity(taskEntity)
                                .setTask(task)
                                .setForm(taskForm)
                                .setTransferToUsers(transferToUsers)
                                .setOperator(operator)
                                .setRemark(remark)
                );
    }

    private List<String> getTransferToUsers(String ext) {
        if (StringUtils.isEmpty(ext)) {
            return Lists.newArrayList();
        }
        Map<String, Object> extMap = JsonUtils.parseMap(ext, String.class, Object.class);
        if (!extMap.containsKey(WorkflowTaskEntity.EXT_TRANSFER_USER_KEY)) {
            return Lists.newArrayList();
        }

        if (extMap.get(WorkflowTaskEntity.EXT_TRANSFER_USER_KEY) instanceof List) {
            return (List<String>) extMap.get(WorkflowTaskEntity.EXT_TRANSFER_USER_KEY);
        }

        return null;
    }

}
