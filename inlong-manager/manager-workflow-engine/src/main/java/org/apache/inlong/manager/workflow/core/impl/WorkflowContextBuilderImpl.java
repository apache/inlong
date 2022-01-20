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

package org.apache.inlong.manager.workflow.core.impl;

import com.google.common.collect.Lists;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.inlong.manager.common.util.JsonUtils;
import org.apache.inlong.manager.common.util.Preconditions;
import org.apache.inlong.manager.common.workflow.WorkflowContextBuilder;
import org.apache.inlong.manager.common.workflow.WorkflowDataAccessor;
import org.apache.inlong.manager.common.model.Action;
import org.apache.inlong.manager.common.model.WorkflowContext;
import org.apache.inlong.manager.common.model.definition.Process;
import org.apache.inlong.manager.common.model.definition.ProcessForm;
import org.apache.inlong.manager.common.model.definition.Task;
import org.apache.inlong.manager.common.model.definition.TaskForm;
import org.apache.inlong.manager.common.model.instance.ProcessInstance;
import org.apache.inlong.manager.common.model.instance.TaskInstance;
import org.apache.inlong.manager.workflow.util.WorkflowFormParserUtils;

/**
 * Workflow context builder
 */
public class WorkflowContextBuilderImpl implements WorkflowContextBuilder {

    private final WorkflowDataAccessor workflowDataAccessor;

    public WorkflowContextBuilderImpl(WorkflowDataAccessor workflowDataAccessor) {
        this.workflowDataAccessor = workflowDataAccessor;
    }

    @Override
    public WorkflowContext buildContextForProcess(String name, String applicant, ProcessForm form) {
        Process process = workflowDataAccessor.processDefinitionStorage().get(name);

        WorkflowContext context = new WorkflowContext();
        context.setProcess(process);
        context.setProcessForm(form);
        context.setApplicant(applicant);

        return context;
    }

    @Override
    public WorkflowContext buildContextForProcess(Integer processInstId) {
        ProcessInstance processInstance = workflowDataAccessor.processInstanceStorage().get(processInstId);
        Preconditions.checkNotNull(processInstance, "process not exist! " + processInstId);

        Process process = workflowDataAccessor.processDefinitionStorage().get(processInstance.getName());

        return new WorkflowContext()
                .setApplicant(processInstance.getApplicant())
                .setProcess(process)
                .setProcessForm(WorkflowFormParserUtils.parseProcessForm(processInstance.getFormData(), process))
                .setProcessInstance(processInstance);
    }

    @Override
    public WorkflowContext buildContextForTask(Integer taskInstId, Action action, String remark, String operator) {
        return buildContextForTask(taskInstId, action, null, null, remark, operator);
    }

    @Override
    public WorkflowContext buildContextForTask(Integer taskInstId, Action action, TaskForm taskForm, String remark,
            String operator) {
        return buildContextForTask(taskInstId, action, taskForm, null, remark, operator);
    }

    @Override
    public WorkflowContext buildContextForTask(Integer taskInstId, Action action, List<String> transferToUsers,
            String remark, String operator) {
        return buildContextForTask(taskInstId, action, null, transferToUsers, remark, operator);
    }

    @Override
    public WorkflowContext buildContextForTask(Integer taskInstId, Action action) {
        TaskInstance taskInstance = workflowDataAccessor.taskInstanceStorage().get(taskInstId);
        Process process = workflowDataAccessor.processDefinitionStorage().get(taskInstance.getProcessName());
        TaskForm taskForm = WorkflowFormParserUtils.parseTaskForm(taskInstance, process);
        List<String> transferToUsers = getTransferToUsers(taskInstance.getExt());
        return buildContextForTask(taskInstId, action, taskForm, transferToUsers, taskInstance.getRemark(),
                taskInstance.getOperator());
    }

    private WorkflowContext buildContextForTask(Integer taskInstId, Action action, TaskForm taskForm,
            List<String> transferToUsers, String remark, String operator) {
        TaskInstance taskInstance = workflowDataAccessor.taskInstanceStorage().get(taskInstId);
        Preconditions.checkNotNull(taskInstance, "task not exist , taskId is " + taskInstId);
        ProcessInstance processInstance = workflowDataAccessor.processInstanceStorage()
                .get(taskInstance.getProcessInstId());
        Process process = workflowDataAccessor.processDefinitionStorage().get(processInstance.getName());
        ProcessForm processForm = WorkflowFormParserUtils.parseProcessForm(processInstance.getFormData(), process);
        Task task = process.getTaskByName(taskInstance.getName());

        return new WorkflowContext().setProcess(process)
                .setApplicant(processInstance.getApplicant())
                .setProcessForm(processForm)
                .setProcessInstance(processInstance)
                .setCurrentElement(task)
                .setActionContext(
                        new WorkflowContext.ActionContext()
                                .setAction(action)
                                .setActionTaskInstance(taskInstance)
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
        if (!extMap.containsKey(TaskInstance.EXT_TRANSFER_USER_KEY)) {
            return Lists.newArrayList();
        }

        if (extMap.get(TaskInstance.EXT_TRANSFER_USER_KEY) instanceof List) {
            return (List<String>) extMap.get(TaskInstance.EXT_TRANSFER_USER_KEY);
        }

        return null;
    }
}
