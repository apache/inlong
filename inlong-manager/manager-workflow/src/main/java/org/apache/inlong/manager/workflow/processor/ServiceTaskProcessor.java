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

package org.apache.inlong.manager.workflow.processor;

import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang3.StringUtils;
import org.apache.inlong.manager.common.enums.TaskStatus;
import org.apache.inlong.manager.common.util.JsonUtils;
import org.apache.inlong.manager.common.util.Preconditions;
import org.apache.inlong.manager.dao.entity.WorkflowProcessEntity;
import org.apache.inlong.manager.dao.entity.WorkflowTaskEntity;
import org.apache.inlong.manager.dao.mapper.WorkflowTaskEntityMapper;
import org.apache.inlong.manager.workflow.WorkflowAction;
import org.apache.inlong.manager.workflow.WorkflowContext;
import org.apache.inlong.manager.workflow.core.impl.WorkflowEventNotifier;
import org.apache.inlong.manager.workflow.definition.ApproverAssign;
import org.apache.inlong.manager.workflow.definition.ServiceTask;
import org.apache.inlong.manager.workflow.definition.WorkflowTask;
import org.apache.inlong.manager.workflow.event.process.ProcessEvent;
import org.apache.inlong.manager.workflow.event.process.ProcessEventNotifier;
import org.apache.inlong.manager.workflow.event.task.TaskEvent;
import org.apache.inlong.manager.workflow.event.task.TaskEventNotifier;

import java.util.Date;
import java.util.List;
import java.util.Set;

/**
 * System task processor
 */
public class ServiceTaskProcessor extends AbstractTaskProcessor<ServiceTask> {

    private static final Set<WorkflowAction> SUPPORT_ACTIONS = ImmutableSet.of(
            WorkflowAction.COMPLETE, WorkflowAction.CANCEL, WorkflowAction.TERMINATE
    );

    private static final Set<TaskStatus> ALLOW_COMPLETE_STATE = ImmutableSet.of(
            TaskStatus.PENDING, TaskStatus.FAILED
    );

    private final TaskEventNotifier taskEventNotifier;
    private final ProcessEventNotifier processEventNotifier;

    public ServiceTaskProcessor(WorkflowTaskEntityMapper taskEntityMapper, WorkflowEventNotifier eventNotifier) {
        super(taskEntityMapper);
        this.taskEventNotifier = eventNotifier.getTaskEventNotifier();
        this.processEventNotifier = eventNotifier.getProcessEventNotifier();
    }

    @Override
    public Class<ServiceTask> watch() {
        return ServiceTask.class;
    }

    @Override
    public void create(ServiceTask serviceTask, WorkflowContext context) {
        WorkflowTaskEntity workflowTaskEntity = saveTaskEntity(serviceTask, context);
        context.getNewTaskList().add(workflowTaskEntity);
        serviceTask.initListeners(context);
        this.taskEventNotifier.notify(TaskEvent.CREATE, context);
    }

    @Override
    public boolean pendingForAction(WorkflowContext context) {
        context.setActionContext(
                new WorkflowContext.ActionContext()
                        .setTask((WorkflowTask) context.getCurrentElement())
                        .setAction(WorkflowAction.COMPLETE)
                        .setTaskEntity(context.getNewTaskList().get(0))
        );
        context.getNewTaskList().clear();
        return false;
    }

    @Override
    public boolean complete(WorkflowContext context) {
        WorkflowContext.ActionContext actionContext = context.getActionContext();
        Preconditions.checkTrue(SUPPORT_ACTIONS.contains(actionContext.getAction()),
                "serviceTask not support action: " + actionContext.getAction());
        WorkflowTaskEntity workflowTaskEntity = actionContext.getTaskEntity();
        Preconditions.checkTrue(ALLOW_COMPLETE_STATE.contains(TaskStatus.valueOf(workflowTaskEntity.getStatus())),
                "task status should allow complete");

        try {
            this.taskEventNotifier.notify(TaskEvent.COMPLETE, context);
            completeTaskEntity(actionContext, workflowTaskEntity, TaskStatus.COMPLETED);
            return true;
        } catch (Exception e) {
            completeTaskEntity(actionContext, workflowTaskEntity, TaskStatus.FAILED);
            this.taskEventNotifier.notify(TaskEvent.FAIL, context);
            this.processEventNotifier.notify(ProcessEvent.FAIL, context);
            return false;
        }
    }

    private WorkflowTaskEntity saveTaskEntity(ServiceTask serviceTask, WorkflowContext context) {
        WorkflowProcessEntity workflowProcessEntity = context.getProcessEntity();
        List<String> approvers = ApproverAssign.DEFAULT_SYSTEM_APPROVER.assign(context);
        WorkflowTaskEntity taskEntity = new WorkflowTaskEntity();
        taskEntity.setType(ServiceTask.class.getSimpleName());
        taskEntity.setProcessId(workflowProcessEntity.getId());
        taskEntity.setProcessName(context.getProcess().getName());
        taskEntity.setProcessDisplayName(context.getProcess().getDisplayName());
        taskEntity.setName(serviceTask.getName());
        taskEntity.setDisplayName(serviceTask.getDisplayName());
        taskEntity.setApplicant(workflowProcessEntity.getApplicant());
        taskEntity.setApprovers(StringUtils.join(approvers, WorkflowTaskEntity.APPROVERS_DELIMITER));
        taskEntity.setStatus(TaskStatus.PENDING.name());
        taskEntity.setStartTime(new Date());

        taskEntityMapper.insert(taskEntity);
        Preconditions.checkNotNull(taskEntity.getId(), "task saved failed");
        return taskEntity;
    }

    private void completeTaskEntity(WorkflowContext.ActionContext actionContext, WorkflowTaskEntity taskEntity,
            TaskStatus taskStatus) {
        taskEntity.setStatus(taskStatus.name());
        taskEntity.setOperator(taskEntity.getApprovers());
        taskEntity.setRemark(actionContext.getRemark());
        taskEntity.setFormData(JsonUtils.toJson(actionContext.getForm()));
        taskEntity.setEndTime(new Date());
        taskEntityMapper.update(taskEntity);
    }

}
