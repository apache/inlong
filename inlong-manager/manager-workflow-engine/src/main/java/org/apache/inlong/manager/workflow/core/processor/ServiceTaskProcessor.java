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

package org.apache.inlong.manager.workflow.core.processor;

import com.google.common.collect.ImmutableSet;
import java.util.Date;
import java.util.List;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.inlong.manager.common.workflow.WorkflowDataAccessor;
import org.apache.inlong.manager.common.event.process.ProcessEvent;
import org.apache.inlong.manager.common.event.process.ProcessEventNotifier;
import org.apache.inlong.manager.common.event.task.TaskEvent;
import org.apache.inlong.manager.common.event.task.TaskEventNotifier;
import org.apache.inlong.manager.workflow.core.impl.WorkflowEventNotifier;
import org.apache.inlong.manager.common.model.Action;
import org.apache.inlong.manager.common.model.TaskState;
import org.apache.inlong.manager.common.model.WorkflowContext;
import org.apache.inlong.manager.common.model.definition.ApproverAssign;
import org.apache.inlong.manager.common.model.definition.ServiceTask;
import org.apache.inlong.manager.common.model.definition.Task;
import org.apache.inlong.manager.common.model.instance.ProcessInstance;
import org.apache.inlong.manager.common.model.instance.TaskInstance;
import org.apache.inlong.manager.common.util.JsonUtils;
import org.apache.inlong.manager.common.util.Preconditions;

/**
 * System task processor
 */
public class ServiceTaskProcessor extends AbstractTaskProcessor<ServiceTask> {

    private static final Set<Action> SUPPORT_ACTIONS = ImmutableSet.of(
            Action.COMPLETE, Action.CANCEL, Action.TERMINATE
    );

    private static final Set<TaskState> ALLOW_COMPLETE_STATE = ImmutableSet.of(
            TaskState.PENDING, TaskState.FAILED
    );

    private TaskEventNotifier taskEventNotifier;
    private ProcessEventNotifier processEventNotifier;

    public ServiceTaskProcessor(WorkflowDataAccessor workflowDataAccessor,
            WorkflowEventNotifier workflowEventNotifier) {
        super(workflowDataAccessor);
        this.taskEventNotifier = workflowEventNotifier.getTaskEventNotifier();
        this.processEventNotifier = workflowEventNotifier.getProcessEventNotifier();
    }

    @Override
    public Class<ServiceTask> watch() {
        return ServiceTask.class;
    }

    @Override
    public void create(ServiceTask serviceTask, WorkflowContext context) {
        TaskInstance taskInstance = createTaskInstance(serviceTask, context);
        context.getNewTaskInstances().add(taskInstance);
        serviceTask.initListeners(context);
        this.taskEventNotifier.notify(TaskEvent.CREATE, context);
    }

    @Override
    public boolean pendingForAction(WorkflowContext context) {
        context.setActionContext(
                new WorkflowContext.ActionContext()
                        .setTask((Task) context.getCurrentElement())
                        .setAction(Action.COMPLETE)
                        .setActionTaskInstance(context.getNewTaskInstances().get(0))
        );
        context.getNewTaskInstances().clear();
        return false;
    }

    @Override
    public boolean complete(WorkflowContext context) {
        WorkflowContext.ActionContext actionContext = context.getActionContext();
        Preconditions.checkTrue(SUPPORT_ACTIONS.contains(actionContext.getAction()),
                () -> "serviceTask not support action:" + actionContext.getAction());
        TaskInstance taskInstance = actionContext.getActionTaskInstance();
        Preconditions.checkTrue(ALLOW_COMPLETE_STATE.contains(TaskState.valueOf(taskInstance.getState())),
                "task state should allow complete");

        try {
            this.taskEventNotifier.notify(TaskEvent.COMPLETE, context);
            completeTaskInstance(actionContext, taskInstance, TaskState.COMPLETED);
            return true;
        } catch (Exception e) {
            completeTaskInstance(actionContext, taskInstance, TaskState.FAILED);
            this.taskEventNotifier.notify(TaskEvent.FAIL, context);
            this.processEventNotifier.notify(ProcessEvent.FAIL, context);
            return false;
        }
    }

    private TaskInstance createTaskInstance(ServiceTask serviceTask, WorkflowContext context) {
        ProcessInstance processInstance = context.getProcessInstance();
        List<String> approvers = ApproverAssign.DEFAULT_SYSTEM_APPROVER.assign(context);
        TaskInstance taskInstance = new TaskInstance()
                .setType(ServiceTask.class.getSimpleName())
                .setProcessInstId(processInstance.getId())
                .setProcessName(context.getProcess().getName())
                .setProcessDisplayName(context.getProcess().getDisplayName())
                .setName(serviceTask.getName())
                .setDisplayName(serviceTask.getDisplayName())
                .setApplicant(processInstance.getApplicant())
                .setApprovers(StringUtils.join(approvers, TaskInstance.APPROVERS_DELIMITER))
                .setState(TaskState.PENDING.name())
                .setStartTime(new Date());

        this.workflowDataAccessor.taskInstanceStorage().insert(taskInstance);
        return taskInstance;
    }

    private void completeTaskInstance(WorkflowContext.ActionContext actionContext, TaskInstance taskInstance,
            TaskState taskState) {
        taskInstance.setState(taskState.name());
        taskInstance.setOperator(taskInstance.getApprovers());
        taskInstance.setRemark(actionContext.getRemark());
        taskInstance.setFormData(JsonUtils.toJson(actionContext.getForm()));
        taskInstance.setEndTime(new Date());
        this.workflowDataAccessor.taskInstanceStorage().update(taskInstance);
    }
}
