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
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.inlong.manager.common.util.JsonUtils;
import org.apache.inlong.manager.common.util.Preconditions;
import org.apache.inlong.manager.common.workflow.WorkflowDataAccessor;
import org.apache.inlong.manager.common.event.task.TaskEvent;
import org.apache.inlong.manager.common.event.task.TaskEventNotifier;
import org.apache.inlong.manager.workflow.core.impl.WorkflowEventNotifier;
import org.apache.inlong.manager.common.exceptions.WorkflowException;
import org.apache.inlong.manager.common.model.Action;
import org.apache.inlong.manager.common.model.TaskState;
import org.apache.inlong.manager.common.model.WorkflowContext;
import org.apache.inlong.manager.common.model.definition.Element;
import org.apache.inlong.manager.common.model.definition.UserTask;
import org.apache.inlong.manager.common.model.instance.ProcessInstance;
import org.apache.inlong.manager.common.model.instance.TaskInstance;

/**
 * User task processor
 */
public class UserTaskProcessor extends AbstractTaskProcessor<UserTask> {

    private static final Set<Action> SHOULD_CHECK_OPERATOR_ACTIONS = ImmutableSet
            .of(Action.APPROVE, Action.REJECT, Action.TRANSFER);

    private static final Set<Action> SUPPORT_ACTIONS = ImmutableSet.of(
            Action.APPROVE, Action.REJECT, Action.TRANSFER, Action.CANCEL, Action.TERMINATE
    );
    private final TaskEventNotifier taskEventNotifier;

    public UserTaskProcessor(WorkflowDataAccessor workflowDataAccessor, WorkflowEventNotifier workflowEventNotifier) {
        super(workflowDataAccessor);
        this.taskEventNotifier = workflowEventNotifier.getTaskEventNotifier();
    }

    @Override
    public Class<UserTask> watch() {
        return UserTask.class;
    }

    @Override
    public void create(UserTask userTask, WorkflowContext context) {
        List<String> approvers = userTask.getApproverAssign().assign(context);
        Preconditions.checkNotEmpty(approvers, "cannot assign approvers for task: " + userTask.getDisplayName()
                + ", as the approvers in table `wf_approver` was empty");

        if (!userTask.isNeedAllApprove()) {
            approvers = Collections.singletonList(StringUtils.join(approvers, TaskInstance.APPROVERS_DELIMITER));
        }

        ProcessInstance processInstance = context.getProcessInstance();
        approvers.stream()
                .map(approver -> createTaskInstance(userTask, processInstance, approver))
                .forEach(context.getNewTaskInstances()::add);

        taskEventNotifier.notify(TaskEvent.CREATE, context);
    }

    @Override
    public boolean pendingForAction(WorkflowContext context) {
        return true;
    }

    @Override
    public boolean complete(WorkflowContext context) {
        WorkflowContext.ActionContext actionContext = context.getActionContext();
        Preconditions.checkTrue(SUPPORT_ACTIONS.contains(actionContext.getAction()),
                () -> "userTask not support action:" + actionContext.getAction());

        TaskInstance taskInstance = actionContext.getActionTaskInstance();
        Preconditions.checkTrue(TaskState.PENDING.name().equalsIgnoreCase(taskInstance.getState()),
                "task state should be pending");

        checkOperator(actionContext);
        completeTaskInstance(actionContext);

        this.taskEventNotifier.notify(toTaskEvent(actionContext.getAction()), context);
        return true;
    }

    @Override
    public List<Element> next(UserTask userTask, WorkflowContext context) {
        WorkflowContext.ActionContext actionContext = context.getActionContext();
        if (userTask.isNeedAllApprove()) {
            TaskInstance taskInstance = actionContext.getActionTaskInstance();
            int pendingTaskCount = this.workflowDataAccessor.taskInstanceStorage()
                    .countTask(taskInstance.getProcessInstId(), taskInstance.getName(), TaskState.PENDING);

            if (pendingTaskCount > 0) {
                return Lists.newArrayList();
            }
        }

        return super.next(userTask, context);
    }

    private TaskInstance createTaskInstance(UserTask userTask, ProcessInstance processInstance, String approvers) {
        TaskInstance taskInstance = new TaskInstance()
                .setType(UserTask.class.getSimpleName())
                .setProcessInstId(processInstance.getId())
                .setProcessName(processInstance.getName())
                .setProcessDisplayName(processInstance.getDisplayName())
                .setApplicant(processInstance.getApplicant())
                .setName(userTask.getName())
                .setDisplayName(userTask.getDisplayName())
                .setApprovers(approvers)
                .setState(TaskState.PENDING.name())
                .setStartTime(new Date());
        this.workflowDataAccessor.taskInstanceStorage().insert(taskInstance);
        Preconditions.checkNotNull(taskInstance.getId(), "task instance id cannot be null");
        return taskInstance;
    }

    private void checkOperator(WorkflowContext.ActionContext actionContext) {
        TaskInstance taskInstance = actionContext.getActionTaskInstance();
        if (!SHOULD_CHECK_OPERATOR_ACTIONS.contains(actionContext.getAction())) {
            return;
        }

        boolean operatorIsApprover = ArrayUtils.contains(
                taskInstance.getApprovers().split(TaskInstance.APPROVERS_DELIMITER), actionContext.getOperator()
        );

        if (!operatorIsApprover) {
            throw new WorkflowException(
                    "current operator " + actionContext.getOperator() + " not in approvers list:" + taskInstance
                            .getApprovers());
        }
    }

    private void completeTaskInstance(WorkflowContext.ActionContext actionContext) {
        TaskInstance taskInstance = actionContext.getActionTaskInstance();

        TaskState taskState = toTaskState(actionContext.getAction());
        taskInstance.setState(taskState.name());
        taskInstance.setOperator(actionContext.getOperator());
        taskInstance.setRemark(actionContext.getRemark());

        UserTask userTask = (UserTask) actionContext.getTask();
        if (needForm(userTask, actionContext.getAction())) {
            Preconditions.checkNotNull(actionContext.getForm(), "form cannot be null");
            Preconditions.checkTrue(actionContext.getForm().getClass().isAssignableFrom(userTask.getFormClass()),
                    () -> "form type not match, should be class " + userTask.getFormClass());
            actionContext.getForm().validate();
            taskInstance.setFormData(JsonUtils.toJson(actionContext.getForm()));
        } else {
            Preconditions.checkNull(actionContext.getForm(), "no form required");
        }
        taskInstance.setEndTime(new Date());
        taskInstance.setExt(handlerExt(actionContext, taskInstance.getExt()));
        this.workflowDataAccessor.taskInstanceStorage().update(taskInstance);
    }

    private boolean needForm(UserTask userTask, Action action) {
        if (userTask.getFormClass() == null) {
            return false;
        }

        return Action.APPROVE.equals(action) || Action.COMPLETE.equals(action);
    }

    private String handlerExt(WorkflowContext.ActionContext actionContext, String oldExt) {
        Map<String, Object> extMap = Optional.ofNullable(oldExt)
                .map(e -> JsonUtils.parseMap(oldExt, String.class, Object.class))
                .orElseGet(Maps::newHashMap);

        if (Action.TRANSFER.equals(actionContext.getAction())) {
            extMap.put(TaskInstance.EXT_TRANSFER_USER_KEY, actionContext.getTransferToUsers());
        }

        return JsonUtils.toJson(extMap);
    }

    private TaskState toTaskState(Action action) {
        switch (action) {
            case APPROVE:
                return TaskState.APPROVED;
            case REJECT:
                return TaskState.REJECTED;
            case CANCEL:
                return TaskState.CANCELED;
            case TRANSFER:
                return TaskState.TRANSFERED;
            case TERMINATE:
                return TaskState.TERMINATED;
            default:
                throw new WorkflowException("unknow action " + this);
        }
    }

    private TaskEvent toTaskEvent(Action action) {
        switch (action) {
            case APPROVE:
                return TaskEvent.APPROVE;
            case REJECT:
                return TaskEvent.REJECT;
            case CANCEL:
                return TaskEvent.CANCEL;
            case TRANSFER:
                return TaskEvent.TRANSFER;
            case TERMINATE:
                return TaskEvent.TERMINATE;
            default:
                throw new WorkflowException("unknow action " + this);
        }
    }

}
