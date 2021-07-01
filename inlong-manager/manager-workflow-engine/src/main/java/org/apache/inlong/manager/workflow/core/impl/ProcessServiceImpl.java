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

import java.util.List;
import org.apache.inlong.manager.common.util.Preconditions;
import org.apache.inlong.manager.workflow.core.ProcessService;
import org.apache.inlong.manager.workflow.core.WorkflowContextBuilder;
import org.apache.inlong.manager.workflow.core.WorkflowDataAccessor;
import org.apache.inlong.manager.workflow.core.WorkflowProcessorExecutor;
import org.apache.inlong.manager.workflow.model.Action;
import org.apache.inlong.manager.workflow.model.TaskState;
import org.apache.inlong.manager.workflow.model.WorkflowContext;
import org.apache.inlong.manager.workflow.model.definition.ProcessForm;
import org.apache.inlong.manager.workflow.model.definition.Task;
import org.apache.inlong.manager.workflow.model.instance.TaskInstance;

/**
 * Process service
 */
public class ProcessServiceImpl implements ProcessService {

    private WorkflowProcessorExecutor workflowProcessorExecutor;
    private WorkflowContextBuilder workflowContextBuilder;
    private WorkflowDataAccessor workflowDataAccessor;

    public ProcessServiceImpl(
            WorkflowProcessorExecutor workflowProcessorExecutor,
            WorkflowContextBuilder workflowContextBuilder,
            WorkflowDataAccessor workflowDataAccessor) {
        this.workflowProcessorExecutor = workflowProcessorExecutor;
        this.workflowContextBuilder = workflowContextBuilder;
        this.workflowDataAccessor = workflowDataAccessor;
    }

    @Override
    public WorkflowContext start(String name, String applicant, ProcessForm form) {
        Preconditions.checkNotEmpty(name, "process name cannot be null");
        Preconditions.checkNotEmpty(applicant, "applicant cannot be null");
        Preconditions.checkNotNull(form, "form cannot be null");

        // build context
        WorkflowContext context = workflowContextBuilder.buildContextForProcess(name, applicant, form);
        this.workflowProcessorExecutor.executeStart(context.getProcess().getStartEvent(), context);
        return context;
    }

    @Override
    public WorkflowContext cancel(Integer processInstId, String operator, String remark) {
        Preconditions.checkNotEmpty(operator, "operator cannot be null");
        Preconditions.checkNotNull(processInstId, "processInstId cannot be null");

        WorkflowContext context = workflowContextBuilder.buildContextForProcess(processInstId);

        List<TaskInstance> pendingTasks = this.workflowDataAccessor.taskInstanceStorage()
                .list(processInstId, TaskState.PENDING);
        for (TaskInstance taskInstance : pendingTasks) {
            Task task = context.getProcess().getTaskByName(taskInstance.getName());
            context.setActionContext(new WorkflowContext.ActionContext()
                    .setAction(Action.CANCEL)
                    .setActionTaskInstance(taskInstance)
                    .setOperator(operator)
                    .setRemark(remark)
                    .setTask(task)
            );
            this.workflowProcessorExecutor.executeComplete(task, context);
        }

        return context;
    }
}
