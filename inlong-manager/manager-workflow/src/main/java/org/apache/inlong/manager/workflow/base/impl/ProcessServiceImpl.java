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

import org.apache.inlong.manager.common.enums.TaskStatus;
import org.apache.inlong.manager.common.util.Preconditions;
import org.apache.inlong.manager.dao.entity.WorkflowTaskEntity;
import org.apache.inlong.manager.dao.mapper.WorkflowTaskEntityMapper;
import org.apache.inlong.manager.workflow.WorkflowAction;
import org.apache.inlong.manager.workflow.WorkflowContext;
import org.apache.inlong.manager.workflow.base.ProcessService;
import org.apache.inlong.manager.workflow.base.ProcessorExecutor;
import org.apache.inlong.manager.workflow.base.WorkflowContextBuilder;
import org.apache.inlong.manager.workflow.definition.ProcessForm;
import org.apache.inlong.manager.workflow.definition.WorkflowTask;

import java.util.List;

/**
 * WorkflowProcess service
 */
public class ProcessServiceImpl implements ProcessService {

    private final ProcessorExecutor processorExecutor;
    private final WorkflowContextBuilder workflowContextBuilder;
    private final WorkflowTaskEntityMapper taskEntityMapper;

    public ProcessServiceImpl(
            ProcessorExecutor processorExecutor,
            WorkflowContextBuilder workflowContextBuilder,
            WorkflowTaskEntityMapper taskEntityMapper) {
        this.processorExecutor = processorExecutor;
        this.workflowContextBuilder = workflowContextBuilder;
        this.taskEntityMapper = taskEntityMapper;
    }

    @Override
    public WorkflowContext start(String name, String applicant, ProcessForm form) {
        Preconditions.checkNotEmpty(name, "process name cannot be null");
        Preconditions.checkNotEmpty(applicant, "applicant cannot be null");
        Preconditions.checkNotNull(form, "form cannot be null");

        // build context
        WorkflowContext context = workflowContextBuilder.buildContextForProcess(name, applicant, form);
        this.processorExecutor.executeStart(context.getProcess().getStartEvent(), context);
        return context;
    }

    @Override
    public WorkflowContext cancel(Integer processId, String operator, String remark) {
        Preconditions.checkNotEmpty(operator, "operator cannot be null");
        Preconditions.checkNotNull(processId, "processId cannot be null");

        WorkflowContext context = workflowContextBuilder.buildContextForProcess(processId);

        List<WorkflowTaskEntity> pendingTasks = taskEntityMapper.selectByProcess(processId, TaskStatus.PENDING);
        for (WorkflowTaskEntity taskEntity : pendingTasks) {
            WorkflowTask task = context.getProcess().getTaskByName(taskEntity.getName());
            context.setActionContext(new WorkflowContext.ActionContext()
                    .setAction(WorkflowAction.CANCEL)
                    .setTaskEntity(taskEntity)
                    .setOperator(operator)
                    .setRemark(remark)
                    .setTask(task)
            );
            this.processorExecutor.executeComplete(task, context);
        }

        return context;
    }

}
