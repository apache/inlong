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

import org.apache.inlong.manager.common.workflow.TaskService;
import org.apache.inlong.manager.common.workflow.WorkflowContextBuilder;
import org.apache.inlong.manager.common.workflow.WorkflowProcessorExecutor;
import org.apache.inlong.manager.common.model.Action;
import org.apache.inlong.manager.common.model.WorkflowContext;
import org.apache.inlong.manager.common.model.definition.TaskForm;

import java.util.List;

/**
 * Task service
 */
public class TaskServiceImpl implements TaskService {

    private WorkflowProcessorExecutor workflowProcessorExecutor;
    private WorkflowContextBuilder workflowContextBuilder;

    public TaskServiceImpl(WorkflowProcessorExecutor workflowProcessorExecutor,
                           WorkflowContextBuilder workflowContextBuilder) {
        this.workflowProcessorExecutor = workflowProcessorExecutor;
        this.workflowContextBuilder = workflowContextBuilder;
    }

    @Override
    public WorkflowContext approve(Integer taskId, String remark, TaskForm form, String operator) {
        //TODO check args

        WorkflowContext context = workflowContextBuilder
                .buildContextForTask(taskId, Action.APPROVE, form, remark, operator);
        workflowProcessorExecutor.executeComplete(context.getActionContext().getTask(), context);

        return context;
    }

    @Override
    public WorkflowContext reject(Integer taskId, String remark, String operator) {
        WorkflowContext context = workflowContextBuilder.buildContextForTask(taskId, Action.REJECT, remark, operator);
        workflowProcessorExecutor.executeComplete(context.getActionContext().getTask(), context);

        return context;
    }

    @Override
    public WorkflowContext transfer(Integer taskId, String remark, List<String> to, String operator) {
        WorkflowContext context = workflowContextBuilder
                .buildContextForTask(taskId, Action.TRANSFER, to, remark, operator);
        workflowProcessorExecutor.executeComplete(context.getActionContext().getTask(), context);

        return context;
    }

    @Override
    public WorkflowContext complete(Integer taskId, String remark, String operator) {
        WorkflowContext context = workflowContextBuilder
                .buildContextForTask(taskId, Action.COMPLETE, remark, operator);
        workflowProcessorExecutor.executeComplete(context.getActionContext().getTask(), context);
        return context;
    }

}
