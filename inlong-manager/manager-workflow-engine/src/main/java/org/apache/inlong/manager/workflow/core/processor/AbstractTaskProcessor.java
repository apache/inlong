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

import com.google.common.base.Joiner;

import org.apache.inlong.manager.workflow.core.WorkflowDataAccessor;
import org.apache.inlong.manager.workflow.model.TaskState;
import org.apache.inlong.manager.workflow.model.WorkflowContext;
import org.apache.inlong.manager.workflow.model.definition.ApproverAssign;
import org.apache.inlong.manager.workflow.model.definition.Task;
import org.apache.inlong.manager.workflow.model.instance.ProcessInstance;
import org.apache.inlong.manager.workflow.model.instance.TaskInstance;
import org.apache.inlong.manager.common.util.Preconditions;

import java.util.Date;

/**
 * Task processor
 */
public abstract class AbstractTaskProcessor<T extends Task> extends AbstractNextableElementProcessor<T> implements
        SkipAbleElementProcessor<T> {

    protected WorkflowDataAccessor workflowDataAccessor;

    public AbstractTaskProcessor(WorkflowDataAccessor workflowDataAccessor) {
        this.workflowDataAccessor = workflowDataAccessor;
    }

    @Override
    public void skip(T task, WorkflowContext context) {
        ProcessInstance processInstance = context.getProcessInstance();
        Date now = new Date();
        String operators = Joiner.on(TaskInstance.APPROVERS_DELIMITER)
                .join(ApproverAssign.DEFAULT_SKIP_APPROVER.assign(context));
        TaskInstance taskInstance = new TaskInstance()
                .setType(task.getClass().getSimpleName())
                .setProcessInstId(processInstance.getId())
                .setProcessName(processInstance.getName())
                .setProcessDisplayName(processInstance.getDisplayName())
                .setApplicant(processInstance.getApplicant())
                .setApprovers(operators)
                .setOperator(operators)
                .setName(task.getName())
                .setDisplayName(task.getDisplayName())
                .setState(TaskState.SKIPPED.name())
                .setRemark("auto skipped")
                .setStartTime(now)
                .setEndTime(now);
        this.workflowDataAccessor.taskInstanceStorage().insert(taskInstance);
        Preconditions.checkNotNull(taskInstance.getId(), "task instance id cannot be null");
    }

}
