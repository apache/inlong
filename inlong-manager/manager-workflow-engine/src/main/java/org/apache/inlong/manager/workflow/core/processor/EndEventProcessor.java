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

import org.apache.inlong.manager.common.workflow.WorkflowDataAccessor;
import org.apache.inlong.manager.common.event.process.ProcessEvent;
import org.apache.inlong.manager.common.event.process.ProcessEventNotifier;
import org.apache.inlong.manager.workflow.core.impl.WorkflowEventNotifier;
import org.apache.inlong.manager.common.exceptions.WorkflowException;
import org.apache.inlong.manager.common.model.Action;
import org.apache.inlong.manager.common.model.ProcessState;
import org.apache.inlong.manager.common.model.TaskState;
import org.apache.inlong.manager.common.model.WorkflowContext;
import org.apache.inlong.manager.common.model.definition.Element;
import org.apache.inlong.manager.common.model.definition.EndEvent;
import org.apache.inlong.manager.common.model.instance.ProcessInstance;
import org.apache.inlong.manager.common.model.instance.TaskInstance;

import java.util.Collections;
import java.util.Date;
import java.util.List;

import lombok.extern.slf4j.Slf4j;

import org.springframework.util.CollectionUtils;

/**
 * End event handler
 */
@Slf4j
public class EndEventProcessor implements WorkflowElementProcessor<EndEvent> {

    private WorkflowDataAccessor workflowDataAccessor;

    private ProcessEventNotifier processEventNotifier;

    public EndEventProcessor(WorkflowDataAccessor workflowDataAccessor, WorkflowEventNotifier workflowEventNotifier) {
        this.workflowDataAccessor = workflowDataAccessor;
        this.processEventNotifier = workflowEventNotifier.getProcessEventNotifier();
    }

    @Override
    public Class<EndEvent> watch() {
        return EndEvent.class;
    }

    @Override
    public void create(EndEvent element, WorkflowContext context) {
        //do nothing
    }

    @Override
    public boolean pendingForAction(WorkflowContext context) {
        return false;
    }

    @Override
    public boolean complete(WorkflowContext context) {
        ProcessInstance processInstance = context.getProcessInstance();
        List<TaskInstance> pendingTasks = workflowDataAccessor.taskInstanceStorage()
                .list(processInstance.getId(), TaskState.PENDING);
        //If there are unfinished tasks, the process cannot be ended
        if (!CollectionUtils.isEmpty(pendingTasks)) {
            log.warn("have pending task ,end event not execute");
            return true;
        }
        WorkflowContext.ActionContext actionContext = context.getActionContext();
        processInstance.setState(getProcessState(actionContext.getAction()).name());
        processInstance.setEndTime(new Date());
        this.workflowDataAccessor.processInstanceStorage().update(processInstance);
        processEventNotifier.notify(mapToEvent(actionContext.getAction()), context);

        return true;
    }

    @Override

    public List<Element> next(EndEvent element, WorkflowContext context) {
        return Collections.emptyList();
    }

    private ProcessState getProcessState(Action action) {
        switch (action) {
            case APPROVE:
            case COMPLETE:
                return ProcessState.COMPLETED;
            case REJECT:
                return ProcessState.REJECTED;
            case CANCEL:
                return ProcessState.CANCELED;
            case TERMINATE:
                return ProcessState.TERMINATED;
            default:
                throw new WorkflowException("unknow action " + action);
        }
    }

    private ProcessEvent mapToEvent(Action action) {
        switch (action) {
            case APPROVE:
            case COMPLETE:
                return ProcessEvent.COMPLETE;
            case REJECT:
                return ProcessEvent.REJECT;
            case CANCEL:
                return ProcessEvent.CANCEL;
            case TERMINATE:
                return ProcessEvent.TERMINATE;
            default:
                throw new WorkflowException("unknow action " + action);
        }
    }
}
