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

package org.apache.inlong.manager.workflow.definition;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.extern.slf4j.Slf4j;
import org.apache.inlong.manager.common.exceptions.WorkflowException;
import org.apache.inlong.manager.common.util.Preconditions;
import org.apache.inlong.manager.workflow.WorkflowAction;
import org.apache.inlong.manager.workflow.WorkflowContext;
import org.apache.inlong.manager.workflow.event.task.TaskEventListener;
import org.springframework.util.CollectionUtils;

import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * System task
 */
@Slf4j
public class ServiceTask extends WorkflowTask {

    private static final Set<WorkflowAction> SUPPORTED_ACTIONS = ImmutableSet
            .of(WorkflowAction.COMPLETE, WorkflowAction.CANCEL, WorkflowAction.TERMINATE);

    private ServiceTaskListenerProvider listenerProvider;

    private ServiceTaskType serviceTaskType;

    private AtomicBoolean isInit = new AtomicBoolean(false);

    @Override
    public WorkflowAction defaultNextAction() {
        return WorkflowAction.COMPLETE;
    }

    @Override
    protected Set<WorkflowAction> supportedActions() {
        return SUPPORTED_ACTIONS;
    }

    @Override
    public List<Element> getNextList(WorkflowAction action, WorkflowContext context) {
        Preconditions.checkTrue(supportedActions().contains(action),
                "not support workflow action " + action + ", it in one of " + supportedActions());
        switch (action) {
            case COMPLETE:
                return super.getNextList(action, context);
            case CANCEL:
            case TERMINATE:
                return Collections.singletonList(context.getProcess().getEndEvent());
            default:
                throw new WorkflowException("unknown workflow action " + action);
        }
    }

    public void addListeners(List<TaskEventListener> listeners) {
        if (CollectionUtils.isEmpty(listeners)) {
            return;
        }
        for (TaskEventListener listener : listeners) {
            if (listener == null) {
                continue;
            }
            addListener(listener);
        }
    }

    public WorkflowTask addListenerProvider(ServiceTaskListenerProvider provider) {
        this.listenerProvider = provider;
        return this;
    }

    public WorkflowTask addServiceTaskType(ServiceTaskType type) {
        this.serviceTaskType = type;
        return this;
    }

    public void initListeners(WorkflowContext workflowContext) {
        if (isInit.compareAndSet(false, true)) {
            if (listenerProvider == null || serviceTaskType == null) {
                return;
            }
            Iterable<TaskEventListener> listeners = listenerProvider.get(workflowContext, serviceTaskType);
            addListeners(Lists.newArrayList(listeners));
        } else {
            log.debug("ServiceTask:{} is already init", getName());
        }
    }

}
