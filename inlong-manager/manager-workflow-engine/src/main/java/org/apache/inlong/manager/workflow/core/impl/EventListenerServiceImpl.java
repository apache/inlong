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

import org.apache.inlong.manager.common.workflow.EventListenerService;
import org.apache.inlong.manager.common.workflow.WorkflowContextBuilder;
import org.apache.inlong.manager.common.workflow.WorkflowDataAccessor;
import org.apache.inlong.manager.common.event.process.ProcessEvent;
import org.apache.inlong.manager.common.event.process.ProcessEventListener;
import org.apache.inlong.manager.common.event.task.TaskEvent;
import org.apache.inlong.manager.common.event.task.TaskEventListener;
import org.apache.inlong.manager.common.exceptions.WorkflowException;
import org.apache.inlong.manager.common.model.Action;
import org.apache.inlong.manager.common.model.WorkflowContext;
import org.apache.inlong.manager.common.model.definition.Element;
import org.apache.inlong.manager.common.model.definition.Process;
import org.apache.inlong.manager.common.model.definition.Task;
import org.apache.inlong.manager.common.model.instance.EventLog;
import org.apache.inlong.manager.common.util.Preconditions;

/**
 * Event listener service
 */
public class EventListenerServiceImpl implements EventListenerService {

    private WorkflowDataAccessor workflowDataAccessor;

    private WorkflowContextBuilder workflowContextBuilder;

    private WorkflowEventNotifier workflowEventNotifier;

    private WorkflowEventListenerManager workflowEventListenerManager;

    public EventListenerServiceImpl(
            WorkflowDataAccessor workflowDataAccessor,
            WorkflowContextBuilder workflowContextBuilder,
            WorkflowEventNotifier workflowEventNotifier,
            WorkflowEventListenerManager workflowEventListenerManager) {
        this.workflowDataAccessor = workflowDataAccessor;
        this.workflowContextBuilder = workflowContextBuilder;
        this.workflowEventNotifier = workflowEventNotifier;
        this.workflowEventListenerManager = workflowEventListenerManager;
    }

    @Override
    public void executeEventListener(Integer eventLogId) {
        EventLog eventLog = workflowDataAccessor.eventLogStorage().get(eventLogId);
        Preconditions.checkNotNull(eventLog, () -> "event log not exist with id:" + eventLogId);

        if (ProcessEvent.class.getSimpleName().equals(eventLog.getEventType())) {
            executeProcessEventListener(eventLog.getProcessInstId(), eventLog.getListener());
            return;
        }

        if (TaskEvent.class.getSimpleName().equals(eventLog.getEventType())) {
            executeTaskEventListener(eventLog.getTaskInstId(), eventLog.getListener());
            return;
        }

        throw new WorkflowException("unknow event type:" + eventLog.getEventType());
    }

    @Override
    public void executeProcessEventListener(Integer processInstId, String listenerName) {
        WorkflowContext context = workflowContextBuilder.buildContextForProcess(processInstId);
        ProcessEvent processEvent = getProcessEventListener(context.getProcess(), listenerName).event();
        context.setCurrentElement(getCurrentElementForProcess(context.getProcess(), processEvent));

        workflowEventNotifier.getProcessEventNotifier().notify(listenerName, true, context);
    }

    @Override
    public void executeTaskEventListener(Integer taskInstId, String listenerName) {
        WorkflowContext context = workflowContextBuilder.buildContextForTask(taskInstId, null);
        TaskEventListener taskEventListener = getTaskEventListener((Task) context.getCurrentElement(), listenerName);
        context.getActionContext().setAction(Action.fromTaskEvent(taskEventListener.event()));
        workflowEventNotifier.getTaskEventNotifier().notify(listenerName, true, context);
    }

    @Override
    public void triggerProcessEvent(Integer processInstId, ProcessEvent processEvent) {
        WorkflowContext context = workflowContextBuilder.buildContextForProcess(processInstId);
        context.setCurrentElement(getCurrentElementForProcess(context.getProcess(), processEvent));
        workflowEventNotifier.getProcessEventNotifier().notify(processEvent, context);
    }

    @Override
    public void triggerTaskEvent(Integer taskInstId, TaskEvent taskEvent) {
        WorkflowContext context = workflowContextBuilder
                .buildContextForTask(taskInstId, Action.fromTaskEvent(taskEvent));
        workflowEventNotifier.getTaskEventNotifier().notify(taskEvent, context);
    }

    private Element getCurrentElementForProcess(Process process, ProcessEvent processEvent) {
        if (ProcessEvent.CREATE.equals(processEvent)) {
            return process.getStartEvent();
        }

        return process.getEndEvent();
    }

    private ProcessEventListener getProcessEventListener(Process process, String listenerName) {
        ProcessEventListener listener = process.listener(listenerName);

        if (listener != null) {
            return listener;
        }

        listener = workflowEventListenerManager.getProcessEventListenerManager().listener(listenerName);
        Preconditions.checkNotNull(listener, "process listener not exist:" + listenerName);

        return listener;
    }

    private TaskEventListener getTaskEventListener(Task task, String listenerName) {
        TaskEventListener listener = task.listener(listenerName);

        if (listener != null) {
            return listener;
        }

        listener = workflowEventListenerManager.getTaskEventListenerManager().listener(listenerName);
        Preconditions.checkNotNull(listener, "task listener not exist:" + listenerName);

        return listener;
    }
}
