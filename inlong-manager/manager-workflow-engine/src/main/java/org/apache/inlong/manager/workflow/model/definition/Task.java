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

package org.apache.inlong.manager.workflow.model.definition;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.apache.inlong.manager.workflow.core.event.task.TaskEvent;
import org.apache.inlong.manager.workflow.core.event.task.TaskEventListener;
import org.apache.inlong.manager.workflow.exception.WorkflowListenerException;
import org.apache.inlong.manager.workflow.model.WorkflowContext;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import lombok.Data;

/**
 * Task
 */
@Data
public abstract class Task extends NextableElement implements SkippableElement {

    private boolean needAllApprove = false;
    private SkipResolver skipResolver = SkipResolver.DEFAULT_NOT_SKIP;

    private Map<TaskEvent, List<TaskEventListener>> syncListeners = Maps.newHashMap();
    private Map<TaskEvent, List<TaskEventListener>> asyncListeners = Maps.newHashMap();
    private Map<String, TaskEventListener> name2EventListenerMap = Maps.newHashMap();

    public Task addListener(TaskEventListener listener) {
        if (name2EventListenerMap.containsKey(listener.name())) {
            throw new WorkflowListenerException("duplicate listener:" + listener.name());
        }
        name2EventListenerMap.put(listener.name(), listener);

        if (listener.async()) {
            this.asyncListeners.computeIfAbsent(listener.event(), a -> Lists.newArrayList()).add(listener);
        } else {
            this.syncListeners.computeIfAbsent(listener.event(), a -> Lists.newArrayList()).add(listener);
        }
        return this;
    }

    public List<TaskEventListener> asyncListeners(TaskEvent taskEvent) {
        return this.asyncListeners.getOrDefault(taskEvent, TaskEventListener.EMPTY_LIST);
    }

    public List<TaskEventListener> syncListeners(TaskEvent taskEvent) {
        return this.syncListeners.getOrDefault(taskEvent, TaskEventListener.EMPTY_LIST);
    }

    public TaskEventListener listener(String listenerName) {
        return this.name2EventListenerMap.get(listenerName);
    }

    @Override
    public Task clone() throws CloneNotSupportedException {
        Task cloneTask = (Task) super.clone();
        cloneTask.setSyncListeners(new HashMap<>(syncListeners));
        cloneTask.setAsyncListeners(new HashMap<>(asyncListeners));
        return cloneTask;
    }

    @Override
    public boolean isSkip(WorkflowContext workflowContext) {
        return Optional.ofNullable(skipResolver)
                .map(skipResolver -> skipResolver.isSkip(workflowContext))
                .orElse(false);
    }
}
