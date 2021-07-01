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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.inlong.manager.common.util.Preconditions;
import org.apache.inlong.manager.workflow.core.event.process.ProcessEvent;
import org.apache.inlong.manager.workflow.core.event.process.ProcessEventListener;
import org.apache.inlong.manager.workflow.exception.WorkflowException;
import org.apache.inlong.manager.workflow.exception.WorkflowListenerException;

/**
 * Process definition
 */
@Data
@NoArgsConstructor
public class Process extends Element {

    private String type;

    private StartEvent startEvent;

    private EndEvent endEvent;

    private Map<String, Task> nameToTaskMap = Maps.newHashMap();

    private Class<? extends ProcessForm> formClass;

    private ProcessDetailHandler processDetailHandler;
    /**
     * Whether to hide, for example, some processes initiated by the system
     */
    private Boolean hidden = false;

    private Map<ProcessEvent, List<ProcessEventListener>> syncListeners = Maps.newHashMap();
    private Map<ProcessEvent, List<ProcessEventListener>> asyncListeners = Maps.newHashMap();
    private Map<String, ProcessEventListener> name2EventListenerMap = Maps.newHashMap();

    private int version;

    public Process addListener(ProcessEventListener listener) {
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

    public List<ProcessEventListener> asyncListeners(ProcessEvent processEvent) {
        return this.asyncListeners.getOrDefault(processEvent, ProcessEventListener.EMPTY_LIST);
    }

    public List<ProcessEventListener> syncListeners(ProcessEvent processEvent) {
        return this.syncListeners.getOrDefault(processEvent, ProcessEventListener.EMPTY_LIST);
    }

    public ProcessEventListener listener(String listenerName) {
        return this.name2EventListenerMap.get(listenerName);
    }

    public Process addTask(Task task) {
        if (this.nameToTaskMap.containsKey(task.getName())) {
            throw new WorkflowException("task name cannot duplicate " + task.getName());
        }
        this.nameToTaskMap.put(task.getName(), task);
        return this;
    }

    public Task getTaskByName(String name) {
        if (!this.nameToTaskMap.containsKey(name)) {
            throw new WorkflowException("cannot find task with the name " + name);
        }
        return nameToTaskMap.get(name);
    }

    @Override
    public void validate() {
        super.validate();
        Preconditions.checkNotEmpty(type, "process type cannot be empty");
        Preconditions.checkNotNull(startEvent, "start event cannot be null");
        Preconditions.checkNotNull(endEvent, "end event cannot be null");

        startEvent.validate();
        endEvent.validate();

        nameToTaskMap.values().forEach(Task::validate);
    }

    @Override
    public Process clone() throws CloneNotSupportedException {
        Process cloneProcess = (Process) super.clone();
        cloneProcess.setStartEvent((StartEvent) this.startEvent.clone());
        cloneProcess.setEndEvent((EndEvent) this.endEvent.clone());

        Map<String, Task> cloneMap = new HashMap<>();
        nameToTaskMap.forEach((k, v) -> {
            try {
                cloneMap.put(k, v.clone());
            } catch (CloneNotSupportedException e) {
                e.printStackTrace();
            }
        });
        cloneProcess.setNameToTaskMap(cloneMap);
        Map<ProcessEvent, List<ProcessEventListener>> cloneSyncListener = Maps.newHashMap();
        Map<ProcessEvent, List<ProcessEventListener>> cloneAsyncListeners = Maps.newHashMap();
        cloneSyncListener.putAll(syncListeners);
        cloneAsyncListeners.putAll(asyncListeners);
        return cloneProcess;
    }
}
