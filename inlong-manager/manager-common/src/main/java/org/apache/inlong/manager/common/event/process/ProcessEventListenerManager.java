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

package org.apache.inlong.manager.common.event.process;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.apache.inlong.manager.common.workflow.WorkflowDataAccessor;
import org.apache.inlong.manager.common.event.EventListenerManager;
import org.apache.inlong.manager.common.dao.EventLogStorage;
import org.apache.inlong.manager.common.exceptions.WorkflowListenerException;

import java.util.List;
import java.util.Map;

/**
 * System default process event listener manager
 */
public class ProcessEventListenerManager implements EventListenerManager<ProcessEvent, ProcessEventListener> {

    private static final List<ProcessEventListener> EMPTY = Lists.newArrayList();
    private Map<ProcessEvent, List<ProcessEventListener>> syncProcessEventListeners = Maps.newHashMap();
    private Map<ProcessEvent, List<ProcessEventListener>> asyncProcessEventListeners = Maps.newHashMap();
    private Map<String, ProcessEventListener> processEventListeners = Maps.newHashMap();

    private WorkflowDataAccessor workflowDataAccessor;

    public ProcessEventListenerManager(WorkflowDataAccessor workflowDataAccessor) {
        this.workflowDataAccessor = workflowDataAccessor;
    }

    @Override
    public void register(ProcessEventListener listener) {
        if (processEventListeners.containsKey(listener.name())) {
            throw new WorkflowListenerException("duplicate listener:" + listener.name());
        }
        processEventListeners.put(listener.name(), listener);

        if (listener.async()) {
            this.asyncProcessEventListeners.computeIfAbsent(listener.event(), a -> Lists.newArrayList())
                    .add(enhanceListener(listener));
            return;
        }

        this.syncProcessEventListeners.computeIfAbsent(listener.event(), a -> Lists.newArrayList())
                .add(enhanceListener(listener));
    }

    private ProcessEventListener enhanceListener(ProcessEventListener processEventListener) {
        EventLogStorage eventLogStorage = workflowDataAccessor.eventLogStorage();
        if (eventLogStorage == null) {
            return processEventListener;
        }

        return new LogableProcessEventListener(processEventListener, eventLogStorage);
    }

    @Override
    public List<ProcessEventListener> asyncListeners(ProcessEvent event) {
        return asyncProcessEventListeners.getOrDefault(event, EMPTY);
    }

    @Override
    public List<ProcessEventListener> syncListeners(ProcessEvent event) {
        return syncProcessEventListeners.getOrDefault(event, EMPTY);
    }

    @Override
    public ProcessEventListener listener(String listenerName) {
        return processEventListeners.get(listenerName);
    }

    @Override
    public EventLogStorage eventLogStorage() {
        return workflowDataAccessor.eventLogStorage();
    }
}
