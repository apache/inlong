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

import org.apache.inlong.manager.workflow.core.ProcessDefinitionStorage;
import org.apache.inlong.manager.workflow.core.WorkflowDataAccessor;
import org.apache.inlong.manager.workflow.dao.EventLogStorage;
import org.apache.inlong.manager.workflow.dao.ProcessInstanceStorage;
import org.apache.inlong.manager.workflow.dao.TaskInstanceStorage;

/**
 * Workflow data accessor
 */
public class WorkflowDataAccessorImpl implements WorkflowDataAccessor {

    private ProcessDefinitionStorage processDefinitionStorage;
    private ProcessInstanceStorage processInstanceStorage;
    private TaskInstanceStorage taskInstanceStorage;
    private EventLogStorage eventLogStorage;

    public WorkflowDataAccessorImpl(
            ProcessDefinitionStorage processDefinitionStorage,
            ProcessInstanceStorage processInstanceStorage,
            TaskInstanceStorage taskInstanceStorage,
            EventLogStorage eventLogStorage) {
        this.processInstanceStorage = processInstanceStorage;
        this.taskInstanceStorage = taskInstanceStorage;
        this.eventLogStorage = eventLogStorage;
        this.processDefinitionStorage = processDefinitionStorage;
    }

    @Override
    public ProcessDefinitionStorage processDefinitionStorage() {
        return processDefinitionStorage;
    }

    @Override
    public ProcessInstanceStorage processInstanceStorage() {
        return processInstanceStorage;
    }

    @Override
    public TaskInstanceStorage taskInstanceStorage() {
        return taskInstanceStorage;
    }

    @Override
    public EventLogStorage eventLogStorage() {
        return eventLogStorage;
    }
}
