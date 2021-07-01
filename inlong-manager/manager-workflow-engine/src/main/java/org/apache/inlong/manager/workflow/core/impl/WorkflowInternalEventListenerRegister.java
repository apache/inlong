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

import org.apache.inlong.manager.workflow.core.QueryService;
import org.apache.inlong.manager.workflow.core.event.process.ProcessEventListenerRegister;
import org.apache.inlong.manager.workflow.core.event.task.TaskEventListenerRegister;
import org.apache.inlong.manager.workflow.model.WorkflowConfig;

/**
 * Register of workflow internal event listener
 */
public class WorkflowInternalEventListenerRegister {

    private WorkflowConfig workflowConfig;

    private WorkflowEventListenerManager workflowEventListenerManager;

    private QueryService queryService;

    public WorkflowInternalEventListenerRegister(
            WorkflowConfig workflowConfig,
            WorkflowEventListenerManager workflowEventListenerManager,
            QueryService queryService) {
        this.workflowConfig = workflowConfig;
        this.workflowEventListenerManager = workflowEventListenerManager;
        this.queryService = queryService;
    }

    public void registerInternalEventListeners(WorkflowConfig workflowConfig,
            WorkflowEventListenerManager workflowEventListenerManager) {
        // Register internal process event listener
        new ProcessEventListenerRegister(workflowConfig,
                workflowEventListenerManager.getProcessEventListenerManager(),
                queryService).registerAll();

        // Register internal task event listener
        new TaskEventListenerRegister(workflowConfig,
                workflowEventListenerManager.getTaskEventListenerManager(),
                queryService).registerAll();
    }
}
