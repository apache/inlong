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

import lombok.extern.slf4j.Slf4j;
import org.apache.inlong.manager.common.util.JsonUtils;
import org.apache.inlong.manager.common.workflow.EventListenerService;
import org.apache.inlong.manager.common.workflow.ProcessDefinitionService;
import org.apache.inlong.manager.common.workflow.ProcessService;
import org.apache.inlong.manager.common.workflow.QueryService;
import org.apache.inlong.manager.common.workflow.TaskService;
import org.apache.inlong.manager.common.workflow.TransactionHelper;
import org.apache.inlong.manager.common.workflow.WorkflowContextBuilder;
import org.apache.inlong.manager.common.workflow.WorkflowDataAccessor;
import org.apache.inlong.manager.common.workflow.WorkflowEngine;
import org.apache.inlong.manager.common.workflow.WorkflowProcessorExecutor;
import org.apache.inlong.manager.common.model.WorkflowConfig;

/**
 * Workflow engine
 */
@Slf4j
public class WorkflowEngineImpl implements WorkflowEngine {

    private ProcessDefinitionService processDefinitionService;

    private ProcessService processService;

    private TaskService taskService;

    private QueryService queryService;

    private EventListenerService eventListenerService;

    private WorkflowDataAccessor workflowDataAccessor;

    /**
     * Construct WorkflowConfig instance
     */
    public WorkflowEngineImpl(WorkflowConfig workflowConfig) {
        log.info("start init workflow engine with config");
        if (log.isDebugEnabled()) {
            log.debug("config:{}", JsonUtils.toJson(workflowConfig));
        }
        // Database transaction assistant
        TransactionHelper transactionHelper = new TransactionHelper(workflowConfig.getPlatformTransactionManager());

        // Get workflow data accessor
        this.workflowDataAccessor = workflowConfig.getWorkflowDataAccessor();

        // Workflow event listener manager
        WorkflowEventListenerManager workflowEventListenerManager = new WorkflowEventListenerManager(workflowConfig);

        // Workflow event notifier
        WorkflowEventNotifier workflowEventNotifier = new WorkflowEventNotifier(workflowEventListenerManager);

        // Workflow component executor
        WorkflowProcessorExecutor workflowProcessorExecutor = new WorkflowProcessorExecutorImpl(workflowDataAccessor,
                workflowEventNotifier, transactionHelper);

        // Workflow context builder
        WorkflowContextBuilder workflowContextBuilder = new WorkflowContextBuilderImpl(
                workflowConfig.getWorkflowDataAccessor());

        // Workflow query service
        this.queryService = new QueryServiceImpl(workflowConfig.getWorkflowDataAccessor());

        // Workflow process definition service
        this.processDefinitionService = new ProcessDefinitionServiceImpl(workflowDataAccessor);

        // Workflow process instance service
        this.processService = new ProcessServiceImpl(workflowProcessorExecutor, workflowContextBuilder,
                workflowDataAccessor);

        // Workflow task service
        this.taskService = new TaskServiceImpl(workflowProcessorExecutor, workflowContextBuilder);

        // Workflow event listener service
        this.eventListenerService = new EventListenerServiceImpl(workflowConfig.getWorkflowDataAccessor(),
                workflowContextBuilder, workflowEventNotifier, workflowEventListenerManager);

        // Register internal event listener
        new WorkflowInternalEventListenerRegister(workflowConfig, workflowEventListenerManager, queryService)
                .registerInternalEventListeners(workflowConfig, workflowEventListenerManager);

        log.info("success init workflow engine");
    }

    @Override
    public ProcessDefinitionService processDefinitionService() {
        return processDefinitionService;
    }

    @Override
    public ProcessService processService() {
        return processService;
    }

    @Override
    public TaskService taskService() {
        return taskService;
    }

    @Override
    public QueryService queryService() {
        return queryService;
    }

    @Override
    public EventListenerService eventListenerService() {
        return eventListenerService;
    }

    @Override
    public WorkflowDataAccessor workflowDataAccessor() {
        return workflowDataAccessor;
    }
}
