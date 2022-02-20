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
import org.apache.inlong.manager.dao.mapper.WorkflowEventLogEntityMapper;
import org.apache.inlong.manager.dao.mapper.WorkflowProcessEntityMapper;
import org.apache.inlong.manager.dao.mapper.WorkflowTaskEntityMapper;
import org.apache.inlong.manager.workflow.WorkflowConfig;
import org.apache.inlong.manager.workflow.core.EventListenerService;
import org.apache.inlong.manager.workflow.core.ProcessDefinitionRepository;
import org.apache.inlong.manager.workflow.core.ProcessDefinitionService;
import org.apache.inlong.manager.workflow.core.ProcessService;
import org.apache.inlong.manager.workflow.core.ProcessorExecutor;
import org.apache.inlong.manager.workflow.core.TaskService;
import org.apache.inlong.manager.workflow.core.TransactionHelper;
import org.apache.inlong.manager.workflow.core.WorkflowContextBuilder;
import org.apache.inlong.manager.workflow.core.WorkflowEngine;
import org.apache.inlong.manager.workflow.event.EventListenerManagerFactory;

/**
 * Workflow engine
 */
@Slf4j
public class WorkflowEngineImpl implements WorkflowEngine {

    private final ProcessDefinitionService processDefService;

    private final ProcessDefinitionRepository processDefRepository;

    private final ProcessService processService;

    private final TaskService taskService;

    private final EventListenerService eventListenerService;

    /**
     * Construct WorkflowConfig instance
     */
    public WorkflowEngineImpl(WorkflowConfig workflowConfig) {
        log.info("start init workflow engine with config: {}", JsonUtils.toJson(workflowConfig));

        // Database transaction assistant
        TransactionHelper transactionHelper = new TransactionHelper(workflowConfig.getTransactionManager());

        // Get workflow data accessor
        this.processDefRepository = workflowConfig.getDefinitionRepository();

        // Workflow event listener manager
        EventListenerManagerFactory listenerManagerFactory = new EventListenerManagerFactory(workflowConfig);

        // Workflow event notifier
        WorkflowEventNotifier workflowEventNotifier = new WorkflowEventNotifier(listenerManagerFactory);

        // Workflow component executor
        WorkflowProcessEntityMapper processEntityMapper = workflowConfig.getProcessEntityMapper();
        WorkflowTaskEntityMapper taskEntityMapper = workflowConfig.getTaskEntityMapper();
        ProcessorExecutor processorExecutor = new ProcessorExecutorImpl(processEntityMapper,
                taskEntityMapper, workflowEventNotifier, transactionHelper);

        // Workflow context builder
        WorkflowContextBuilder contextBuilder = new WorkflowContextBuilderImpl(
                processDefRepository, processEntityMapper, taskEntityMapper);

        // Workflow process definition service
        this.processDefService = new ProcessDefinitionServiceImpl(processDefRepository);

        // Workflow process instance service
        this.processService = new ProcessServiceImpl(processorExecutor, contextBuilder, taskEntityMapper);

        // Workflow task service
        this.taskService = new TaskServiceImpl(processorExecutor, contextBuilder);

        // Workflow event listener service
        WorkflowEventLogEntityMapper eventLogMapper = workflowConfig.getEventLogMapper();
        this.eventListenerService = new EventListenerServiceImpl(contextBuilder, workflowEventNotifier,
                listenerManagerFactory, eventLogMapper);

        log.info("success init workflow engine");
    }

    @Override
    public ProcessDefinitionService processDefinitionService() {
        return processDefService;
    }

    @Override
    public ProcessDefinitionRepository processDefinitionStorage() {
        return processDefRepository;
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
    public EventListenerService eventListenerService() {
        return eventListenerService;
    }

}
