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

package org.apache.inlong.manager.service.workflow;

import org.apache.inlong.manager.common.workflow.EventListenerService;
import org.apache.inlong.manager.common.workflow.ProcessDefinitionService;
import org.apache.inlong.manager.common.workflow.QueryService;
import org.apache.inlong.manager.common.workflow.WorkflowDataAccessor;
import org.apache.inlong.manager.common.workflow.WorkflowEngine;
import org.apache.inlong.manager.workflow.core.impl.MemoryProcessDefinitionStorage;
import org.apache.inlong.manager.workflow.core.impl.WorkflowDataAccessorImpl;
import org.apache.inlong.manager.workflow.core.impl.WorkflowEngineImpl;
import org.apache.inlong.manager.common.dao.EventLogStorage;
import org.apache.inlong.manager.common.dao.ProcessInstanceStorage;
import org.apache.inlong.manager.common.dao.TaskInstanceStorage;
import org.apache.inlong.manager.common.model.WorkflowConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;

/**
 * Workflow engine config
 */
@Component
@Configuration
public class WorkflowEngineConfig {

    @Autowired
    private ProcessInstanceStorage processInstanceStorage;

    @Autowired
    private TaskInstanceStorage taskInstanceStorage;

    @Autowired
    private EventLogStorage eventLogStorage;

    @Autowired
    private PlatformTransactionManager platformTransactionManager;

    @Bean
    public WorkflowDataAccessor workflowDataAccessor() {
        return new WorkflowDataAccessorImpl(
                new MemoryProcessDefinitionStorage(),
                processInstanceStorage,
                taskInstanceStorage,
                eventLogStorage
        );
    }

    @Bean
    public WorkflowEngine workflowEngineer() {
        WorkflowConfig workFlowConfig = new WorkflowConfig()
                .setWorkflowDataAccessor(this.workflowDataAccessor())
                .setPlatformTransactionManager(platformTransactionManager);

        return new WorkflowEngineImpl(workFlowConfig);
    }

    @Bean
    public QueryService queryService(WorkflowEngine workflowEngine) {
        return workflowEngine.queryService();
    }

    @Bean
    public EventListenerService eventListenerService(WorkflowEngine workflowEngine) {
        return workflowEngine.eventListenerService();
    }

    @Bean
    public ProcessDefinitionService processDefinitionService(WorkflowEngine workflowEngine) {
        return workflowEngine.processDefinitionService();
    }

}
