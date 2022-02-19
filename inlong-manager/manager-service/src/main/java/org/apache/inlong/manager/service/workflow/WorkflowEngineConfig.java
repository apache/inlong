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

import org.apache.inlong.manager.dao.mapper.WorkflowEventLogEntityMapper;
import org.apache.inlong.manager.dao.mapper.WorkflowProcessEntityMapper;
import org.apache.inlong.manager.dao.mapper.WorkflowTaskEntityMapper;
import org.apache.inlong.manager.workflow.WorkflowConfig;
import org.apache.inlong.manager.workflow.base.EventListenerService;
import org.apache.inlong.manager.workflow.base.ProcessDefinitionService;
import org.apache.inlong.manager.workflow.base.WorkflowEngine;
import org.apache.inlong.manager.workflow.base.WorkflowQueryService;
import org.apache.inlong.manager.workflow.base.impl.MemoryProcessDefinitionRepository;
import org.apache.inlong.manager.workflow.base.impl.WorkflowEngineImpl;
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
    private WorkflowQueryService queryService;
    @Autowired
    private MemoryProcessDefinitionRepository memoryProcessRepository;
    @Autowired
    private WorkflowProcessEntityMapper processEntityMapper;
    @Autowired
    private WorkflowTaskEntityMapper taskEntityMapper;
    @Autowired
    private WorkflowEventLogEntityMapper eventLogMapper;
    @Autowired
    private PlatformTransactionManager platformTransactionManager;

    @Bean
    public WorkflowEngine workflowEngineer() {
        WorkflowConfig workflowConfig = new WorkflowConfig()
                .setQueryService(queryService)
                .setProcessEntityMapper(processEntityMapper)
                .setTaskEntityMapper(taskEntityMapper)
                .setEventLogMapper(eventLogMapper)
                .setDefinitionRepository(memoryProcessRepository)
                .setTransactionManager(platformTransactionManager);

        return new WorkflowEngineImpl(workflowConfig);
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
