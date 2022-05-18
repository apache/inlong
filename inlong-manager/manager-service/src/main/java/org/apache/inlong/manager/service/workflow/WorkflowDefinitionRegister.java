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

import lombok.extern.slf4j.Slf4j;
import org.apache.inlong.manager.workflow.core.WorkflowEngine;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.List;

@Service
@Slf4j
public class WorkflowDefinitionRegister {

    @Autowired
    private WorkflowEngine workflowEngine;
    @Autowired
    private List<WorkflowDefinition> workflowDefinitions;

    @PostConstruct
    public void registerDefinition() {
        workflowDefinitions.forEach(definition -> {
            try {
                workflowEngine.processDefinitionService().register(definition.defineProcess());
                log.info("success register workflow definition: {}", definition.getProcessName());
            } catch (Exception e) {
                log.error("failed to register workflow definition {}", definition.getProcessName(), e);
            }
        });
    }

}
