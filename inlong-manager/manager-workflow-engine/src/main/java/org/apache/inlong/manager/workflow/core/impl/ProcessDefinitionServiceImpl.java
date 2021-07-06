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

import org.apache.inlong.manager.workflow.core.ProcessDefinitionService;
import org.apache.inlong.manager.workflow.core.WorkflowDataAccessor;
import org.apache.inlong.manager.workflow.model.definition.Process;
import org.apache.inlong.manager.common.util.Preconditions;

/**
 * Process definition service
 */
public class ProcessDefinitionServiceImpl implements ProcessDefinitionService {

    private WorkflowDataAccessor workflowDataAccessor;

    public ProcessDefinitionServiceImpl(
            WorkflowDataAccessor workflowDataAccessor) {
        this.workflowDataAccessor = workflowDataAccessor;
    }

    @Override
    public void register(Process process) {
        //check process definition
        Preconditions.checkNotNull(process, "process cannot be null");
        process.validate();
        // save process definition
        this.workflowDataAccessor.processDefinitionStorage().add(process);
    }

    @Override
    public Process getByName(String name) {
        return this.workflowDataAccessor.processDefinitionStorage().get(name);
    }

}
