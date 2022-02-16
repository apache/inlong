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

package org.apache.inlong.manager.service.workflow.business;

import lombok.extern.slf4j.Slf4j;
import org.apache.inlong.manager.common.model.definition.EndEvent;
import org.apache.inlong.manager.common.model.definition.Process;
import org.apache.inlong.manager.common.model.definition.ServiceTask;
import org.apache.inlong.manager.common.model.definition.ServiceTaskType;
import org.apache.inlong.manager.common.model.definition.StartEvent;
import org.apache.inlong.manager.common.workflow.bussiness.UpdateBusinessWorkflowForm;
import org.apache.inlong.manager.service.workflow.ProcessName;
import org.apache.inlong.manager.service.workflow.ServiceTaskListenerFactory;
import org.apache.inlong.manager.service.workflow.WorkflowDefinition;
import org.apache.inlong.manager.service.workflow.business.listener.UpdateBusinessInfoCompleteListener;
import org.apache.inlong.manager.service.workflow.business.listener.UpdateBusinessInfoListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class RestartBusinessWorkflowDefinition implements WorkflowDefinition {

    @Autowired
    private UpdateBusinessInfoListener updateBusinessInfoListener;

    @Autowired
    private UpdateBusinessInfoCompleteListener updateBusinessInfoCompleteListener;

    @Autowired
    private ServiceTaskListenerFactory serviceTaskListenerFactory;

    @Override
    public Process defineProcess() {
        // Configuration process
        Process process = new Process();
        process.addListener(updateBusinessInfoListener);
        process.addListener(updateBusinessInfoCompleteListener);
        process.setType("Business Resource Restart");
        process.setName(getProcessName().name());
        process.setDisplayName(getProcessName().getDisplayName());
        process.setFormClass(UpdateBusinessWorkflowForm.class);
        process.setVersion(1);
        process.setHidden(true);

        // Start node
        StartEvent startEvent = new StartEvent();
        process.setStartEvent(startEvent);

        //stop datasource
        ServiceTask restartDataSourceTask = new ServiceTask();
        restartDataSourceTask.setName("restartDataSource");
        restartDataSourceTask.setDisplayName("Business-RestartDataSource");
        restartDataSourceTask.addServiceTaskType(ServiceTaskType.RESTART_SOURCE);
        restartDataSourceTask.addListenerProvider(serviceTaskListenerFactory);
        process.addTask(restartDataSourceTask);

        //stop sort
        ServiceTask restartSortTask = new ServiceTask();
        restartSortTask.setName("restartSort");
        restartSortTask.setDisplayName("Business-RestartSort");
        restartSortTask.addServiceTaskType(ServiceTaskType.RESTART_SORT);
        restartSortTask.addListenerProvider(serviceTaskListenerFactory);
        process.addTask(restartSortTask);

        // End node
        EndEvent endEvent = new EndEvent();
        process.setEndEvent(endEvent);

        startEvent.addNext(restartDataSourceTask);
        restartDataSourceTask.addNext(restartSortTask);
        restartSortTask.addNext(endEvent);

        return process;
    }

    @Override
    public ProcessName getProcessName() {
        return ProcessName.RESTART_BUSINESS_WORKFLOW;
    }
}
