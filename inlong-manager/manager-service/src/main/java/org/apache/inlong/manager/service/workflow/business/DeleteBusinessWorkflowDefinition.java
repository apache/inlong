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
import org.apache.inlong.manager.common.pojo.workflow.form.UpdateBusinessProcessForm;
import org.apache.inlong.manager.service.workflow.ProcessName;
import org.apache.inlong.manager.service.workflow.ServiceTaskListenerFactory;
import org.apache.inlong.manager.service.workflow.WorkflowDefinition;
import org.apache.inlong.manager.service.workflow.business.listener.UpdateBusinessInfoCompleteListener;
import org.apache.inlong.manager.service.workflow.business.listener.UpdateBusinessInfoListener;
import org.apache.inlong.manager.workflow.definition.EndEvent;
import org.apache.inlong.manager.workflow.definition.ServiceTask;
import org.apache.inlong.manager.workflow.definition.ServiceTaskType;
import org.apache.inlong.manager.workflow.definition.StartEvent;
import org.apache.inlong.manager.workflow.definition.WorkflowProcess;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class DeleteBusinessWorkflowDefinition implements WorkflowDefinition {

    @Autowired
    private UpdateBusinessInfoListener updateBusinessInfoListener;

    @Autowired
    private UpdateBusinessInfoCompleteListener updateBusinessInfoCompleteListener;

    @Autowired
    private ServiceTaskListenerFactory serviceTaskListenerFactory;

    @Override
    public WorkflowProcess defineProcess() {
        // Configuration process
        WorkflowProcess process = new WorkflowProcess();
        process.addListener(updateBusinessInfoListener);
        process.addListener(updateBusinessInfoCompleteListener);
        process.setType("Business Resource Delete");
        process.setName(getProcessName().name());
        process.setDisplayName(getProcessName().getDisplayName());
        process.setFormClass(UpdateBusinessProcessForm.class);
        process.setVersion(1);
        process.setHidden(1);

        // Start node
        StartEvent startEvent = new StartEvent();
        process.setStartEvent(startEvent);

        //stop datasource
        ServiceTask deleteDataSourceTask = new ServiceTask();
        deleteDataSourceTask.setName("deleteDataSource");
        deleteDataSourceTask.setDisplayName("Business-DeleteDataSource");
        deleteDataSourceTask.addServiceTaskType(ServiceTaskType.DELETE_SOURCE);
        deleteDataSourceTask.addListenerProvider(serviceTaskListenerFactory);
        process.addTask(deleteDataSourceTask);

        //stop sort
        ServiceTask deleteSortTask = new ServiceTask();
        deleteSortTask.setName("deleteSort");
        deleteSortTask.setDisplayName("Business-DeleteSort");
        deleteSortTask.addServiceTaskType(ServiceTaskType.DELETE_SORT);
        deleteSortTask.addListenerProvider(serviceTaskListenerFactory);
        process.addTask(deleteSortTask);

        // End node
        EndEvent endEvent = new EndEvent();
        process.setEndEvent(endEvent);

        startEvent.addNext(deleteDataSourceTask);
        deleteDataSourceTask.addNext(deleteSortTask);
        deleteSortTask.addNext(endEvent);

        return process;
    }

    @Override
    public ProcessName getProcessName() {
        return ProcessName.DELETE_BUSINESS_WORKFLOW;
    }

}
