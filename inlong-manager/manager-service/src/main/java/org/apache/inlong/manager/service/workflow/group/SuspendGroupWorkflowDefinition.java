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

package org.apache.inlong.manager.service.workflow.group;

import lombok.extern.slf4j.Slf4j;
import org.apache.inlong.manager.common.pojo.workflow.form.UpdateGroupProcessForm;
import org.apache.inlong.manager.service.workflow.ProcessName;
import org.apache.inlong.manager.service.workflow.ServiceTaskListenerFactory;
import org.apache.inlong.manager.service.workflow.WorkflowDefinition;
import org.apache.inlong.manager.service.workflow.group.listener.UpdateGroupCompleteListener;
import org.apache.inlong.manager.service.workflow.group.listener.UpdateGroupListener;
import org.apache.inlong.manager.workflow.definition.EndEvent;
import org.apache.inlong.manager.workflow.definition.ServiceTask;
import org.apache.inlong.manager.workflow.definition.ServiceTaskType;
import org.apache.inlong.manager.workflow.definition.StartEvent;
import org.apache.inlong.manager.workflow.definition.WorkflowProcess;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Suspend inlong group process definition
 */
@Slf4j
@Component
public class SuspendGroupWorkflowDefinition implements WorkflowDefinition {

    @Autowired
    private UpdateGroupListener updateGroupListener;

    @Autowired
    private UpdateGroupCompleteListener updateGroupCompleteListener;

    @Autowired
    private ServiceTaskListenerFactory serviceTaskListenerFactory;

    @Override
    public WorkflowProcess defineProcess() {
        // Configuration process
        WorkflowProcess process = new WorkflowProcess();
        process.addListener(updateGroupListener);
        process.addListener(updateGroupCompleteListener);
        process.setType("Group Resource Suspend");
        process.setName(getProcessName().name());
        process.setDisplayName(getProcessName().getDisplayName());
        process.setFormClass(UpdateGroupProcessForm.class);
        process.setVersion(1);
        process.setHidden(1);

        // Start node
        StartEvent startEvent = new StartEvent();
        process.setStartEvent(startEvent);

        //stop datasource
        ServiceTask stopDataSourceTask = new ServiceTask();
        stopDataSourceTask.setName("stopStreamSink");
        stopDataSourceTask.setDisplayName("Group-StopSink");
        stopDataSourceTask.addServiceTaskType(ServiceTaskType.STOP_SOURCE);
        stopDataSourceTask.addListenerProvider(serviceTaskListenerFactory);
        process.addTask(stopDataSourceTask);

        //stop sort
        ServiceTask stopSortTask = new ServiceTask();
        stopSortTask.setName("stopSort");
        stopSortTask.setDisplayName("Group-StopSort");
        stopSortTask.addServiceTaskType(ServiceTaskType.STOP_SORT);
        stopSortTask.addListenerProvider(serviceTaskListenerFactory);
        process.addTask(stopSortTask);

        // End node
        EndEvent endEvent = new EndEvent();
        process.setEndEvent(endEvent);

        startEvent.addNext(stopDataSourceTask);
        stopDataSourceTask.addNext(stopSortTask);
        stopSortTask.addNext(endEvent);

        return process;
    }

    @Override
    public ProcessName getProcessName() {
        return ProcessName.SUSPEND_GROUP_PROCESS;
    }

}
