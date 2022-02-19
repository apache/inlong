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
import org.apache.inlong.manager.service.workflow.ProcessName;
import org.apache.inlong.manager.service.workflow.ServiceTaskListenerFactory;
import org.apache.inlong.manager.service.workflow.WorkflowDefinition;
import org.apache.inlong.manager.service.workflow.business.listener.BusinessCompleteProcessListener;
import org.apache.inlong.manager.service.workflow.business.listener.BusinessFailedProcessListener;
import org.apache.inlong.manager.service.workflow.business.listener.InitBusinessInfoListener;
import org.apache.inlong.manager.workflow.definition.EndEvent;
import org.apache.inlong.manager.workflow.definition.ServiceTask;
import org.apache.inlong.manager.workflow.definition.ServiceTaskType;
import org.apache.inlong.manager.workflow.definition.StartEvent;
import org.apache.inlong.manager.workflow.definition.WorkflowProcess;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Create workflow definitions for business resources
 */
@Slf4j
@Component
public class CreateBusinessWorkflowDefinition implements WorkflowDefinition {

    @Autowired
    private InitBusinessInfoListener initBusinessInfoListener;
    @Autowired
    private BusinessCompleteProcessListener businessCompleteProcessListener;
    @Autowired
    private BusinessFailedProcessListener businessFailedProcessListener;
    @Autowired
    private ServiceTaskListenerFactory serviceTaskListenerFactory;

    @Override
    public WorkflowProcess defineProcess() {

        // Configuration process
        WorkflowProcess process = new WorkflowProcess();
        process.addListener(initBusinessInfoListener);
        process.addListener(businessCompleteProcessListener);
        process.addListener(businessFailedProcessListener);

        process.setType("Business Resource Creation");
        process.setName(getProcessName().name());
        process.setDisplayName(getProcessName().getDisplayName());
        process.setFormClass(BusinessResourceProcessForm.class);
        process.setVersion(1);
        process.setHidden(1);

        // Start node
        StartEvent startEvent = new StartEvent();
        process.setStartEvent(startEvent);

        // init DataSource
        ServiceTask initDataSourceTask = new ServiceTask();
        initDataSourceTask.setName("initDataSource");
        initDataSourceTask.setDisplayName("Business-InitDataSource");
        initDataSourceTask.addServiceTaskType(ServiceTaskType.INIT_SOURCE);
        initDataSourceTask.addListenerProvider(serviceTaskListenerFactory);
        process.addTask(initDataSourceTask);

        // init MQ resource
        ServiceTask initMQResourceTask = new ServiceTask();
        initMQResourceTask.setName("initMQ");
        initMQResourceTask.setDisplayName("Business-InitMQ");
        initMQResourceTask.addServiceTaskType(ServiceTaskType.INIT_MQ);
        initMQResourceTask.addListenerProvider(serviceTaskListenerFactory);
        process.addTask(initMQResourceTask);

        // init Sort resource
        ServiceTask initSortResourceTask = new ServiceTask();
        initSortResourceTask.setName("initSort");
        initSortResourceTask.setDisplayName("Business-InitSort");
        initSortResourceTask.addServiceTaskType(ServiceTaskType.INIT_SORT);
        initSortResourceTask.addListenerProvider(serviceTaskListenerFactory);
        process.addTask(initSortResourceTask);

        // init storage
        ServiceTask initStorageTask = new ServiceTask();
        initStorageTask.setName("initStorage");
        initStorageTask.setDisplayName("Business-InitStorage");
        initStorageTask.addServiceTaskType(ServiceTaskType.INIT_STORAGE);
        initStorageTask.addListenerProvider(serviceTaskListenerFactory);
        process.addTask(initStorageTask);

        // End node
        EndEvent endEvent = new EndEvent();
        process.setEndEvent(endEvent);

        startEvent.addNext(initDataSourceTask);
        initDataSourceTask.addNext(initMQResourceTask);
        initMQResourceTask.addNext(initSortResourceTask);
        initSortResourceTask.addNext(initStorageTask);
        initStorageTask.addNext(endEvent);

        return process;
    }

    @Override
    public ProcessName getProcessName() {
        return ProcessName.CREATE_BUSINESS_RESOURCE;
    }

}
