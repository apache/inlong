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

package org.apache.inlong.manager.service.workflow.stream;

import lombok.extern.slf4j.Slf4j;
import org.apache.inlong.manager.common.pojo.workflow.form.StreamResourceProcessForm;
import org.apache.inlong.manager.service.workflow.ProcessName;
import org.apache.inlong.manager.service.workflow.WorkflowDefinition;
import org.apache.inlong.manager.service.workflow.listener.StreamTaskListenerFactory;
import org.apache.inlong.manager.service.workflow.stream.listener.StreamCompleteProcessListener;
import org.apache.inlong.manager.service.workflow.stream.listener.StreamFailedProcessListener;
import org.apache.inlong.manager.service.workflow.stream.listener.StreamInitProcessListener;
import org.apache.inlong.manager.workflow.definition.EndEvent;
import org.apache.inlong.manager.workflow.definition.ServiceTask;
import org.apache.inlong.manager.workflow.definition.ServiceTaskType;
import org.apache.inlong.manager.workflow.definition.StartEvent;
import org.apache.inlong.manager.workflow.definition.WorkflowProcess;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Inlong stream access resource creation
 */
@Component
@Slf4j
public class CreateStreamWorkflowDefinition implements WorkflowDefinition {

    @Autowired
    private StreamInitProcessListener streamInitProcessListener;
    @Autowired
    private StreamFailedProcessListener streamFailedProcessListener;
    @Autowired
    private StreamCompleteProcessListener streamCompleteProcessListener;
    @Autowired
    private StreamTaskListenerFactory streamTaskListenerFactory;

    @Override
    public WorkflowProcess defineProcess() {
        // Configuration process
        WorkflowProcess process = new WorkflowProcess();
        process.addListener(streamInitProcessListener);
        process.addListener(streamFailedProcessListener);
        process.addListener(streamCompleteProcessListener);

        process.setType("Stream resource creation");
        process.setName(getProcessName().name());
        process.setDisplayName(getProcessName().getDisplayName());
        process.setFormClass(StreamResourceProcessForm.class);
        process.setVersion(1);
        process.setHidden(1);

        // Start node
        StartEvent startEvent = new StartEvent();
        process.setStartEvent(startEvent);

        // init DataSource
        ServiceTask initDataSourceTask = new ServiceTask();
        initDataSourceTask.setName("initSource");
        initDataSourceTask.setDisplayName("Stream-InitSource");
        initDataSourceTask.addServiceTaskType(ServiceTaskType.INIT_SOURCE);
        initDataSourceTask.addListenerProvider(streamTaskListenerFactory);
        process.addTask(initDataSourceTask);

        // init MQ topic
        ServiceTask initMQResourceTask = new ServiceTask();
        initMQResourceTask.setName("initMQ");
        initMQResourceTask.setDisplayName("Stream-InitMQ");
        initMQResourceTask.addServiceTaskType(ServiceTaskType.INIT_MQ);
        initMQResourceTask.addListenerProvider(streamTaskListenerFactory);
        process.addTask(initMQResourceTask);

        // init sort config
        ServiceTask initSortResourceTask = new ServiceTask();
        initSortResourceTask.setName("initSort");
        initSortResourceTask.setDisplayName("Stream-InitSort");
        initSortResourceTask.addServiceTaskType(ServiceTaskType.INIT_SORT);
        initSortResourceTask.addListenerProvider(streamTaskListenerFactory);
        process.addTask(initSortResourceTask);

        // init DataSink
        ServiceTask initSinkTask = new ServiceTask();
        initSinkTask.setName("initSink");
        initSinkTask.setDisplayName("Stream-InitSink");
        initSinkTask.addServiceTaskType(ServiceTaskType.INIT_SINK);
        initSinkTask.addListenerProvider(streamTaskListenerFactory);
        process.addTask(initSinkTask);

        // End node
        EndEvent endEvent = new EndEvent();
        process.setEndEvent(endEvent);

        startEvent.addNext(initDataSourceTask);
        initDataSourceTask.addNext(initMQResourceTask);
        initMQResourceTask.addNext(initSortResourceTask);
        initSortResourceTask.addNext(initSinkTask);
        initSinkTask.addNext(endEvent);

        return process;
    }

    @Override
    public ProcessName getProcessName() {
        return ProcessName.CREATE_STREAM_RESOURCE;
    }

}
