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

package org.apache.inlong.manager.service.workflow.cluster;

import org.apache.inlong.manager.common.enums.ProcessName;
import org.apache.inlong.manager.pojo.workflow.form.process.ClusterResourceProcessForm;
import org.apache.inlong.manager.service.listener.ClusterTaskListenerFactory;
import org.apache.inlong.manager.service.listener.cluster.InitClusterCompleteListener;
import org.apache.inlong.manager.service.listener.cluster.InitClusterFailedListener;
import org.apache.inlong.manager.service.listener.cluster.InitClusterListener;
import org.apache.inlong.manager.service.workflow.WorkflowDefinition;
import org.apache.inlong.manager.workflow.definition.EndEvent;
import org.apache.inlong.manager.workflow.definition.ServiceTask;
import org.apache.inlong.manager.workflow.definition.ServiceTaskType;
import org.apache.inlong.manager.workflow.definition.StartEvent;
import org.apache.inlong.manager.workflow.definition.WorkflowProcess;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Sync cluster resource workflow definition
 */
@Component
@Slf4j
public class SyncClusterResourceWorkflowDefinition implements WorkflowDefinition {

    @Autowired
    private InitClusterListener initClusterListener;
    @Autowired
    private InitClusterCompleteListener initClusterCompleteListener;
    @Autowired
    private InitClusterFailedListener initClusterFailedListener;
    @Autowired
    private ClusterTaskListenerFactory clusterTaskListenerFactory;

    @Override
    public WorkflowProcess defineProcess() {
        // Configuration process
        WorkflowProcess process = new WorkflowProcess();
        process.setName(getProcessName().name());
        process.setType(getProcessName().getDisplayName());
        process.setDisplayName(getProcessName().getDisplayName());
        process.setFormClass(ClusterResourceProcessForm.class);
        process.setVersion(1);
        process.setHidden(1);

        // Set up the listener
        process.addListener(initClusterListener);
        process.addListener(initClusterFailedListener);
        process.addListener(initClusterCompleteListener);

        // Start node
        StartEvent startEvent = new StartEvent();
        process.setStartEvent(startEvent);

        // Init MQ
        ServiceTask initMQTask = new ServiceTask();
        initMQTask.setName("InitMQ");
        initMQTask.setDisplayName("Cluster-InitMQ");
        initMQTask.setServiceTaskType(ServiceTaskType.INIT_MQ);
        initMQTask.setListenerFactory(clusterTaskListenerFactory);
        process.addTask(initMQTask);

        // End node
        EndEvent endEvent = new EndEvent();
        process.setEndEvent(endEvent);

        startEvent.addNext(initMQTask);
        initMQTask.addNext(endEvent);

        return process;
    }

    @Override
    public ProcessName getProcessName() {
        return ProcessName.CREATE_CLUSTER_RESOURCE;
    }

}
