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

package org.apache.inlong.manager.service.workflow.newbusiness;

import java.util.List;
import org.apache.inlong.manager.common.pojo.workflow.WorkflowApproverFilterContext;
import org.apache.inlong.manager.service.core.WorkflowApproverService;
import org.apache.inlong.manager.service.workflow.ProcessName;
import org.apache.inlong.manager.service.workflow.WorkflowDefinition;
import org.apache.inlong.manager.service.workflow.newbusiness.listener.ApproveCancelProcessListener;
import org.apache.inlong.manager.service.workflow.newbusiness.listener.ApprovePassTaskListener;
import org.apache.inlong.manager.service.workflow.newbusiness.listener.ApproveRejectProcessListener;
import org.apache.inlong.manager.service.workflow.newbusiness.listener.StartCreateResourceProcessListener;
import org.apache.inlong.manager.workflow.model.definition.EndEvent;
import org.apache.inlong.manager.workflow.model.definition.Process;
import org.apache.inlong.manager.workflow.model.definition.StartEvent;
import org.apache.inlong.manager.workflow.model.definition.UserTask;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * New data access process definition
 */
@Component
public class NewBusinessWorkflowDefinition implements WorkflowDefinition {

    @Autowired
    private ApprovePassTaskListener approvePassTaskListener;
    @Autowired
    private ApproveCancelProcessListener approveCancelProcessListener;
    @Autowired
    private ApproveRejectProcessListener approveRejectProcessListener;
    @Autowired
    private StartCreateResourceProcessListener startCreateResourceProcessListener;
    @Autowired
    private WorkflowApproverService workflowApproverService;

    @Override
    public Process define() {
        // Configuration process
        Process process = new Process();
        process.setType(getName().getDisplayName());
        process.setName(getName().name());
        process.setDisplayName(getName().getDisplayName());
        process.setFormClass(NewBusinessWorkflowForm.class);
        process.setVersion(1);

        // Set up the listener
        process.addListener(approveCancelProcessListener);
        process.addListener(approveRejectProcessListener);
        // Initiate the process of creating business resources,
        // and set the business status to [Configuration Successful]/[Configuration Failed] according to its completion
        process.addListener(startCreateResourceProcessListener);

        // Start node
        StartEvent startEvent = new StartEvent();
        process.setStartEvent(startEvent);

        // End node
        EndEvent endEvent = new EndEvent();
        process.setEndEvent(endEvent);

        // System administrator approval
        UserTask adminUserTask = new UserTask();
        adminUserTask.setName("ut_admin");
        adminUserTask.setDisplayName("System Administrator");
        adminUserTask.setFormClass(NewBusinessApproveForm.class);
        adminUserTask.setApproverAssign(context -> getTaskApprovers(adminUserTask.getName()));
        adminUserTask.addListener(approvePassTaskListener);
        process.addTask(adminUserTask);

        // Configuration order relationship
        startEvent.addNext(adminUserTask);
        // If you need another approval process, you can add it here

        adminUserTask.addNext(endEvent);

        return process;
    }

    @Override
    public ProcessName getName() {
        return ProcessName.NEW_BUSINESS_WORKFLOW;
    }

    private List<String> getTaskApprovers(String taskName) {
        return workflowApproverService.getApprovers(getName().name(), taskName,
                WorkflowApproverFilterContext.builder().build());
    }

}
