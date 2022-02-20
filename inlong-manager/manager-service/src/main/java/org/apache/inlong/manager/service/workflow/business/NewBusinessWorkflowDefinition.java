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

import org.apache.inlong.manager.common.pojo.workflow.WorkflowApproverFilterContext;
import org.apache.inlong.manager.common.pojo.workflow.form.BusinessAdminApproveForm;
import org.apache.inlong.manager.common.pojo.workflow.form.NewBusinessProcessForm;
import org.apache.inlong.manager.service.core.WorkflowApproverService;
import org.apache.inlong.manager.service.workflow.ProcessName;
import org.apache.inlong.manager.service.workflow.WorkflowDefinition;
import org.apache.inlong.manager.service.workflow.business.listener.BusinessCancelProcessListener;
import org.apache.inlong.manager.service.workflow.business.listener.BusinessPassTaskListener;
import org.apache.inlong.manager.service.workflow.business.listener.BusinessRejectProcessListener;
import org.apache.inlong.manager.service.workflow.business.listener.StartCreateResourceProcessListener;
import org.apache.inlong.manager.workflow.definition.EndEvent;
import org.apache.inlong.manager.workflow.definition.StartEvent;
import org.apache.inlong.manager.workflow.definition.UserTask;
import org.apache.inlong.manager.workflow.definition.WorkflowProcess;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * New data access process definition
 */
@Component
public class NewBusinessWorkflowDefinition implements WorkflowDefinition {

    @Autowired
    private BusinessPassTaskListener businessPassTaskListener;
    @Autowired
    private BusinessCancelProcessListener businessCancelProcessListener;
    @Autowired
    private BusinessRejectProcessListener approveRejectProcessListener;
    @Autowired
    private StartCreateResourceProcessListener startCreateResourceProcessListener;
    @Autowired
    private WorkflowApproverService workflowApproverService;

    @Override
    public WorkflowProcess defineProcess() {
        // Configuration process
        WorkflowProcess process = new WorkflowProcess();
        process.setType(getProcessName().getDisplayName());
        process.setName(getProcessName().name());
        process.setDisplayName(getProcessName().getDisplayName());
        process.setFormClass(NewBusinessProcessForm.class);
        process.setVersion(1);

        // Set up the listener
        process.addListener(businessCancelProcessListener);
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
        adminUserTask.setFormClass(BusinessAdminApproveForm.class);
        adminUserTask.setApproverAssign(context -> getTaskApprovers(adminUserTask.getName()));
        adminUserTask.addListener(businessPassTaskListener);
        process.addTask(adminUserTask);

        // Configuration order relationship
        startEvent.addNext(adminUserTask);
        // If you need another approval process, you can add it here
        adminUserTask.addNext(endEvent);

        return process;
    }

    @Override
    public ProcessName getProcessName() {
        return ProcessName.NEW_BUSINESS_WORKFLOW;
    }

    /**
     * Get task approvers by task name
     */
    private List<String> getTaskApprovers(String taskName) {
        String processName = this.getProcessName().name();
        WorkflowApproverFilterContext context = new WorkflowApproverFilterContext();
        return workflowApproverService.getApprovers(processName, taskName, context);
    }

}
