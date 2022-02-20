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

package org.apache.inlong.manager.service.workflow.consumption;

import org.apache.inlong.manager.common.pojo.business.BusinessInfo;
import org.apache.inlong.manager.common.pojo.workflow.WorkflowApproverFilterContext;
import org.apache.inlong.manager.common.pojo.workflow.form.ConsumptionAdminApproveForm;
import org.apache.inlong.manager.common.pojo.workflow.form.NewConsumptionProcessForm;
import org.apache.inlong.manager.service.core.BusinessService;
import org.apache.inlong.manager.service.core.WorkflowApproverService;
import org.apache.inlong.manager.service.workflow.ProcessName;
import org.apache.inlong.manager.service.workflow.WorkflowDefinition;
import org.apache.inlong.manager.service.workflow.consumption.listener.ConsumptionCancelProcessListener;
import org.apache.inlong.manager.service.workflow.consumption.listener.ConsumptionCompleteProcessListener;
import org.apache.inlong.manager.service.workflow.consumption.listener.ConsumptionPassTaskListener;
import org.apache.inlong.manager.service.workflow.consumption.listener.ConsumptionRejectProcessListener;
import org.apache.inlong.manager.workflow.WorkflowContext;
import org.apache.inlong.manager.workflow.definition.EndEvent;
import org.apache.inlong.manager.workflow.definition.StartEvent;
import org.apache.inlong.manager.workflow.definition.UserTask;
import org.apache.inlong.manager.workflow.definition.WorkflowProcess;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * New data consumption workflow definition
 */
@Component
public class NewConsumptionWorkflowDefinition implements WorkflowDefinition {

    public static final String UT_ADMINT_NAME = "ut_admin";
    public static final String UT_BIZ_OWNER_NAME = "ut_biz_owner";

    @Autowired
    private ConsumptionCompleteProcessListener consumptionCompleteProcessListener;

    @Autowired
    private ConsumptionPassTaskListener consumptionPassTaskListener;

    @Autowired
    private ConsumptionRejectProcessListener consumptionRejectProcessListener;

    @Autowired
    private ConsumptionCancelProcessListener consumptionCancelProcessListener;

    @Autowired
    private WorkflowApproverService workflowApproverService;

    @Autowired
    private NewConsumptionProcessDetailHandler newConsumptionProcessDetailHandler;

    @Autowired
    private BusinessService businessService;

    @Override
    public WorkflowProcess defineProcess() {
        // Define process information
        WorkflowProcess process = new WorkflowProcess();
        process.setType("Data Consumption Resource Creation");
        process.setName(getProcessName().name());
        process.setDisplayName(getProcessName().getDisplayName());
        process.setFormClass(NewConsumptionProcessForm.class);
        process.setVersion(1);
        process.setProcessDetailHandler(newConsumptionProcessDetailHandler);

        // Start node
        StartEvent startEvent = new StartEvent();
        process.setStartEvent(startEvent);

        // End node
        EndEvent endEvent = new EndEvent();
        process.setEndEvent(endEvent);

        // Business approval tasks
        UserTask bizOwnerUserTask = new UserTask();
        bizOwnerUserTask.setName(UT_BIZ_OWNER_NAME);
        bizOwnerUserTask.setDisplayName("Business Approval");
        bizOwnerUserTask.setApproverAssign(this::bizOwnerUserTaskApprover);
        process.addTask(bizOwnerUserTask);

        // System administrator approval
        UserTask adminUserTask = new UserTask();
        adminUserTask.setName(UT_ADMINT_NAME);
        adminUserTask.setDisplayName("System Administrator");
        adminUserTask.setFormClass(ConsumptionAdminApproveForm.class);
        adminUserTask.setApproverAssign(this::adminUserTaskApprover);
        adminUserTask.addListener(consumptionPassTaskListener);
        process.addTask(adminUserTask);

        // Set order relationship
        startEvent.addNext(bizOwnerUserTask);
        bizOwnerUserTask.addNext(adminUserTask);
        adminUserTask.addNext(endEvent);

        // Set up the listener
        process.addListener(consumptionCompleteProcessListener);
        process.addListener(consumptionRejectProcessListener);
        process.addListener(consumptionCancelProcessListener);

        return process;
    }

    private List<String> adminUserTaskApprover(WorkflowContext context) {
        return workflowApproverService.getApprovers(getProcessName().name(), UT_ADMINT_NAME,
                new WorkflowApproverFilterContext());
    }

    private List<String> bizOwnerUserTaskApprover(WorkflowContext context) {
        NewConsumptionProcessForm form = (NewConsumptionProcessForm) context.getProcessForm();
        BusinessInfo businessInfo = businessService.get(form.getConsumptionInfo().getInlongGroupId());
        if (businessInfo == null || businessInfo.getInCharges() == null) {
            return Collections.emptyList();
        }

        return Arrays.asList(businessInfo.getInCharges().split(","));
    }

    @Override
    public ProcessName getProcessName() {
        return ProcessName.NEW_CONSUMPTION_WORKFLOW;
    }

}
