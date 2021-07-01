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

package org.apache.inlong.manager.service.workflow.newbusiness.listener;

import lombok.extern.slf4j.Slf4j;
import org.apache.inlong.manager.service.core.BusinessService;
import org.apache.inlong.manager.service.workflow.ProcessName;
import org.apache.inlong.manager.service.workflow.WorkflowService;
import org.apache.inlong.manager.service.workflow.newbusiness.CreateResourceWorkflowForm;
import org.apache.inlong.manager.service.workflow.newbusiness.NewBusinessWorkflowForm;
import org.apache.inlong.manager.workflow.core.event.ListenerResult;
import org.apache.inlong.manager.workflow.core.event.process.ProcessEvent;
import org.apache.inlong.manager.workflow.core.event.process.ProcessEventListener;
import org.apache.inlong.manager.workflow.exception.WorkflowListenerException;
import org.apache.inlong.manager.workflow.model.WorkflowContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * After the new business is approved, initiate a listener for other processes
 */
@Slf4j
@Component
public class StartCreateResourceProcessListener implements ProcessEventListener {

    @Autowired
    private BusinessService businessService;
    @Autowired
    private WorkflowService workflowService;

    @Override
    public ProcessEvent event() {
        return ProcessEvent.COMPLETE;
    }

    /**
     * Initiate the process of creating business resources after new business access approved
     */
    @Override
    public ListenerResult listen(WorkflowContext context) throws WorkflowListenerException {
        NewBusinessWorkflowForm workflowForm = (NewBusinessWorkflowForm) context.getProcessForm();

        String bid = workflowForm.getBusinessId();
        CreateResourceWorkflowForm resourceWorkflowForm = new CreateResourceWorkflowForm();
        resourceWorkflowForm.setBusinessInfo(businessService.get(bid));
        String username = context.getApplicant();
        workflowService.start(ProcessName.CREATE_BUSINESS_RESOURCE, username, resourceWorkflowForm);

        return ListenerResult.success();
    }

    @Override
    public boolean async() {
        return true;
    }
}
