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

package org.apache.inlong.manager.service.workflow.business.listener;

import lombok.extern.slf4j.Slf4j;
import org.apache.inlong.manager.common.enums.EntityStatus;
import org.apache.inlong.manager.common.exceptions.WorkflowListenerException;
import org.apache.inlong.manager.service.core.BusinessService;
import org.apache.inlong.manager.service.workflow.business.NewBusinessProcessForm;
import org.apache.inlong.manager.workflow.WorkflowContext;
import org.apache.inlong.manager.workflow.event.ListenerResult;
import org.apache.inlong.manager.workflow.event.process.ProcessEvent;
import org.apache.inlong.manager.workflow.event.process.ProcessEventListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Approve reject listener for new business
 */
@Slf4j
@Component
public class BusinessRejectProcessListener implements ProcessEventListener {

    @Autowired
    private BusinessService businessService;

    @Override
    public ProcessEvent event() {
        return ProcessEvent.REJECT;
    }

    @Override
    public ListenerResult listen(WorkflowContext context) throws WorkflowListenerException {
        NewBusinessProcessForm form = (NewBusinessProcessForm) context.getProcessForm();
        // after reject, update business status to [BIZ_APPROVE_REJECT]
        String groupId = form.getInlongGroupId();
        String username = context.getApplicant();
        businessService.updateStatus(groupId, EntityStatus.BIZ_APPROVE_REJECTED.getCode(), username);
        return ListenerResult.success();
    }

    @Override
    public boolean async() {
        return false;
    }

}
