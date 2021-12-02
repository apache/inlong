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
import org.apache.inlong.manager.common.enums.EntityStatus;
import org.apache.inlong.manager.service.core.BusinessService;
import org.apache.inlong.manager.service.core.DataStreamService;
import org.apache.inlong.manager.service.workflow.business.BusinessResourceWorkflowForm;
import org.apache.inlong.manager.workflow.core.event.ListenerResult;
import org.apache.inlong.manager.workflow.core.event.process.ProcessEvent;
import org.apache.inlong.manager.workflow.core.event.process.ProcessEventListener;
import org.apache.inlong.manager.workflow.exception.WorkflowListenerException;
import org.apache.inlong.manager.workflow.model.WorkflowContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Event listener for completed creation of data stream resource
 */
@Slf4j
@Component
public class StreamCompleteProcessListener implements ProcessEventListener {

    @Autowired
    private BusinessService businessService;
    @Autowired
    private DataStreamService streamService;

    @Override
    public ProcessEvent event() {
        return ProcessEvent.COMPLETE;
    }

    /**
     * The creation process ends normally, modify the status of business and all data stream
     * belong to the business to [CONFIG_SUCCESSFUL]
     */
    @Override
    public ListenerResult listen(WorkflowContext context) throws WorkflowListenerException {
        BusinessResourceWorkflowForm form = (BusinessResourceWorkflowForm) context.getProcessForm();
        String groupId = form.getInlongGroupId();
        String streamId = form.getInlongStreamId();
        String username = context.getApplicant();

        // update business status
        businessService.updateStatus(groupId, EntityStatus.BIZ_CONFIG_SUCCESSFUL.getCode(), username);
        // update data stream status
        streamService.updateStatus(groupId, streamId, EntityStatus.DATA_STREAM_CONFIG_SUCCESSFUL.getCode(), username);
        return ListenerResult.success();
    }

    @Override
    public boolean async() {
        return false;
    }

}
