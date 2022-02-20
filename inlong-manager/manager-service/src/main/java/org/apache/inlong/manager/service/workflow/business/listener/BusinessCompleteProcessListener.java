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
import org.apache.inlong.manager.common.workflow.bussiness.BusinessResourceWorkflowForm;
import org.apache.inlong.manager.common.pojo.business.BusinessInfo;
import org.apache.inlong.manager.dao.mapper.SourceFileDetailEntityMapper;
import org.apache.inlong.manager.service.core.BusinessService;
import org.apache.inlong.manager.service.core.DataStreamService;
import org.apache.inlong.manager.common.event.ListenerResult;
import org.apache.inlong.manager.common.event.process.ProcessEvent;
import org.apache.inlong.manager.common.event.process.ProcessEventListener;
import org.apache.inlong.manager.common.exceptions.WorkflowListenerException;
import org.apache.inlong.manager.common.model.WorkflowContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Create business resources [process completion] event listener
 */
@Slf4j
@Component
public class BusinessCompleteProcessListener implements ProcessEventListener {

    @Autowired
    private BusinessService businessService;
    @Autowired
    private DataStreamService dataStreamService;
    @Autowired
    private SourceFileDetailEntityMapper fileDetailMapper;

    @Override
    public ProcessEvent event() {
        return ProcessEvent.COMPLETE;
    }

    /**
     * After the process of creating business resources is completed, modify the status of business and all data stream
     * belong to this business to [Configuration Successful] [Configuration Failed]
     * <p/>{@link BusinessFailedProcessListener#listen}
     */
    @Override
    public ListenerResult listen(WorkflowContext context) throws WorkflowListenerException {
        BusinessResourceWorkflowForm form = (BusinessResourceWorkflowForm) context.getProcessForm();

        String groupId = form.getInlongGroupId();
        String username = context.getApplicant();
        BusinessInfo businessInfo = form.getBusinessInfo();
        businessInfo.setStatus(EntityStatus.BIZ_CONFIG_SUCCESSFUL.getCode());
        // update business status and props
        businessService.update(businessInfo, username);
        // update data stream status
        dataStreamService.updateStatus(groupId, null, EntityStatus.DATA_STREAM_CONFIG_SUCCESSFUL.getCode(), username);
        // update file data source status
        fileDetailMapper.updateStatusAfterApprove(groupId, null, EntityStatus.AGENT_ADD.getCode(), username);

        return ListenerResult.success();
    }

    @Override
    public boolean async() {
        return false;
    }
}
