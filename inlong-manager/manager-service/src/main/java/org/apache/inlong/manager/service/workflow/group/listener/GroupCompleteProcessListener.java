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

package org.apache.inlong.manager.service.workflow.group.listener;

import lombok.extern.slf4j.Slf4j;
import org.apache.inlong.manager.common.enums.EntityStatus;
import org.apache.inlong.manager.common.exceptions.WorkflowListenerException;
import org.apache.inlong.manager.common.pojo.group.InlongGroupRequest;
import org.apache.inlong.manager.common.pojo.workflow.form.GroupResourceProcessForm;
import org.apache.inlong.manager.dao.mapper.SourceFileDetailEntityMapper;
import org.apache.inlong.manager.service.core.InlongGroupService;
import org.apache.inlong.manager.service.core.InlongStreamService;
import org.apache.inlong.manager.workflow.WorkflowContext;
import org.apache.inlong.manager.workflow.event.ListenerResult;
import org.apache.inlong.manager.workflow.event.process.ProcessEvent;
import org.apache.inlong.manager.workflow.event.process.ProcessEventListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Create group resources [process completion] event listener
 */
@Slf4j
@Component
public class GroupCompleteProcessListener implements ProcessEventListener {

    @Autowired
    private InlongGroupService groupService;
    @Autowired
    private InlongStreamService streamService;
    @Autowired
    private SourceFileDetailEntityMapper fileDetailMapper;

    @Override
    public ProcessEvent event() {
        return ProcessEvent.COMPLETE;
    }

    /**
     * After the process of creating inlong group resources is completed, modify the status of related
     * inlong group and all inlong stream to [Configuration Successful] [Configuration Failed]
     * <p/>{@link GroupFailedProcessListener#listen}
     */
    @Override
    public ListenerResult listen(WorkflowContext context) throws WorkflowListenerException {
        GroupResourceProcessForm form = (GroupResourceProcessForm) context.getProcessForm();

        String groupId = form.getInlongGroupId();
        String username = context.getApplicant();
        InlongGroupRequest groupInfo = form.getGroupInfo();
        groupInfo.setStatus(EntityStatus.GROUP_CONFIG_SUCCESSFUL.getCode());
        // update inlong group status and props
        groupService.update(groupInfo, username);
        // update inlong stream status
        streamService.updateStatus(groupId, null, EntityStatus.STREAM_CONFIG_SUCCESSFUL.getCode(), username);
        // update file data source status
        fileDetailMapper.updateStatusAfterApprove(groupId, null, EntityStatus.AGENT_ADD.getCode(), username);

        return ListenerResult.success();
    }

    @Override
    public boolean async() {
        return false;
    }

}
