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
import org.apache.inlong.manager.common.enums.GroupState;
import org.apache.inlong.manager.common.pojo.group.InlongGroupInfo;
import org.apache.inlong.manager.common.pojo.workflow.form.UpdateGroupProcessForm;
import org.apache.inlong.manager.common.pojo.workflow.form.UpdateGroupProcessForm.OperateType;
import org.apache.inlong.manager.service.core.InlongGroupService;
import org.apache.inlong.manager.workflow.WorkflowContext;
import org.apache.inlong.manager.workflow.event.ListenerResult;
import org.apache.inlong.manager.workflow.event.process.ProcessEvent;
import org.apache.inlong.manager.workflow.event.process.ProcessEventListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Update completed listener for inlong group
 */
@Slf4j
@Component
public class UpdateGroupCompleteListener implements ProcessEventListener {

    @Autowired
    private InlongGroupService groupService;

    @Override
    public ProcessEvent event() {
        return ProcessEvent.COMPLETE;
    }

    @Override
    public ListenerResult listen(WorkflowContext context) throws Exception {
        UpdateGroupProcessForm form = (UpdateGroupProcessForm) context.getProcessForm();
        String username = context.getApplicant();
        OperateType operateType = form.getOperateType();
        InlongGroupInfo groupInfo = form.getGroupInfo();
        Integer nextStatus;
        switch (operateType) {
            case RESTART:
                nextStatus = GroupState.GROUP_RESTART.getCode();
                break;
            case SUSPEND:
                nextStatus = GroupState.GROUP_SUSPEND.getCode();
                break;
            case DELETE:
                nextStatus = GroupState.GROUP_DELETE.getCode();
                break;
            default:
                throw new RuntimeException(String.format("Unsupported operation=%s for Inlong group", operateType));
        }
        groupService.updateStatus(groupInfo.getInlongGroupId(), nextStatus, username);
        return ListenerResult.success();
    }

    @Override
    public boolean async() {
        return false;
    }

}
