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
import org.apache.inlong.manager.common.exceptions.WorkflowListenerException;
import org.apache.inlong.manager.common.pojo.stream.InlongStreamInfo;
import org.apache.inlong.manager.common.pojo.workflow.form.GroupResourceProcessForm;
import org.apache.inlong.manager.common.pojo.workflow.form.NewGroupProcessForm;
import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.dao.entity.InlongStreamEntity;
import org.apache.inlong.manager.dao.mapper.InlongStreamEntityMapper;
import org.apache.inlong.manager.service.core.InlongGroupService;
import org.apache.inlong.manager.service.workflow.ProcessName;
import org.apache.inlong.manager.service.workflow.WorkflowService;
import org.apache.inlong.manager.workflow.WorkflowContext;
import org.apache.inlong.manager.workflow.event.ListenerResult;
import org.apache.inlong.manager.workflow.event.process.ProcessEvent;
import org.apache.inlong.manager.workflow.event.process.ProcessEventListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * After the new inlong group is approved, initiate a listener for other processes
 */
@Slf4j
@Component
public class StartCreateGroupProcessListener implements ProcessEventListener {

    @Autowired
    private InlongGroupService groupService;
    @Autowired
    private WorkflowService workflowService;

    @Autowired
    private InlongStreamEntityMapper streamMapper;

    @Override
    public ProcessEvent event() {
        return ProcessEvent.COMPLETE;
    }

    /**
     * Initiate the process of creating inlong group resources after new inlong group access approved
     */
    @Override
    public ListenerResult listen(WorkflowContext context) throws WorkflowListenerException {
        NewGroupProcessForm form = (NewGroupProcessForm) context.getProcessForm();

        String groupId = form.getInlongGroupId();
        GroupResourceProcessForm processForm = new GroupResourceProcessForm();
        processForm.setGroupInfo(groupService.get(groupId));
        String username = context.getApplicant();
        List<InlongStreamEntity> inlongStreamEntityList = streamMapper.selectByGroupId(groupId);
        List<InlongStreamInfo> inlongStreamInfoList = CommonBeanUtils.copyListProperties(inlongStreamEntityList,
                InlongStreamInfo::new);
        processForm.setInlongStreamInfoList(inlongStreamInfoList);
        workflowService.start(ProcessName.CREATE_GROUP_RESOURCE, username, processForm);
        return ListenerResult.success();
    }

    @Override
    public boolean async() {
        return true;
    }

}
