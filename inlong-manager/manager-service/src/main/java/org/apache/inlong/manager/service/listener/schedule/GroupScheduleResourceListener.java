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

package org.apache.inlong.manager.service.listener.schedule;

import org.apache.inlong.manager.common.consts.InlongConstants;
import org.apache.inlong.manager.common.enums.GroupOperateType;
import org.apache.inlong.manager.common.enums.TaskEvent;
import org.apache.inlong.manager.pojo.group.InlongGroupExtInfo;
import org.apache.inlong.manager.pojo.group.InlongGroupInfo;
import org.apache.inlong.manager.pojo.workflow.form.process.GroupResourceProcessForm;
import org.apache.inlong.manager.pojo.workflow.form.process.ProcessForm;
import org.apache.inlong.manager.service.schedule.ScheduleOperator;
import org.apache.inlong.manager.workflow.WorkflowContext;
import org.apache.inlong.manager.workflow.event.ListenerResult;
import org.apache.inlong.manager.workflow.event.task.ScheduleOperateListener;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
@Slf4j
public class GroupScheduleResourceListener implements ScheduleOperateListener {

    @Autowired
    private ScheduleOperator scheduleOperator;

    @Override
    public TaskEvent event() {
        return TaskEvent.COMPLETE;
    }

    @Override
    public boolean accept(WorkflowContext context) {
        ProcessForm processForm = context.getProcessForm();
        String groupId = processForm.getInlongGroupId();
        if (!(processForm instanceof GroupResourceProcessForm)) {
            log.info("not add schedule group listener, not groupResourceProcessForm for groupId [{}]", groupId);
            return false;
        }

        GroupResourceProcessForm groupProcessForm = (GroupResourceProcessForm) processForm;
        if (groupProcessForm.getGroupOperateType() != GroupOperateType.INIT) {
            log.info("not add schedule group listener, as the operate was not INIT for groupId [{}]", groupId);
            return false;
        }

        log.info("add group schedule resource listener for groupId [{}]", groupId);
        return InlongConstants.DATASYNC_OFFLINE_MODE.equals(groupProcessForm.getGroupInfo().getInlongGroupMode());
    }

    @Override
    public ListenerResult listen(WorkflowContext context) throws Exception {
        GroupResourceProcessForm form = (GroupResourceProcessForm) context.getProcessForm();
        InlongGroupInfo groupInfo = form.getGroupInfo();
        final String groupId = groupInfo.getInlongGroupId();
        log.info("begin to process schedule resource for groupId={}", groupId);

        // handle schedule info after group approved
        scheduleOperator.handleGroupApprove(groupId);

        // after register schedule info successfully, add ext property to group ext info
        saveInfo(groupInfo, InlongConstants.REGISTER_SCHEDULE_STATUS,
                InlongConstants.REGISTERED, groupInfo.getExtList());
        log.info("success to process schedule resource for group={}", groupId);
        return ListenerResult.success();
    }

    /**
     * Save stream ext info into list.
     */
    private void saveInfo(InlongGroupInfo streamInfo, String keyName, String keyValue,
            List<InlongGroupExtInfo> extInfoList) {
        InlongGroupExtInfo extInfo = new InlongGroupExtInfo();
        extInfo.setInlongGroupId(streamInfo.getInlongGroupId());
        extInfo.setKeyName(keyName);
        extInfo.setKeyValue(keyValue);
        extInfoList.add(extInfo);
    }
}
