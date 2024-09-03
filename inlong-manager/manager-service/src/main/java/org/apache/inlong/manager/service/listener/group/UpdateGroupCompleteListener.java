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

package org.apache.inlong.manager.service.listener.group;

import org.apache.inlong.manager.common.consts.InlongConstants;
import org.apache.inlong.manager.common.enums.GroupOperateType;
import org.apache.inlong.manager.common.enums.GroupStatus;
import org.apache.inlong.manager.common.enums.ProcessEvent;
import org.apache.inlong.manager.common.enums.SourceStatus;
import org.apache.inlong.manager.common.enums.StreamStatus;
import org.apache.inlong.manager.pojo.group.InlongGroupInfo;
import org.apache.inlong.manager.pojo.group.InlongGroupRequest;
import org.apache.inlong.manager.pojo.stream.InlongStreamInfo;
import org.apache.inlong.manager.pojo.workflow.form.process.GroupResourceProcessForm;
import org.apache.inlong.manager.service.group.InlongGroupService;
import org.apache.inlong.manager.service.source.StreamSourceService;
import org.apache.inlong.manager.service.stream.InlongStreamService;
import org.apache.inlong.manager.workflow.WorkflowContext;
import org.apache.inlong.manager.workflow.event.ListenerResult;
import org.apache.inlong.manager.workflow.event.process.ProcessEventListener;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * The listener of InlongGroup when update operates successfully.
 */
@Slf4j
@Component
public class UpdateGroupCompleteListener implements ProcessEventListener {

    @Autowired
    private InlongGroupService groupService;
    @Autowired
    private StreamSourceService sourceService;
    @Autowired
    private InlongStreamService streamService;

    @Override
    public ProcessEvent event() {
        return ProcessEvent.COMPLETE;
    }

    @Override
    public ListenerResult listen(WorkflowContext context) {
        GroupResourceProcessForm form = (GroupResourceProcessForm) context.getProcessForm();
        String groupId = form.getInlongGroupId();
        GroupOperateType operateType = form.getGroupOperateType();
        log.info("begin to execute UpdateGroupCompleteListener for groupId={}, operateType={}", groupId, operateType);

        // update inlong group status and other configs
        InlongGroupInfo groupInfo = form.getGroupInfo();
        InlongGroupRequest groupRequest = groupInfo.genRequest();
        String operator = context.getOperator();
        List<InlongStreamInfo> streamInfos = form.getStreamInfos();
        if (CollectionUtils.isNotEmpty(streamInfos)) {
            streamInfos.forEach(streamInfo -> streamService.updateWithoutCheck(streamInfo.genRequest(), operator));
        }
        switch (operateType) {
            case SUSPEND:
                streamService.updateStatus(groupId, null, StreamStatus.CONFIG_OFFLINE_SUCCESSFUL.getCode(), operator);
                groupService.updateStatus(groupId, GroupStatus.CONFIG_OFFLINE_SUCCESSFUL.getCode(), operator);
                groupService.update(groupRequest, operator);
                break;
            case RESTART:
                streamService.updateStatus(groupId, null, StreamStatus.CONFIG_SUCCESSFUL.getCode(), operator);
                groupService.updateStatus(groupId, GroupStatus.CONFIG_SUCCESSFUL.getCode(), operator);
                groupService.update(groupRequest, operator);
                break;
            case DELETE:
                // delete process completed, then delete the group info
                groupService.delete(groupId, operator);
                break;
            default:
                log.warn("unsupported operate={} for inlong group", operateType);
                break;
        }

        // if the inlong group is dataSync mode, the stream source needs to be processed.
        if (InlongConstants.DATASYNC_REALTIME_MODE.equals(groupInfo.getInlongGroupMode())
                || InlongConstants.DATASYNC_OFFLINE_MODE.equals(groupInfo.getInlongGroupMode())) {
            changeSource4DataSync(groupId, operateType, operator);
        }

        log.info("success to execute UpdateGroupCompleteListener for groupId={}, operateType={}", groupId, operateType);
        return ListenerResult.success();
    }

    private void changeSource4DataSync(String groupId, GroupOperateType operateType, String operator) {
        switch (operateType) {
            case SUSPEND:
                sourceService.updateStatus(groupId, null, SourceStatus.SOURCE_STOP.getCode(), operator);
                break;
            case RESTART:
                sourceService.updateStatus(groupId, null, SourceStatus.SOURCE_NORMAL.getCode(), operator);
                break;
            case DELETE:
                sourceService.logicDeleteAll(groupId, null, operator);
                break;
            default:
                log.warn("unsupported operate={} for inlong group", operateType);
                break;
        }
    }

}
