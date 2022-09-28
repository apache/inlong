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

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.inlong.manager.common.consts.InlongConstants;
import org.apache.inlong.manager.common.enums.GroupOperateType;
import org.apache.inlong.manager.common.enums.GroupStatus;
import org.apache.inlong.manager.common.enums.ProcessEvent;
import org.apache.inlong.manager.common.enums.ProcessName;
import org.apache.inlong.manager.common.enums.SourceStatus;
import org.apache.inlong.manager.pojo.group.InlongGroupInfo;
import org.apache.inlong.manager.pojo.stream.InlongStreamInfo;
import org.apache.inlong.manager.pojo.workflow.form.process.GroupResourceProcessForm;
import org.apache.inlong.manager.pojo.workflow.form.process.StreamResourceProcessForm;
import org.apache.inlong.manager.service.group.InlongGroupService;
import org.apache.inlong.manager.service.source.StreamSourceService;
import org.apache.inlong.manager.service.workflow.WorkflowService;
import org.apache.inlong.manager.workflow.WorkflowContext;
import org.apache.inlong.manager.workflow.event.ListenerResult;
import org.apache.inlong.manager.workflow.event.process.ProcessEventListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy;
import java.util.concurrent.TimeUnit;

/**
 * The listener of InlongGroup when update operates successfully.
 */
@Slf4j
@Component
public class UpdateGroupCompleteListener implements ProcessEventListener {

    private final ExecutorService executorService = new ThreadPoolExecutor(
            20,
            40,
            0L,
            TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>(),
            new ThreadFactoryBuilder().setNameFormat("inlong-stream-process-%s").build(),
            new CallerRunsPolicy());
    @Autowired
    private InlongGroupService groupService;
    @Autowired
    private StreamSourceService sourceService;
    @Autowired
    private WorkflowService workflowService;

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
        String operator = context.getOperator();
        switch (operateType) {
            case SUSPEND:
                groupService.updateStatus(groupId, GroupStatus.SUSPENDED.getCode(), operator);
                break;
            case RESTART:
                groupService.updateStatus(groupId, GroupStatus.RESTARTED.getCode(), operator);
                break;
            case DELETE:
                groupService.updateStatus(groupId, GroupStatus.DELETED.getCode(), operator);
                break;
            default:
                log.warn("unsupported operate={} for inlong group", operateType);
                break;
        }
        InlongGroupInfo groupInfo = form.getGroupInfo();
        groupService.update(groupInfo.genRequest(), operator);
        if (Objects.equals(operateType, GroupOperateType.DELETE)) {
            List<InlongStreamInfo> streamList = form.getStreamInfos();
            for (InlongStreamInfo streamInfo : streamList) {
                StreamResourceProcessForm processForm = genStreamProcessForm(groupInfo, streamInfo, operateType);
                executorService.execute(
                        () -> workflowService.start(ProcessName.DELETE_STREAM_RESOURCE, operator, processForm));
            }
        }
        // if the inlong group is lightweight mode, the stream source needs to be processed.
        if (InlongConstants.LIGHTWEIGHT_MODE.equals(groupInfo.getLightweight())) {
            changeSource4Lightweight(groupId, operateType, operator);
        }

        log.info("success to execute UpdateGroupCompleteListener for groupId={}, operateType={}", groupId, operateType);
        return ListenerResult.success();
    }

    private void changeSource4Lightweight(String groupId, GroupOperateType operateType, String operator) {
        switch (operateType) {
            case SUSPEND:
                sourceService.updateStatus(groupId, null, SourceStatus.SOURCE_FROZEN.getCode(), operator);
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

    private StreamResourceProcessForm genStreamProcessForm(InlongGroupInfo groupInfo, InlongStreamInfo streamInfo,
            GroupOperateType operateType) {
        StreamResourceProcessForm processForm = new StreamResourceProcessForm();
        processForm.setGroupInfo(groupInfo);
        processForm.setStreamInfo(streamInfo);
        processForm.setGroupOperateType(operateType);
        return processForm;
    }

}
