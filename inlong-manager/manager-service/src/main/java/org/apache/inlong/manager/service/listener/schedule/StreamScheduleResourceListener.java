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
import org.apache.inlong.manager.pojo.stream.InlongStreamExtInfo;
import org.apache.inlong.manager.pojo.stream.InlongStreamInfo;
import org.apache.inlong.manager.pojo.workflow.form.process.ProcessForm;
import org.apache.inlong.manager.pojo.workflow.form.process.StreamResourceProcessForm;
import org.apache.inlong.manager.workflow.WorkflowContext;
import org.apache.inlong.manager.workflow.event.ListenerResult;
import org.apache.inlong.manager.workflow.event.task.ScheduleOperateListener;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
@Slf4j
public class StreamScheduleResourceListener implements ScheduleOperateListener {

    @Override
    public TaskEvent event() {
        return TaskEvent.COMPLETE;
    }

    @Override
    public boolean accept(WorkflowContext context) {
        ProcessForm processForm = context.getProcessForm();
        String groupId = processForm.getInlongGroupId();
        if (!(processForm instanceof StreamResourceProcessForm)) {
            log.info("not add schedule stream listener, not StreamResourceProcessForm for groupId [{}]", groupId);
            return false;
        }

        StreamResourceProcessForm streamProcessForm = (StreamResourceProcessForm) processForm;
        String streamId = streamProcessForm.getStreamInfo().getInlongStreamId();
        if (streamProcessForm.getGroupOperateType() != GroupOperateType.INIT) {
            log.info("not add schedule stream listener, as the operate was not INIT for groupId [{}] streamId [{}]",
                    groupId, streamId);
            return false;
        }

        log.info("add startup stream listener for groupId [{}] streamId [{}]", groupId, streamId);
        return InlongConstants.DATASYNC_OFFLINE_MODE.equals(streamProcessForm.getGroupInfo().getInlongGroupMode());
    }

    @Override
    public ListenerResult listen(WorkflowContext context) throws Exception {
        StreamResourceProcessForm form = (StreamResourceProcessForm) context.getProcessForm();
        InlongStreamInfo streamInfo = form.getStreamInfo();
        final String groupId = streamInfo.getInlongGroupId();
        final String streamId = streamInfo.getInlongStreamId();
        log.info("begin to register schedule info for groupId={}, streamId={}", groupId, streamId);

        // todo: register schedule info to schedule service

        // after register schedule info successfully, add ext property to stream info
        saveInfo(streamInfo, InlongConstants.REGISTER_SCHEDULE_STATUS,
                InlongConstants.REGISTERED, streamInfo.getExtList());
        log.info("success to register schedule info for group [" + groupId + "] and stream [" + streamId + "]");
        return ListenerResult.success();
    }

    /**
     * Save stream ext info into list.
     */
    private void saveInfo(InlongStreamInfo streamInfo, String keyName, String keyValue,
            List<InlongStreamExtInfo> extInfoList) {
        InlongStreamExtInfo extInfo = new InlongStreamExtInfo();
        extInfo.setInlongGroupId(streamInfo.getInlongGroupId());
        extInfo.setInlongStreamId(streamInfo.getInlongStreamId());
        extInfo.setKeyName(keyName);
        extInfo.setKeyValue(keyValue);
        extInfoList.add(extInfo);
    }
}
