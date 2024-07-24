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

package org.apache.inlong.manager.service.listener.source;

import org.apache.inlong.manager.common.consts.InlongConstants;
import org.apache.inlong.manager.common.enums.GroupOperateType;
import org.apache.inlong.manager.common.enums.SourceStatus;
import org.apache.inlong.manager.common.enums.TaskEvent;
import org.apache.inlong.manager.pojo.source.SourceRequest;
import org.apache.inlong.manager.pojo.source.StreamSource;
import org.apache.inlong.manager.pojo.stream.InlongStreamInfo;
import org.apache.inlong.manager.pojo.workflow.form.process.StreamResourceProcessForm;
import org.apache.inlong.manager.service.source.SourceOperatorFactory;
import org.apache.inlong.manager.service.source.StreamSourceOperator;
import org.apache.inlong.manager.service.source.StreamSourceService;
import org.apache.inlong.manager.workflow.WorkflowContext;
import org.apache.inlong.manager.workflow.event.ListenerResult;
import org.apache.inlong.manager.workflow.event.task.SourceOperateListener;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;

@Slf4j
@Component
public class SourceStartListener implements SourceOperateListener {

    @Autowired
    protected StreamSourceService streamSourceService;
    @Autowired
    private SourceOperatorFactory operatorFactory;

    @Override
    public String name() {
        return getClass().getSimpleName();
    }

    @Override
    public TaskEvent event() {
        return TaskEvent.COMPLETE;
    }

    @Override
    public boolean accept(WorkflowContext context) {
        if (isGroupProcessForm(context)) {
            return false;
        }
        StreamResourceProcessForm processForm = (StreamResourceProcessForm) context.getProcessForm();
        return InlongConstants.STANDARD_MODE.equals(processForm.getGroupInfo().getInlongGroupMode())
                && processForm.getGroupOperateType() == GroupOperateType.INIT;
    }

    @Override
    public ListenerResult listen(WorkflowContext context) throws Exception {
        StreamResourceProcessForm form = (StreamResourceProcessForm) context.getProcessForm();
        String operator = context.getOperator();
        InlongStreamInfo streamInfo = form.getStreamInfo();
        final String groupId = streamInfo.getInlongGroupId();
        final String streamId = streamInfo.getInlongStreamId();
        if (InlongConstants.DATASYNC_REALTIME_MODE.equals(form.getGroupInfo().getInlongGroupMode())
                || InlongConstants.DATASYNC_OFFLINE_MODE.equals(form.getGroupInfo().getInlongGroupMode())) {
            streamSourceService.updateStatus(groupId, streamId, SourceStatus.SOURCE_NORMAL.getCode(), operator);
        } else {
            streamSourceService.updateStatus(groupId, streamId, SourceStatus.TO_BE_ISSUED_ADD.getCode(), operator);
        }
        log.info("begin to update agent task config for groupId={}, streamId={}", groupId, streamId);
        List<StreamSource> sources = streamSourceService.listSource(groupId, streamId);
        for (StreamSource source : sources) {
            SourceRequest request = source.genSourceRequest();
            StreamSourceOperator sourceOperator = operatorFactory.getInstance(request.getSourceType());
            sourceOperator.updateAgentTaskConfig(request, operator);
        }
        log.info("success to update agent task config for groupId={}, streamId={}", groupId, streamId);
        return ListenerResult.success();
    }
}
