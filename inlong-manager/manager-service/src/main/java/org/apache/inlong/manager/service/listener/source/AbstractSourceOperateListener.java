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

import org.apache.inlong.manager.common.enums.GroupOperateType;
import org.apache.inlong.manager.common.enums.TaskEvent;
import org.apache.inlong.manager.pojo.group.InlongGroupInfo;
import org.apache.inlong.manager.pojo.source.SourceRequest;
import org.apache.inlong.manager.pojo.source.StreamSource;
import org.apache.inlong.manager.pojo.stream.InlongStreamBriefInfo;
import org.apache.inlong.manager.pojo.workflow.form.process.GroupResourceProcessForm;
import org.apache.inlong.manager.pojo.workflow.form.process.ProcessForm;
import org.apache.inlong.manager.service.source.StreamSourceService;
import org.apache.inlong.manager.service.stream.InlongStreamService;
import org.apache.inlong.manager.workflow.WorkflowContext;
import org.apache.inlong.manager.workflow.event.ListenerResult;
import org.apache.inlong.manager.workflow.event.task.SourceOperateListener;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Event listener of operate resources, such as delete, stop, restart sources.
 */
@Slf4j
@Component
public abstract class AbstractSourceOperateListener implements SourceOperateListener {

    @Autowired
    protected InlongStreamService streamService;

    @Autowired
    protected StreamSourceService streamSourceService;

    @Override
    public TaskEvent event() {
        return TaskEvent.COMPLETE;
    }

    @Override
    public ListenerResult listen(WorkflowContext context) throws Exception {
        log.info("operate stream source for groupId={}", context.getProcessForm().getInlongGroupId());
        InlongGroupInfo groupInfo = getGroupInfo(context.getProcessForm());
        final String groupId = groupInfo.getInlongGroupId();
        List<InlongStreamBriefInfo> streamResponses = streamService.listBriefWithSink(groupId);
        streamResponses
                .forEach(stream -> operateStreamSources(groupId, stream.getInlongStreamId(), context.getOperator()));
        return ListenerResult.success();
    }

    /**
     * Operate stream sources, such as delete, stop, restart.
     */
    protected void operateStreamSources(String groupId, String streamId, String operator) {
        List<StreamSource> sources = streamSourceService.listSource(groupId, streamId);
        sources.forEach(source -> {
            operateStreamSource(source.genSourceRequest(), operator);
        });
    }

    /**
     * Operate stream sources ,such as delete, stop, restart.
     */
    public abstract void operateStreamSource(SourceRequest sourceRequest, String operator);

    private GroupOperateType getOperateType(ProcessForm processForm) {
        if (processForm instanceof GroupResourceProcessForm) {
            return ((GroupResourceProcessForm) processForm).getGroupOperateType();
        } else {
            log.error("illegal process form {} to get inlong group info", processForm.getFormName());
            throw new RuntimeException("Unsupported process form " + processForm.getFormName());
        }
    }

    private InlongGroupInfo getGroupInfo(ProcessForm processForm) {
        if (processForm instanceof GroupResourceProcessForm) {
            GroupResourceProcessForm groupResourceProcessForm = (GroupResourceProcessForm) processForm;
            return groupResourceProcessForm.getGroupInfo();
        } else {
            log.error("illegal process form {} to get inlong group info", processForm.getFormName());
            throw new RuntimeException("Unsupported process form " + processForm.getFormName());
        }
    }

}
