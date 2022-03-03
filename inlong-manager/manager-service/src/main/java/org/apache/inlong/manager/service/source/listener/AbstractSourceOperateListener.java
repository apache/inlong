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

package org.apache.inlong.manager.service.source.listener;

import lombok.extern.slf4j.Slf4j;
import org.apache.inlong.manager.common.enums.SourceType;
import org.apache.inlong.manager.common.pojo.group.InlongGroupInfo;
import org.apache.inlong.manager.common.pojo.source.SourceRequest;
import org.apache.inlong.manager.common.pojo.source.SourceResponse;
import org.apache.inlong.manager.common.pojo.source.binlog.BinlogSourceRequest;
import org.apache.inlong.manager.common.pojo.source.binlog.BinlogSourceResponse;
import org.apache.inlong.manager.common.pojo.source.kafka.KafkaSourceRequest;
import org.apache.inlong.manager.common.pojo.source.kafka.KafkaSourceResponse;
import org.apache.inlong.manager.common.pojo.stream.StreamBriefResponse;
import org.apache.inlong.manager.common.pojo.workflow.form.GroupResourceProcessForm;
import org.apache.inlong.manager.common.pojo.workflow.form.ProcessForm;
import org.apache.inlong.manager.common.pojo.workflow.form.UpdateGroupProcessForm;
import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.service.core.InlongStreamService;
import org.apache.inlong.manager.service.source.StreamSourceService;
import org.apache.inlong.manager.workflow.WorkflowContext;
import org.apache.inlong.manager.workflow.event.ListenerResult;
import org.apache.inlong.manager.workflow.event.task.DataSourceOperateListener;
import org.apache.inlong.manager.workflow.event.task.TaskEvent;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;

@Slf4j
@Component
public abstract class AbstractSourceOperateListener implements DataSourceOperateListener {

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
        log.info("Delete data source for context={}", context);
        InlongGroupInfo groupInfo = getGroupInfo(context.getProcessForm());
        final String groupId = groupInfo.getInlongGroupId();
        List<StreamBriefResponse> streamBriefResponses = streamService.getBriefList(groupId);
        streamBriefResponses.forEach(streamBriefResponse -> {
            operateStreamSources(groupId, streamBriefResponse.getInlongStreamId(), context.getApplicant());
        });
        return ListenerResult.success();
    }

    protected void operateStreamSources(String groupId, String streamId, String operator) {
        List<SourceResponse> sourceResponses = streamSourceService.listSource(groupId, streamId);
        sourceResponses.forEach(sourceResponse -> {
            SourceRequest sourceRequest = createSourceRequest(sourceResponse);
            operateStreamSource(sourceRequest, operator);
        });
    }

    public SourceRequest createSourceRequest(SourceResponse sourceResponse) {
        String sourceType = sourceResponse.getSourceType();
        SourceType type = SourceType.valueOf(sourceType);
        switch (type) {
            case BINLOG:
                return CommonBeanUtils.copyProperties((BinlogSourceResponse) sourceResponse, BinlogSourceRequest::new);
            case KAFKA:
                return CommonBeanUtils.copyProperties((KafkaSourceResponse) sourceResponse, KafkaSourceRequest::new);
            default:
                throw new IllegalArgumentException(
                        String.format("Unsupported type=%s for DataSourceOperateListener", type));
        }
    }

    public abstract void operateStreamSource(SourceRequest sourceRequest, String operator);

    private InlongGroupInfo getGroupInfo(ProcessForm processForm) {
        if (processForm instanceof GroupResourceProcessForm) {
            GroupResourceProcessForm groupResourceProcessForm = (GroupResourceProcessForm) processForm;
            return groupResourceProcessForm.getGroupInfo();
        } else if (processForm instanceof UpdateGroupProcessForm) {
            UpdateGroupProcessForm updateGroupProcessForm = (UpdateGroupProcessForm) processForm;
            return updateGroupProcessForm.getGroupInfo();
        } else {
            log.error("Illegal ProcessForm {} to get inlong group info", processForm.getFormName());
            throw new RuntimeException(String.format("Unsupported ProcessForm {%s} in CreateSortConfigListener",
                    processForm.getFormName()));
        }
    }

    @Override
    public boolean async() {
        return false;
    }

}
