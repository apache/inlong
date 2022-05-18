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

package org.apache.inlong.manager.service.sort;

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.inlong.manager.common.enums.GroupOperateType;
import org.apache.inlong.manager.common.exceptions.WorkflowListenerException;
import org.apache.inlong.manager.common.pojo.group.InlongGroupExtInfo;
import org.apache.inlong.manager.common.pojo.group.InlongGroupInfo;
import org.apache.inlong.manager.common.pojo.sink.SinkResponse;
import org.apache.inlong.manager.common.pojo.stream.InlongStreamInfo;
import org.apache.inlong.manager.common.pojo.workflow.form.StreamResourceProcessForm;
import org.apache.inlong.manager.common.settings.InlongGroupSettings;
import org.apache.inlong.manager.service.sink.StreamSinkService;
import org.apache.inlong.manager.service.sort.util.DataFlowUtils;
import org.apache.inlong.manager.workflow.WorkflowContext;
import org.apache.inlong.manager.workflow.event.ListenerResult;
import org.apache.inlong.manager.workflow.event.task.SortOperateListener;
import org.apache.inlong.manager.workflow.event.task.TaskEvent;
import org.apache.inlong.sort.protocol.DataFlowInfo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Create sort config for one stream if Zookeeper is disabled.
 */
@Slf4j
@Component
public class CreateStreamSortConfigListener implements SortOperateListener {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper(); // thread safe

    @Autowired
    private StreamSinkService streamSinkService;
    @Autowired
    private DataFlowUtils dataFlowUtils;

    @Override
    public TaskEvent event() {
        return TaskEvent.COMPLETE;
    }

    @Override
    public ListenerResult listen(WorkflowContext context) throws Exception {
        StreamResourceProcessForm form = (StreamResourceProcessForm) context.getProcessForm();
        GroupOperateType groupOperateType = form.getGroupOperateType();
        if (groupOperateType == GroupOperateType.SUSPEND || groupOperateType == GroupOperateType.DELETE) {
            return ListenerResult.success();
        }
        InlongStreamInfo streamInfo = form.getStreamInfo();
        final String groupId = streamInfo.getInlongGroupId();
        final String streamId = streamInfo.getInlongStreamId();
        List<SinkResponse> sinkResponses = streamSinkService.listSink(groupId, streamId);
        if (CollectionUtils.isEmpty(sinkResponses)) {
            log.warn("Sink not found by groupId={}", groupId);
            return ListenerResult.success();
        }
        //TODO must be compile with new protocal of sort
        try {
            InlongGroupInfo groupInfo = form.getGroupInfo();
            Map<String, DataFlowInfo> dataFlowInfoMap = sinkResponses.stream().map(sink -> {
                        DataFlowInfo flowInfo = dataFlowUtils.createDataFlow(groupInfo, sink);
                        return Pair.of(sink.getInlongStreamId(), flowInfo);
                    }
            ).collect(Collectors.toMap(Pair::getKey, Pair::getValue));

            String dataFlows = OBJECT_MAPPER.writeValueAsString(dataFlowInfoMap);
            InlongGroupExtInfo extInfo = new InlongGroupExtInfo();
            extInfo.setInlongGroupId(groupId);
            String keyName = InlongGroupSettings.DATA_FLOW + ":" + streamId;
            extInfo.setKeyName(keyName);
            extInfo.setKeyValue(dataFlows);
            if (groupInfo.getExtList() == null) {
                groupInfo.setExtList(Lists.newArrayList());
            }
            upsertDataFlow(groupInfo, extInfo, keyName);
        } catch (Exception e) {
            log.error("create sort config failed for sink list={} of groupId={}, streamId={}", sinkResponses, groupId,
                    streamId, e);
            throw new WorkflowListenerException("create sort config failed: " + e.getMessage());
        }
        return ListenerResult.success();
    }

    private void upsertDataFlow(InlongGroupInfo groupInfo, InlongGroupExtInfo extInfo, String keyName) {
        groupInfo.getExtList().removeIf(ext -> keyName.equals(ext.getKeyName()));
        groupInfo.getExtList().add(extInfo);
    }

    @Override
    public boolean async() {
        return false;
    }
}
