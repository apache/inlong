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

package org.apache.inlong.manager.service.thirdparty.sort;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.inlong.common.pojo.dataproxy.PulsarClusterInfo;
import org.apache.inlong.manager.common.beans.ClusterBean;
import org.apache.inlong.manager.common.enums.Constant;
import org.apache.inlong.manager.common.exceptions.WorkflowListenerException;
import org.apache.inlong.manager.common.pojo.group.InlongGroupExtInfo;
import org.apache.inlong.manager.common.pojo.group.InlongGroupInfo;
import org.apache.inlong.manager.common.pojo.sink.SinkBriefResponse;
import org.apache.inlong.manager.common.pojo.sink.SinkResponse;
import org.apache.inlong.manager.common.pojo.source.SourceResponse;
import org.apache.inlong.manager.common.pojo.stream.InlongStreamInfo;
import org.apache.inlong.manager.common.pojo.stream.StreamBriefResponse;
import org.apache.inlong.manager.common.pojo.workflow.form.GroupResourceProcessForm;
import org.apache.inlong.manager.common.pojo.workflow.form.ProcessForm;
import org.apache.inlong.manager.common.pojo.workflow.form.UpdateGroupProcessForm;
import org.apache.inlong.manager.common.settings.InlongGroupSettings;
import org.apache.inlong.manager.service.CommonOperateService;
import org.apache.inlong.manager.service.core.InlongStreamService;
import org.apache.inlong.manager.service.sink.StreamSinkService;
import org.apache.inlong.manager.service.source.StreamSourceService;
import org.apache.inlong.manager.service.thirdparty.sort.util.SinkInfoUtils;
import org.apache.inlong.manager.service.thirdparty.sort.util.SourceInfoUtils;
import org.apache.inlong.manager.workflow.WorkflowContext;
import org.apache.inlong.manager.workflow.event.ListenerResult;
import org.apache.inlong.manager.workflow.event.task.SortOperateListener;
import org.apache.inlong.manager.workflow.event.task.TaskEvent;
import org.apache.inlong.sort.protocol.DataFlowInfo;
import org.apache.inlong.sort.protocol.sink.SinkInfo;
import org.apache.inlong.sort.protocol.source.SourceInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Create sort config when disable the ZooKeeper
 */
@Component
public class CreateSortConfigListener implements SortOperateListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(CreateSortConfigListener.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper(); // thread safe

    @Autowired
    private CommonOperateService commonOperateService;
    @Autowired
    private ClusterBean clusterBean;
    @Autowired
    private InlongStreamService streamService;
    @Autowired
    private StreamSinkService streamSinkService;
    @Autowired
    private StreamSourceService streamSourceService;

    @Override
    public TaskEvent event() {
        return TaskEvent.CREATE;
    }

    @Override
    public ListenerResult listen(WorkflowContext context) throws Exception {
        LOGGER.info("Create sort config for context={}", context);
        ProcessForm form = context.getProcessForm();
        InlongGroupInfo groupInfo = this.getGroupInfo(form);
        String groupId = groupInfo.getInlongGroupId();
        if (StringUtils.isEmpty(groupId)) {
            LOGGER.warn("GroupId is null for context={}", context);
            return ListenerResult.success();
        }

        List<StreamBriefResponse> streamBriefResponses = streamService.getBriefList(groupId);
        if (CollectionUtils.isEmpty(streamBriefResponses)) {
            LOGGER.warn("Stream not found by groupId={}", groupId);
            return ListenerResult.success();
        }

        Map<String, DataFlowInfo> dataFlowInfoMap = streamBriefResponses.stream().map(streamResponse -> {
                    DataFlowInfo flowInfo = createDataFlow(streamResponse, groupInfo);
                    return Pair.of(streamResponse.getInlongStreamId(), flowInfo);
                }
        ).collect(Collectors.toMap(Pair::getKey, Pair::getValue));

        String dataFlows = OBJECT_MAPPER.writeValueAsString(dataFlowInfoMap);
        InlongGroupExtInfo extInfo = new InlongGroupExtInfo();
        extInfo.setInlongGroupId(groupId);
        extInfo.setKeyName(InlongGroupSettings.DATA_FLOW);
        extInfo.setKeyValue(dataFlows);
        if (groupInfo.getExtList() == null) {
            groupInfo.setExtList(Lists.newArrayList());
        }
        upsertDataFlow(groupInfo, extInfo);

        return ListenerResult.success();
    }

    private void upsertDataFlow(InlongGroupInfo groupInfo, InlongGroupExtInfo extInfo) {
        groupInfo.getExtList().removeIf(ext -> InlongGroupSettings.DATA_FLOW.equals(ext.getKeyName()));
        groupInfo.getExtList().add(extInfo);
    }

    private DataFlowInfo createDataFlow(StreamBriefResponse streamResponse, InlongGroupInfo groupInfo) {
        // TODO only support one source and one sink
        final String groupId = streamResponse.getInlongGroupId();
        final String streamId = streamResponse.getInlongStreamId();
        final InlongStreamInfo streamInfo = streamService.get(groupId, streamId);
        List<SourceResponse> sourceResponses = streamSourceService.listSource(groupId, streamId);
        if (CollectionUtils.isEmpty(sourceResponses)) {
            throw new WorkflowListenerException(String.format("Source not found by groupId=%s and streamId=%s",
                    groupId, streamId));
        }
        final SourceResponse sourceResponse = sourceResponses.get(0);
        List<SinkBriefResponse> sinkBriefResponses = streamResponse.getSinkList();
        if (CollectionUtils.isEmpty(sinkBriefResponses)) {
            throw new WorkflowListenerException(String.format("Sink not found by groupId=%s and streamId=%s",
                    groupId, streamId));
        }

        final SinkBriefResponse sinkBriefResponse = sinkBriefResponses.get(0);
        String sinkType = sinkBriefResponse.getSinkType();
        int sinkId = sinkBriefResponse.getId();
        final SinkResponse sinkResponse = streamSinkService.get(sinkId, sinkType);

        // Get source info
        String masterAddress = commonOperateService.getSpecifiedParam(Constant.TUBE_MASTER_URL);
        PulsarClusterInfo pulsarCluster = commonOperateService.getPulsarClusterInfo();
        SourceInfo sourceInfo = SourceInfoUtils.createSourceInfo(pulsarCluster, masterAddress, clusterBean, groupInfo,
                streamInfo, sourceResponse, sinkResponse);
        // Get sink info
        SinkInfo sinkInfo = SinkInfoUtils.createSinkInfo(sourceResponse, sinkResponse);

        return new DataFlowInfo(sinkId, sourceInfo, sinkInfo);
    }

    private InlongGroupInfo getGroupInfo(ProcessForm processForm) {
        if (processForm instanceof GroupResourceProcessForm) {
            GroupResourceProcessForm groupResourceProcessForm = (GroupResourceProcessForm) processForm;
            return groupResourceProcessForm.getGroupInfo();
        } else if (processForm instanceof UpdateGroupProcessForm) {
            UpdateGroupProcessForm updateGroupProcessForm = (UpdateGroupProcessForm) processForm;
            return updateGroupProcessForm.getGroupInfo();
        } else {
            LOGGER.error("Illegal ProcessForm {} to get inlong group info", processForm.getFormName());
            throw new WorkflowListenerException(
                    String.format("Unsupported ProcessForm {%s}", processForm.getFormName()));
        }
    }

    @Override
    public boolean async() {
        return false;
    }
}
