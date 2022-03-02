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

import com.google.common.collect.Lists;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.inlong.manager.common.beans.ClusterBean;
import org.apache.inlong.manager.common.enums.Constant;
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
import org.apache.inlong.manager.common.util.Preconditions;
import org.apache.inlong.manager.service.CommonOperateService;
import org.apache.inlong.manager.service.core.InlongStreamService;
import org.apache.inlong.manager.service.sink.StreamSinkService;
import org.apache.inlong.manager.service.source.StreamSourceService;
import org.apache.inlong.manager.service.thirdparty.sort.util.SerializationUtils;
import org.apache.inlong.manager.service.thirdparty.sort.util.SinkInfoUtils;
import org.apache.inlong.manager.service.thirdparty.sort.util.SortFieldFormatUtils;
import org.apache.inlong.manager.service.thirdparty.sort.util.SourceInfoUtils;
import org.apache.inlong.manager.workflow.WorkflowContext;
import org.apache.inlong.manager.workflow.event.ListenerResult;
import org.apache.inlong.manager.workflow.event.task.SortOperateListener;
import org.apache.inlong.manager.workflow.event.task.TaskEvent;
import org.apache.inlong.sort.formats.common.FormatInfo;
import org.apache.inlong.sort.protocol.DataFlowInfo;
import org.apache.inlong.sort.protocol.FieldInfo;
import org.apache.inlong.sort.protocol.deserialization.DeserializationInfo;
import org.apache.inlong.sort.protocol.sink.SinkInfo;
import org.apache.inlong.sort.protocol.source.PulsarSourceInfo;
import org.apache.inlong.sort.protocol.source.SourceInfo;
import org.apache.inlong.sort.protocol.source.TubeSourceInfo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class CreateSortConfigListener implements SortOperateListener {

    @Autowired
    private CommonOperateService commonOperateService;
    @Autowired
    private ClusterBean clusterBean;
    @Autowired
    private InlongStreamService inlongStreamService;
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
        log.info("Create sort config for context={}", context);
        ProcessForm form = context.getProcessForm();
        InlongGroupInfo groupInfo = getGroupInfo(form);
        String groupId = groupInfo.getInlongGroupId();
        if (StringUtils.isEmpty(groupId)) {
            log.warn("GroupId is null for context={}", context);
            return ListenerResult.success();
        }
        List<StreamBriefResponse> streamBriefResponses = inlongStreamService.getBriefList(groupId);
        if (CollectionUtils.isEmpty(streamBriefResponses)) {
            log.warn("Stream not found by groupId={}", groupId);
            return ListenerResult.success();
        }

        Map<String, DataFlowInfo> dataFlowInfoMap = streamBriefResponses.stream().map(streamBriefResponse -> {
                            DataFlowInfo flowInfo = createDataFlow(streamBriefResponse, groupInfo);
                            if (flowInfo != null) {
                                return Pair.of(streamBriefResponse.getInlongStreamId(), flowInfo);
                            } else {
                                return null;
                            }
                        }
                ).filter(pair -> pair != null)
                .collect(Collectors.toMap(pair -> pair.getKey(),
                        pair -> pair.getValue()));
        final ObjectMapper objectMapper = new ObjectMapper();
        String dataFlows = objectMapper.writeValueAsString(dataFlowInfoMap);
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
        Iterator<InlongGroupExtInfo> inlongGroupExtInfoIterator = groupInfo.getExtList().iterator();
        while (inlongGroupExtInfoIterator.hasNext()) {
            InlongGroupExtInfo inlongGroupExtInfo = inlongGroupExtInfoIterator.next();
            if (InlongGroupSettings.DATA_FLOW.equals(inlongGroupExtInfo.getKeyName())) {
                inlongGroupExtInfoIterator.remove();
            }
        }
        groupInfo.getExtList().add(extInfo);
    }

    private DataFlowInfo createDataFlow(StreamBriefResponse streamBriefResponse,
            InlongGroupInfo inlongGroupInfo) {
        //TODO only support one source and one sink
        final String groupId = streamBriefResponse.getInlongGroupId();
        final String streamId = streamBriefResponse.getInlongStreamId();
        final InlongStreamInfo streamInfo = inlongStreamService.get(groupId, streamId);
        List<SourceResponse> sourceResponses = streamSourceService.listSource(groupId, streamId);
        if (CollectionUtils.isEmpty(sourceResponses)) {
            throw new RuntimeException(String.format("No source found by stream=%s", streamBriefResponse));
        }
        final SourceResponse sourceResponse = sourceResponses.get(0);
        List<SinkBriefResponse> sinkBriefResponses = streamBriefResponse.getSinkList();
        if (CollectionUtils.isEmpty(sinkBriefResponses)) {
            throw new RuntimeException(String.format("No sink found by stream=%s", streamBriefResponse));
        }
        final SinkBriefResponse sinkBriefResponse = sinkBriefResponses.get(0);
        String sinkType = sinkBriefResponse.getSinkType();
        int sinkId = sinkBriefResponse.getId();
        final SinkResponse sinkResponse = streamSinkService.get(sinkId, sinkType);
        SourceInfo sourceInfo = createSourceInfo(inlongGroupInfo, streamInfo, sourceResponse);
        SinkInfo sinkInfo = SinkInfoUtils.createSinkInfo(streamInfo, sourceResponse, sinkResponse);
        return new DataFlowInfo(sinkId, sourceInfo, sinkInfo);
    }

    private SourceInfo createSourceInfo(InlongGroupInfo groupInfo, InlongStreamInfo streamInfo,
            SourceResponse sourceResponse) {
        String middleWareType = groupInfo.getMiddlewareType();

        List<FieldInfo> fieldInfos = Lists.newArrayList();
        if (CollectionUtils.isNotEmpty(streamInfo.getFieldList())) {
            fieldInfos = streamInfo.getFieldList().stream().map(inlongStreamFieldInfo -> {
                FormatInfo formatInfo = SortFieldFormatUtils.convertFieldFormat(
                        inlongStreamFieldInfo.getFieldType().toLowerCase());
                return new FieldInfo(inlongStreamFieldInfo.getFieldName(), formatInfo);
            }).collect(Collectors.toList());
        }
        DeserializationInfo deserializationInfo = SerializationUtils.createDeserializationInfo(sourceResponse,
                streamInfo);
        if (Constant.MIDDLEWARE_PULSAR.equals(middleWareType)) {
            return createPulsarSourceInfo(groupInfo, streamInfo, deserializationInfo, fieldInfos);
        } else if (Constant.MIDDLEWARE_TUBE.equals(middleWareType)) {
            return createTubeSourceInfo(groupInfo, deserializationInfo, fieldInfos);
        } else {
            throw new RuntimeException(
                    String.format("MiddleWare:{} not support in CreateSortConfigListener", middleWareType));
        }
    }

    private PulsarSourceInfo createPulsarSourceInfo(InlongGroupInfo groupInfo,
            InlongStreamInfo streamInfo,
            DeserializationInfo deserializationInfo,
            List<FieldInfo> fieldInfos) {
        String topicName = streamInfo.getMqResourceObj();
        String pulsarAdminUrl = commonOperateService.getSpecifiedParam(Constant.PULSAR_ADMINURL);
        String pulsarServiceUrl = commonOperateService.getSpecifiedParam(Constant.PULSAR_SERVICEURL);
        return SourceInfoUtils.createPulsarSourceInfo(groupInfo, topicName, deserializationInfo,
                fieldInfos, clusterBean.getAppName(), clusterBean.getDefaultTenant(), pulsarAdminUrl, pulsarServiceUrl);
    }

    private TubeSourceInfo createTubeSourceInfo(InlongGroupInfo groupInfo,
            DeserializationInfo deserializationInfo,
            List<FieldInfo> fieldInfos) {
        String masterAddress = commonOperateService.getSpecifiedParam(Constant.TUBE_MASTER_URL);
        Preconditions.checkNotNull(masterAddress, "tube cluster address cannot be empty");
        String topic = groupInfo.getMqResourceObj();
        // The consumer group name is: taskName_topicName_consumer_group
        String consumerGroup = clusterBean.getAppName() + "_" + topic + "_consumer_group";
        return new TubeSourceInfo(topic, masterAddress, consumerGroup,
                deserializationInfo, fieldInfos.toArray(new FieldInfo[0]));
    }

    private InlongGroupInfo getGroupInfo(ProcessForm processForm) {
        if (processForm instanceof GroupResourceProcessForm) {
            GroupResourceProcessForm groupResourceProcessForm = (GroupResourceProcessForm) processForm;
            return groupResourceProcessForm.getGroupInfo();
        } else if (processForm instanceof UpdateGroupProcessForm) {
            UpdateGroupProcessForm updateGroupProcessForm = (UpdateGroupProcessForm) processForm;
            return updateGroupProcessForm.getGroupInfo();
        } else {
            log.error("Illegal ProcessForm {} to get inlong group info", processForm.getFormName());
            throw new RuntimeException(String.format("Unsupport ProcessForm {} in CreateSortConfigListener",
                    processForm.getFormName()));
        }
    }

    @Override
    public boolean async() {
        return false;
    }
}
