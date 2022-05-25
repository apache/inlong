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
import com.google.common.collect.Maps;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.inlong.common.pojo.dataproxy.PulsarClusterInfo;
import org.apache.inlong.manager.common.enums.GroupOperateType;
import org.apache.inlong.manager.common.enums.MQType;
import org.apache.inlong.manager.common.enums.SourceType;
import org.apache.inlong.manager.common.exceptions.WorkflowListenerException;
import org.apache.inlong.manager.common.pojo.group.InlongGroupExtInfo;
import org.apache.inlong.manager.common.pojo.group.InlongGroupInfo;
import org.apache.inlong.manager.common.pojo.sink.SinkResponse;
import org.apache.inlong.manager.common.pojo.source.SourceResponse;
import org.apache.inlong.manager.common.pojo.source.kafka.KafkaSourceResponse;
import org.apache.inlong.manager.common.pojo.source.pulsar.PulsarSourceResponse;
import org.apache.inlong.manager.common.pojo.stream.InlongStreamInfo;
import org.apache.inlong.manager.common.pojo.workflow.form.GroupResourceProcessForm;
import org.apache.inlong.manager.common.settings.InlongGroupSettings;
import org.apache.inlong.manager.service.CommonOperateService;
import org.apache.inlong.manager.service.sink.StreamSinkService;
import org.apache.inlong.manager.service.sort.util.ExtractNodeUtils;
import org.apache.inlong.manager.service.sort.util.LoadNodeUtils;
import org.apache.inlong.manager.service.source.StreamSourceService;
import org.apache.inlong.manager.workflow.WorkflowContext;
import org.apache.inlong.manager.workflow.event.ListenerResult;
import org.apache.inlong.manager.workflow.event.task.SortOperateListener;
import org.apache.inlong.manager.workflow.event.task.TaskEvent;
import org.apache.inlong.sort.protocol.GroupInfo;
import org.apache.inlong.sort.protocol.StreamInfo;
import org.apache.inlong.sort.protocol.node.Node;
import org.apache.inlong.sort.protocol.transformation.relation.NodeRelationShip;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Component
@Slf4j
public class CreateSortConfigListenerV2 implements SortOperateListener {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Autowired
    private StreamSourceService sourceService;

    @Autowired
    private StreamSinkService sinkService;

    @Autowired
    private CommonOperateService commonOperateService;

    @Override
    public TaskEvent event() {
        return TaskEvent.COMPLETE;
    }

    @Override
    public ListenerResult listen(WorkflowContext context) throws Exception {
        log.info("Create sort config V2 for groupId={}", context.getProcessForm().getInlongGroupId());
        GroupResourceProcessForm form = (GroupResourceProcessForm) context.getProcessForm();
        GroupOperateType groupOperateType = form.getGroupOperateType();
        if (groupOperateType == GroupOperateType.SUSPEND || groupOperateType == GroupOperateType.DELETE) {
            return ListenerResult.success();
        }
        InlongGroupInfo groupInfo = form.getGroupInfo();
        List<InlongStreamInfo> streamInfos = form.getStreamInfos();
        final String groupId = groupInfo.getInlongGroupId();
        GroupInfo configInfo = createGroupInfo(groupInfo, streamInfos);
        String dataFlows = OBJECT_MAPPER.writeValueAsString(configInfo);

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

    private GroupInfo createGroupInfo(InlongGroupInfo groupInfo, List<InlongStreamInfo> streamInfoList) {
        String groupId = groupInfo.getInlongGroupId();
        List<SinkResponse> sinkResponses = sinkService.listSink(groupId, null);
        Map<String, List<SinkResponse>> sinkResponseMap = sinkResponses.stream()
                .collect(Collectors.groupingBy(sinkResponse -> sinkResponse.getInlongStreamId(), HashMap::new,
                        Collectors.toCollection(ArrayList::new)));
        Map<String, List<SourceResponse>> sourceResponseMap = createPulsarSources(groupInfo, streamInfoList);
        List<StreamInfo> streamInfos = streamInfoList.stream()
                .map(inlongStreamInfo -> new StreamInfo(inlongStreamInfo.getInlongStreamId(),
                        createNodesForStream(
                                sourceResponseMap.get(inlongStreamInfo.getInlongStreamId()),
                                sinkResponseMap.get(inlongStreamInfo.getInlongStreamId())),
                        createNodeRelationShipsForStream(
                                sourceResponseMap.get(inlongStreamInfo.getInlongStreamId()),
                                sinkResponseMap.get(inlongStreamInfo.getInlongStreamId())))
                ).collect(Collectors.toList());
        return new GroupInfo(groupInfo.getInlongGroupId(), streamInfos);
    }

    private Map<String, List<SourceResponse>> createPulsarSources(InlongGroupInfo groupInfo,
            List<InlongStreamInfo> streamInfoList) {
        MQType mqType = MQType.forType(groupInfo.getMqType());
        if (mqType != MQType.PULSAR) {
            String errMsg = String.format("Unsupported MqType={} for Inlong", mqType);
            log.error(errMsg);
            throw new WorkflowListenerException(errMsg);
        }
        Map<String, List<SourceResponse>> sourceReponses = Maps.newHashMap();
        PulsarClusterInfo pulsarCluster = commonOperateService.getPulsarClusterInfo(groupInfo.getMqType());
        streamInfoList.stream().forEach(streamInfo -> {
            PulsarSourceResponse pulsarSourceResponse = new PulsarSourceResponse();
            pulsarSourceResponse.setSourceName(streamInfo.getInlongStreamId());
            pulsarSourceResponse.setNamespace(groupInfo.getMqResource());
            pulsarSourceResponse.setTopic(streamInfo.getMqResource());
            pulsarSourceResponse.setAdminUrl(pulsarCluster.getAdminUrl());
            pulsarSourceResponse.setServiceUrl(pulsarCluster.getBrokerServiceUrl());
            pulsarSourceResponse.setInlongComponent(true);
            List<SourceResponse> sourceResponses = sourceService.listSource(groupInfo.getInlongGroupId(),
                    streamInfo.getInlongStreamId());
            for (SourceResponse sourceResponse : sourceResponses) {
                if (StringUtils.isEmpty(pulsarSourceResponse.getSerializationType())
                        && StringUtils.isNotEmpty(sourceResponse.getSerializationType())) {
                    pulsarSourceResponse.setSerializationType(sourceResponse.getSerializationType());
                }
                if (SourceType.forType(sourceResponse.getSourceType()) == SourceType.KAFKA) {
                    pulsarSourceResponse.setPrimaryKey(((KafkaSourceResponse) sourceResponse).getPrimaryKey());
                }
            }
            pulsarSourceResponse.setScanStartupMode("earliest");
            pulsarSourceResponse.setFieldList(streamInfo.getFieldList());
            sourceReponses.computeIfAbsent(streamInfo.getInlongStreamId(), key -> Lists.newArrayList())
                    .add(pulsarSourceResponse);
        });
        return sourceReponses;
    }

    private List<Node> createNodesForStream(
            List<SourceResponse> sourceResponses,
            List<SinkResponse> sinkResponses) {
        List<Node> nodes = Lists.newArrayList();
        nodes.addAll(ExtractNodeUtils.createExtractNodes(sourceResponses));
        nodes.addAll(LoadNodeUtils.createLoadNodes(sinkResponses));
        return nodes;
    }

    private List<NodeRelationShip> createNodeRelationShipsForStream(
            List<SourceResponse> sourceResponses,
            List<SinkResponse> sinkResponses) {
        NodeRelationShip relationShip = new NodeRelationShip();
        List<String> inputs = sourceResponses.stream().map(sourceResponse -> sourceResponse.getSourceName())
                .collect(Collectors.toList());
        List<String> outputs = sinkResponses.stream().map(sinkResponse -> sinkResponse.getSinkName())
                .collect(Collectors.toList());
        relationShip.setInputs(inputs);
        relationShip.setOutputs(outputs);
        return Lists.newArrayList(relationShip);
    }

    private void upsertDataFlow(InlongGroupInfo groupInfo, InlongGroupExtInfo extInfo) {
        groupInfo.getExtList().removeIf(ext -> InlongGroupSettings.DATA_FLOW.equals(ext.getKeyName()));
        groupInfo.getExtList().add(extInfo);
    }

    @Override
    public boolean async() {
        return false;
    }
}
