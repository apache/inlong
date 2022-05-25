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
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.inlong.common.pojo.dataproxy.PulsarClusterInfo;
import org.apache.inlong.manager.common.enums.GroupOperateType;
import org.apache.inlong.manager.common.enums.MQType;
import org.apache.inlong.manager.common.enums.SourceType;
import org.apache.inlong.manager.common.exceptions.WorkflowListenerException;
import org.apache.inlong.manager.common.pojo.group.InlongGroupInfo;
import org.apache.inlong.manager.common.pojo.sink.SinkResponse;
import org.apache.inlong.manager.common.pojo.source.SourceResponse;
import org.apache.inlong.manager.common.pojo.source.kafka.KafkaSourceResponse;
import org.apache.inlong.manager.common.pojo.source.pulsar.PulsarSourceResponse;
import org.apache.inlong.manager.common.pojo.stream.InlongStreamExtInfo;
import org.apache.inlong.manager.common.pojo.stream.InlongStreamInfo;
import org.apache.inlong.manager.common.pojo.workflow.form.StreamResourceProcessForm;
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

import java.util.List;
import java.util.stream.Collectors;

/**
 * Create sort config for one stream if Zookeeper is disabled.
 */
@Slf4j
@Component
public class CreateStreamSortConfigListener implements SortOperateListener {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper(); // thread safe

    @Autowired
    private StreamSourceService streamSourceService;
    @Autowired
    private StreamSinkService streamSinkService;
    @Autowired
    private CommonOperateService commonOperateService;

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
        InlongGroupInfo groupInfo = form.getGroupInfo();
        InlongStreamInfo streamInfo = form.getStreamInfo();
        final String groupId = streamInfo.getInlongGroupId();
        final String streamId = streamInfo.getInlongStreamId();
        List<SinkResponse> sinkResponses = streamSinkService.listSink(groupId, streamId);
        if (CollectionUtils.isEmpty(sinkResponses)) {
            log.warn("Sink not found by groupId={}", groupId);
            return ListenerResult.success();
        }
        try {
            List<SourceResponse> sourceResponses = createPulsarSources(groupInfo, streamInfo);
            List<Node> nodes = createNodesForStream(sourceResponses, sinkResponses);
            List<NodeRelationShip> nodeRelationShips = createNodeRelationShipsForStream(sourceResponses, sinkResponses);
            StreamInfo sortStreamInfo = new StreamInfo(streamId, nodes, nodeRelationShips);
            GroupInfo sortGroupInfo = new GroupInfo(groupId, Lists.newArrayList(sortStreamInfo));
            String dataFlows = OBJECT_MAPPER.writeValueAsString(sortGroupInfo);
            InlongStreamExtInfo extInfo = new InlongStreamExtInfo();
            extInfo.setInlongGroupId(groupId);
            extInfo.setInlongStreamId(streamId);
            String keyName = InlongGroupSettings.DATA_FLOW;
            extInfo.setKeyName(keyName);
            extInfo.setKeyValue(dataFlows);
            if (streamInfo.getExtList() == null) {
                groupInfo.setExtList(Lists.newArrayList());
            }
            upsertDataFlow(streamInfo, extInfo, keyName);
        } catch (Exception e) {
            log.error("create sort config failed for sink list={} of groupId={}, streamId={}", sinkResponses, groupId,
                    streamId, e);
            throw new WorkflowListenerException("create sort config failed: " + e.getMessage());
        }
        return ListenerResult.success();
    }

    private List<SourceResponse> createPulsarSources(InlongGroupInfo groupInfo, InlongStreamInfo streamInfo) {
        MQType mqType = MQType.forType(groupInfo.getMqType());
        if (mqType != MQType.PULSAR) {
            String errMsg = String.format("Unsupported MqType={} for Inlong", mqType);
            log.error(errMsg);
            throw new WorkflowListenerException(errMsg);
        }
        PulsarClusterInfo pulsarCluster = commonOperateService.getPulsarClusterInfo(groupInfo.getMqType());
        PulsarSourceResponse pulsarSourceResponse = new PulsarSourceResponse();
        pulsarSourceResponse.setSourceName(streamInfo.getInlongStreamId());
        pulsarSourceResponse.setNamespace(groupInfo.getMqResource());
        pulsarSourceResponse.setTopic(streamInfo.getMqResource());
        pulsarSourceResponse.setAdminUrl(pulsarCluster.getAdminUrl());
        pulsarSourceResponse.setServiceUrl(pulsarCluster.getBrokerServiceUrl());
        List<SourceResponse> sourceResponses = streamSourceService.listSource(groupInfo.getInlongGroupId(),
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
        return Lists.newArrayList(pulsarSourceResponse);
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

    private void upsertDataFlow(InlongStreamInfo streamInfo, InlongStreamExtInfo extInfo, String keyName) {
        streamInfo.getExtList().removeIf(ext -> keyName.equals(ext.getKeyName()));
        streamInfo.getExtList().add(extInfo);
    }

    @Override
    public boolean async() {
        return false;
    }
}
