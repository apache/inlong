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

package org.apache.inlong.manager.service.resource.sort;

import org.apache.inlong.audit.CdcIdEnum;
import org.apache.inlong.audit.entity.AuditComponent;
import org.apache.inlong.audit.entity.AuditInformation;
import org.apache.inlong.audit.entity.AuditProxy;
import org.apache.inlong.audit.entity.CdcType;
import org.apache.inlong.audit.entity.FlowType;
import org.apache.inlong.common.enums.IndicatorType;
import org.apache.inlong.manager.common.consts.InlongConstants;
import org.apache.inlong.manager.common.consts.SinkType;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.pojo.group.InlongGroupInfo;
import org.apache.inlong.manager.pojo.sink.StreamSink;
import org.apache.inlong.manager.pojo.sort.node.NodeFactory;
import org.apache.inlong.manager.pojo.sort.util.NodeRelationUtils;
import org.apache.inlong.manager.pojo.sort.util.TransformNodeUtils;
import org.apache.inlong.manager.pojo.source.StreamSource;
import org.apache.inlong.manager.pojo.stream.InlongStreamExtInfo;
import org.apache.inlong.manager.pojo.stream.InlongStreamInfo;
import org.apache.inlong.manager.pojo.stream.StreamField;
import org.apache.inlong.manager.pojo.transform.TransformResponse;
import org.apache.inlong.manager.service.core.AuditService;
import org.apache.inlong.manager.service.source.StreamSourceService;
import org.apache.inlong.manager.service.transform.StreamTransformService;
import org.apache.inlong.sort.protocol.GroupInfo;
import org.apache.inlong.sort.protocol.StreamInfo;
import org.apache.inlong.sort.protocol.node.Node;
import org.apache.inlong.sort.protocol.node.transform.TransformNode;
import org.apache.inlong.sort.protocol.transformation.relation.NodeRelation;

import com.google.common.base.Joiner;
import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Default Sort config operator, used to create a Sort config for the InlongGroup with ZK disabled.
 */
@Service
public class SortFlinkConfigOperator implements SortConfigOperator {

    private static final Logger LOGGER = LoggerFactory.getLogger(SortFlinkConfigOperator.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Autowired
    private StreamSourceService sourceService;
    @Autowired
    private StreamTransformService transformService;
    @Autowired
    private AuditService auditService;
    @Autowired
    private NodeFactory nodeFactory;

    @Override
    public Boolean accept(List<String> sinkTypeList) {
        for (String sinkType : sinkTypeList) {
            if (SinkType.SORT_FLINK_SINK.contains(sinkType)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void buildConfig(InlongGroupInfo groupInfo, InlongStreamInfo streamInfo, boolean isStream)
            throws Exception {
        if (isStream) {
            LOGGER.warn("no need to build sort config for stream process when disable zk");
            return;
        }
        if (groupInfo == null || streamInfo == null) {
            LOGGER.warn("no need to build sort config as the group is null or stream is empty when disable zk");
            return;
        }
        List<StreamSink> sinkList = new ArrayList<>();
        for (StreamSink sink : streamInfo.getSinkList()) {
            if (SinkType.SORT_FLINK_SINK.contains(sink.getSinkType())) {
                sinkList.add(sink);
            }
        }
        if (CollectionUtils.isEmpty(sinkList)) {
            return;
        }
        GroupInfo sortConfigInfo = this.getGroupInfo(groupInfo, streamInfo, sinkList);
        String dataflow = OBJECT_MAPPER.writeValueAsString(sortConfigInfo);
        this.addToStreamExt(streamInfo, dataflow);

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("success to build sort config, isStream={}, dataflow={}", isStream, dataflow);
        }
    }

    private GroupInfo getGroupInfo(InlongGroupInfo groupInfo, InlongStreamInfo inlongStreamInfo,
            List<StreamSink> sinkInfos) {
        String streamId = inlongStreamInfo.getInlongStreamId();
        // get source info
        Map<String, List<StreamSource>> sourceMap = sourceService.getSourcesMap(groupInfo,
                Collections.singletonList(inlongStreamInfo));
        List<TransformResponse> transformList = transformService.listTransform(groupInfo.getInlongGroupId(), streamId);
        Map<String, List<TransformResponse>> transformMap = transformList.stream()
                .collect(Collectors.groupingBy(TransformResponse::getInlongStreamId, HashMap::new,
                        Collectors.toCollection(ArrayList::new)));

        List<StreamInfo> sortStreamInfos = new ArrayList<>();
        Map<String, StreamField> fieldMap = new HashMap<>();
        inlongStreamInfo.getSourceList().forEach(
                source -> parseConstantFieldMap(source.getSourceName(), source.getFieldList(), fieldMap));

        List<TransformResponse> transformResponses = transformMap.get(streamId);
        if (CollectionUtils.isNotEmpty(transformResponses)) {
            transformResponses.forEach(
                    trans -> parseConstantFieldMap(trans.getTransformName(), trans.getFieldList(), fieldMap));
        }

        // build a stream info from the nodes and relations
        List<StreamSource> sources = sourceMap.get(streamId);
        for (StreamSink sinkInfo : sinkInfos) {
            CommonBeanUtils.copyProperties(inlongStreamInfo, sinkInfo, true);
            addAuditId(sinkInfo.getProperties(), sinkInfo.getSinkType(), IndicatorType.SEND_SUCCESS);
        }

        for (StreamSource source : sources) {
            source.setFieldList(new ArrayList<>(inlongStreamInfo.getFieldList()));
        }
        List<NodeRelation> relations;

        if (InlongConstants.STANDARD_MODE.equals(groupInfo.getInlongGroupMode())) {
            if (CollectionUtils.isNotEmpty(transformResponses)) {
                relations = NodeRelationUtils.createNodeRelations(inlongStreamInfo);
                // in standard mode(include Data Ingestion and Synchronization), replace upstream source node and
                // transform input fields node to MQ node (which is InLong stream id)
                String mqNodeName = sources.get(0).getSourceName();
                Set<String> nodeNameSet = getInputNodeNames(sources, transformResponses);
                adjustTransformField(transformResponses, nodeNameSet, mqNodeName);
                adjustNodeRelations(relations, nodeNameSet, mqNodeName);
            } else {
                relations = NodeRelationUtils.createNodeRelations(sources, sinkInfos);
            }

            for (int i = 0; i < sources.size(); i++) {
                addAuditId(sources.get(i).getProperties(), sinkInfos.get(0).getSinkType(),
                        IndicatorType.RECEIVED_SUCCESS);
            }
        } else {
            if (CollectionUtils.isNotEmpty(transformResponses)) {
                List<String> sourcesNames = sources.stream().map(StreamSource::getSourceName)
                        .collect(Collectors.toList());
                List<String> transFormNames = transformResponses.stream().map(TransformResponse::getTransformName)
                        .collect(Collectors.toList());
                relations = Arrays.asList(NodeRelationUtils.createNodeRelation(sourcesNames, transFormNames),
                        NodeRelationUtils.createNodeRelation(transFormNames,
                                sinkInfos.stream().map(StreamSink::getSinkName).collect(Collectors.toList())));
            } else {
                relations = NodeRelationUtils.createNodeRelations(sources, sinkInfos);
            }

            for (StreamSource source : sources) {
                addAuditId(source.getProperties(), source.getSourceType(), IndicatorType.RECEIVED_SUCCESS);
            }
        }

        // create extract-transform-load nodes
        List<Node> nodes = this.createNodes(sources, transformResponses, sinkInfos, fieldMap);

        StreamInfo streamInfo = new StreamInfo(streamId, nodes, relations);
        sortStreamInfos.add(streamInfo);

        // rebuild joinerNode relation
        NodeRelationUtils.optimizeNodeRelation(streamInfo, transformResponses);

        return new GroupInfo(groupInfo.getInlongGroupId(), sortStreamInfos);
    }

    /**
     * Deduplicate to get the node names of Source and Transform.
     */
    private Set<String> getInputNodeNames(List<StreamSource> sources, List<TransformResponse> transforms) {
        Set<String> result = new HashSet<>();
        if (CollectionUtils.isNotEmpty(sources)) {
            result.addAll(sources.stream().map(StreamSource::getSourceName).collect(Collectors.toSet()));
        }
        if (CollectionUtils.isNotEmpty(transforms)) {
            result.addAll(transforms.stream().map(TransformResponse::getTransformName).collect(Collectors.toSet()));
        }
        return result;
    }

    /**
     * Set origin node to mq node for transform fields if necessary.
     *
     * In standard mode(include Data Ingestion and Synchronization) for InlongGroup, transform input node must either be
     * mq source node or transform node, otherwise replace it with mq node name.
     */
    private void adjustTransformField(List<TransformResponse> transforms, Set<String> nodeNameSet, String mqNodeName) {
        for (TransformResponse transform : transforms) {
            for (StreamField field : transform.getFieldList()) {
                if (!nodeNameSet.contains(field.getOriginNodeName())) {
                    field.setOriginNodeName(mqNodeName);
                }
            }
        }
    }

    /**
     * Set the input node to MQ node for NodeRelations
     */
    private void adjustNodeRelations(List<NodeRelation> relations, Set<String> nodeNameSet, String mqNodeName) {
        for (NodeRelation relation : relations) {
            ListIterator<String> iterator = relation.getInputs().listIterator();
            while (iterator.hasNext()) {
                if (!nodeNameSet.contains(iterator.next())) {
                    iterator.set(mqNodeName);
                }
            }
        }
    }

    private List<Node> createNodes(List<StreamSource> sources, List<TransformResponse> transformResponses,
            List<StreamSink> sinks, Map<String, StreamField> constantFieldMap) {
        List<Node> nodes = new ArrayList<>();
        if (Objects.equals(sources.size(), sinks.size()) && Objects.equals(sources.size(), 1)) {
            return nodeFactory.addBuiltInField(sources.get(0), sinks.get(0), transformResponses, constantFieldMap);
        }
        List<TransformNode> transformNodes =
                TransformNodeUtils.createTransformNodes(transformResponses, constantFieldMap);
        nodes.addAll(nodeFactory.createExtractNodes(sources));
        nodes.addAll(transformNodes);
        nodes.addAll(nodeFactory.createLoadNodes(sinks, constantFieldMap));
        return nodes;
    }

    /**
     * Get constant field from stream fields
     *
     * @param nodeId node id
     * @param fields stream fields
     * @param constantFieldMap constant field map
     */
    private void parseConstantFieldMap(String nodeId, List<StreamField> fields,
            Map<String, StreamField> constantFieldMap) {
        if (CollectionUtils.isEmpty(fields)) {
            return;
        }
        for (StreamField field : fields) {
            if (field.getFieldValue() != null) {
                constantFieldMap.put(String.format("%s-%s", nodeId, field.getFieldName()), field);
            }
        }
    }

    /**
     * Add config into inlong stream ext info
     */
    private void addToStreamExt(InlongStreamInfo streamInfo, String value) {
        if (streamInfo.getExtList() == null) {
            streamInfo.setExtList(new ArrayList<>());
        }

        InlongStreamExtInfo extInfo = new InlongStreamExtInfo();
        extInfo.setInlongGroupId(streamInfo.getInlongGroupId());
        extInfo.setInlongStreamId(streamInfo.getInlongStreamId());
        extInfo.setKeyName(InlongConstants.DATAFLOW);
        extInfo.setKeyValue(value);

        streamInfo.getExtList().removeIf(ext -> extInfo.getKeyName().equals(ext.getKeyName()));
        streamInfo.getExtList().add(extInfo);
    }

    private void addAuditId(Map<String, Object> properties, String type, IndicatorType indicatorType) {
        try {
            String auditId = auditService.getAuditId(type, indicatorType);
            List<AuditProxy> auditProxyList = auditService.getAuditProxy(AuditComponent.SORT.getComponent());
            properties.putIfAbsent("metrics.audit.key", auditId);
            properties.putIfAbsent("metrics.audit.proxy.hosts",
                    Joiner.on(InlongConstants.AMPERSAND).join(auditProxyList));
            List<AuditInformation> cdcAuditInfoList = auditService.getCdcAuditInfoList(type, indicatorType);
            FlowType flowType = indicatorType.getCode() % 2 == 0 ? FlowType.INPUT : FlowType.OUTPUT;
            List<String> cdcAuditIdList =
                    cdcAuditInfoList.stream().map(v -> {
                        for (CdcType cdcType : CdcType.values()) {
                            if (Objects.equals(CdcIdEnum.getCdcId(type, flowType, cdcType), v.getAuditId())) {
                                return cdcType.name() + InlongConstants.EQUAL + v.getAuditId();
                            }
                        }
                        throw new BusinessException("current audit id can not find cdc audit information");
                    }).collect(Collectors.toList());
            if (CollectionUtils.isNotEmpty(cdcAuditIdList)) {
                properties.putIfAbsent("metrics.changelog.audit.key",
                        Joiner.on(InlongConstants.AMPERSAND).join(cdcAuditIdList));
            }
        } catch (Exception e) {
            LOGGER.error("Current type ={} is not set auditId", type);
        }

    }
}
