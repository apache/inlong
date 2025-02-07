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

package org.apache.inlong.manager.pojo.sort.node;

import org.apache.inlong.manager.pojo.sink.StreamSink;
import org.apache.inlong.manager.pojo.sort.node.base.ExtractNodeProvider;
import org.apache.inlong.manager.pojo.sort.node.base.LoadNodeProvider;
import org.apache.inlong.manager.pojo.sort.util.FieldInfoUtils;
import org.apache.inlong.manager.pojo.sort.util.TransformNodeUtils;
import org.apache.inlong.manager.pojo.source.StreamSource;
import org.apache.inlong.manager.pojo.stream.StreamField;
import org.apache.inlong.manager.pojo.transform.TransformResponse;
import org.apache.inlong.sort.protocol.node.ExtractNode;
import org.apache.inlong.sort.protocol.node.LoadNode;
import org.apache.inlong.sort.protocol.node.Node;
import org.apache.inlong.sort.protocol.node.transform.TransformNode;

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * The node factory
 */
@Slf4j
@Service
public class NodeFactory {

    @Autowired
    private LoadNodeProviderFactory loadNodeProviderFactory;
    @Autowired
    private ExtractNodeProviderFactory extractNodeProviderFactory;

    /**
     * Create extract nodes from the given sources.
     */
    public List<ExtractNode> createExtractNodes(List<StreamSource> sourceInfos) {
        if (CollectionUtils.isEmpty(sourceInfos)) {
            return Lists.newArrayList();
        }
        return sourceInfos.stream().map(v -> {
            String sourceType = v.getSourceType();
            return extractNodeProviderFactory.getExtractNodeProvider(sourceType).createExtractNode(v);
        }).collect(Collectors.toList());
    }

    /**
     * Create load nodes from the given sinks.
     */
    public List<LoadNode> createLoadNodes(List<StreamSink> sinkInfos,
            Map<String, StreamField> constantFieldMap) {
        if (CollectionUtils.isEmpty(sinkInfos)) {
            return Lists.newArrayList();
        }
        return sinkInfos.stream().map(v -> {
            String sinkType = v.getSinkType();
            return loadNodeProviderFactory.getLoadNodeProvider(sinkType).createLoadNode(v, constantFieldMap);
        }).collect(Collectors.toList());
    }

    /**
     * Create extract node from the given source.
     */
    public ExtractNode createExtractNode(StreamSource sourceInfo) {
        if (sourceInfo == null) {
            return null;
        }
        String sourceType = sourceInfo.getSourceType();
        return extractNodeProviderFactory.getExtractNodeProvider(sourceType).createExtractNode(sourceInfo);
    }

    /**
     * Create load node from the given sink.
     */
    public LoadNode createLoadNode(StreamSink sinkInfo, Map<String, StreamField> constantFieldMap) {
        if (sinkInfo == null) {
            return null;
        }
        String sinkType = sinkInfo.getSinkType();
        return loadNodeProviderFactory.getLoadNodeProvider(sinkType).createLoadNode(sinkInfo, constantFieldMap);
    }

    /**
     * Add built-in field for extra node and load node
     */
    public List<Node> addBuiltInField(StreamSource sourceInfo, StreamSink sinkInfo,
            List<TransformResponse> transformResponses, Map<String, StreamField> constantFieldMap) {
        ExtractNodeProvider extractNodeProvider = extractNodeProviderFactory.getExtractNodeProvider(
                sourceInfo.getSourceType());
        LoadNodeProvider loadNodeProvider = loadNodeProviderFactory.getLoadNodeProvider(sinkInfo.getSinkType());

        if (loadNodeProvider.isSinkMultiple(sinkInfo)) {
            sourceInfo.setFieldList(loadNodeProvider.addStreamFieldsForSinkMultiple(sourceInfo.getFieldList()));
            sinkInfo.setSinkFieldList(loadNodeProvider.addSinkFieldsForSinkMultiple(sinkInfo.getSinkFieldList()));
        }
        if (FieldInfoUtils.compareFields(extractNodeProvider.getMetaFields(), loadNodeProvider.getMetaFields())) {
            if (loadNodeProvider.needInlongPropertiesField(sinkInfo)) {
                loadNodeProvider.addInlongPropertiesFieldForStream(sourceInfo.getFieldList());
                loadNodeProvider.addInlongPropertiesFieldForSink(sinkInfo.getSinkFieldList());
            }
            if (extractNodeProvider.needInlongPropertiesField(sourceInfo)) {
                extractNodeProvider.addInlongPropertiesFieldForStream(sourceInfo.getFieldList());
            }
            extractNodeProvider.addStreamMetaFields(sourceInfo.getFieldList());
            if (CollectionUtils.isNotEmpty(transformResponses)) {
                transformResponses.forEach(v -> {
                    if (loadNodeProvider.needInlongPropertiesField(sinkInfo)) {
                        loadNodeProvider.addInlongPropertiesFieldForStream(v.getFieldList());
                    }
                    if (extractNodeProvider.needInlongPropertiesField(sourceInfo)) {
                        extractNodeProvider.addInlongPropertiesFieldForStream(v.getFieldList());
                    }
                    extractNodeProvider.addStreamMetaFields(v.getFieldList());
                });
            }
            loadNodeProvider.addSinkMetaFields(sinkInfo.getSinkFieldList());
        }

        ExtractNode extractNode = extractNodeProvider.createExtractNode(sourceInfo);
        List<TransformNode> transformNodes =
                TransformNodeUtils.createTransformNodes(transformResponses, constantFieldMap);
        LoadNode loadNode = loadNodeProvider.createLoadNode(sinkInfo, constantFieldMap);

        List<Node> nodes = new ArrayList<>();
        nodes.add(extractNode);
        nodes.addAll(transformNodes);
        nodes.add(loadNode);
        return nodes;
    }
}
