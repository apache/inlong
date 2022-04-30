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

package org.apache.inlong.manager.service.sort.util;

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.shaded.com.google.common.collect.Maps;
import org.apache.inlong.manager.common.enums.TransformType;
import org.apache.inlong.manager.common.pojo.stream.InlongStreamInfo;
import org.apache.inlong.manager.common.pojo.stream.StreamField;
import org.apache.inlong.manager.common.pojo.stream.StreamNode;
import org.apache.inlong.manager.common.pojo.stream.StreamPipeline;
import org.apache.inlong.manager.common.pojo.stream.StreamSink;
import org.apache.inlong.manager.common.pojo.stream.StreamSource;
import org.apache.inlong.manager.common.pojo.stream.StreamTransform;
import org.apache.inlong.manager.common.pojo.transform.TransformDefinition;
import org.apache.inlong.manager.common.pojo.transform.TransformResponse;
import org.apache.inlong.manager.common.pojo.transform.joiner.JoinerDefinition;
import org.apache.inlong.manager.common.pojo.transform.joiner.JoinerDefinition.JoinMode;
import org.apache.inlong.manager.common.util.StreamParseUtils;
import org.apache.inlong.sort.protocol.FieldInfo;
import org.apache.inlong.sort.protocol.StreamInfo;
import org.apache.inlong.sort.protocol.node.Node;
import org.apache.inlong.sort.protocol.node.transform.TransformNode;
import org.apache.inlong.sort.protocol.transformation.FilterFunction;
import org.apache.inlong.sort.protocol.transformation.LogicOperator;
import org.apache.inlong.sort.protocol.transformation.function.SingleValueFilterFunction;
import org.apache.inlong.sort.protocol.transformation.operator.AndOperator;
import org.apache.inlong.sort.protocol.transformation.operator.EmptyOperator;
import org.apache.inlong.sort.protocol.transformation.operator.EqualOperator;
import org.apache.inlong.sort.protocol.transformation.relation.InnerJoinNodeRelationShip;
import org.apache.inlong.sort.protocol.transformation.relation.LeftOuterJoinNodeRelationShip;
import org.apache.inlong.sort.protocol.transformation.relation.NodeRelationShip;
import org.apache.inlong.sort.protocol.transformation.relation.RightOuterJoinNodeRelationShip;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
public class NodeRelationShipUtils {

    public static List<NodeRelationShip> createNodeRelationShipsForStream(InlongStreamInfo streamInfo) {
        String tempView = streamInfo.getTempView();
        if (StringUtils.isEmpty(tempView)) {
            log.warn("StreamNodeRelationShip is empty for Stream={}", streamInfo);
            return Lists.newArrayList();
        }
        StreamPipeline pipeline = StreamParseUtils.parseStreamPipeline(streamInfo.getTempView(),
                streamInfo.getInlongStreamId());
        List<NodeRelationShip> nodeRelationShips = pipeline.getPipeline().stream()
                .map(streamNodeRelationShip -> new NodeRelationShip(
                        Lists.newArrayList(streamNodeRelationShip.getInputNodes()),
                        Lists.newArrayList(streamNodeRelationShip.getOutputNodes())))
                .collect(
                        Collectors.toList());
        return nodeRelationShips;
    }

    public static void optimizeNodeRelationShips(StreamInfo streamInfo, List<TransformResponse> transformResponses) {
        if (CollectionUtils.isEmpty(transformResponses)) {
            return;
        }
        Map<String, TransformDefinition> transformTypeMap = transformResponses.stream().collect(
                Collectors.toMap(transformResponse -> transformResponse.getTransformName(),
                        transformResponse -> {
                            TransformType transformType = TransformType.forType(transformResponse.getTransformType());
                            return StreamParseUtils.parseTransformDefinition(transformResponse.getTransformDefinition(),
                                    transformType);
                        }));
        List<Node> nodes = streamInfo.getNodes();
        Map<String, TransformNode> joinNodes = nodes.stream().filter(node -> node instanceof TransformNode)
                .map(node -> (TransformNode) node)
                .filter(transformNode -> {
                    TransformDefinition transformDefinition = transformTypeMap.get(transformNode.getName());
                    return transformDefinition.getTransformType() == TransformType.JOINER;
                }).collect(Collectors.toMap(transformNode -> transformNode.getName(), transformNode -> transformNode));
        List<NodeRelationShip> relationShips = streamInfo.getRelations();
        Iterator<NodeRelationShip> shipIterator = relationShips.listIterator();
        List<NodeRelationShip> joinRelationShips = Lists.newArrayList();
        while (shipIterator.hasNext()) {
            NodeRelationShip relationShip = shipIterator.next();
            List<String> outputs = relationShip.getOutputs();
            if (outputs.size() == 1) {
                String nodeName = outputs.get(0);
                if (joinNodes.get(nodeName) != null) {
                    TransformDefinition transformDefinition = transformTypeMap.get(nodeName);
                    TransformNode transformNode = joinNodes.get(nodeName);
                    joinRelationShips.add(createNodeRelationShip((JoinerDefinition) transformDefinition,
                            relationShip,
                            transformNode.getId()));
                    shipIterator.remove();
                }
            }
        }
        relationShips.addAll(joinRelationShips);
    }

    private static NodeRelationShip createNodeRelationShip(JoinerDefinition joinerDefinition,
            NodeRelationShip nodeRelationShip, String nodeId) {
        JoinMode joinMode = joinerDefinition.getJoinMode();
        String leftNode = getNodeName(joinerDefinition.getLeftNode());
        String rightNode = getNodeName(joinerDefinition.getRightNode());
        List<String> preNodes = Lists.newArrayList(leftNode, rightNode);
        List<StreamField> leftJoinFields = joinerDefinition.getLeftJoinFields();
        List<StreamField> rightJoinFields = joinerDefinition.getRightJoinFields();
        List<FilterFunction> filterFunctions = Lists.newArrayList();
        for (int index = 0; index < leftJoinFields.size(); index++) {
            StreamField leftField = leftJoinFields.get(index);
            StreamField rightField = rightJoinFields.get(index);
            LogicOperator operator = null;
            if (index != leftJoinFields.size() - 1) {
                operator = AndOperator.getInstance();
            } else {
                operator = EmptyOperator.getInstance();
            }
            filterFunctions.add(
                    createFilterFunction(leftField, rightField, leftNode, rightNode, operator));
        }
        Map<String, List<FilterFunction>> joinConditions = Maps.newHashMap();
        joinConditions.put(rightNode, filterFunctions);
        switch (joinMode) {
            case LEFT_JOIN:
                return new LeftOuterJoinNodeRelationShip(preNodes, nodeRelationShip.getOutputs(),
                        joinConditions);
            case INNER_JOIN:
                return new RightOuterJoinNodeRelationShip(preNodes, nodeRelationShip.getOutputs(),
                        joinConditions);
            case RIGHT_JOIN:
                return new InnerJoinNodeRelationShip(preNodes, nodeRelationShip.getOutputs(),
                        joinConditions);
            default:
                throw new IllegalArgumentException(String.format("Unsupported join mode=%s for inlong", joinMode));
        }
    }

    private static SingleValueFilterFunction createFilterFunction(StreamField leftField, StreamField rightField,
            String leftNode, String rightNode, LogicOperator operator) {
        FieldInfo sourceField = new FieldInfo(leftField.getFieldName(), leftNode,
                FieldInfoUtils.convertFieldFormat(leftField.getFieldType().name(), leftField.getFieldFormat()));
        FieldInfo targetField = new FieldInfo(rightField.getFieldName(), rightNode,
                FieldInfoUtils.convertFieldFormat(rightField.getFieldType().name(), rightField.getFieldFormat()));
        return new SingleValueFilterFunction(operator, sourceField, EqualOperator.getInstance(), targetField);
    }

    private static String getNodeName(StreamNode node) {
        if (node instanceof StreamSource) {
            return ((StreamSource) node).getSourceName();
        } else if (node instanceof StreamSink) {
            return ((StreamSink) node).getSinkName();
        } else {
            return ((StreamTransform) node).getTransformName();
        }
    }

}
