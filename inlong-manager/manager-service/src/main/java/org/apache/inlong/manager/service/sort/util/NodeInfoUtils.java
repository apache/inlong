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
import org.apache.commons.collections.CollectionUtils;
import org.apache.inlong.manager.common.enums.TransformType;
import org.apache.inlong.manager.common.pojo.sink.SinkResponse;
import org.apache.inlong.manager.common.pojo.source.SourceResponse;
import org.apache.inlong.manager.common.pojo.transform.TransformDefinition;
import org.apache.inlong.manager.common.pojo.transform.TransformResponse;
import org.apache.inlong.manager.common.util.StreamParseUtils;
import org.apache.inlong.sort.protocol.node.ExtractNode;
import org.apache.inlong.sort.protocol.node.LoadNode;
import org.apache.inlong.sort.protocol.node.transform.TransformNode;

import java.util.List;
import java.util.stream.Collectors;

public class NodeInfoUtils {

    public static List<ExtractNode> createExtractNodes(List<SourceResponse> sourceResponses) {
        if (CollectionUtils.isEmpty(sourceResponses)) {
            return Lists.newArrayList();
        }
        //todo transfer sourceResponse to extractNode
        return Lists.newArrayList();
    }

    public static List<LoadNode> createLoadNodes(List<SinkResponse> sinkResponses) {
        if (CollectionUtils.isEmpty(sinkResponses)) {
            return Lists.newArrayList();
        }
        //todo transfer sinkResponse to LoadNode
        return Lists.newArrayList();
    }

    public static List<TransformNode> createTransformNodes(List<TransformResponse> transformResponses) {
        if (CollectionUtils.isEmpty(transformResponses)) {
            return Lists.newArrayList();
        }
        List<TransformNode> transformNodes = transformResponses.stream()
                .map(transformResponse -> createTransformNode(transformResponse)).collect(Collectors.toList());
        return transformNodes;
    }

    public static TransformNode createTransformNode(TransformResponse transformResponse) {
        TransformType transformType = TransformType.forType(transformResponse.getTransformType());
        TransformDefinition transformDefinition = StreamParseUtils.parseTransformDefinition(
                transformResponse.getTransformDefinition(), transformType);
        TransformNode transformNode = new TransformNode();
        transformNode.setId(transformResponse.getTransformName());
        transformNode.setName(transformResponse.getTransformName());
        //todo parse transformResponse
        return transformNode;
    }
}
