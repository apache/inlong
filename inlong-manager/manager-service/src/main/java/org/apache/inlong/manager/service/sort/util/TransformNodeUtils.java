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
import org.apache.inlong.manager.common.pojo.transform.TransformResponse;
import org.apache.inlong.sort.protocol.FieldInfo;
import org.apache.inlong.sort.protocol.node.transform.TransformNode;

import java.util.List;
import java.util.stream.Collectors;

public class TransformNodeUtils {

    public static List<TransformNode>  createTransformNodes(List<TransformResponse> transformResponses) {
        if (CollectionUtils.isEmpty(transformResponses)) {
            return Lists.newArrayList();
        }
        List<TransformNode> transformNodes = transformResponses.stream()
                .map(transformResponse -> createTransformNode(transformResponse)).collect(Collectors.toList());
        return transformNodes;
    }

    public static TransformNode createTransformNode(TransformResponse transformResponse) {
        TransformNode transformNode = new TransformNode();
        transformNode.setId(transformResponse.getTransformName());
        transformNode.setName(transformResponse.getTransformName());
        List<FieldInfo> fieldInfos = transformResponse.getFieldList().stream().map(streamFieldInfo -> {
            String transformName = transformResponse.getTransformName();
            String fieldType = streamFieldInfo.getFieldType();
            String fieldFormat = streamFieldInfo.getFieldFormat();
            String fieldName = streamFieldInfo.getFieldName();
            FieldInfo fieldInfo = new FieldInfo(fieldName, transformName,
                    FieldInfoUtils.convertFieldFormat(fieldType, fieldFormat));
            return fieldInfo;
        }).collect(Collectors.toList());
        transformNode.setFields(fieldInfos);
        transformNode.setFieldRelationShips(FieldRelationShipUtils.createFieldRelationShips(transformResponse));
        transformNode.setFilters(
                FilterFunctionUtils.createFilterFunctions(transformResponse));
        return transformNode;
    }

}
