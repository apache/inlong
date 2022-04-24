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
import com.google.common.collect.Sets;
import org.apache.inlong.manager.common.enums.TransformType;
import org.apache.inlong.manager.common.pojo.stream.InlongStreamFieldInfo;
import org.apache.inlong.manager.common.pojo.stream.StreamField;
import org.apache.inlong.manager.common.pojo.transform.TransformDefinition;
import org.apache.inlong.manager.common.pojo.transform.TransformResponse;
import org.apache.inlong.manager.common.pojo.transform.splitter.SplitterDefinition;
import org.apache.inlong.manager.common.pojo.transform.splitter.SplitterDefinition.SplitRule;
import org.apache.inlong.manager.common.util.StreamParseUtils;
import org.apache.inlong.sort.protocol.FieldInfo;
import org.apache.inlong.sort.protocol.transformation.FieldRelationShip;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class FieldRelationShipUtils {

    public static List<FieldRelationShip> createFieldRelationShips(TransformResponse transformResponse) {
        TransformType transformType = TransformType.forType(transformResponse.getTransformType());
        TransformDefinition transformDefinition = StreamParseUtils.parseTransformDefinition(
                transformResponse.getTransformDefinition(), transformType);
        List<InlongStreamFieldInfo> fieldList = transformResponse.getFieldList();
        String transformName = transformResponse.getTransformName();
        switch (transformType) {
            case DE_DUPLICATION:
                //todo need sort
                return Lists.newArrayList();
            case FILTER:
                return createFieldRelationShips(fieldList, transformName);
            case SPLITTER:
                SplitterDefinition splitterDefinition = (SplitterDefinition) transformDefinition;
                return createFieldRelationShips(fieldList, transformName, splitterDefinition.getSplitRules());
            case JOINER:
            case STRING_REPLACER:
            default:
                throw new UnsupportedOperationException(
                        String.format("Unsupported transformType=%s for Inlong", transformType));
        }
    }

    private static List<FieldRelationShip> createFieldRelationShips(List<InlongStreamFieldInfo> fieldList,
            String transformName) {
        return fieldList.stream().map(streamFieldInfo ->
                        new FieldInfo(streamFieldInfo.getFieldName(), transformName,
                                FieldInfoUtils.convertFieldFormat(streamFieldInfo.getFieldType(),
                                        streamFieldInfo.getFieldFormat())))
                .map(fieldInfo -> new FieldRelationShip(fieldInfo, fieldInfo))
                .collect(Collectors.toList());
    }

    private static List<FieldRelationShip> createFieldRelationShips(List<InlongStreamFieldInfo> fieldList,
            String transformName, List<SplitRule> splitRules) {
        Set<String> splitFields = Sets.newHashSet();
        List<FieldRelationShip> fieldRelationShips = splitRules.stream().map(splitRule -> {
            StreamField sourceField = splitRule.getSourceField();
            splitFields.add(sourceField.getFieldName());
            FieldInfo fieldInfo = new FieldInfo(sourceField.getFieldName(), transformName,
                    FieldInfoUtils.convertFieldFormat(sourceField.getFieldType().name(), sourceField.getFieldFormat()));

        }).collect(Collectors.toList());

    }

}
