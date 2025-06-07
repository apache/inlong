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

package org.apache.inlong.manager.service.transform;

import org.apache.inlong.manager.common.consts.InlongConstants;
import org.apache.inlong.manager.common.enums.TransformType;
import org.apache.inlong.manager.pojo.sink.SinkField;
import org.apache.inlong.manager.pojo.sort.util.FilterFunctionUtils;
import org.apache.inlong.manager.pojo.sort.util.StreamParseUtils;
import org.apache.inlong.manager.pojo.transform.TransformDefinition;
import org.apache.inlong.manager.pojo.transform.TransformResponse;
import org.apache.inlong.manager.pojo.transform.filter.FilterDefinition;
import org.apache.inlong.sort.protocol.transformation.FilterFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class TransformSqlParser {

    private static final Logger log = LoggerFactory.getLogger(TransformSqlParser.class);

    public static String parse(List<TransformResponse> transformResponseList, List<SinkField> sinkFields) {
        log.info("start to parse transform sql for transform list={}", transformResponseList);
        StringBuilder result = new StringBuilder()
                .append(genSimpleSelectSql(sinkFields))
                .append(getFilterSql(transformResponseList));
        log.info("success to parse transform for transform list={}, result={}", transformResponseList, result);
        return result.toString().replaceAll("`", "");
    }

    private static StringBuilder genSimpleSelectSql(List<SinkField> fields) {
        StringBuilder sb = new StringBuilder("SELECT ");
        for (SinkField field : fields) {
            sb.append("`").append(field.getSourceFieldName()).append("`,");
        }
        sb.deleteCharAt(sb.length() - 1).append(" FROM SOURCE");
        return sb;
    }

    private static StringBuilder getFilterSql(List<TransformResponse> filterList) {
        StringBuilder stringBuilder = new StringBuilder();
        for (TransformResponse transformResponse : filterList) {
            TransformType transformType = TransformType.forType(transformResponse.getTransformType());
            TransformDefinition transformDefinition = StreamParseUtils.parseTransformDefinition(
                    transformResponse.getTransformDefinition(), transformType);
            String transformName = transformResponse.getTransformName();
            switch (transformType) {
                case FILTER:
                    stringBuilder.append(" WHERE");
                    FilterDefinition filterDefinition = (FilterDefinition) transformDefinition;
                    List<FilterFunction> filterFunctions =
                            FilterFunctionUtils.createFilterFunctions(filterDefinition, transformName);
                    for (FilterFunction filterFunction : filterFunctions) {
                        stringBuilder.append(InlongConstants.BLANK).append(filterFunction.format());
                    }
                    break;
                case SPLITTER:
                case STRING_REPLACER:
                case ENCRYPT:
                case DE_DUPLICATION:
                case JOINER:
                case LOOKUP_JOINER:
                case TEMPORAL_JOINER:
                case INTERVAL_JOINER:
                    break;
                default:
                    throw new UnsupportedOperationException(
                            String.format("Unsupported transformType=%s", transformType));
            }
        }
        return stringBuilder;
    }

}
