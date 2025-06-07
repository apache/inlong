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

package org.apache.inlong.manager.pojo.sort.util;

import org.apache.inlong.common.pojo.sort.dataflow.field.format.FormatInfo;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.StringTypeInfo;
import org.apache.inlong.manager.common.enums.FieldType;
import org.apache.inlong.manager.common.enums.TransformType;
import org.apache.inlong.manager.common.util.Preconditions;
import org.apache.inlong.manager.pojo.stream.StreamField;
import org.apache.inlong.manager.pojo.transform.TransformDefinition;
import org.apache.inlong.manager.pojo.transform.TransformDefinition.OperationType;
import org.apache.inlong.manager.pojo.transform.TransformDefinition.RuleRelation;
import org.apache.inlong.manager.pojo.transform.TransformResponse;
import org.apache.inlong.manager.pojo.transform.filter.FilterDefinition;
import org.apache.inlong.manager.pojo.transform.filter.FilterDefinition.FilterMode;
import org.apache.inlong.manager.pojo.transform.filter.FilterDefinition.FilterRule;
import org.apache.inlong.manager.pojo.transform.filter.FilterDefinition.TargetValue;
import org.apache.inlong.sort.protocol.FieldInfo;
import org.apache.inlong.sort.protocol.enums.FilterStrategy;
import org.apache.inlong.sort.protocol.transformation.CompareOperator;
import org.apache.inlong.sort.protocol.transformation.ConstantParam;
import org.apache.inlong.sort.protocol.transformation.FilterFunction;
import org.apache.inlong.sort.protocol.transformation.FunctionParam;
import org.apache.inlong.sort.protocol.transformation.LogicOperator;
import org.apache.inlong.sort.protocol.transformation.MultiValueCompareOperator;
import org.apache.inlong.sort.protocol.transformation.SingleValueCompareOperator;
import org.apache.inlong.sort.protocol.transformation.StringConstantParam;
import org.apache.inlong.sort.protocol.transformation.function.CustomFunction;
import org.apache.inlong.sort.protocol.transformation.function.MultiValueFilterFunction;
import org.apache.inlong.sort.protocol.transformation.function.SingleValueFilterFunction;
import org.apache.inlong.sort.protocol.transformation.operator.AndOperator;
import org.apache.inlong.sort.protocol.transformation.operator.EmptyOperator;
import org.apache.inlong.sort.protocol.transformation.operator.EqualOperator;
import org.apache.inlong.sort.protocol.transformation.operator.InOperator;
import org.apache.inlong.sort.protocol.transformation.operator.IsNotNullOperator;
import org.apache.inlong.sort.protocol.transformation.operator.IsNullOperator;
import org.apache.inlong.sort.protocol.transformation.operator.LessThanOperator;
import org.apache.inlong.sort.protocol.transformation.operator.LessThanOrEqualOperator;
import org.apache.inlong.sort.protocol.transformation.operator.MoreThanOperator;
import org.apache.inlong.sort.protocol.transformation.operator.MoreThanOrEqualOperator;
import org.apache.inlong.sort.protocol.transformation.operator.NotEqualOperator;
import org.apache.inlong.sort.protocol.transformation.operator.OrOperator;

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Util for creat filter function.
 */
@Slf4j
public class FilterFunctionUtils {

    /**
     * Create functions of filter.
     */
    public static List<FilterFunction> createFilterFunctions(TransformResponse transformResponse) {
        TransformType transformType = TransformType.forType(transformResponse.getTransformType());
        TransformDefinition transformDefinition = StreamParseUtils.parseTransformDefinition(
                transformResponse.getTransformDefinition(), transformType);
        String transformName = transformResponse.getTransformName();
        switch (transformType) {
            case FILTER:
                FilterDefinition filterDefinition = (FilterDefinition) transformDefinition;
                return createFilterFunctions(filterDefinition, transformName);
            case DE_DUPLICATION:
            case SPLITTER:
            case JOINER:
            case LOOKUP_JOINER:
            case TEMPORAL_JOINER:
            case INTERVAL_JOINER:
            case STRING_REPLACER:
            case ENCRYPT:
                return Lists.newArrayList();
            default:
                throw new UnsupportedOperationException(String.format("Unsupported transformType=%s", transformType));
        }
    }

    /**
     * Create functions of filter.
     */
    public static List<FilterFunction> createFilterFunctions(FilterDefinition filterDefinition, String transformName) {
        FilterMode filterMode = filterDefinition.getFilterMode();
        Preconditions.expectFalse(filterMode == FilterMode.SCRIPT,
                String.format("Unsupported filterMode=%s for inlong", filterMode));
        List<FilterRule> filterRules = filterDefinition.getFilterRules();
        List<FilterFunction> filterFunctions = filterRules.stream()
                .map(filterRule -> createFilterFunction(filterRule, transformName)).collect(Collectors.toList());
        // Move logicOperator to preFunction
        for (int index = filterFunctions.size() - 1; index > 0; index--) {
            FilterFunction function = filterFunctions.get(index);
            FilterFunction preFunction = filterFunctions.get(index - 1);
            function.setLogicOperator(preFunction.getLogicOperator());
        }
        (filterFunctions.get(0)).setLogicOperator(EmptyOperator.getInstance());
        return filterFunctions;
    }

    /**
     * Parse filter strategy from TransformResponse and convert to the filter strategy of sort protocol
     *
     * @param transformResponse The transform response that may contain filter operation
     * @return The filter strategy, see {@link FilterStrategy}
     */
    public static FilterStrategy parseFilterStrategy(TransformResponse transformResponse) {
        TransformType transformType = TransformType.forType(transformResponse.getTransformType());
        TransformDefinition transformDefinition = StreamParseUtils.parseTransformDefinition(
                transformResponse.getTransformDefinition(), transformType);
        switch (transformType) {
            case FILTER:
                FilterDefinition filterDefinition = (FilterDefinition) transformDefinition;
                switch (filterDefinition.getFilterStrategy()) {
                    case REMOVE:
                        return FilterStrategy.REMOVE;
                    case RETAIN:
                        return FilterStrategy.RETAIN;
                    default:
                        return FilterStrategy.RETAIN;
                }
            case DE_DUPLICATION:
            case SPLITTER:
            case JOINER:
            case LOOKUP_JOINER:
            case TEMPORAL_JOINER:
            case INTERVAL_JOINER:
            case STRING_REPLACER:
            case ENCRYPT:
                return null;
            default:
                throw new UnsupportedOperationException(
                        String.format("Unsupported transformType=%s", transformType));
        }
    }

    private static FilterFunction createFilterFunction(FilterRule filterRule, String transformName) {
        StreamField streamField = filterRule.getSourceField();
        String fieldType = streamField.getFieldType();
        String fieldFormat = streamField.getFieldFormat();
        String fieldName = streamField.getFieldName();
        FunctionParam sourceFieldInfo;
        if (FieldType.FUNCTION.name().equalsIgnoreCase(streamField.getFieldType())) {
            sourceFieldInfo = new CustomFunction(streamField.getFieldName());
        } else {
            sourceFieldInfo = new FieldInfo(fieldName, transformName,
                    FieldInfoUtils.convertFieldFormat(fieldType, fieldFormat));
        }
        OperationType operationType = filterRule.getOperationType();
        CompareOperator compareOperator = parseCompareOperator(operationType);
        TargetValue targetValue = filterRule.getTargetValue();
        FunctionParam target = parseTargetValue(targetValue, transformName);
        RuleRelation relationWithPost = filterRule.getRelationWithPost();
        LogicOperator logicOperator = parseLogicOperator(relationWithPost);
        if (compareOperator instanceof SingleValueCompareOperator) {
            return new SingleValueFilterFunction(logicOperator, sourceFieldInfo,
                    (SingleValueCompareOperator) compareOperator, target);
        } else {
            List<FunctionParam> targets = new ArrayList<>();
            if (CollectionUtils.isNotEmpty(filterRule.getTargetValues())) {
                for (TargetValue value : filterRule.getTargetValues()) {
                    targets.add(parseTargetValue(value, transformName));
                }
            } else {
                targets.add(target);
            }
            return new MultiValueFilterFunction(sourceFieldInfo, targets, (MultiValueCompareOperator) compareOperator,
                    logicOperator);
        }
    }

    private static LogicOperator parseLogicOperator(RuleRelation relation) {
        if (relation == null) {
            return EmptyOperator.getInstance();
        }
        switch (relation) {
            case OR:
                return OrOperator.getInstance();
            case AND:
                return AndOperator.getInstance();
            default:
                return EmptyOperator.getInstance();
        }
    }

    private static FunctionParam parseTargetValue(TargetValue value, String transformName) {
        if (value == null) {
            return new ConstantParam("");
        }
        boolean isConstant = value.isConstant();
        StreamField targetField = value.getTargetField();
        String fieldType = targetField.getFieldType();
        if (isConstant) {
            FormatInfo formatInfo = FieldInfoUtils.convertFieldFormat(
                    targetField.getFieldType(), targetField.getFieldFormat());
            String constant = value.getTargetConstant();
            return formatInfo.getTypeInfo() == StringTypeInfo.INSTANCE ? new StringConstantParam(constant)
                    : new ConstantParam(constant);
        } else {
            String fieldFormat = targetField.getFieldFormat();
            String fieldName = targetField.getFieldName();
            if (FieldType.FUNCTION.name().equalsIgnoreCase(fieldType)) {
                return new CustomFunction(fieldName);
            } else {
                return new FieldInfo(fieldName, transformName,
                        FieldInfoUtils.convertFieldFormat(fieldType, fieldFormat));
            }
        }
    }

    private static CompareOperator parseCompareOperator(OperationType operationType) {
        switch (operationType) {
            case eq:
                return EqualOperator.getInstance();
            case ge:
                return MoreThanOrEqualOperator.getInstance();
            case gt:
                return MoreThanOperator.getInstance();
            case le:
                return LessThanOrEqualOperator.getInstance();
            case lt:
                return LessThanOperator.getInstance();
            case ne:
                return NotEqualOperator.getInstance();
            case is_null:
                return IsNullOperator.getInstance();
            case not_null:
                return IsNotNullOperator.getInstance();
            case in:
                return InOperator.getInstance();
            default:
                throw new IllegalArgumentException(String.format("Unsupported operateType=%s", operationType));
        }
    }
}
