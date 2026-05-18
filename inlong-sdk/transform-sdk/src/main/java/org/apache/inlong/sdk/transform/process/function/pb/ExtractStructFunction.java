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

package org.apache.inlong.sdk.transform.process.function.pb;

import org.apache.inlong.sdk.transform.decode.PbNode;
import org.apache.inlong.sdk.transform.decode.PbSourceData;
import org.apache.inlong.sdk.transform.decode.SourceData;
import org.apache.inlong.sdk.transform.process.Context;
import org.apache.inlong.sdk.transform.process.function.FunctionConstant;
import org.apache.inlong.sdk.transform.process.function.TransformFunction;
import org.apache.inlong.sdk.transform.process.operator.OperatorTools;
import org.apache.inlong.sdk.transform.process.parser.ColumnParser;
import org.apache.inlong.sdk.transform.process.parser.ValueParser;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType;
import com.google.protobuf.DynamicMessage;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;

import java.util.ArrayList;
import java.util.List;

/**
 * ExtractStructFunction  ->  extract_struct(path, field1, field2, field3...)
 * description:
 * - Only works on protobuf source data; returns NULL if the source is not a PbSourceData.
 * - Returns NULL if 'path' is missing/invalid, or the path cannot be resolved to a
 *   DynamicMessage in the protobuf source data.
 * - Otherwise, returns a GenericRowData whose arity equals the number of declared
 *   fields (field1, field2, ...). For each declared field:
 *     - if the field can be resolved on the located message, the corresponding
 *       position is filled with the resolved value;
 *     - otherwise (field not found, or the parameter is not a column reference),
 *       the corresponding position is set to NULL.
 */
@TransformFunction(type = FunctionConstant.PB_TYPE, names = {
        "extract_struct"}, parameter = "(path, field1,field2,field3...)", descriptions = {
                "- Only works on protobuf source data; returns NULL if the source is not a PbSourceData;",
                "- Returns NULL if 'path' is missing/invalid, or the path cannot be resolved "
                        + "to a DynamicMessage in the protobuf source data;",
                "- Otherwise, returns a GenericRowData whose arity equals the number of declared fields. "
                        + "Each position is filled with the resolved field value, or NULL if the field "
                        + "cannot be resolved on the located message."
        }, examples = {
                "extract_struct($root.person,name,age) = +I(\"Alice\",11)"
        })
public class ExtractStructFunction implements ValueParser {

    private final ValueParser pathParser;
    private final List<ValueParser> fieldParsers;
    private String path;

    public ExtractStructFunction(Function expr) {
        List<Expression> expressions = expr.getParameters().getExpressions();
        this.pathParser = OperatorTools.buildParser(expressions.get(0));
        if (pathParser instanceof ColumnParser) {
            this.path = ((ColumnParser) pathParser).getFieldName();
        }
        this.fieldParsers = new ArrayList<>();
        for (int i = 1; i < expressions.size(); i++) {
            this.fieldParsers.add(OperatorTools.buildParser(expressions.get(i)));
        }
    }

    @Override
    public Object parse(SourceData sourceData, int rowIndex, Context context) {
        if (!(sourceData instanceof PbSourceData)) {
            return null;
        }
        if (path == null) {
            return null;
        }
        PbSourceData pbData = (PbSourceData) sourceData;
        // parse path
        List<PbNode> pathChildNodes = pbData.parseStructNodeList(path, pbData.getRootDesc());
        if (pathChildNodes == null || pathChildNodes.size() == 0) {
            return null;
        }
        // check message type
        PbNode lastNode = pathChildNodes.get(pathChildNodes.size() - 1);
        if (!lastNode.getFieldDesc().getJavaType().equals(JavaType.MESSAGE)) {
            return null;
        }
        // get data
        Object currentNode = pbData.findFieldNode(rowIndex, path);
        if (currentNode == null) {
            return null;
        }
        // array node
        if (lastNode.isArrayType() && !lastNode.isHasArrayIndex()) {
            if (!(currentNode instanceof List)) {
                return null;
            }
            List<?> currentNodeList = (List<?>) currentNode;
            List<GenericRowData> valueResult = new ArrayList<>(currentNodeList.size());
            for (Object nodeValue : currentNodeList) {
                if (!(nodeValue instanceof DynamicMessage)) {
                    continue;
                }
                DynamicMessage currentValue = (DynamicMessage) nodeValue;
                GenericRowData item = buildStruct(pbData, rowIndex, context, currentValue);
                valueResult.add(item);
            }
            GenericArrayData result = new GenericArrayData(valueResult.toArray());
            return result;
        } else {
            // struct node
            if (!(currentNode instanceof DynamicMessage)) {
                return null;
            }
            DynamicMessage currentValue = (DynamicMessage) currentNode;
            return buildStruct(pbData, rowIndex, context, currentValue);
        }
    }

    private GenericRowData buildStruct(PbSourceData pbData, int rowIndex, Context context,
            DynamicMessage currentValue) {
        Descriptor currentDesc = currentValue.getDescriptorForType();
        GenericRowData result = new GenericRowData(fieldParsers.size());
        int index = 0;
        for (ValueParser parser : fieldParsers) {
            if (parser instanceof ColumnParser) {
                ColumnParser columnParser = (ColumnParser) parser;
                String fieldName = columnParser.getFieldName();
                List<PbNode> childNodes = pbData.parseStructNodeList(fieldName, currentDesc);
                if (childNodes == null || childNodes.size() == 0) {
                    result.setField(index++, null);
                    continue;
                }
                Object fieldValue = pbData.findNodeValueByCache(childNodes, currentValue);
                PbNode lastNode = childNodes.get(childNodes.size() - 1);
                if (lastNode.isArrayType() && !lastNode.isHasArrayIndex()) {
                    if (!(fieldValue instanceof List)) {
                        result.setField(index++, null);
                        continue;
                    }
                    List<?> valueList = (List<?>) fieldValue;
                    List<Object> valueResult = new ArrayList<>(valueList.size());
                    for (Object value : valueList) {
                        Object transformedValue = pbData.buildFieldValue(lastNode.getFieldDesc(), value);
                        valueResult.add(transformedValue);
                    }
                    GenericArrayData arrayItem = new GenericArrayData(valueResult.toArray());
                    result.setField(index++, arrayItem);
                } else {
                    Object transformedValue = pbData.buildFieldValue(lastNode.getFieldDesc(), fieldValue);
                    result.setField(index++, transformedValue);
                }
            } else if (parser instanceof ExtractBinaryFunction) {
                ExtractBinaryFunction extractBinaryFunc = (ExtractBinaryFunction) parser;
                extractBinaryFunc.setParentRoot(currentValue);
                extractBinaryFunc.setParentDesc(currentDesc);
                Object fieldValue = extractBinaryFunc.parse(pbData, rowIndex, context);
                result.setField(index++, fieldValue);
            } else {
                result.setField(index++, null);
            }
        }
        return result;
    }
}
