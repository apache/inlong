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

import java.util.ArrayList;
import java.util.List;

/**
 * ExtractStructExcludingFunction  ->  extract_struct_excluding(path, excludeField1, excludeField2, ...)
 * description:
 * - Only works on protobuf source data; returns NULL if the source is not a PbSourceData.
 * - Returns NULL if 'path' is missing/invalid, or the path cannot be resolved to a
 *   message-typed node in the protobuf source data.
 * - Each {@code excludeFieldN} is the name (relative to 'path') of a sub-field that
 *   should be REMOVED from a copy of the located message before returning. Parameters
 *   that cannot be resolved on the located message, or are not plain column references,
 *   are silently ignored. The original record is never mutated.
 * - When 'path' resolves to:
 *     - a single message: returns a GenericRowData built from a trimmed copy of that
 *       message (the excluded fields are cleared);
 *     - a repeated (array) field of messages: returns a GenericArrayData whose elements
 *       are the trimmed GenericRowData for every array element.
 */
@TransformFunction(type = FunctionConstant.PB_TYPE, names = {
        "extract_struct_excluding"}, parameter = "(path, excludeField1, excludeField2, ...)", descriptions = {
                "- Only works on protobuf source data; returns NULL if the source is not a PbSourceData;",
                "- Returns NULL if 'path' is missing/invalid, or the path cannot be resolved "
                        + "to a message-typed node;",
                "- Each excludeFieldN is the name (relative to 'path') of a sub-field to REMOVE "
                        + "from a copy of the located message; unknown / non-column-reference "
                        + "parameters are silently ignored;",
                "- When 'path' resolves to a single message, returns a trimmed GenericRowData. "
                        + "When 'path' resolves to a repeated message field, returns a "
                        + "GenericArrayData whose elements are the trimmed GenericRowData for "
                        + "every array element. The original record is never mutated."
        }, examples = {
                "extract_struct_excluding($root.person,address,phone) "
                        + "= <GenericRowData of person without address and phone>"
        })
public class ExtractStructExcludingFunction implements ValueParser {

    private final ValueParser pathParser;
    private final List<ValueParser> fieldParsers;
    private String path;
    private boolean isKeepMessage = false;

    public ExtractStructExcludingFunction(Function expr) {
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
        if (PbSourceData.ROOT.equals(path)) {
            return buildStruct(pbData, rowIndex, context, pbData.getRoot());
        }
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
            List<Object> valueResult = new ArrayList<>(currentNodeList.size());
            for (Object nodeValue : currentNodeList) {
                if (!(nodeValue instanceof DynamicMessage)) {
                    continue;
                }
                DynamicMessage currentValue = (DynamicMessage) nodeValue;
                Object item = buildStruct(pbData, rowIndex, context, currentValue);
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

    private Object buildStruct(PbSourceData pbData, int rowIndex, Context context,
            DynamicMessage rawValue) {
        DynamicMessage.Builder currentValue = rawValue.toBuilder();
        Descriptor currentDesc = currentValue.getDescriptorForType();
        for (ValueParser parser : fieldParsers) {
            if (parser instanceof ColumnParser) {
                ColumnParser columnParser = (ColumnParser) parser;
                String fieldName = columnParser.getFieldName();
                List<PbNode> childNodes = pbData.parseStructNodeList(fieldName, currentDesc);
                if (childNodes == null || childNodes.size() == 0) {
                    continue;
                }
                pbData.clearNodeValue(childNodes, currentValue);
            }
        }
        if (isKeepMessage()) {
            return currentValue.build();
        }
        Object result = pbData.buildStructData(currentDesc, currentValue.build());
        return result;
    }

    /**
     * get isKeepMessage
     * @return the isKeepMessage
     */
    public boolean isKeepMessage() {
        return isKeepMessage;
    }

    /**
     * set isKeepMessage
     * @param isKeepMessage the isKeepMessage to set
     */
    public void setKeepMessage(boolean isKeepMessage) {
        this.isKeepMessage = isKeepMessage;
    }
}
