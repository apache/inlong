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

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.MessageLite;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.table.data.GenericArrayData;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * ExtractBinaryFunction  ->  extract_binary(path)
 * description:
 * - Only works on protobuf source data; returns NULL if the source is not a PbSourceData.
 * - Returns NULL if 'path' is missing/invalid, or the path cannot be resolved to a value
 *   in the protobuf message.
 * - For primitive / struct / map nodes and array nodes with an explicit array index,
 *   returns the matched value serialized as a {@code byte[]}.
 * - For array nodes without an array index, returns a {@link GenericArrayData} whose
 *   elements are the {@code byte[]} representation of each list value.
 */
@TransformFunction(type = FunctionConstant.PB_TYPE, names = {
        "extract_binary"}, parameter = "(path)", descriptions = {
                "- Only works on protobuf source data; returns NULL if the source is not a PbSourceData;",
                "- Returns NULL if 'path' is missing/invalid, or the path cannot be resolved "
                        + "to a value in the protobuf message;",
                "- For primitive / struct / map nodes and array nodes with an explicit array index, "
                        + "returns the matched value serialized as a byte[];",
                "- For array nodes without an array index, returns a GenericArrayData whose elements "
                        + "are the byte[] representation of each list value."
        }, examples = {
                "extract_binary($root.feature) = [62,111]"
        })
public class ExtractBinaryFunction implements ValueParser {

    private final ValueParser pathParser;
    private Descriptor parentDesc;
    private DynamicMessage parentRoot;

    public ExtractBinaryFunction(Function expr) {
        List<Expression> expressions = expr.getParameters().getExpressions();
        this.pathParser = OperatorTools.buildParser(expressions.get(0));
    }

    @Override
    public Object parse(SourceData sourceData, int rowIndex, Context context) {
        // data
        if (!(sourceData instanceof PbSourceData)) {
            return null;
        }
        if (pathParser instanceof ColumnParser) {
            return this.parseByColumnParser(sourceData, rowIndex, context);
        }
        if (pathParser instanceof ExtractStructExcludingFunction) {
            ExtractStructExcludingFunction excluding = (ExtractStructExcludingFunction) pathParser;
            excluding.setKeepMessage(true);
            Object result = excluding.parse(sourceData, rowIndex, context);
            return toByteArray(result);
        }
        Object result = this.pathParser.parse(sourceData, rowIndex, context);
        return toByteArray(result);
    }

    public Object parseByColumnParser(SourceData sourceData, int rowIndex, Context context) {
        String path = ((ColumnParser) pathParser).getFieldName();
        if (path == null) {
            return null;
        }
        PbSourceData pbData = (PbSourceData) sourceData;
        if (StringUtils.equals(PbSourceData.ROOT, path)) {
            return pbData.getRoot().toByteArray();
        }
        // node list
        List<PbNode> childNodes = null;
        boolean isParentData = false;
        if (StringUtils.startsWith(path, PbSourceData.ROOT_KEY)) {
            childNodes = pbData.parseStructNodeList(path, pbData.getRootDesc());
        } else if (StringUtils.startsWith(path, PbSourceData.CHILD_KEY)) {
            if (pbData.getChildDesc() == null) {
                return null;
            }
            childNodes = pbData.parseStructNodeList(path, pbData.getChildDesc());
        } else if (parentDesc != null) {
            childNodes = pbData.parseStructNodeList(path, parentDesc);
            isParentData = true;
        }
        if (childNodes == null || childNodes.size() <= 0) {
            return null;
        }
        // value
        Object currentNode = null;
        if (isParentData) {
            currentNode = pbData.findNodeValueByCache(childNodes, parentRoot);
        } else {
            currentNode = pbData.findFieldNode(rowIndex, path);
        }
        if (currentNode == null) {
            return null;
        }
        // array
        PbNode lastNode = childNodes.get(childNodes.size() - 1);
        // primitive
        if (lastNode.isPrimitiveType()) {
            return toByteArray(currentNode);
        }
        // struct
        if (lastNode.isStructType()) {
            return toByteArray(currentNode);
        }
        // array
        if (lastNode.isArrayType()) {
            if (!lastNode.isHasArrayIndex()) {
                if (!(currentNode instanceof List)) {
                    return null;
                }
                List<?> valueList = (List<?>) currentNode;
                List<Object> fieldResult = new ArrayList<>(valueList.size());
                for (Object value : valueList) {
                    fieldResult.add(toByteArray(value));
                }
                return new GenericArrayData(fieldResult.toArray());
            }
            return toByteArray(currentNode);
        }
        // map
        if (lastNode.isMapType()) {
            return toByteArray(currentNode);
        }
        return null;
    }

    private Object toByteArray(Object currentNode) {
        if (currentNode == null) {
            return null;
        }
        if (currentNode instanceof MessageLite) {
            return ((MessageLite) currentNode).toByteArray();
        }
        if (currentNode instanceof ByteString) {
            return ((ByteString) currentNode).toByteArray();
        }
        if (currentNode instanceof List) {
            List<?> valueList = (List<?>) currentNode;
            List<Object> fieldResult = new ArrayList<>(valueList.size());
            for (Object value : valueList) {
                fieldResult.add(toByteArray(value));
            }
            return new GenericArrayData(fieldResult.toArray());
        }
        return String.valueOf(currentNode).getBytes(StandardCharsets.ISO_8859_1);
    }

    /**
     * get parentDesc
     * @return the parentDesc
     */
    public Descriptor getParentDesc() {
        return parentDesc;
    }

    /**
     * set parentDesc
     * @param parentDesc the parentDesc to set
     */
    public void setParentDesc(Descriptor parentDesc) {
        this.parentDesc = parentDesc;
    }

    /**
     * get parentRoot
     * @return the parentRoot
     */
    public DynamicMessage getParentRoot() {
        return parentRoot;
    }

    /**
     * set parentRoot
     * @param parentRoot the parentRoot to set
     */
    public void setParentRoot(DynamicMessage parentRoot) {
        this.parentRoot = parentRoot;
    }

}
