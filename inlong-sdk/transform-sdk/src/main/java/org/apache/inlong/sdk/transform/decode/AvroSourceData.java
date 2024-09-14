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

package org.apache.inlong.sdk.transform.decode;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.StringUtils;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

public class AvroSourceData implements SourceData {

    public static final String ROOT_KEY = "$root";

    public static final String CHILD_KEY = "$child";

    private GenericRecord root;

    private List<GenericRecord> childRoot;

    private Charset srcCharset;

    public AvroSourceData(GenericRecord root, List<GenericRecord> childRoot, Charset srcCharset) {
        this.root = root;
        this.childRoot = childRoot;
        this.srcCharset = srcCharset;
    }

    @Override
    public int getRowCount() {
        if (this.childRoot == null) {
            return 1;
        } else {
            return this.childRoot.size();
        }
    }

    @Override
    public String getField(int rowNum, String fieldName) {
        try {
            List<AvroNode> childNodes = new ArrayList<>();
            String[] nodeStrings = fieldName.split("\\.");
            for (String nodeString : nodeStrings) {
                childNodes.add(new AvroNode(nodeString));
            }
            // parse
            if (childNodes.size() == 0) {
                return "";
            }
            // first node
            AvroNode firstNode = childNodes.get(0);
            Object current = root;
            Schema curSchema = root.getSchema();
            if (StringUtils.equals(ROOT_KEY, firstNode.getName())) {
                current = root;
                curSchema = root.getSchema();
            } else if (StringUtils.equals(CHILD_KEY, firstNode.getName())) {
                if (rowNum < childRoot.size()) {
                    current = childRoot.get(rowNum);
                    curSchema = childRoot.get(rowNum).getSchema();
                } else {
                    return "";
                }
            } else {
                // error data
                return "";
            }
            if (current == null) {
                // error data
                return "";
            }
            // parse other node
            for (int i = 1; i < childNodes.size(); i++) {
                AvroNode node = childNodes.get(i);
                if (curSchema.getType() != Type.RECORD) {
                    // error data
                    return "";
                }
                Object newElement = ((GenericRecord) current).get(node.getName());
                if (newElement == null) {
                    // error data
                    return "";
                }
                // node is not array
                if (!node.isArray()) {
                    curSchema = curSchema.getField(node.getName()).schema();
                    current = newElement;
                    continue;
                }
                // node is an array
                current = getElementFromArray(node, newElement, curSchema);
                if (current == null) {
                    // error data
                    return "";
                }
            }
            return getNodeAsString(current, curSchema);
        } catch (Exception e) {
            return "";
        }
    }

    private Object getElementFromArray(AvroNode node, Object curElement, Schema curSchema) {
        if (node.getArrayIndices().isEmpty()) {
            // error data
            return null;
        }
        for (int index : node.getArrayIndices()) {
            if (curSchema.getType() != Type.ARRAY) {
                // error data
                return null;
            }
            List<?> newArray = (List<?>) curElement;
            if (index >= newArray.size()) {
                // error data
                return null;
            }
            curSchema = curSchema.getElementType();
            curElement = newArray.get(index);
        }
        return curElement;
    }

    private String getNodeAsString(Object node, Schema schema) {
        String fieldValue = "";
        Type fieldType = schema.getType();
        switch (fieldType) {
            case INT:
            case LONG:
            case FLOAT:
            case DOUBLE:
            case STRING:
            case BOOLEAN:
            case ENUM:
                fieldValue = String.valueOf(node);
                break;
            case BYTES:
                ByteBuffer byteBuffer = (ByteBuffer) node;
                byte[] bytes = new byte[byteBuffer.remaining()];
                byteBuffer.get(bytes);
                fieldValue = new String(bytes, srcCharset);
                break;
            case FIXED:
                byteBuffer = (ByteBuffer) node;
                bytes = new byte[schema.getFixedSize()];
                fieldValue = new String(bytes, srcCharset);
        }
        return fieldValue;
    }

}
