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

import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType;
import lombok.Data;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * PbNode
 * 
 */
@Data
public class PbNode {

    public static final Logger LOG = LoggerFactory.getLogger(PbNode.class);

    private String name;
    private FieldDescriptor fieldDesc;
    private boolean isLastNode = false;
    // primitive
    private boolean isPrimitiveType = false;
    // array
    private boolean isArrayType = false;
    private boolean hasArrayIndex = false;
    private Integer arrayIndex;
    // struct
    private boolean isStructType = false;
    // map
    private boolean isMapType = false;
    private boolean hasMapKey = false;
    private Object mapKey;
    private FieldDescriptor mapKeyDesc;
    private FieldDescriptor mapValueDesc;
    // parent path
    private String parentPath;
    private String currentPath;
    private String currentIndexPath;

    public PbNode(Descriptors.Descriptor parentDesc, String parentPath, String nodeString, boolean isLastNode) {
        try {
            if (parentDesc == null) {
                return;
            }
            this.isLastNode = isLastNode;
            // parse name & index
            int beginIndex = nodeString.indexOf('(');
            String indexString = null;
            if (beginIndex < 0) {
                this.name = nodeString;
            } else {
                this.name = StringUtils.trim(nodeString.substring(0, beginIndex));
                int endIndex = nodeString.lastIndexOf(')');
                if (endIndex >= 0) {
                    indexString = nodeString.substring(beginIndex + 1, endIndex);
                }
            }
            // cache path key
            this.parentPath = parentPath;
            if (this.parentPath == null) {
                this.currentPath = this.name;
                this.currentIndexPath = nodeString;
            } else {
                this.currentPath = this.parentPath + "." + this.name;
                this.currentIndexPath = this.parentPath + "." + nodeString;
            }
            // field desc
            this.fieldDesc = parentDesc.findFieldByName(name);
            if (this.fieldDesc == null) {
                return;
            }
            // map
            if (this.fieldDesc.getJavaType() == JavaType.MESSAGE
                    && isMapDescriptor(this.fieldDesc.getMessageType())) {
                this.isMapType = true;
                this.mapKeyDesc = this.fieldDesc.getMessageType().getFields().get(0);
                this.mapValueDesc = this.fieldDesc.getMessageType().getFields().get(1);
                if (indexString != null) {
                    this.hasMapKey = true;
                    this.mapKey = parseMapKey(indexString, mapKeyDesc);
                }
                return;
            }
            // array
            if (this.fieldDesc.isRepeated()) {
                this.isArrayType = true;
                this.arrayIndex = NumberUtils.toInt(indexString, -1);
                if (arrayIndex >= 0) {
                    this.hasArrayIndex = true;
                }
                return;
            }
            // struct
            if (this.fieldDesc.getJavaType() == JavaType.MESSAGE) {
                this.isStructType = true;
                return;
            }
            // primitive
            this.isPrimitiveType = true;
        } catch (RuntimeException t) {
            LOG.error("Fail to PbNode,error:{},fullName:{},nodePath:{},isLastNode:{}",
                    t.getMessage(), parentDesc.getName(), nodeString, isLastNode, t);
            throw t;
        }
    }

    private static Object parseMapKey(String indexString, FieldDescriptor mapKeyDesc) {
        switch (mapKeyDesc.getJavaType()) {
            case STRING:
                return indexString;
            case INT:
                return NumberUtils.toInt(indexString, 0);
            case LONG:
                return NumberUtils.toLong(indexString, 0);
            case FLOAT:
                return NumberUtils.toFloat(indexString, 0);
            case DOUBLE:
                return NumberUtils.toDouble(indexString, 0);
            case BOOLEAN:
                return Boolean.TRUE.toString().equals(indexString);
            case ENUM:
                return mapKeyDesc.getEnumType().findValueByName(indexString);
            case BYTE_STRING:
            case MESSAGE:
            default:
                return indexString;
        }
    }

    /**
     * parseNodePath
     * @param rootDesc
     * @param nodePath
     * @return
     */
    public static List<PbNode> parseNodePath(Descriptors.Descriptor rootDesc, String nodePath) {
        if (StringUtils.isBlank(nodePath)) {
            return null;
        }
        List<PbNode> nodes = new ArrayList<>();
        String[] nodeStrings = nodePath.split("\\.");
        final int lastIndex = nodeStrings.length - 1;
        String parentPath = null;
        StringBuilder currentPathBuilder = new StringBuilder();
        Descriptors.Descriptor current = rootDesc;
        for (int i = 0; i <= lastIndex; i++) {
            if (current == null) {
                return null;
            }
            // pbNode
            String nodeString = nodeStrings[i];
            PbNode pbNode = new PbNode(current, parentPath, nodeString, (i == lastIndex));
            if (parentPath == null) {
                currentPathBuilder.append(nodeString);
            } else {
                currentPathBuilder.append(".").append(nodeString);
            }
            parentPath = currentPathBuilder.toString();
            if (pbNode.getFieldDesc() == null) {
                return null;
            }
            // primitive
            if (pbNode.isPrimitiveType()) {
                current = null;
                nodes.add(pbNode);
                continue;
            } else if (pbNode.isArrayType()) {
                // array
                if (pbNode.getFieldDesc().getJavaType() == JavaType.MESSAGE) {
                    current = pbNode.getFieldDesc().getMessageType();
                } else {
                    current = null;
                }
                nodes.add(pbNode);
                continue;
            } else if (pbNode.isMapType()) {
                // map
                if (pbNode.isHasMapKey()) {
                    if (pbNode.getMapValueDesc().getJavaType() == JavaType.MESSAGE) {
                        current = pbNode.getMapValueDesc().getMessageType();
                    } else {
                        current = null;
                    }
                } else {
                    current = null;
                }
                nodes.add(pbNode);
                continue;
            } else if (pbNode.isStructType()) {
                // struct
                current = pbNode.getFieldDesc().getMessageType();
                nodes.add(pbNode);
                continue;
            } else {
                return null;
            }
        }
        return nodes;
    }

    public static boolean isMapDescriptor(Descriptors.Descriptor descriptor) {
        if (descriptor.getOptions().getMapEntry()) {
            return true;
        }

        if (descriptor.getFields().size() == 2) {
            Descriptors.FieldDescriptor keyField = descriptor.findFieldByNumber(1);
            Descriptors.FieldDescriptor valueField = descriptor.findFieldByNumber(2);

            if (keyField != null && valueField != null &&
                    "key".equals(keyField.getName()) &&
                    "value".equals(valueField.getName())) {
                return true;
            }
        }
        return false;
    }
}
