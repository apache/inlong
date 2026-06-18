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

import org.apache.inlong.sdk.transform.process.Context;

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType;
import com.google.protobuf.DynamicMessage;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * JsonSourceData
 * 
 */
public class PbSourceData extends AbstractSourceData {

    private static final Logger LOG = LoggerFactory.getLogger(PbSourceData.class);

    public static final String ROOT = "$root";

    public static final String ROOT_KEY = "$root.";

    public static final String CHILD_KEY = "$child.";

    private Descriptors.Descriptor rootDesc;

    private Descriptors.Descriptor childDesc;

    private Map<String, List<PbNode>> columnNodeMap = new ConcurrentHashMap<>();

    private DynamicMessage root;

    private List<DynamicMessage> childRoot;

    protected Charset srcCharset;

    private Map<DynamicMessage, Map<String, Object>> nodeValueCache = new IdentityHashMap<>();
    private Map<DynamicMessage, Map<String, Map<Object, Object>>> mapNodeCache = new IdentityHashMap<>();

    /**
     * Constructor
     */
    public PbSourceData(DynamicMessage root, List<DynamicMessage> childRoot,
            Descriptors.Descriptor rootDesc, Descriptors.Descriptor childDesc,
            Map<String, List<PbNode>> columnNodeMap,
            Charset srcCharset, Context context) {
        this.root = root;
        this.childRoot = childRoot;
        this.rootDesc = rootDesc;
        this.childDesc = childDesc;
        this.columnNodeMap = columnNodeMap;
        this.srcCharset = srcCharset;
        this.context = context;
    }

    /**
     * Constructor
     */
    public PbSourceData(DynamicMessage root,
            Descriptors.Descriptor rootDesc,
            Map<String, List<PbNode>> columnNodeMap,
            Charset srcCharset) {
        this.root = root;
        this.rootDesc = rootDesc;
        this.columnNodeMap = columnNodeMap;
        this.srcCharset = srcCharset;
    }

    /**
     * getRowCount
     * @return
     */
    @Override
    public int getRowCount() {
        if (this.childRoot == null) {
            return 1;
        } else {
            return this.childRoot.size();
        }
    }

    /**
     * getField
     * @param rowNum
     * @param fieldName
     * @return
     */
    @Override
    @SuppressWarnings({"rawtypes", "unchecked"})
    public Object getField(int rowNum, String fieldName) {
        try {
            // check(root);
            if (isContextField(fieldName)) {
                return getContextField(fieldName);
            }
            Object fieldValue = findFieldNode(rowNum, fieldName);
            List<PbNode> childNodes = this.columnNodeMap.get(fieldName);
            if (childNodes == null || childNodes.size() == 0) {
                return null;
            }
            PbNode lastNode = childNodes.get(childNodes.size() - 1);
            // primitive
            if (lastNode.isPrimitiveType()) {
                if (fieldValue instanceof ByteString) {
                    ByteString byteString = (ByteString) fieldValue;
                    return byteString.toByteArray();
                } else {
                    return fieldValue;
                }
            }
            // struct
            if (lastNode.isStructType()) {
                if (!(fieldValue instanceof DynamicMessage)) {
                    return null;
                }
                return buildStructData(lastNode.getFieldDesc().getMessageType(), (DynamicMessage) fieldValue);
            }
            // array
            if (lastNode.isArrayType()) {
                if (!lastNode.isHasArrayIndex()) {
                    if (!(fieldValue instanceof List)) {
                        return null;
                    }
                    List<Object> valueList = (List) fieldValue;
                    List<Object> result = new ArrayList<>(valueList.size());
                    for (Object value : valueList) {
                        Object itemValue = this.buildFieldValue(lastNode.getFieldDesc(), value);
                        if (itemValue != null) {
                            result.add(itemValue);
                        }
                    }
                    return new GenericArrayData(result.toArray());
                }
                return this.buildFieldValue(lastNode.getFieldDesc(), fieldValue);
            }
            // map
            if (lastNode.isMapType()) {
                if (!lastNode.isHasMapKey()) {
                    return buildMapData(lastNode.getFieldDesc().getMessageType(), fieldValue);
                }
                return this.buildFieldValue(lastNode.getMapValueDesc(), fieldValue);
            }
            return null;
        } catch (Exception e) {
            LOG.error("fail to getField,error:{},rowNum:{},fieldName:{}", e.getMessage(), rowNum, fieldName, e);
            return null;
        }
    }

    public Object buildFieldValue(FieldDescriptor fieldDesc, Object nodeValue) {
        if (fieldDesc == null || nodeValue == null) {
            return null;
        }
        switch (fieldDesc.getJavaType()) {
            case STRING:
                if (nodeValue instanceof BinaryStringData) {
                    return nodeValue;
                }
                if (nodeValue instanceof String) {
                    return new BinaryStringData((String) nodeValue);
                }
                return new BinaryStringData(String.valueOf(nodeValue));
            case INT:
                if (nodeValue instanceof Integer) {
                    return nodeValue;
                }
                return NumberUtils.toInt(String.valueOf(nodeValue), 0);
            case LONG:
                if (nodeValue instanceof Long) {
                    return nodeValue;
                }
                return NumberUtils.toLong(String.valueOf(nodeValue), 0);
            case FLOAT:
                if (nodeValue instanceof Float) {
                    return nodeValue;
                }
                return NumberUtils.toFloat(String.valueOf(nodeValue), 0);
            case DOUBLE:
                if (nodeValue instanceof Double) {
                    return nodeValue;
                }
                return NumberUtils.toDouble(String.valueOf(nodeValue), 0);
            case BOOLEAN:
                if (nodeValue instanceof Boolean) {
                    return nodeValue;
                }
                return Boolean.TRUE.toString().equals(String.valueOf(nodeValue));
            case ENUM:
                if (nodeValue instanceof EnumValueDescriptor) {
                    EnumValueDescriptor enumDesc = (EnumValueDescriptor) nodeValue;
                    return enumDesc.getIndex();
                }
                return null;
            case BYTE_STRING:
                if (nodeValue instanceof byte[]) {
                    return nodeValue;
                } else if (nodeValue instanceof ByteString) {
                    return ((ByteString) nodeValue).toByteArray();
                } else {
                    return String.valueOf(nodeValue).getBytes(StandardCharsets.ISO_8859_1);
                }
            case MESSAGE:
                return this.buildStructData(fieldDesc.getMessageType(), nodeValue);
            default:
                return nodeValue;
        }
    }

    public Object buildStructData(Descriptors.Descriptor messageType, Object nodeValue) {
        // map
        if (PbNode.isMapDescriptor(messageType)) {
            return this.buildMapData(messageType, nodeValue);
        }
        // struct
        if (!(nodeValue instanceof DynamicMessage)) {
            return null;
        }
        DynamicMessage msgObj = (DynamicMessage) nodeValue;
        GenericRowData result = new GenericRowData(messageType.getFields().size());
        int index = 0;
        for (FieldDescriptor fieldDesc : messageType.getFields()) {
            if (!fieldDesc.isRepeated() && !msgObj.hasField(fieldDesc)) {
                result.setField(index++, null);
                continue;
            }
            Object fieldValue = msgObj.getField(fieldDesc);
            if (fieldValue == null) {
                result.setField(index++, null);
                continue;
            }
            // field
            if (!fieldDesc.isRepeated()) {
                Object fieldResult = this.buildFieldValue(fieldDesc, fieldValue);
                result.setField(index++, fieldResult);
                continue;
            }
            // array
            if (!(fieldValue instanceof List)) {
                result.setField(index++, null);
                continue;
            }
            // map
            if (fieldDesc.getJavaType().equals(JavaType.MESSAGE)
                    && PbNode.isMapDescriptor(fieldDesc.getMessageType())) {
                result.setField(index++, buildMapData(fieldDesc.getMessageType(), fieldValue));
            } else {
                List<?> valueList = (List<?>) fieldValue;
                List<Object> fieldResult = new ArrayList<>(valueList.size());
                for (Object value : valueList) {
                    fieldResult.add(this.buildFieldValue(fieldDesc, value));
                }
                result.setField(index++, new GenericArrayData(fieldResult.toArray()));
            }
        }
        return result;
    }

    protected Object buildMapData(Descriptors.Descriptor messageType, Object nodeValue) {
        if (!(nodeValue instanceof List)) {
            return null;
        }
        Descriptors.FieldDescriptor keyField = messageType.findFieldByNumber(1);
        Descriptors.FieldDescriptor valueField = messageType.findFieldByNumber(2);
        List<?> subNodeValueList = (List<?>) nodeValue;
        Map<Object, Object> result = new HashMap<>();
        for (Object value : subNodeValueList) {
            if (!(value instanceof DynamicMessage)) {
                continue;
            }
            DynamicMessage subnodeValue = (DynamicMessage) value;
            if (subnodeValue.hasField(keyField) && subnodeValue.hasField(valueField)) {
                Object keyValue = buildFieldValue(keyField, subnodeValue.getField(keyField));
                Object valueValue = buildFieldValue(valueField, subnodeValue.getField(valueField));
                if (keyValue != null && valueValue != null) {
                    result.put(keyValue, valueValue);
                }
            }
        }
        return new GenericMapData(result);
    }

    /**
     * get rootDesc
     * @return the rootDesc
     */
    public Descriptors.Descriptor getRootDesc() {
        return rootDesc;
    }

    /**
     * get childDesc
     * @return the childDesc
     */
    public Descriptors.Descriptor getChildDesc() {
        return childDesc;
    }

    /**
     * get root
     * @return the root
     */
    public DynamicMessage getRoot() {
        return root;
    }

    /**
     * get childRoot
     * @return the childRoot
     */
    public List<DynamicMessage> getChildRoot() {
        return childRoot;
    }

    public Object findFieldNode(int rowNum, String fieldName) {
        Object fieldValue = null;
        try {
            if (StringUtils.startsWith(fieldName, ROOT_KEY)) {
                fieldValue = this.findRootField(fieldName);
            } else if (StringUtils.startsWith(fieldName, CHILD_KEY)) {
                if (childRoot != null && rowNum < childRoot.size()) {
                    fieldValue = this.findChildField(rowNum, fieldName);
                }
            } else {
                List<PbNode> childNodes = this.columnNodeMap.get(fieldName);
                if (childNodes == null) {
                    childNodes = PbNode.parseNodePath(rootDesc, fieldName);
                    if (childNodes == null) {
                        childNodes = new ArrayList<>();
                    }
                    this.columnNodeMap.put(fieldName, childNodes);
                }
                // error config
                if (childNodes.size() == 0) {
                    return null;
                }
                // parse other node
                fieldValue = this.findNodeValueByCache(childNodes, root);
            }
            return fieldValue;
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }
        return fieldValue;
    }

    public List<PbNode> parseStructNodeList(String srcFieldName, Descriptor currentDesc) {
        List<PbNode> childNodes = this.columnNodeMap.get(srcFieldName);
        if (childNodes == null) {
            String fieldName = srcFieldName;
            if (StringUtils.startsWith(fieldName, ROOT_KEY)) {
                fieldName = srcFieldName.substring(ROOT_KEY.length());
            } else if (StringUtils.startsWith(fieldName, CHILD_KEY)) {
                fieldName = srcFieldName.substring(CHILD_KEY.length());
            }
            childNodes = PbNode.parseNodePath(currentDesc, fieldName);
            if (childNodes == null) {
                childNodes = new ArrayList<>();
            }
            this.columnNodeMap.put(srcFieldName, childNodes);
        }
        return childNodes;
    }

    private Object findRootField(String srcFieldName) {
        List<PbNode> childNodes = this.columnNodeMap.get(srcFieldName);
        if (childNodes == null) {
            String fieldName = srcFieldName.substring(ROOT_KEY.length());
            childNodes = PbNode.parseNodePath(rootDesc, fieldName);
            if (childNodes == null) {
                childNodes = new ArrayList<>();
            }
            this.columnNodeMap.put(srcFieldName, childNodes);
        }
        // error config
        if (childNodes.size() == 0) {
            return null;
        }
        // parse other node
        Object fieldValue = this.findNodeValueByCache(childNodes, root);
        return fieldValue;
    }

    private Object findChildField(int rowNum, String srcFieldName) {
        if (this.childRoot == null || this.childDesc == null) {
            return null;
        }
        List<PbNode> childNodes = this.columnNodeMap.get(srcFieldName);
        if (childNodes == null) {
            String fieldName = srcFieldName.substring(CHILD_KEY.length());
            childNodes = PbNode.parseNodePath(childDesc, fieldName);
            if (childNodes == null) {
                childNodes = new ArrayList<>();
            }
            this.columnNodeMap.put(srcFieldName, childNodes);
        }
        // error config
        if (childNodes.size() == 0) {
            return null;
        }
        // parse other node
        DynamicMessage child = childRoot.get(rowNum);
        Object fieldValue = this.findNodeValueByCache(childNodes, child);
        return fieldValue;
    }

    public Object findNodeValueByCache(List<PbNode> childNodes, DynamicMessage root) {
        Map<String, Object> subNodeValueCache = this.nodeValueCache.computeIfAbsent(root,
                k -> new HashMap<>());
        Map<String, Map<Object, Object>> subMapNodeCache = this.mapNodeCache.computeIfAbsent(root,
                k -> new HashMap<>());
        for (int i = childNodes.size() - 1; i >= 0; i--) {
            PbNode node = childNodes.get(i);
            // index path
            Object subNodeValue = subNodeValueCache.get(node.getCurrentIndexPath());
            if (subNodeValue != null) {
                if (i == childNodes.size() - 1) {
                    return subNodeValue;
                } else {
                    if (subNodeValue instanceof DynamicMessage) {
                        List<PbNode> subChildNodes = childNodes.subList(i + 1, childNodes.size());
                        return this.findNodeValue(subChildNodes, (DynamicMessage) subNodeValue);
                    } else {
                        return null;
                    }
                }
            }
            // path
            subNodeValue = subNodeValueCache.get(node.getCurrentPath());
            if (subNodeValue != null) {
                if (i == childNodes.size() - 1) {
                    // primitive
                    if (node.isPrimitiveType()) {
                        return subNodeValue;
                    }
                    // struct
                    if (node.isStructType()) {
                        return subNodeValue;
                    }
                    // array
                    if (node.isArrayType()) {
                        if (!node.isHasArrayIndex()) {
                            return subNodeValue;
                        }
                        if (!(subNodeValue instanceof List)) {
                            return null;
                        }
                        List<?> nodeValueList = (List<?>) subNodeValue;
                        if (node.getArrayIndex() >= nodeValueList.size()) {
                            return null;
                        }
                        Object newNode = nodeValueList.get(node.getArrayIndex());
                        if (!(newNode instanceof DynamicMessage)) {
                            return null;
                        }
                        return newNode;
                    }
                    // map
                    if (node.isMapType()) {
                        if (!node.isHasMapKey()) {
                            return subNodeValue;
                        }
                        final Object mapNodeValue = subNodeValue;
                        Map<Object, Object> mapCache = subMapNodeCache.computeIfAbsent(node.getCurrentPath(),
                                k -> parseMapNode(mapNodeValue, node));
                        Object fieldValue = mapCache.get(node.getMapKey());
                        if (fieldValue == null || !(fieldValue instanceof DynamicMessage)) {
                            return null;
                        }
                        return fieldValue;
                    }
                    return null;
                } else {
                    // primitive
                    if (node.isPrimitiveType()) {
                        return null;
                    }
                    // struct
                    if (node.isStructType()) {
                        List<PbNode> subChildNodes = childNodes.subList(i + 1, childNodes.size());
                        return this.findNodeValue(subChildNodes, (DynamicMessage) subNodeValue);
                    }
                    // array
                    if (node.isArrayType()) {
                        if (!node.isHasArrayIndex()) {
                            return null;
                        }
                        if (!(subNodeValue instanceof List)) {
                            return null;
                        }
                        List<?> nodeValueList = (List<?>) subNodeValue;
                        if (node.getArrayIndex() >= nodeValueList.size()) {
                            return null;
                        }
                        Object newNode = nodeValueList.get(node.getArrayIndex());
                        if (!(newNode instanceof DynamicMessage)) {
                            return null;
                        }
                        List<PbNode> subChildNodes = childNodes.subList(i + 1, childNodes.size());
                        return this.findNodeValue(subChildNodes, (DynamicMessage) newNode);
                    }
                    // map
                    if (node.isMapType()) {
                        if (!node.isHasMapKey()) {
                            return null;
                        }
                        final Object mapNodeValue = subNodeValue;
                        Map<Object, Object> mapCache = subMapNodeCache.computeIfAbsent(node.getCurrentPath(),
                                k -> parseMapNode(mapNodeValue, node));
                        Object fieldValue = mapCache.get(node.getMapKey());
                        if (fieldValue == null || !(fieldValue instanceof DynamicMessage)) {
                            return null;
                        }
                        List<PbNode> subChildNodes = childNodes.subList(i + 1, childNodes.size());
                        return this.findNodeValue(subChildNodes, (DynamicMessage) fieldValue);
                    }
                    return null;
                }
            }
        }
        return this.findNodeValue(childNodes, root);
    }

    private static Map<Object, Object> parseMapNode(Object nodeValue, PbNode node) {
        if (!(nodeValue instanceof List)) {
            return new HashMap<>();
        }
        List<?> nodeValueList = (List<?>) nodeValue;
        Map<Object, Object> mapCache = new HashMap<>();
        for (Object value : nodeValueList) {
            if (!(value instanceof DynamicMessage)) {
                continue;
            }
            DynamicMessage msg = (DynamicMessage) value;
            if (msg.hasField(node.getMapKeyDesc()) && msg.hasField(node.getMapValueDesc())) {
                Object keyValue = msg.getField(node.getMapKeyDesc());
                Object valueValue = msg.getField(node.getMapValueDesc());
                mapCache.put(keyValue, valueValue);
            }
        }
        return mapCache;
    }

    // @SuppressWarnings({"rawtypes", "unchecked"})
    public Object findNodeValue(List<PbNode> childNodes, DynamicMessage root) {
        Map<String, Object> subNodeValueCache = this.nodeValueCache.computeIfAbsent(root,
                k -> new HashMap<>());
        Map<String, Map<Object, Object>> subMapNodeCache = this.mapNodeCache.computeIfAbsent(root,
                k -> new HashMap<>());
        DynamicMessage current = root;
        for (int i = 0; i < childNodes.size(); i++) {
            PbNode node = childNodes.get(i);
            if (!node.getFieldDesc().isRepeated() && !current.hasField(node.getFieldDesc())) {
                break;
            }
            Object nodeValue = current.getField(node.getFieldDesc());
            if (nodeValue == null) {
                // error data
                break;
            }
            if (node.isLastNode()) {
                // primitive
                if (node.isPrimitiveType()) {
                    if (nodeValue instanceof ByteString) {
                        ByteString byteString = (ByteString) nodeValue;
                        return byteString.toByteArray();
                    } else if (node.getFieldDesc().getJavaType().equals(JavaType.STRING)) {
                        return new BinaryStringData(String.valueOf(nodeValue));
                    } else {
                        return nodeValue;
                    }
                }
                // struct
                if (node.isStructType()) {
                    subNodeValueCache.put(node.getCurrentPath(), nodeValue);
                    return nodeValue;
                }
                // array
                if (node.isArrayType()) {
                    subNodeValueCache.put(node.getCurrentPath(), nodeValue);
                    if (!node.isHasArrayIndex()) {
                        return nodeValue;
                    }
                    if (!(nodeValue instanceof List)) {
                        return null;
                    }
                    List<?> nodeValueList = (List<?>) nodeValue;
                    if (node.getArrayIndex() >= nodeValueList.size()) {
                        return null;
                    }
                    Object arrayIndexNodeValue = nodeValueList.get(node.getArrayIndex());
                    subNodeValueCache.put(node.getCurrentIndexPath(), arrayIndexNodeValue);
                    return arrayIndexNodeValue;
                }
                // map
                if (node.isMapType()) {
                    subNodeValueCache.put(node.getCurrentPath(), nodeValue);
                    if (!node.isHasMapKey()) {
                        return nodeValue;
                    }
                    final Object mapNodeValue = nodeValue;
                    Map<Object, Object> mapCache = subMapNodeCache.computeIfAbsent(node.getCurrentPath(),
                            k -> parseMapNode(mapNodeValue, node));
                    Object fieldValue = mapCache.get(node.getMapKey());
                    subNodeValueCache.put(node.getCurrentIndexPath(), fieldValue);
                    return fieldValue;
                }
                return null;
            } else {
                // primitive
                if (node.isPrimitiveType()) {
                    return null;
                }
                // struct
                if (node.isStructType()) {
                    subNodeValueCache.put(node.getCurrentPath(), nodeValue);
                    if (!(nodeValue instanceof DynamicMessage)) {
                        return null;
                    }
                    current = (DynamicMessage) nodeValue;
                    continue;
                }
                // array
                if (node.isArrayType()) {
                    subNodeValueCache.put(node.getCurrentPath(), nodeValue);
                    if (!node.isHasArrayIndex()) {
                        return null;
                    }
                    if (!(nodeValue instanceof List)) {
                        return null;
                    }
                    List<?> nodeValueList = (List<?>) nodeValue;
                    if (node.getArrayIndex() >= nodeValueList.size()) {
                        return null;
                    }
                    Object newNode = nodeValueList.get(node.getArrayIndex());
                    subNodeValueCache.put(node.getCurrentIndexPath(), newNode);
                    if (!(newNode instanceof DynamicMessage)) {
                        return null;
                    }
                    current = (DynamicMessage) newNode;
                    continue;
                }
                // map
                if (node.isMapType()) {
                    subNodeValueCache.put(node.getCurrentPath(), nodeValue);
                    if (!node.isHasMapKey()) {
                        return null;
                    }
                    final Object mapNodeValue = nodeValue;
                    Map<Object, Object> mapCache = subMapNodeCache.computeIfAbsent(node.getCurrentPath(),
                            k -> parseMapNode(mapNodeValue, node));
                    Object fieldValue = mapCache.get(node.getMapKey());
                    subNodeValueCache.put(node.getCurrentIndexPath(), fieldValue);
                    if (fieldValue == null || !(fieldValue instanceof DynamicMessage)) {
                        return null;
                    }
                    current = (DynamicMessage) fieldValue;
                    continue;
                }
                return null;
            }
        }
        return null;
    }

    /**
     * Clear the leaf field referenced by {@code childNodes} on a copy of {@code root}.
     * <p>
     * Implementation notes (important):
     * <ul>
     *   <li>Intermediate nodes are descended by reading the value out of the parent
     *       builder, creating a sub-builder via {@link DynamicMessage#toBuilder()},
     *       recursing into it, and then writing the rebuilt sub-message back via
     *       {@code setField} / {@code setRepeatedField}. We never rely on automatic
     *       reverse propagation from {@code getFieldBuilder}, which is not consistent
     *       across protobuf-java versions for {@code DynamicMessage.Builder}.</li>
     *   <li>Repeated and map entries are NEVER mutated through the list returned by
     *       {@code getField} (it is an unmodifiable view in many protobuf versions).
     *       Instead the field is cleared and the kept entries are re-added via
     *       {@code addRepeatedField}, which is the portable way to "remove" an entry
     *       from a {@code DynamicMessage.Builder}.</li>
     * </ul>
     *
     * @param childNodes path to the leaf node to clear
     * @param root       the top-level builder; modifications are applied to it
     */
    public void clearNodeValue(List<PbNode> childNodes, DynamicMessage.Builder root) {
        if (childNodes == null || childNodes.isEmpty() || root == null) {
            return;
        }
        clearNodeValueRec(root, childNodes, 0);
    }

    /**
     * Recursive helper. Modifies {@code builder} in place; for nested levels, every
     * change is written back to {@code builder} via setField/setRepeatedField as we
     * unwind, so the topmost caller sees the modification.
     */
    private void clearNodeValueRec(DynamicMessage.Builder builder, List<PbNode> nodes, int from) {
        PbNode node = nodes.get(from);
        FieldDescriptor fd = node.getFieldDesc();
        if (fd == null) {
            return;
        }

        boolean isLast = (from == nodes.size() - 1);

        // ============================== LEAF ==============================
        if (isLast) {
            // primitive / struct / repeated-without-index / map-without-key: clearField wipes
            // the whole field. This is the safe single-call PB API.
            if (node.isPrimitiveType()
                    || node.isStructType()
                    || (node.isArrayType() && !node.isHasArrayIndex())
                    || (node.isMapType() && !node.isHasMapKey())) {
                builder.clearField(fd);
                return;
            }
            // repeated with explicit index: rebuild the list without the target element.
            if (node.isArrayType() && node.isHasArrayIndex()) {
                removeRepeatedAt(builder, fd, node.getArrayIndex());
                return;
            }
            // map with explicit key: rebuild the entries dropping the matching key.
            if (node.isMapType() && node.isHasMapKey()) {
                removeMapEntryByKey(builder, fd, node.getMapKeyDesc(), node.getMapKey());
                return;
            }
            return;
        }

        // ============================ INTERMEDIATE ============================
        // primitive intermediate: invalid path, abort
        if (node.isPrimitiveType()) {
            return;
        }

        // struct intermediate: descend into the message field, mutate, then setField back
        if (node.isStructType()) {
            if (!builder.hasField(fd)) {
                return;
            }
            Object child = builder.getField(fd);
            if (!(child instanceof DynamicMessage)) {
                return;
            }
            DynamicMessage.Builder childBuilder = ((DynamicMessage) child).toBuilder();
            clearNodeValueRec(childBuilder, nodes, from + 1);
            builder.setField(fd, childBuilder.build());
            return;
        }

        // array intermediate (only meaningful with an explicit index): descend into that element
        if (node.isArrayType()) {
            if (!node.isHasArrayIndex() || fd.getJavaType() != JavaType.MESSAGE) {
                return;
            }
            int idx = node.getArrayIndex();
            int count = builder.getRepeatedFieldCount(fd);
            if (idx < 0 || idx >= count) {
                return;
            }
            Object element = builder.getRepeatedField(fd, idx);
            if (!(element instanceof DynamicMessage)) {
                return;
            }
            DynamicMessage.Builder eb = ((DynamicMessage) element).toBuilder();
            clearNodeValueRec(eb, nodes, from + 1);
            builder.setRepeatedField(fd, idx, eb.build());
            return;
        }

        // map intermediate: only meaningful with an explicit key.
        // Descend into the value of the matching entry, mutate it, and replace the entry.
        if (node.isMapType()) {
            if (!node.isHasMapKey()) {
                return;
            }
            FieldDescriptor mapKeyDesc = node.getMapKeyDesc();
            FieldDescriptor mapValueDesc = node.getMapValueDesc();
            if (mapKeyDesc == null || mapValueDesc == null
                    || mapValueDesc.getJavaType() != JavaType.MESSAGE) {
                return;
            }
            int count = builder.getRepeatedFieldCount(fd);
            for (int i = 0; i < count; i++) {
                Object entry = builder.getRepeatedField(fd, i);
                if (!(entry instanceof DynamicMessage)) {
                    continue;
                }
                DynamicMessage entryMsg = (DynamicMessage) entry;
                if (!entryMsg.hasField(mapKeyDesc) || !entryMsg.hasField(mapValueDesc)) {
                    continue;
                }
                Object keyVal = entryMsg.getField(mapKeyDesc);
                if (keyVal == null || !Objects.equals(node.getMapKey(), keyVal)) {
                    continue;
                }
                Object valObj = entryMsg.getField(mapValueDesc);
                if (!(valObj instanceof DynamicMessage)) {
                    return;
                }
                DynamicMessage.Builder valBuilder = ((DynamicMessage) valObj).toBuilder();
                clearNodeValueRec(valBuilder, nodes, from + 1);
                DynamicMessage.Builder entryBuilder = entryMsg.toBuilder();
                entryBuilder.setField(mapValueDesc, valBuilder.build());
                builder.setRepeatedField(fd, i, entryBuilder.build());
                return;
            }
        }
    }

    /**
     * Remove the {@code targetIndex}-th element from the given repeated field on
     * {@code builder}. {@code DynamicMessage.Builder} does not expose
     * {@code removeRepeatedField} portably across protobuf-java versions, so we
     * rebuild the field via clearField + addRepeatedField.
     */
    private static void removeRepeatedAt(DynamicMessage.Builder builder,
            FieldDescriptor fd, int targetIndex) {
        int count = builder.getRepeatedFieldCount(fd);
        if (targetIndex < 0 || targetIndex >= count) {
            return;
        }
        List<Object> kept = new ArrayList<>(count - 1);
        for (int i = 0; i < count; i++) {
            if (i == targetIndex) {
                continue;
            }
            kept.add(builder.getRepeatedField(fd, i));
        }
        builder.clearField(fd);
        for (Object v : kept) {
            builder.addRepeatedField(fd, v);
        }
    }

    /**
     * Remove the map entry whose key equals {@code targetKey} from the given map field
     * on {@code builder}. Uses the portable clearField + addRepeatedField approach.
     */
    private static void removeMapEntryByKey(DynamicMessage.Builder builder,
            FieldDescriptor fd, FieldDescriptor mapKeyDesc, Object targetKey) {
        if (mapKeyDesc == null || targetKey == null) {
            return;
        }
        int count = builder.getRepeatedFieldCount(fd);
        List<Object> kept = new ArrayList<>(count);
        boolean removed = false;
        for (int i = 0; i < count; i++) {
            Object entry = builder.getRepeatedField(fd, i);
            if (entry instanceof DynamicMessage) {
                if (!((DynamicMessage) entry).hasField(mapKeyDesc)) {
                    continue;
                }
                Object keyVal = ((DynamicMessage) entry).getField(mapKeyDesc);
                if (Objects.equals(targetKey, keyVal)) {
                    removed = true;
                    continue;
                }
            }
            kept.add(entry);
        }
        if (!removed) {
            return;
        }
        builder.clearField(fd);
        for (Object e : kept) {
            builder.addRepeatedField(fd, e);
        }
    }
}
