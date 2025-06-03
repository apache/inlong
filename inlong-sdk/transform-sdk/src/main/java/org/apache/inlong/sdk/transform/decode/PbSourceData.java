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
import com.google.protobuf.DynamicMessage;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * JsonSourceData
 * 
 */
public class PbSourceData extends AbstractSourceData {

    private static final Logger LOG = LoggerFactory.getLogger(PbSourceData.class);

    public static final String ROOT_KEY = "$root.";

    public static final String CHILD_KEY = "$child.";

    private Descriptors.Descriptor rootDesc;

    private Descriptors.Descriptor childDesc;

    private Map<String, List<PbNode>> columnNodeMap = new ConcurrentHashMap<>();

    private DynamicMessage root;

    private List<DynamicMessage> childRoot;

    private Charset srcCharset;

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
    public String getField(int rowNum, String fieldName) {
        String fieldValue = "";
        try {
            if (isContextField(fieldName)) {
                return getContextField(fieldName);
            }
            if (StringUtils.startsWith(fieldName, ROOT_KEY)) {
                fieldValue = this.getRootField(fieldName);
            } else if (StringUtils.startsWith(fieldName, CHILD_KEY)) {
                if (childRoot != null && rowNum < childRoot.size()) {
                    fieldValue = this.getChildField(rowNum, fieldName);
                }
            }
            return fieldValue;
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }
        return fieldValue;
    }

    /**
     * getRootField
     * @param fieldName
     * @return
     */
    private String getRootField(String srcFieldName) {
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
            return "";
        }
        // parse other node
        String fieldValue = this.getNodeValue(childNodes, root);
        return fieldValue;
    }

    /**
     * getChildField
     * @param rowNum
     * @param srcFieldName
     * @return
     */
    private String getChildField(int rowNum, String srcFieldName) {
        if (this.childRoot == null || this.childDesc == null) {
            return "";
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
            return "";
        }
        // parse other node
        DynamicMessage child = childRoot.get(rowNum);
        String fieldValue = this.getNodeValue(childNodes, child);
        return fieldValue;
    }

    /**
     * getNodeValue
     * @param childNodes
     * @param root
     * @return
     */
    private String getNodeValue(List<PbNode> childNodes, DynamicMessage root) {
        String fieldValue = "";
        DynamicMessage current = root;
        for (int i = 0; i < childNodes.size(); i++) {
            PbNode node = childNodes.get(i);
            Object nodeValue = current.getField(node.getFieldDesc());
            if (nodeValue == null) {
                // error data
                break;
            }
            if (node.isLastNode()) {
                switch (node.getFieldDesc().getJavaType()) {
                    case STRING:
                    case INT:
                    case LONG:
                    case FLOAT:
                    case DOUBLE:
                    case BOOLEAN:
                        fieldValue = String.valueOf(nodeValue);
                        break;
                    case BYTE_STRING:
                        ByteString byteString = (ByteString) nodeValue;
                        fieldValue = new String(byteString.toByteArray(), srcCharset);
                        break;
                    case ENUM:
                        fieldValue = String.valueOf(nodeValue);
                        break;
                    case MESSAGE:
                        fieldValue = String.valueOf(nodeValue);
                        break;
                }
                break;
            }
            if (!node.isArray()) {
                if (!(nodeValue instanceof DynamicMessage)) {
                    // error data
                    break;
                }
                current = (DynamicMessage) nodeValue;
            } else {
                if (!(nodeValue instanceof List)) {
                    // error data
                    break;
                }
                List<?> nodeList = (List<?>) nodeValue;
                if (node.getArrayIndex() >= nodeList.size()) {
                    // error data
                    break;
                }
                Object nodeElement = nodeList.get(node.getArrayIndex());
                if (!(nodeElement instanceof DynamicMessage)) {
                    // error data
                    break;
                }
                current = (DynamicMessage) nodeElement;
            }
        }
        return fieldValue;
    }
}
