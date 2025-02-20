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

import org.apache.commons.lang3.StringUtils;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;

/**
 * ParquetSourceData
 */
public class ParquetSourceData implements SourceData {

    private static final Logger LOG = LoggerFactory.getLogger(ParquetSourceData.class);

    public static final String ROOT_KEY = "$root.";

    public static final String CHILD_KEY = "$child.";

    private Group rootGroup;
    private Group childParent;
    private String childName;
    private Charset srcCharset;
    private Type childType;
    private int rowCount = -1;

    public ParquetSourceData(Group root, String childPath, Charset srcCharset) {
        this.rootGroup = root;
        String pathStr = "";
        if (!StringUtils.isEmpty(childPath)) {
            pathStr = childPath;
        }
        if (!StringUtils.isEmpty(pathStr)) {
            String[] pathNodes = pathStr.split("\\.");
            this.childName = pathNodes[pathNodes.length - 1];
            this.childParent = parsePath(rootGroup, pathNodes);
            if (this.childParent != null) {
                this.childType = this.childParent.getType().getType(this.childName);
                this.rowCount = this.childParent.getFieldRepetitionCount(this.childName);
            }
        }
        this.srcCharset = srcCharset;
    }
    @Override
    public int getRowCount() {
        if (this.childParent == null) {
            return 1;
        } else {
            return rowCount;
        }
    }

    @Override
    public String getField(int rowNum, String fieldName) {
        String fieldValue = "";
        try {
            if (StringUtils.startsWith(fieldName, ROOT_KEY)) {
                // Dealing with multi-level paths
                fieldName = fieldName.substring(ROOT_KEY.length());
                fieldValue = parseFields(fieldName, rootGroup);
            } else if (StringUtils.startsWith(fieldName, CHILD_KEY)) {
                // To meet various situations
                if (childType instanceof GroupType) {
                    if (childParent != null && rowNum < getRowCount()) {
                        Group group = childParent.getGroup(childName, rowNum);
                        // Dealing with multi-level paths
                        fieldName = fieldName.substring(CHILD_KEY.length());
                        fieldValue = parseFields(fieldName, group);
                    }
                } else {
                    fieldValue = getFieldValue(childParent, childName).toString();
                }
            }
            return fieldValue;
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }
        return fieldValue;
    }

    private String parseFields(String fieldName, Group rootGroup) {
        String[] pathNodes = fieldName.split("\\.");
        Group parNode = parsePath(rootGroup, pathNodes);
        return getFieldValue(parNode, pathNodes[pathNodes.length - 1]).toString();
    }

    /**
     * Resolve to the parent Group of the path for subsequent value taking
     * @param root Starting Group for Analysis
     * @param path Analyze the path, separated by dots "."
     * @return The parent group of the path
     */
    public Group parsePath(Group root, String[] path) {
        Object cur = root, last = null;
        int lastIdx = path.length - 1;
        for (int i = 0; i <= lastIdx; i++) {
            if (cur instanceof Group) {
                Object value = getFieldValue((Group) cur, path[i]);
                last = cur;
                cur = value;
            } else if (i == lastIdx) {
                return (Group) last;
            } else {
                return null;
            }
            if (cur == null) {
                return null;
            }
        }
        return (Group) last;
    }

    public Object getFieldValue(Group group, String fieldName) {
        try {
            int idx = 0, start = fieldName.indexOf('(');
            if (start != -1) {
                idx = Integer.parseInt(fieldName.substring(start + 1, fieldName.indexOf(')')));
                fieldName = fieldName.substring(0, start);
            }
            Type field = group.getType().getType(fieldName);
            if (field.isPrimitive()) {
                switch (field.asPrimitiveType().getPrimitiveTypeName()) {
                    case INT32:
                        return group.getInteger(fieldName, idx);
                    case INT64:
                        return group.getLong(fieldName, idx);
                    case INT96:
                        return group.getInt96(fieldName, idx);
                    case FLOAT:
                        return group.getFloat(fieldName, idx);
                    case DOUBLE:
                        return group.getDouble(fieldName, idx);
                    case BOOLEAN:
                        return group.getBoolean(fieldName, idx);
                    case FIXED_LEN_BYTE_ARRAY:
                        return group.getBinary(fieldName, idx).getBytes();
                    case BINARY:
                        return new String(group.getBinary(fieldName, idx).getBytes(), srcCharset);
                    default:
                        LOG.error("Unsupported type for field: {} ,field name: {}", field, fieldName);
                        return null;
                }
            } else {
                return group.getGroup(fieldName, idx);
            }
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            return null;
        }
    }

}
