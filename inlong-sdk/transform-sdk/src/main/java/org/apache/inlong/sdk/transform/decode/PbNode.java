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

import java.util.ArrayList;
import java.util.List;

/**
 * PbNode
 * 
 */
@Data
public class PbNode {

    private String name;
    private FieldDescriptor fieldDesc;
    private Descriptors.Descriptor messageType;
    private boolean isArray = false;
    private int arrayIndex = -1;
    private boolean isLastNode = false;

    public PbNode(Descriptors.Descriptor messageDesc, String nodeString, boolean isLastNode) {
        int beginIndex = nodeString.indexOf('(');
        if (beginIndex < 0) {
            this.name = nodeString;
            this.fieldDesc = messageDesc.findFieldByName(name);
            if (this.fieldDesc.getJavaType() == JavaType.MESSAGE) {
                this.messageType = this.fieldDesc.getMessageType();
            }
        } else {
            this.name = StringUtils.trim(nodeString.substring(0, beginIndex));
            this.fieldDesc = messageDesc.findFieldByName(name);
            if (this.fieldDesc.getJavaType() == JavaType.MESSAGE) {
                this.messageType = this.fieldDesc.getMessageType();
            }
            int endIndex = nodeString.lastIndexOf(')');
            if (endIndex >= 0) {
                this.isArray = true;
                this.arrayIndex = NumberUtils.toInt(nodeString.substring(beginIndex + 1, endIndex), -1);
                if (this.arrayIndex < 0) {
                    this.arrayIndex = 0;
                }
            }
        }
        this.isLastNode = isLastNode;
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
        int lastIndex = nodeStrings.length - 1;
        Descriptors.Descriptor current = rootDesc;
        for (int i = 0; i <= lastIndex; i++) {
            if (current == null) {
                return null;
            }
            String nodeString = nodeStrings[i];
            PbNode pbNode = new PbNode(current, nodeString, (i == lastIndex));
            current = pbNode.getMessageType();
            nodes.add(pbNode);
        }
        return nodes;
    }
}
