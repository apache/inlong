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

import org.apache.inlong.sdk.transform.pojo.PbSourceInfo;
import org.apache.inlong.sdk.transform.process.Context;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Base64;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * PbSourceDecoder
 * 
 */
public class PbSourceDecoder extends SourceDecoder<String> {

    private static final Logger LOG = LoggerFactory.getLogger(PbSourceDecoder.class);

    protected PbSourceInfo sourceInfo;
    private Charset srcCharset = Charset.defaultCharset();
    private String protoDescription;
    private String rootMessageType;
    private Descriptors.Descriptor rootDesc;
    private String rowsNodePath;
    private List<PbNode> childNodes;
    private Descriptors.Descriptor childDesc;

    private Map<String, List<PbNode>> columnNodeMap = new ConcurrentHashMap<>();

    /**
     * Constructor
     * @param sourceInfo
     * @throws DescriptorValidationException 
     * @throws InvalidProtocolBufferException 
     */
    public PbSourceDecoder(PbSourceInfo sourceInfo) {
        try {
            this.sourceInfo = sourceInfo;
            if (!StringUtils.isBlank(sourceInfo.getCharset())) {
                this.srcCharset = Charset.forName(sourceInfo.getCharset());
            }
            this.protoDescription = sourceInfo.getProtoDescription();
            this.rootMessageType = sourceInfo.getRootMessageType();
            // parse description
            byte[] protoBytes = Base64.getDecoder().decode(protoDescription);
            DescriptorProtos.FileDescriptorSet descriptorSet = DescriptorProtos.FileDescriptorSet.parseFrom(protoBytes);
            // format proto
            DescriptorProtos.FileDescriptorProto fileDesc = formatFileDescription(descriptorSet, rootMessageType);
            Descriptors.FileDescriptor fileDescriptor = Descriptors.FileDescriptor.buildFrom(fileDesc,
                    new Descriptors.FileDescriptor[]{});
            // find root
            String fullRootMessageType = formatTypeName(findRootType(descriptorSet, rootMessageType));
            this.rootDesc = fileDescriptor.findMessageTypeByName(fullRootMessageType);
            // child
            this.rowsNodePath = sourceInfo.getRowsNodePath();
            this.childNodes = PbNode.parseNodePath(rootDesc, rowsNodePath);
            if (this.childNodes != null && this.childNodes.size() > 0) {
                this.childDesc = this.childNodes.get(this.childNodes.size() - 1).getFieldDesc().getMessageType();
            }
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            throw new TransformException(e.getMessage(), e);
        }
    }

    /**
     * decode
     * @param srcBytes
     * @param context
     * @return
     * @throws InvalidProtocolBufferException 
     */
    @SuppressWarnings("unchecked")
    @Override
    public SourceData decode(byte[] srcBytes, Context context) {
        try {
            // decode
            DynamicMessage.Builder builder = DynamicMessage.newBuilder(rootDesc);
            DynamicMessage root = builder.mergeFrom(srcBytes).build();
            // child
            List<DynamicMessage> childRoot = null;
            if (this.childNodes != null && this.childNodes.size() > 0) {
                DynamicMessage current = root;
                for (PbNode node : childNodes) {
                    Object nodeValue = current.getField(node.getFieldDesc());
                    if (nodeValue == null) {
                        // error data
                        return new PbSourceData(root, rootDesc, columnNodeMap, srcCharset);
                    }
                    if (node.isLastNode()) {
                        if (!(nodeValue instanceof List)) {
                            // error data
                            return new PbSourceData(root, rootDesc, columnNodeMap, srcCharset);
                        } else {
                            childRoot = (List<DynamicMessage>) nodeValue;
                            break;
                        }
                    }
                    if (!node.isArrayType()) {
                        if (!(nodeValue instanceof DynamicMessage)) {
                            // error data
                            return new PbSourceData(root, rootDesc, columnNodeMap, srcCharset);
                        }
                        current = (DynamicMessage) nodeValue;
                    } else {
                        if (!(nodeValue instanceof List)) {
                            // error data
                            return new PbSourceData(root, rootDesc, columnNodeMap, srcCharset);
                        }
                        List<?> nodeList = (List<?>) nodeValue;
                        if (node.getArrayIndex() >= nodeList.size()) {
                            // error data
                            return new PbSourceData(root, rootDesc, columnNodeMap, srcCharset);
                        }
                        Object nodeElement = nodeList.get(node.getArrayIndex());
                        if (!(nodeElement instanceof DynamicMessage)) {
                            // error data
                            return new PbSourceData(root, rootDesc, columnNodeMap, srcCharset);
                        }
                        current = (DynamicMessage) nodeElement;
                    }
                }
            }
            return new PbSourceData(root, childRoot, rootDesc, childDesc, columnNodeMap, srcCharset, context);
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            return null;
        }
    }

    public SourceData decode(String input, Context context) {
        byte[] srcBytes = Base64.getDecoder().decode(input);
        return decode(srcBytes, context);
    }

    private static DescriptorProtos.FileDescriptorProto formatFileDescription(
            DescriptorProtos.FileDescriptorSet descriptorSet,
            String rootMessageType) throws DescriptorValidationException, IOException {
        // load
        Map<String, DescriptorProtos.DescriptorProto> messageTypeMap = new ConcurrentHashMap<>();
        Map<String, DescriptorProtos.EnumDescriptorProto> enumTypeMap = new ConcurrentHashMap<>();
        loadTypeFromFileSet(descriptorSet, messageTypeMap, enumTypeMap);
        // find root type
        String rootTypeName = findRootType(descriptorSet, rootMessageType);
        // build root type
        DescriptorProtos.FileDescriptorProto.Builder newFileBuilder = DescriptorProtos.FileDescriptorProto.newBuilder();
        Set<String> addedTypeNames = new HashSet<>();
        formatMessageTypeDescription(rootTypeName, newFileBuilder, messageTypeMap, enumTypeMap, addedTypeNames);
        // DescriptorProtos.FileDescriptorSet newDescriptorSet = DescriptorProtos.FileDescriptorSet.newBuilder()
        // .addFile(newFileBuilder).build();
        // byte[] newProtoBytes = newDescriptorSet.toByteArray();
        return newFileBuilder.build();
    }

    private static void formatMessageTypeDescription(String typeName,
            DescriptorProtos.FileDescriptorProto.Builder newFileBuilder,
            Map<String, DescriptorProtos.DescriptorProto> messageTypeMap,
            Map<String, DescriptorProtos.EnumDescriptorProto> enumTypeMap,
            Set<String> addedTypeNames) {
        DescriptorProtos.DescriptorProto typeDesc = messageTypeMap.get(typeName);
        if (typeDesc == null) {
            return;
        }
        DescriptorProtos.DescriptorProto.Builder typeBuilder = DescriptorProtos.DescriptorProto.newBuilder();
        String newTypeName = formatTypeName(typeName);
        typeBuilder.setName(newTypeName);
        for (DescriptorProtos.FieldDescriptorProto fieldDesc : typeDesc.getFieldList()) {
            DescriptorProtos.FieldDescriptorProto.Builder fieldBuilder = DescriptorProtos.FieldDescriptorProto
                    .newBuilder().mergeFrom(fieldDesc);
            if (fieldDesc.getType().equals(FieldDescriptorProto.Type.TYPE_MESSAGE)) {
                String fieldTypeName = fieldDesc.getTypeName();
                String newFieldTypeName = formatTypeName(fieldTypeName);
                fieldBuilder.setTypeName(newFieldTypeName);
                if (!addedTypeNames.contains(newFieldTypeName)) {
                    addedTypeNames.add(newFieldTypeName);
                    formatMessageTypeDescription(fieldTypeName, newFileBuilder, messageTypeMap, enumTypeMap,
                            addedTypeNames);
                }
            } else if (fieldDesc.getType().equals(FieldDescriptorProto.Type.TYPE_ENUM)) {
                String fieldTypeName = fieldDesc.getTypeName();
                String newFieldTypeName = formatTypeName(fieldTypeName);
                fieldBuilder.setTypeName(newFieldTypeName);
                if (!addedTypeNames.contains(newFieldTypeName)) {
                    addedTypeNames.add(newFieldTypeName);
                    formatEnumTypeDescription(fieldTypeName, newFileBuilder, enumTypeMap, addedTypeNames);
                }
            }
            typeBuilder.addField(fieldBuilder);
        }
        newFileBuilder.addMessageType(typeBuilder.build());
    }

    private static void formatEnumTypeDescription(String typeName,
            DescriptorProtos.FileDescriptorProto.Builder newFileBuilder,
            Map<String, DescriptorProtos.EnumDescriptorProto> enumTypeMap,
            Set<String> addedTypeNames) {
        DescriptorProtos.EnumDescriptorProto typeDesc = enumTypeMap.get(typeName);
        if (typeDesc == null) {
            return;
        }
        DescriptorProtos.EnumDescriptorProto.Builder typeBuilder = DescriptorProtos.EnumDescriptorProto.newBuilder()
                .mergeFrom(typeDesc);
        String newTypeName = formatTypeName(typeName);
        typeBuilder.setName(newTypeName);
        newFileBuilder.addEnumType(typeBuilder.build());
    }

    private static String formatTypeName(String typeName) {
        return typeName.substring(1).replace('.', '_');
    }

    private static String findRootType(DescriptorProtos.FileDescriptorSet descriptorSet,
            String rootMessageType) {
        for (DescriptorProtos.FileDescriptorProto fileDesc : descriptorSet.getFileList()) {
            String packageName = fileDesc.getPackage();
            String packagePrefix = "";
            if (!StringUtils.isBlank(packageName)) {
                packagePrefix = "." + packageName;
            }
            // message type
            for (DescriptorProtos.DescriptorProto typeDesc : fileDesc.getMessageTypeList()) {
                if (StringUtils.equals(typeDesc.getName(), rootMessageType)) {
                    String fullTypeName = packagePrefix + "." + typeDesc.getName();
                    return fullTypeName;
                }
            }
        }
        return null;
    }

    private static void loadTypeFromFileSet(DescriptorProtos.FileDescriptorSet descriptorSet,
            Map<String, DescriptorProtos.DescriptorProto> messageTypeMap,
            Map<String, DescriptorProtos.EnumDescriptorProto> enumTypeMap) {
        for (DescriptorProtos.FileDescriptorProto fileDesc : descriptorSet.getFileList()) {
            loadTypeFromFile(fileDesc, messageTypeMap, enumTypeMap);
        }
    }

    private static void loadTypeFromFile(DescriptorProtos.FileDescriptorProto fileDesc,
            Map<String, DescriptorProtos.DescriptorProto> messageTypeMap,
            Map<String, DescriptorProtos.EnumDescriptorProto> enumTypeMap) {
        String packageName = fileDesc.getPackage();
        String packagePrefix = "";
        if (!StringUtils.isBlank(packageName)) {
            packagePrefix = "." + packageName;
        }
        for (DescriptorProtos.DescriptorProto typeDesc : fileDesc.getMessageTypeList()) {
            loadTypeFromMessage(fileDesc, packagePrefix, typeDesc, messageTypeMap, enumTypeMap);
        }
        for (DescriptorProtos.EnumDescriptorProto typeDesc : fileDesc.getEnumTypeList()) {
            String enumTypeName = packagePrefix + "." + typeDesc.getName();
            enumTypeMap.putIfAbsent(enumTypeName, typeDesc);
        }
    }

    private static void loadTypeFromMessage(DescriptorProtos.FileDescriptorProto fileDesc,
            String parentTypeName,
            DescriptorProtos.DescriptorProto typeDesc,
            Map<String, DescriptorProtos.DescriptorProto> messageTypeMap,
            Map<String, DescriptorProtos.EnumDescriptorProto> enumTypeMap) {
        String fullTypeName = parentTypeName + "." + typeDesc.getName();
        messageTypeMap.putIfAbsent(fullTypeName, typeDesc);
        for (DescriptorProtos.DescriptorProto nestedTypeDesc : typeDesc.getNestedTypeList()) {
            loadTypeFromMessage(fileDesc, fullTypeName, nestedTypeDesc, messageTypeMap, enumTypeMap);
        }
        for (DescriptorProtos.EnumDescriptorProto enumTypeDesc : typeDesc.getEnumTypeList()) {
            String enumTypeName = fullTypeName + "." + enumTypeDesc.getName();
            enumTypeMap.putIfAbsent(enumTypeName, enumTypeDesc);
        }
    }
}
