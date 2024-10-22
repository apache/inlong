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

package org.apache.inlong.sdk.transform.encode;

import org.apache.inlong.sdk.transform.pojo.FieldInfo;
import org.apache.inlong.sdk.transform.pojo.PbSinkInfo;
import org.apache.inlong.sdk.transform.process.Context;

import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;

import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

public class PbSinkEncoder extends SinkEncoder<byte[]> {

    protected PbSinkInfo sinkInfo;

    private Descriptors.Descriptor dynamicDescriptor;

    private final Map<String, Descriptors.FieldDescriptor.Type> fieldTypes;

    public PbSinkEncoder(PbSinkInfo pbSinkInfo) {
        super(pbSinkInfo.getFields());
        this.sinkInfo = pbSinkInfo;
        this.fieldTypes = new HashMap<>();
        for (FieldInfo field : this.fields) {
            fieldTypes.put(field.getName(), Descriptors.FieldDescriptor.Type.STRING);
        }
        // decode protoDescription
        this.dynamicDescriptor = decodeProtoDescription(pbSinkInfo.getProtoDescription());
    }

    @Override
    public byte[] encode(SinkData sinkData, Context context) {
        try {
            DynamicMessage.Builder dynamicBuilder = DynamicMessage.newBuilder(dynamicDescriptor);

            // Dynamically fill message fields
            for (String key : sinkData.keyList()) {
                Descriptors.FieldDescriptor fieldDescriptor = dynamicDescriptor.findFieldByName(key);
                if (fieldDescriptor != null) {
                    String fieldValue = sinkData.getField(key);
                    if (fieldValue != null) {
                        Object value = convertValue(fieldDescriptor, fieldValue);
                        dynamicBuilder.setField(fieldDescriptor, value);
                    }
                }
            }
            // Serialize to byte[]
            return dynamicBuilder.build().toByteArray();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Decode the base64-encoded proto description into a Descriptor
     *
     * @param base64ProtoDescription The base64-encoded proto description
     * @return The dynamic Descriptor
     */
    private Descriptors.Descriptor decodeProtoDescription(String base64ProtoDescription) {
        try {
            byte[] protoBytes = Base64.getDecoder().decode(base64ProtoDescription);
            DescriptorProtos.FileDescriptorSet fileDescriptorSet =
                    DescriptorProtos.FileDescriptorSet.parseFrom(protoBytes);
            Descriptors.FileDescriptor fileDescriptor = Descriptors.FileDescriptor.buildFrom(
                    fileDescriptorSet.getFile(0), new Descriptors.FileDescriptor[]{});
            return fileDescriptor.getMessageTypes().get(0);
        } catch (Exception e) {
            throw new RuntimeException("Failed to decode protoDescription", e);
        }
    }

    private Object convertValue(Descriptors.FieldDescriptor fieldDescriptor, Object value) {
        switch (fieldDescriptor.getType()) {
            case STRING:
                return value.toString();
            case INT32:
                return Integer.parseInt(value.toString());
            case INT64:
                return Long.parseLong(value.toString());
            case BOOL:
                return Boolean.parseBoolean(value.toString());
            case BYTES:
                return ByteString.copyFromUtf8(value.toString());
            default:
                throw new IllegalArgumentException("Unsupported field type: " + fieldDescriptor.getType());
        }
    }

}
