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

package org.apache.inlong.sdk.transform.process.processor;

import org.apache.inlong.sdk.transform.pojo.FieldInfo;

import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
/**
 * AbstractProcessorTestBase
 * description: define static parameters for Processor tests
 */
public abstract class AbstractProcessorTestBase {

    protected List<FieldInfo> getTestFieldList(String... fieldNames) {
        List<FieldInfo> fields = new ArrayList<>();
        for (String fieldName : fieldNames) {
            FieldInfo field = new FieldInfo();
            field.setName(fieldName);
            fields.add(field);
        }
        return fields;
    }

    protected byte[] getPbTestData() {
        String srcString =
                "CgNzaWQSIAoJbXNnVmFsdWU0ELCdrqruMRoMCgNrZXkSBXZhbHVlEiMKCm1zZ1ZhbHVlNDIQsp2uqu4xGg4KBGtleTISBnZhbHVlMhgB";
        byte[] srcBytes = Base64.getDecoder().decode(srcString);
        return srcBytes;
    }

    protected String getPbTestDescription() {
        final String transformProto = "syntax = \"proto3\";\n"
                + "package test;\n"
                + "message SdkMessage {\n"
                + "  bytes msg = 1;\n"
                + "  int64 msgTime = 2;\n"
                + "  map<string, string> extinfo = 3;\n"
                + "}\n"
                + "message SdkDataRequest {\n"
                + "  string sid = 1;\n"
                + "  repeated SdkMessage msgs = 2;\n"
                + "  uint64 packageID = 3;\n"
                + "}";
        String transformBase64 = "CrcCCg90cmFuc2Zvcm0ucHJvdG8SBHRlc3QirQEKClNka01lc3NhZ2USEAoDbXNnGAEgASgMUg"
                + "Ntc2cSGAoHbXNnVGltZRgCIAEoA1IHbXNnVGltZRI3CgdleHRpbmZvGAMgAygLMh0udGVzdC5TZGtNZXNzYWdlLk"
                + "V4dGluZm9FbnRyeVIHZXh0aW5mbxo6CgxFeHRpbmZvRW50cnkSEAoDa2V5GAEgASgJUgNrZXkSFAoFdmFsdWUY"
                + "AiABKAlSBXZhbHVlOgI4ASJmCg5TZGtEYXRhUmVxdWVzdBIQCgNzaWQYASABKAlSA3NpZBIkCgRtc2dzGAIgAygLMh"
                + "AudGVzdC5TZGtNZXNzYWdlUgRtc2dzEhwKCXBhY2thZ2VJRBgDIAEoBFIJcGFja2FnZUlEYgZwcm90bzM=";
        return transformBase64;
    }

    protected byte[] getAvroTestData() {
        String srcString = "T2JqAQIWYXZyby5zY2hlbWHIBXsidHlwZSI6InJlY29yZCIsIm5hbWUiOiJTZGtEYXRhUmVxdWVzdCIs"
                + "Im5hbWVzcGFjZSI6InRlc3QiLCJmaWVsZHMiOlt7Im5hbWUiOiJzaWQiLCJ0eXBlIjoic3RyaW5nIn0seyJuYW1lIj"
                + "oibXNncyIsInR5cGUiOnsidHlwZSI6ImFycmF5IiwiaXRlbXMiOnsidHlwZSI6InJlY29yZCIsIm5hbWUiOiJTZGtNZ"
                + "XNzYWdlIiwiZmllbGRzIjpbeyJuYW1lIjoibXNnIiwidHlwZSI6ImJ5dGVzIn0seyJuYW1lIjoibXNnVGltZSIsInR5"
                + "cGUiOiJsb25nIn0seyJuYW1lIjoiZXh0aW5mbyIsInR5cGUiOnsidHlwZSI6Im1hcCIsInZhbHVlcyI6InN0cmluZyJ"
                + "9fV19fX0seyJuYW1lIjoicGFja2FnZUlEIiwidHlwZSI6ImxvbmcifV19AI7h/J8SaFCGp012msD3lKMCngEIc2lkMQ"
                + "QKQXBwbGXyhcYJBAhrZXkxCGtleTEIa2V5Mgx2YWx1ZTEADEJhbmFuYeSLjBMECGtleTEIa2V5MghrZXkyDHZhbHVlM"
                + "gAAgIkPjuH8nxJoUIanTXaawPeUow==";
        byte[] srcBytes = Base64.getDecoder().decode(srcString);
        return srcBytes;
    }
}
