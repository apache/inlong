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

import org.apache.inlong.sdk.transform.decode.ParquetInputByteArray;
import org.apache.inlong.sdk.transform.pojo.FieldInfo;

import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;
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

    protected String getParquetTestDescription() {
        return "message SdkDataRequest { "
                + "required binary sid (UTF8); "
                + "required int64 packageID; "
                + "repeated group msgs { "
                + "  required binary msg (UTF8); "
                + "  required int64 msgTime; "
                + "  optional group extinfo (MAP) { "
                + "    repeated group key_value { "
                + "      required binary key (UTF8); "
                + "      required binary value (UTF8); "
                + "    } "
                + "  } "
                + "} "
                + "}";
    }

    protected byte[] getParquetTestData() {
        String srcString = "UEFSMRUAFRoVHiwVAhUAFQgVCBwYCXNlc3Npb25fMRgJc2Vzc2lvbl8xFg" +
                "AAAAANMAkAAABzZXNzaW9uXzEVABUQFRQsFQIVABUIFQgcGAjpAwAAAAAAABgI6QMAAAA" +
                "AAAAWAAAAAAgc6QMAAAAAAAAVABVMFUwsFQQVABUGFQYcGAtIZWxsbyBXb3JsZBgHQm9u" +
                "am91chYAAAAAJhQCAAAAAwIFBmgDCwAAAEhlbGxvIFdvcmxkBwAAAEJvbmpvdXIVABU4F" +
                "TgsFQQVABUGFQYcGAjellVhAAAAABgI05ZVYQAAAAAWAAAAABwUAgAAAAMCBQZAA9OWVW" +
                "EAAAAA3pZVYQAAAAAVBBUQFRRMFQIVBAAACBwEAAAAbGFuZxUAFSAVJCwVBBUEFQYVBhw" +
                "YBGxhbmcYBGxhbmcWAAAAABA8AwAAAAMEAAMAAAADDwAAAxUAFTQVNCwVBBUAFQYVBhwY" +
                "AkZSGAJFThYAAAAAGhgDAAAAAwQABQc0DwACAAAARU4CAAAARlIVAhmsSA5TZGtEYXRhU" +
                "mVxdWVzdBUGABUMJQAYA3NpZCUAABUEJQAYCXBhY2thZ2VJRAA1BBgEbXNncxUGABUMJQ" +
                "AYA21zZyUAABUEJQAYB21zZ1RpbWUANQIYB2V4dGluZm8VAhUCADUEGAlrZXlfdmFsdWU" +
                "VBAAVDCUAGANrZXklAAAVDCUAGAV2YWx1ZSUAABYCGRwZbCYIHBUMGSUIABkYA3NpZBUC" +
                "FgIWcBZ0Jgg8GAlzZXNzaW9uXzEYCXNlc3Npb25fMRYAAAAAJnwcFQQZJQgAGRgJcGFja" +
                "2FnZUlEFQIWAhZiFmYmfDwYCOkDAAAAAAAAGAjpAwAAAAAAABYAAAAAJuIBHBUMGSUABh" +
                "koBG1zZ3MDbXNnFQIWBBaiARaiASbiATwYC0hlbGxvIFdvcmxkGAdCb25qb3VyFgAAAAA" +
                "mhAMcFQQZJQAGGSgEbXNncwdtc2dUaW1lFQIWBBaKARaKASaEAzwYCN6WVWEAAAAAGAjT" +
                "llVhAAAAABYAAAAAJo4EHBUMGSUEBhlIBG1zZ3MHZXh0aW5mbwlrZXlfdmFsdWUDa2V5F" +
                "QIWBBaMARaUASaOBDwYBGxhbmcYBGxhbmcWAAAAACaiBRwVDBklAAYZSARtc2dzB2V4dG" +
                "luZm8Ja2V5X3ZhbHVlBXZhbHVlFQIWBBZuFm4mogU8GAJGUhgCRU4WAAAAABb4BRYCACh" +
                "JcGFycXVldC1tciB2ZXJzaW9uIDEuOC4xIChidWlsZCA0YWJhNGRhZTdiYjBkNGVkYmNm" +
                "NzkyM2FlMTMzOWYyOGZkM2Y3ZmNmKQBeAgAAUEFSMQ==";
        byte[] srcBytes = Base64.getDecoder().decode(srcString);
        return srcBytes;
    }

    protected byte[] getAvroTestData() {
        String srcString = "T2JqAQIWYXZyby5zY2hlbWHIBXsidHlwZSI6InJlY29yZCIsIm5hbWUiOiJTZGtE"
                + "YXRhUmVxdWVzdCIsIm5hbWVzcGFjZSI6InRlc3QiLCJmaWVsZHMiOlt7Im5hbWUi"
                + "OiJzaWQiLCJ0eXBlIjoic3RyaW5nIn0seyJuYW1lIjoibXNncyIsInR5cGUiOnsi"
                + "dHlwZSI6ImFycmF5IiwiaXRlbXMiOnsidHlwZSI6InJlY29yZCIsIm5hbWUiOiJT"
                + "ZGtNZXNzYWdlIiwiZmllbGRzIjpbeyJuYW1lIjoibXNnIiwidHlwZSI6ImJ5dGVz"
                + "In0seyJuYW1lIjoibXNnVGltZSIsInR5cGUiOiJsb25nIn0seyJuYW1lIjoiZXh0"
                + "aW5mbyIsInR5cGUiOnsidHlwZSI6Im1hcCIsInZhbHVlcyI6InN0cmluZyJ9fV19"
                + "fX0seyJuYW1lIjoicGFja2FnZUlEIiwidHlwZSI6ImxvbmcifV19AMt7kQjpgkXl"
                + "EjM4Iv+oOJYClgEIc2lkMQQKQXBwbGXyhcYJBARrMQx2YWx1ZTEEazIMdmFsdWUy"
                + "AAxCYW5hbmHki4wTBARrMQx2YWx1ZTMEazIMdmFsdWU0AACAiQ/Le5EI6YJF5RIz"
                + "OCL/qDiW";
        byte[] srcBytes = Base64.getDecoder().decode(srcString);
        return srcBytes;
    }

    public static List<String> ParquetByteArray2CsvStr(byte[] parquetBytes) throws IOException {
        InputFile inputFile = new ParquetInputByteArray(parquetBytes);
        List<String> strRows = new ArrayList<>();
        try (ParquetFileReader reader = ParquetFileReader.open(inputFile)) {
            ParquetMetadata footer = reader.getFooter();
            MessageType schema = footer.getFileMetaData().getSchema();
            int fieldSize = schema.getFields().size();
            PageReadStore pages;

            while ((pages = reader.readNextRowGroup()) != null) {
                long rows = pages.getRowCount();

                ColumnIOFactory factory = new ColumnIOFactory();
                MessageColumnIO columnIO = factory.getColumnIO(schema);

                RecordMaterializer<Group> recordMaterializer = new GroupRecordConverter(schema);

                RecordReader<Group> recordReader = columnIO.getRecordReader(pages, recordMaterializer);

                for (int i = 0; i < rows; i++) {
                    Group group = recordReader.read();
                    if (group != null) {
                        StringBuilder builder = new StringBuilder();
                        for (int j = 0; j < fieldSize; j++) {
                            builder.append(group.getValueToString(j, 0) + "|");
                        }
                        strRows.add(builder.substring(0, builder.length() - 1));
                    }
                }
            }
        }
        return strRows;
    }
}
