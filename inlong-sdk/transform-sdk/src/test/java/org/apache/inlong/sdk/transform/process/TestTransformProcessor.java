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

package org.apache.inlong.sdk.transform.process;

import org.apache.inlong.sdk.transform.decode.SourceDecoderFactory;
import org.apache.inlong.sdk.transform.encode.SinkEncoderFactory;
import org.apache.inlong.sdk.transform.pojo.CsvSinkInfo;
import org.apache.inlong.sdk.transform.pojo.CsvSourceInfo;
import org.apache.inlong.sdk.transform.pojo.FieldInfo;
import org.apache.inlong.sdk.transform.pojo.JsonSourceInfo;
import org.apache.inlong.sdk.transform.pojo.KvSinkInfo;
import org.apache.inlong.sdk.transform.pojo.KvSourceInfo;
import org.apache.inlong.sdk.transform.pojo.PbSourceInfo;
import org.apache.inlong.sdk.transform.pojo.TransformConfig;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;

/**
 * TestTransformProcessor
 * 
 */
public class TestTransformProcessor {

    @Test
    public void testCsv2Kv() throws Exception {
        List<FieldInfo> fields = new ArrayList<>();
        FieldInfo ftime = new FieldInfo();
        ftime.setName("ftime");
        fields.add(ftime);
        FieldInfo extinfo = new FieldInfo();
        extinfo.setName("extinfo");
        fields.add(extinfo);
        CsvSourceInfo csvSource = new CsvSourceInfo("UTF-8", '|', '\\', fields);
        KvSinkInfo kvSink = new KvSinkInfo("UTF-8", fields);
        String transformSql = "select ftime,extinfo from source where extinfo='ok'";
        TransformConfig config = new TransformConfig(transformSql);
        // case1
        TransformProcessor<String, String> processor1 = TransformProcessor
                .create(config, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));

        List<String> output1 = processor1.transform("2024-04-28 00:00:00|ok", new HashMap<>());
        Assert.assertEquals(1, output1.size());
        Assert.assertEquals(output1.get(0), "ftime=2024-04-28 00:00:00&extinfo=ok");
        // case2
        config.setTransformSql("select ftime,extinfo from source where extinfo!='ok'");
        TransformProcessor<String, String> processor2 = TransformProcessor
                .create(config, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));

        List<String> output2 = processor2.transform("2024-04-28 00:00:00|ok", new HashMap<>());
        Assert.assertEquals(0, output2.size());
    }

    @Test
    public void testCsv2KvNoField() throws Exception {
        CsvSourceInfo csvSource = new CsvSourceInfo("UTF-8", '|', '\\', null);
        KvSinkInfo kvSink = new KvSinkInfo("UTF-8", null);
        String transformSql = "select $1 ftime,$2 extinfo from source where $2='ok'";
        TransformConfig config = new TransformConfig(transformSql);
        // case1
        TransformProcessor<String, String> processor1 = TransformProcessor
                .create(config, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        List<String> output1 = processor1.transform("2024-04-28 00:00:00|ok", new HashMap<>());
        Assert.assertEquals(1, output1.size());
        Assert.assertEquals(output1.get(0), "ftime=2024-04-28 00:00:00&extinfo=ok");
        // case2
        config.setTransformSql("select $1 ftime,$2 extinfo from source where $2!='ok'");
        TransformProcessor<String, String> processor2 = TransformProcessor
                .create(config, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        List<String> output2 = processor2.transform("2024-04-28 00:00:00|ok", new HashMap<>());
        Assert.assertEquals(0, output2.size());
    }

    @Test
    public void testKv2Csv() throws Exception {
        List<FieldInfo> fields = new ArrayList<>();
        FieldInfo ftime = new FieldInfo();
        ftime.setName("ftime");
        fields.add(ftime);
        FieldInfo extinfo = new FieldInfo();
        extinfo.setName("extinfo");
        fields.add(extinfo);
        KvSourceInfo kvSource = new KvSourceInfo("UTF-8", fields);
        CsvSinkInfo csvSink = new CsvSinkInfo("UTF-8", '|', '\\', fields);
        String transformSql = "select ftime,extinfo from source where extinfo='ok'";
        TransformConfig config = new TransformConfig(transformSql);
        // case1
        TransformProcessor<String, String> processor1 = TransformProcessor
                .create(config, SourceDecoderFactory.createKvDecoder(kvSource),
                        SinkEncoderFactory.createCsvEncoder(csvSink));
        List<String> output1 = processor1.transform("ftime=2024-04-28 00:00:00&extinfo=ok", new HashMap<>());
        Assert.assertEquals(1, output1.size());
        Assert.assertEquals(output1.get(0), "2024-04-28 00:00:00|ok");
        // case2
        config.setTransformSql("select ftime,extinfo from source where extinfo!='ok'");
        TransformProcessor<String, String> processor2 = TransformProcessor
                .create(config, SourceDecoderFactory.createKvDecoder(kvSource),
                        SinkEncoderFactory.createCsvEncoder(csvSink));
        List<String> output2 = processor2.transform("ftime=2024-04-28 00:00:00&extinfo=ok", new HashMap<>());
        Assert.assertEquals(0, output2.size());
    }

    @Test
    public void testKv2CsvNoField() throws Exception {
        KvSourceInfo kvSource = new KvSourceInfo("UTF-8", null);
        CsvSinkInfo csvSink = new CsvSinkInfo("UTF-8", '|', '\\', null);
        String transformSql = "select ftime,extinfo from source where extinfo='ok'";
        TransformConfig config = new TransformConfig(transformSql);
        // case1
        TransformProcessor<String, String> processor1 = TransformProcessor
                .create(config, SourceDecoderFactory.createKvDecoder(kvSource),
                        SinkEncoderFactory.createCsvEncoder(csvSink));
        List<String> output1 = processor1.transform("ftime=2024-04-28 00:00:00&extinfo=ok", new HashMap<>());
        Assert.assertEquals(1, output1.size());
        Assert.assertEquals(output1.get(0), "2024-04-28 00:00:00|ok");
        // case2
        config.setTransformSql("select ftime,extinfo from source where extinfo!='ok'");
        TransformProcessor<String, String> processor2 = TransformProcessor
                .create(config, SourceDecoderFactory.createKvDecoder(kvSource),
                        SinkEncoderFactory.createCsvEncoder(csvSink));
        List<String> output2 = processor2.transform("ftime=2024-04-28 00:00:00&extinfo=ok", new HashMap<>());
        Assert.assertEquals(0, output2.size());
    }

    @Test
    public void testJson2Csv() throws Exception {
        List<FieldInfo> fields = this.getTestFieldList();
        JsonSourceInfo jsonSource = new JsonSourceInfo("UTF-8", "msgs");
        CsvSinkInfo csvSink = new CsvSinkInfo("UTF-8", '|', '\\', fields);
        String transformSql = "select $root.sid,$root.packageID,$child.msgTime,$child.msg from source";
        TransformConfig config = new TransformConfig(transformSql);
        // case1
        TransformProcessor<String, String> processor = TransformProcessor
                .create(config, SourceDecoderFactory.createJsonDecoder(jsonSource),
                        SinkEncoderFactory.createCsvEncoder(csvSink));
        String srcString = "{\n"
                + "  \"sid\":\"value1\",\n"
                + "  \"packageID\":\"value2\",\n"
                + "  \"msgs\":[\n"
                + "  {\"msg\":\"value4\",\"msgTime\":1713243918000},\n"
                + "  {\"msg\":\"v4\",\"msgTime\":1713243918000}\n"
                + "  ]\n"
                + "}";
        List<String> output = processor.transform(srcString, new HashMap<>());
        Assert.assertEquals(2, output.size());
        Assert.assertEquals(output.get(0), "value1|value2|1713243918000|value4");
        Assert.assertEquals(output.get(1), "value1|value2|1713243918000|v4");
    }

    @Test
    public void testJson2CsvForOne() throws Exception {
        List<FieldInfo> fields = this.getTestFieldList();
        JsonSourceInfo jsonSource = new JsonSourceInfo("UTF-8", "");
        CsvSinkInfo csvSink = new CsvSinkInfo("UTF-8", '|', '\\', fields);
        String transformSql = "select $root.sid,$root.packageID,$root.msgs(1).msgTime,$root.msgs(0).msg from source";
        TransformConfig config = new TransformConfig(transformSql);
        // case1
        TransformProcessor<String, String> processor = TransformProcessor
                .create(config, SourceDecoderFactory.createJsonDecoder(jsonSource),
                        SinkEncoderFactory.createCsvEncoder(csvSink));
        String srcString = "{\n"
                + "  \"sid\":\"value1\",\n"
                + "  \"packageID\":\"value2\",\n"
                + "  \"msgs\":[\n"
                + "  {\"msg\":\"value4\",\"msgTime\":1713243918000},\n"
                + "  {\"msg\":\"v4\",\"msgTime\":1713243918000}\n"
                + "  ]\n"
                + "}";
        List<String> output = processor.transform(srcString, new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals(output.get(0), "value1|value2|1713243918000|value4");
    }

    @Test
    public void testPb2Csv() throws Exception {
        List<FieldInfo> fields = this.getTestFieldList();
        String transformBase64 = this.getPbTestDescription();
        PbSourceInfo pbSource = new PbSourceInfo("UTF-8", transformBase64, "SdkDataRequest", "msgs");
        CsvSinkInfo csvSink = new CsvSinkInfo("UTF-8", '|', '\\', fields);
        String transformSql = "select $root.sid,$root.packageID,$child.msgTime,$child.msg from source";
        TransformConfig config = new TransformConfig(transformSql);
        // case1
        TransformProcessor<byte[], String> processor = TransformProcessor
                .create(config, SourceDecoderFactory.createPbDecoder(pbSource),
                        SinkEncoderFactory.createCsvEncoder(csvSink));
        byte[] srcBytes = this.getPbTestData();
        List<String> output = processor.transform(srcBytes);
        Assert.assertEquals(2, output.size());
        Assert.assertEquals(output.get(0), "sid|1|1713243918000|msgValue4");
        Assert.assertEquals(output.get(1), "sid|1|1713243918002|msgValue42");
    }

    private List<FieldInfo> getTestFieldList() {
        List<FieldInfo> fields = new ArrayList<>();
        FieldInfo sid = new FieldInfo();
        sid.setName("sid");
        fields.add(sid);
        FieldInfo packageID = new FieldInfo();
        packageID.setName("packageID");
        fields.add(packageID);
        FieldInfo msgTime = new FieldInfo();
        msgTime.setName("msgTime");
        fields.add(msgTime);
        FieldInfo msg = new FieldInfo();
        msg.setName("msg");
        fields.add(msg);
        return fields;
    }

    private byte[] getPbTestData() {
        String srcString =
                "CgNzaWQSIAoJbXNnVmFsdWU0ELCdrqruMRoMCgNrZXkSBXZhbHVlEiMKCm1zZ1ZhbHVlNDIQsp2uqu4xGg4KBGtleTISBnZhbHVlMhgB";
        byte[] srcBytes = Base64.getDecoder().decode(srcString);
        return srcBytes;
    }

    private String getPbTestDescription() {
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

    @Test
    public void testPb2CsvForOne() throws Exception {
        List<FieldInfo> fields = this.getTestFieldList();
        String transformBase64 = this.getPbTestDescription();
        PbSourceInfo pbSource = new PbSourceInfo("UTF-8", transformBase64, "SdkDataRequest", null);
        CsvSinkInfo csvSink = new CsvSinkInfo("UTF-8", '|', '\\', fields);
        String transformSql = "select $root.sid,$root.packageID,$root.msgs(1).msgTime,$root.msgs(0).msg from source";
        TransformConfig config = new TransformConfig(transformSql);
        // case1
        TransformProcessor<byte[], String> processor = TransformProcessor
                .create(config, SourceDecoderFactory.createPbDecoder(pbSource),
                        SinkEncoderFactory.createCsvEncoder(csvSink));
        byte[] srcBytes = this.getPbTestData();
        List<String> output = processor.transform(srcBytes, new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals(output.get(0), "sid|1|1713243918002|msgValue4");
    }

    @Test
    public void testPb2CsvForAdd() throws Exception {
        List<FieldInfo> fields = this.getTestFieldList();
        String transformBase64 = this.getPbTestDescription();
        PbSourceInfo pbSource = new PbSourceInfo("UTF-8", transformBase64, "SdkDataRequest", null);
        CsvSinkInfo csvSink = new CsvSinkInfo("UTF-8", '|', '\\', fields);
        String transformSql = "select $root.sid,"
                + "($root.msgs(1).msgTime-$root.msgs(0).msgTime)/$root.packageID field2,"
                + "$root.packageID*($root.msgs(0).msgTime*$root.packageID+$root.msgs(1).msgTime/$root.packageID)"
                + "*$root.packageID field3,"
                + "$root.msgs(0).msg field4 from source "
                + "where $root.packageID<($root.msgs(0).msgTime+$root.msgs(1).msgTime"
                + "+$root.msgs(0).msgTime+$root.msgs(1).msgTime)";
        TransformConfig config = new TransformConfig(transformSql);
        // case1
        TransformProcessor<byte[], String> processor = TransformProcessor
                .create(config, SourceDecoderFactory.createPbDecoder(pbSource),
                        SinkEncoderFactory.createCsvEncoder(csvSink));
        byte[] srcBytes = this.getPbTestData();
        List<String> output = processor.transform(srcBytes, new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals(output.get(0), "sid|2|3426487836002|msgValue4");
    }

    @Test
    public void testPb2CsvForConcat() throws Exception {
        List<FieldInfo> fields = this.getTestFieldList();
        String transformBase64 = this.getPbTestDescription();
        PbSourceInfo pbSource = new PbSourceInfo("UTF-8", transformBase64, "SdkDataRequest", "msgs");
        CsvSinkInfo csvSink = new CsvSinkInfo("UTF-8", '|', '\\', fields);
        String transformSql = "select $root.sid,$root.packageID,$child.msgTime,"
                + "concat($root.sid,$root.packageID,$child.msgTime,$child.msg) msg,$root.msgs.msgTime.msg from source";
        TransformConfig config = new TransformConfig(transformSql);
        // case1
        TransformProcessor<byte[], String> processor = TransformProcessor
                .create(config, SourceDecoderFactory.createPbDecoder(pbSource),
                        SinkEncoderFactory.createCsvEncoder(csvSink));
        byte[] srcBytes = this.getPbTestData();
        List<String> output = processor.transform(srcBytes, new HashMap<>());
        Assert.assertTrue(output.size() == 2);
        Assert.assertEquals(output.get(0), "sid|1|1713243918000|sid11713243918000msgValue4");
        Assert.assertEquals(output.get(1), "sid|1|1713243918002|sid11713243918002msgValue42");
    }

    @Test
    public void testPb2CsvForNow() throws Exception {
        List<FieldInfo> fields = this.getTestFieldList();
        String transformBase64 = this.getPbTestDescription();
        PbSourceInfo pbSource = new PbSourceInfo("UTF-8", transformBase64, "SdkDataRequest", "msgs");
        CsvSinkInfo csvSink = new CsvSinkInfo("UTF-8", '|', '\\', fields);
        String transformSql = "select now() from source";
        TransformConfig config = new TransformConfig(transformSql);
        // case1
        TransformProcessor<byte[], String> processor = TransformProcessor
                .create(config, SourceDecoderFactory.createPbDecoder(pbSource),
                        SinkEncoderFactory.createCsvEncoder(csvSink));
        byte[] srcBytes = this.getPbTestData();
        List<String> output = processor.transform(srcBytes, new HashMap<>());
        Assert.assertEquals(2, output.size());
    }
}
