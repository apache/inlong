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

import org.apache.inlong.sdk.transform.decode.SourceDecoderFactory;
import org.apache.inlong.sdk.transform.encode.SinkEncoderFactory;
import org.apache.inlong.sdk.transform.pojo.CsvSourceInfo;
import org.apache.inlong.sdk.transform.pojo.FieldInfo;
import org.apache.inlong.sdk.transform.pojo.JsonSourceInfo;
import org.apache.inlong.sdk.transform.pojo.KvSourceInfo;
import org.apache.inlong.sdk.transform.pojo.PbSinkInfo;
import org.apache.inlong.sdk.transform.pojo.PbSourceInfo;
import org.apache.inlong.sdk.transform.pojo.TransformConfig;
import org.apache.inlong.sdk.transform.process.TransformProcessor;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;

public class TestAny2PbProcessor extends AbstractProcessorTestBase {

    @Test
    public void testPb2Pb() throws Exception {
        List<FieldInfo> fields = this.getTestFieldList("sid", "packageID", "msgTime", "msg");
        String transformBase64 = this.getPbTestDescription();
        PbSourceInfo pbSource = new PbSourceInfo("UTF-8", transformBase64, "SdkDataRequest", "msgs");
        PbSinkInfo pbSink = new PbSinkInfo("UTF-8", transformBase64, fields);
        String transformSql = "select $root.sid,$root.packageID,$child.msgTime,$child.msg from source";
        TransformConfig config = new TransformConfig(transformSql);
        TransformProcessor<String, byte[]> processor = TransformProcessor
                .create(config, SourceDecoderFactory.createPbDecoder(pbSource),
                        SinkEncoderFactory.createPbEncoder(pbSink));
        byte[] srcBytes = this.getPbTestData();
        List<byte[]> output = processor.transformForBytes(srcBytes, new HashMap<>());
        Assert.assertEquals(2, output.size());

        // case1:
        // encode pb: {test.SdkMessage.msg=<ByteString@2a7f1f10 size=9 contents="msgValue4">,
        // test.SdkMessage.msgTime=1713243918000}
        // result output: [10, 9, 109, 115, 103, 86, 97, 108, 117, 101, 52, 16,-80, -99, -82, -86, -18, 49]

        byte[] res1 = {10, 9, 109, 115, 103, 86, 97, 108, 117, 101, 52, 16, -80, -99, -82, -86, -18, 49};
        Assert.assertArrayEquals(output.get(0), res1);

        // case2:
        // encode pb: {test.SdkMessage.msg=<ByteString@7526515b size=10 contents="msgValue42">,
        // test.SdkMessage.msgTime=1713243918002}
        // result output: [10, 10, 109, 115, 103, 86, 97, 108, 117, 101, 52, 50, 16, -78, -99, -82, -86, -18, 49]

        byte[] res2 = {10, 10, 109, 115, 103, 86, 97, 108, 117, 101, 52, 50, 16, -78, -99, -82, -86, -18, 49};
        Assert.assertArrayEquals(output.get(1), res2);
    }

    @Test
    public void testCsv2PbForOne() throws Exception {
        List<FieldInfo> fields = this.getTestFieldList("sid", "packageID", "msgTime", "msg");
        String transformBase64 = this.getPbTestDescription();
        CsvSourceInfo csvSource = new CsvSourceInfo("UTF-8", '|', '\\', fields);
        PbSinkInfo pbSink = new PbSinkInfo("UTF-8", transformBase64, fields);
        String transformSql = "select sid,packageID,msgTime,msg from source";
        TransformConfig config = new TransformConfig(transformSql);
        TransformProcessor<String, byte[]> processor = TransformProcessor
                .create(config, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createPbEncoder(pbSink));
        List<byte[]> output = processor.transform("sid|1|1713243918000|msgValue4");
        Assert.assertEquals(1, output.size());

        // case1:
        // encode pb: {test.SdkMessage.msg=<ByteString@6574a52c size=9 contents="msgValue4">,
        // test.SdkMessage.msgTime=1713243918000}
        // result output: [10, 9, 109, 115, 103, 86, 97, 108, 117, 101, 52, 16, -80, -99, -82, -86, -18, 49]

        byte[] res = {10, 9, 109, 115, 103, 86, 97, 108, 117, 101, 52, 16, -80, -99, -82, -86, -18, 49};
        Assert.assertArrayEquals(output.get(0), res);
    }

    @Test
    public void testKv2Pb() throws Exception {
        List<FieldInfo> fields = this.getTestFieldList("sid", "packageID", "msgTime", "msg");
        KvSourceInfo kvSource = new KvSourceInfo("UTF-8", fields);
        String transformBase64 = this.getPbTestDescription();
        PbSinkInfo pbSink = new PbSinkInfo("UTF-8", transformBase64, fields);
        String transformSql = "select sid,packageID,msgTime,msg from source";
        TransformConfig config = new TransformConfig(transformSql);
        TransformProcessor<String, byte[]> processor = TransformProcessor
                .create(config, SourceDecoderFactory.createKvDecoder(kvSource),
                        SinkEncoderFactory.createPbEncoder(pbSink));
        List<byte[]> output =
                processor.transform("sid=sid&packageID=1&msgTime=1713243918000&msg=msgValue4", new HashMap<>());
        Assert.assertEquals(1, output.size());

        // case1:
        // encode pb: {test.SdkMessage.msg=<ByteString@6574a52c size=9 contents="msgValue4">,
        // test.SdkMessage.msgTime=1713243918000}
        // result output: [10, 9, 109, 115, 103, 86, 97, 108, 117, 101, 52, 16, -80, -99, -82, -86, -18, 49]

        byte[] res = {10, 9, 109, 115, 103, 86, 97, 108, 117, 101, 52, 16, -80, -99, -82, -86, -18, 49};
        Assert.assertArrayEquals(output.get(0), res);
    }

    @Test
    public void testJson2Pb() throws Exception {
        List<FieldInfo> fields = this.getTestFieldList("sid", "packageID", "msgTime", "msg");
        JsonSourceInfo jsonSource = new JsonSourceInfo("UTF-8", "msgs");
        String transformBase64 = this.getPbTestDescription();
        PbSinkInfo pbSink = new PbSinkInfo("UTF-8", transformBase64, fields);
        String transformSql = "select $root.sid,$root.packageID,$child.msgTime,$child.msg from source";
        TransformConfig config = new TransformConfig(transformSql);
        TransformProcessor<String, byte[]> processor = TransformProcessor
                .create(config, SourceDecoderFactory.createJsonDecoder(jsonSource),
                        SinkEncoderFactory.createPbEncoder(pbSink));
        String srcString1 = "{\n"
                + "  \"sid\":\"value1\",\n"
                + "  \"packageID\":\"value2\",\n"
                + "  \"msgs\":[\n"
                + "  {\"msg\":\"value4\",\"msgTime\":1713243918000},\n"
                + "  {\"msg\":\"v4\",\"msgTime\":1713243918000}\n"
                + "  ]\n"
                + "}";
        List<byte[]> output = processor.transform(srcString1, new HashMap<>());
        Assert.assertEquals(2, output.size());

        // case1:
        // encode pb: {test.SdkMessage.msg=<ByteString@7bd7d6d6 size=6 contents="value4">,
        // test.SdkMessage.msgTime=1713243918000}
        // result output: [10, 6, 118, 97, 108, 117, 101, 52, 16, -80, -99, -82, -86, -18, 49]

        byte[] res1 = {10, 6, 118, 97, 108, 117, 101, 52, 16, -80, -99, -82, -86, -18, 49};
        Assert.assertArrayEquals(output.get(0), res1);

        // case1:
        // encode pb: {test.SdkMessage.msg=<ByteString@5745ca0e size=2 contents="v4">,
        // test.SdkMessage.msgTime=1713243918000}
        // result output: [10, 2, 118, 52, 16, -80, -99, -82, -86, -18, 49]

        byte[] res2 = {10, 2, 118, 52, 16, -80, -99, -82, -86, -18, 49};
        Assert.assertArrayEquals(output.get(1), res2);
    }

}
