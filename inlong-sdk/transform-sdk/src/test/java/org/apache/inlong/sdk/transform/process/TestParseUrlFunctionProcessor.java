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
import org.apache.inlong.sdk.transform.pojo.CsvSourceInfo;
import org.apache.inlong.sdk.transform.pojo.FieldInfo;
import org.apache.inlong.sdk.transform.pojo.KvSinkInfo;
import org.apache.inlong.sdk.transform.pojo.TransformConfig;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * TestParseUrlFunctionProcessor
 * description: test the parse_url function in transform processor
 */
public class TestParseUrlFunctionProcessor {

    private static final List<FieldInfo> srcFields = new ArrayList<>();
    private static final List<FieldInfo> dstFields = new ArrayList<>();
    private static final CsvSourceInfo csvSource;
    private static final KvSinkInfo kvSink;

    static {
        for (int i = 1; i < 4; i++) {
            FieldInfo field = new FieldInfo();
            field.setName("string" + i);
            srcFields.add(field);
        }
        FieldInfo field = new FieldInfo();
        field.setName("result");
        dstFields.add(field);
        csvSource = new CsvSourceInfo("UTF-8", '|', '\\', srcFields);
        kvSink = new KvSinkInfo("UTF-8", dstFields);
    }

    @Test
    public void testParseUrlFunction() throws Exception {
        String transformSql1 = "select parse_url(string1, string2) from source";
        TransformConfig config1 = new TransformConfig(transformSql1);
        TransformProcessor<String, String> processor1 = TransformProcessor
                .create(config1, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));

        // case1: parse_url('http://facebook.com/path1/p.php?k1=v1&k2=v2#Ref1', 'HOST')
        List<String> output1 = processor1
                .transform("http://facebook.com/path1/p.php?k1=v1&k2=v2#Ref1|HOST|banana|cloud|1", new HashMap<>());
        Assert.assertEquals(1, output1.size());
        Assert.assertEquals(output1.get(0), "result=facebook.com");

        String transformSql2 = "select parse_url(string1, string2) from source";
        TransformConfig config2 = new TransformConfig(transformSql2);
        TransformProcessor<String, String> processor2 = TransformProcessor
                .create(config2, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        // case2: parse_url('http://facebook.com/path1/p.php?k1=v1&k2=v2#Ref1', 'QUERY')
        List<String> output2 = processor2
                .transform("http://facebook.com/path1/p.php?k1=v1&k2=v2#Ref1|QUERY|banana|cloud|1", new HashMap<>());
        Assert.assertEquals(1, output2.size());
        Assert.assertEquals(output2.get(0), "result=k1=v1&k2=v2");

        String transformSql3 = "select parse_url(string1, string2, string3) from source";
        TransformConfig config3 = new TransformConfig(transformSql3);
        TransformProcessor<String, String> processor3 = TransformProcessor
                .create(config3, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        // case3: parse_url('http://facebook.com/path1/p.php?k1=v1&k2=v2#Ref1', 'QUERY', 'k1')
        List<String> output3 = processor3.transform("http://facebook.com/path1/p.php?k1=v1&k2=v2#Ref1|QUERY|k1|cloud|1",
                new HashMap<>());
        Assert.assertEquals(1, output3.size());
        Assert.assertEquals(output3.get(0), "result=v1");

        String transformSql4 = "select parse_url(string1, string2, string3) from source";
        TransformConfig config4 = new TransformConfig(transformSql4);
        TransformProcessor<String, String> processor4 = TransformProcessor
                .create(config4, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        // case4: parse_url('http://facebook.com/path1/p.php?k1=v1&k2=v2#Ref1', 'QUERY', 'k1')
        List<String> output4 = processor4.transform("http://facebook.com/path1/p.php?k1=v1&k2=v2#Ref1|QUERY|k1|cloud|1",
                new HashMap<>());
        Assert.assertEquals(1, output4.size());
        Assert.assertEquals(output4.get(0), "result=v1");

        String transformSql5 = "select parse_url(stringX, string2, string3) from source";
        TransformConfig config5 = new TransformConfig(transformSql5);
        TransformProcessor<String, String> processor5 = TransformProcessor
                .create(config5, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        // case5: parse_url(null, 'QUERY', 'k1')
        List<String> output5 = processor5.transform("http://facebook.com/path1/p.php?k1=v1&k2=v2#Ref1|QUERY|k1|cloud|1",
                new HashMap<>());
        Assert.assertEquals(1, output5.size());
        Assert.assertEquals(output5.get(0), "result=");

        String transformSql6 = "select parse_url(string1, string2, stringX) from source";
        TransformConfig config6 = new TransformConfig(transformSql6);
        TransformProcessor<String, String> processor6 = TransformProcessor
                .create(config6, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        // case6: parse_url('http://facebook.com/path1/p.php?k1=v1&k2=v2#Ref1', 'QUERY', null)
        List<String> output6 = processor6.transform("http://facebook.com/path1/p.php?k1=v1&k2=v2#Ref1|QUERY|k1|cloud|1",
                new HashMap<>());
        Assert.assertEquals(1, output6.size());
        Assert.assertEquals(output6.get(0), "result=");
        Assert.assertEquals(output3.get(0), "result=v1");

        String transformSql7 = "select parse_url(string1, string2) from source";
        TransformConfig config7 = new TransformConfig(transformSql7);
        TransformProcessor<String, String> processor7 = TransformProcessor
                .create(config7, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        // case7: parse_url('http://facebook.com/path1/p.php?k1=v1&k2=v2#Ref1', 'PROTOCOL')
        List<String> output7 = processor7
                .transform("http://facebook.com/path1/p.php?k1=v1&k2=v2#Ref1|PROTOCOL|k1|cloud|1", new HashMap<>());
        Assert.assertEquals(1, output7.size());
        Assert.assertEquals(output7.get(0), "result=http");

        String transformSql8 = "select parse_url(string1, string2) from source";
        TransformConfig config8 = new TransformConfig(transformSql8);
        TransformProcessor<String, String> processor8 = TransformProcessor
                .create(config8, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        // case8: parse_url('http://facebook.com/path1/p.php?k1=v1&k2=v2#Ref1', 'USERINFO')
        List<String> output8 = processor8
                .transform("http://facebook.com/path1/p.php?k1=v1&k2=v2#Ref1|USERINFO|k1|cloud|1", new HashMap<>());
        Assert.assertEquals(1, output8.size());
        Assert.assertEquals(output8.get(0), "result=");

        String transformSql9 = "select parse_url(string1, string2) from source";
        TransformConfig config9 = new TransformConfig(transformSql9);
        TransformProcessor<String, String> processor9 = TransformProcessor
                .create(config9, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        // case9: parse_url('https://user:password@www.example.com:8080/path/to/resource?name=value#section',
        // 'AUTHORITY')
        List<String> output9 = processor9.transform(
                "https://user:password@www.example.com:8080/path/to/resource?name=value#section|AUTHORITY|k1|cloud|1",
                new HashMap<>());
        Assert.assertEquals(1, output9.size());
        Assert.assertEquals(output9.get(0), "result=user:password@www.example.com:8080");
    }
}
