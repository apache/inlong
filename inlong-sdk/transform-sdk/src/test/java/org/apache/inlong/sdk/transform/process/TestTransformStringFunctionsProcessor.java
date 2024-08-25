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
 * TestTransformStringFunctionsProcessor
 * description: test the string functions in transform processor
 */
public class TestTransformStringFunctionsProcessor {

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
        for (int i = 1; i < 4; i++) {
            FieldInfo field = new FieldInfo();
            field.setName("numeric" + i);
            srcFields.add(field);
        }
        FieldInfo field = new FieldInfo();
        field.setName("result");
        dstFields.add(field);
        csvSource = new CsvSourceInfo("UTF-8", '|', '\\', srcFields);
        kvSink = new KvSinkInfo("UTF-8", dstFields);
    }

    @Test
    public void testSubstringFunction() throws Exception {
        String transformSql1 = "select substring(string2, numeric1) from source";
        TransformConfig config1 = new TransformConfig(transformSql1);
        TransformProcessor<String, String> processor1 = TransformProcessor
                .create(config1, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        // case1: substring('banana', 2)
        List<String> output1 = processor1.transform("apple|banana|cloud|2|1|3", new HashMap<>());
        Assert.assertEquals(1, output1.size());
        Assert.assertEquals(output1.get(0), "result=anana");
        String transformSql2 = "select substring(string1, numeric1, numeric3) from source";
        TransformConfig config2 = new TransformConfig(transformSql2);
        TransformProcessor<String, String> processor2 = TransformProcessor
                .create(config2, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        // case2: substring('apple', 1, 3)
        List<String> output2 = processor2.transform("apple|banana|cloud|1|1|3", new HashMap<>());
        Assert.assertEquals(1, output2.size());
        Assert.assertEquals(output2.get(0), "result=app");
        // case3: substring('apple', 2, 9)
        List<String> output3 = processor2.transform("apple|banana|cloud|2|1|9", new HashMap<>());
        Assert.assertEquals(1, output3.size());
        Assert.assertEquals(output3.get(0), "result=pple");
    }

    @Test
    public void testLocateFunction() throws Exception {
        String transformSql1 = "select locate(string1, string2) from source";
        TransformConfig config1 = new TransformConfig(transformSql1);
        TransformProcessor<String, String> processor1 = TransformProcessor
                .create(config1, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        // case1: locate('app', 'apple')
        List<String> output1 = processor1.transform("app|apple|cloud|2|1|3", new HashMap<>());
        Assert.assertEquals(1, output1.size());
        Assert.assertEquals(output1.get(0), "result=1");
        // case2: locate('ape', 'apple')
        List<String> output2 = processor1.transform("ape|apple|cloud|2|1|3", new HashMap<>());
        Assert.assertEquals(1, output2.size());
        Assert.assertEquals(output2.get(0), "result=0");
        String transformSql2 = "select locate(string1, string2, numeric1) from source";
        TransformConfig config2 = new TransformConfig(transformSql2);
        TransformProcessor<String, String> processor2 = TransformProcessor
                .create(config2, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        // case3: locate('app', 'appapp', 2)
        List<String> output3 = processor2.transform("app|appapp|cloud|2|1|3", new HashMap<>());
        Assert.assertEquals(1, output3.size());
        Assert.assertEquals(output3.get(0), "result=4");
        // case4: locate('app', 'appape', 2)
        List<String> output4 = processor2.transform("app|appape|cloud|2|1|9", new HashMap<>());
        Assert.assertEquals(1, output4.size());
        Assert.assertEquals(output4.get(0), "result=0");
        // case5: locate('app', null)
        List<String> output5 = processor1.transform("app", new HashMap<>());
        Assert.assertEquals(1, output5.size());
        Assert.assertEquals(output5.get(0), "result=null");
    }
    @Test
    public void testReplicateFunction() throws Exception {
        String transformSql1 = "select replicate(string1, numeric1) from source";
        TransformConfig config1 = new TransformConfig(transformSql1);
        TransformProcessor<String, String> processor1 = TransformProcessor
                .create(config1, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        // case1: replicate('apple', 2)
        List<String> output1 = processor1.transform("apple|banana|cloud|2|1|3", new HashMap<>());
        Assert.assertEquals(1, output1.size());
        Assert.assertEquals(output1.get(0), "result=appleapple");
        String transformSql2 = "select replicate(string2, numeric2) from source";
        TransformConfig config2 = new TransformConfig(transformSql2);
        TransformProcessor<String, String> processor2 = TransformProcessor
                .create(config2, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        // case2: replicate('banana', 3)
        List<String> output2 = processor2.transform("apple|banana|cloud|1|3|3", new HashMap<>());
        Assert.assertEquals(1, output2.size());
        Assert.assertEquals(output2.get(0), "result=bananabananabanana");
        // case3: replicate('banana', 1)
        List<String> output3 = processor2.transform("apple|banana|cloud|1|1|3", new HashMap<>());
        Assert.assertEquals(1, output2.size());
        Assert.assertEquals(output3.get(0), "result=banana");
        // case3: replicate('cloud', 0)
        String transformSql3 = "select replicate(string3, numeric3) from source";
        TransformConfig config3 = new TransformConfig(transformSql3);
        TransformProcessor<String, String> processor3 = TransformProcessor
                .create(config3, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        List<String> output4 = processor3.transform("apple|banana|cloud|2|1|0", new HashMap<>());
        Assert.assertEquals(1, output4.size());
        Assert.assertEquals(output4.get(0), "result=");
    }

    @Test
    public void testTrimFunction() throws Exception {
        String transformSql1 = "select trim(string1) from source";
        TransformConfig config1 = new TransformConfig(transformSql1);
        TransformProcessor<String, String> processor1 = TransformProcessor
                .create(config1, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        // case1: trim(' in long')
        List<String> output1 = processor1.transform(" in long|in long | in long ", new HashMap<>());
        Assert.assertEquals(1, output1.size());
        Assert.assertEquals(output1.get(0), "result=in long");
        String transformSql2 = "select trim(string2) from source";
        TransformConfig config2 = new TransformConfig(transformSql2);
        TransformProcessor<String, String> processor2 = TransformProcessor
                .create(config2, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        // case2: trim('in long ')
        List<String> output2 = processor2.transform(" in long|in long | in long ", new HashMap<>());
        Assert.assertEquals(1, output2.size());
        Assert.assertEquals(output2.get(0), "result=in long");
        String transformSql3 = "select trim(string2) from source";
        TransformConfig config3 = new TransformConfig(transformSql2);
        TransformProcessor<String, String> processor3 = TransformProcessor
                .create(config2, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        // case3: trim(' in long ')
        List<String> output3 = processor3.transform(" in long|in long | in long ", new HashMap<>());
        Assert.assertEquals(1, output3.size());
        Assert.assertEquals(output3.get(0), "result=in long");
    }

    @Test
    public void testToBase64Function() throws Exception {
        String transformSql = "select to_base64(string1) from source";
        TransformConfig config = new TransformConfig(transformSql);
        TransformProcessor<String, String> processor1 = TransformProcessor
                .create(config, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        // to_base64('app-fun')
        List<String> output1 = processor1.transform("app-fun", new HashMap<>());
        Assert.assertEquals(1, output1.size());
        Assert.assertEquals(output1.get(0), "result=YXBwLWZ1bg==");
        // to_base64('hello world')
        List<String> output2 = processor1.transform("hello world", new HashMap<>());
        Assert.assertEquals(1, output2.size());
        Assert.assertEquals(output2.get(0), "result=aGVsbG8gd29ybGQ=");
    }
}
