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
    public void testLowerFunction() throws Exception {
        String transformSql1 = "select lower(string1) from source";
        TransformConfig config1 = new TransformConfig(transformSql1);
        TransformProcessor<String, String> processor1 = TransformProcessor
                .create(config1, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        // case1: lower("ApPlE")
        List<String> output1 = processor1.transform("ApPlE|banana|cloud|2|1|3", new HashMap<>());
        Assert.assertEquals(1, output1.size());
        Assert.assertEquals(output1.get(0), "result=apple");

        // case2: lower("")
        List<String> output2 = processor1.transform("|banana|cloud|2|1|3", new HashMap<>());
        Assert.assertEquals(1, output2.size());
        Assert.assertEquals(output2.get(0), "result=");

        // case3: lower(null)
        String transformSql2 = "select lower(xxd) from source";
        TransformConfig config2 = new TransformConfig(transformSql2);
        TransformProcessor<String, String> processor2 = TransformProcessor
                .create(config2, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        List<String> output3 = processor2.transform("ApPlE|banana|cloud|2|1|3", new HashMap<>());
        Assert.assertEquals(1, output3.size());
        Assert.assertEquals(output3.get(0), "result=null");
    }

    @Test
    public void testUpperFunction() throws Exception {
        String transformSql1 = "select upper(string1) from source";
        TransformConfig config1 = new TransformConfig(transformSql1);
        TransformProcessor<String, String> processor1 = TransformProcessor
                .create(config1, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        // case1: upper("ApPlE")
        List<String> output1 = processor1.transform("ApPlE|banana|cloud|2|1|3", new HashMap<>());
        Assert.assertEquals(1, output1.size());
        Assert.assertEquals(output1.get(0), "result=APPLE");

        // case2: upper("")
        List<String> output2 = processor1.transform("|banana|cloud|2|1|3", new HashMap<>());
        Assert.assertEquals(1, output2.size());
        Assert.assertEquals(output2.get(0), "result=");

        // case3: upper(null)
        String transformSql2 = "select upper(xxd) from source";
        TransformConfig config2 = new TransformConfig(transformSql2);
        TransformProcessor<String, String> processor2 = TransformProcessor
                .create(config2, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        List<String> output3 = processor2.transform("ApPlE|banana|cloud|2|1|3", new HashMap<>());
        Assert.assertEquals(1, output3.size());
        Assert.assertEquals(output3.get(0), "result=null");
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
    public void testSpaceFunction() throws Exception {
        String transformSql = null, data = null;
        TransformConfig config = null;
        TransformProcessor<String, String> processor = null;
        List<String> output = null;

        transformSql = "select space(numeric1) from source";
        config = new TransformConfig(transformSql);
        processor = TransformProcessor
                .create(config, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        // case1: space(5)
        data = "hello world|banana|cloud|5|3|3";
        output = processor.transform(data, new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=     ", output.get(0));

        // case2: space(-1)
        data = "hello world|banana|cloud|-1|3|3";
        output = processor.transform(data, new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=", output.get(0));

        // case3: space(null)
        transformSql = "select space(xxd) from source";
        config = new TransformConfig(transformSql);
        processor = TransformProcessor
                .create(config, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        data = "hello world|banana|cloud|5|3|3";
        output = processor.transform(data, new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=null", output.get(0));

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
    public void testReverseFunction() throws Exception {
        String transformSql1 = "select reverse(string1) from source";
        TransformConfig config1 = new TransformConfig(transformSql1);
        TransformProcessor<String, String> processor1 = TransformProcessor
                .create(config1, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        // case1: reverse('apple')
        List<String> output1 = processor1.transform("apple|banana|cloud|2|1|3", new HashMap<>());
        Assert.assertEquals(1, output1.size());
        Assert.assertEquals(output1.get(0), "result=elppa");
        // case2: reverse('ban ana ')
        String transformSql2 = "select reverse(string2) from source";
        TransformConfig config2 = new TransformConfig(transformSql2);
        TransformProcessor<String, String> processor2 = TransformProcessor
                .create(config2, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        List<String> output2 = processor2.transform("apple|ban ana |cloud|2|1|3", new HashMap<>());
        Assert.assertEquals(1, output2.size());
        Assert.assertEquals(output2.get(0), "result= ana nab");
        // case3: reverse(12345)
        List<String> output3 = processor1.transform("12345|banana|cloud|2|1|3", new HashMap<>());
        Assert.assertEquals(1, output3.size());
        Assert.assertEquals(output3.get(0), "result=54321");
        // case4: reverse(null)
        List<String> output4 = processor1.transform("|banana|cloud|2|1|3", new HashMap<>());
        Assert.assertEquals(1, output4.size());
        Assert.assertEquals(output4.get(0), "result=");
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

    @Test
    public void testLengthFunction() throws Exception {
        String transformSql = "select length(string1) from source";
        TransformConfig config = new TransformConfig(transformSql);
        TransformProcessor<String, String> processor1 = TransformProcessor
                .create(config, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        // case1: length('hello world')
        List<String> output1 = processor1.transform("hello world|apple|cloud|2|1|3", new HashMap<>());
        Assert.assertEquals(1, output1.size());
        Assert.assertEquals("result=11", output1.get(0));

        transformSql = "select length(xxd) from source";
        config = new TransformConfig(transformSql);
        processor1 = TransformProcessor
                .create(config, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        // case2: length(null)
        output1 = processor1.transform("hello world|apple|cloud|2|1|3", new HashMap<>());
        Assert.assertEquals(1, output1.size());
        Assert.assertEquals("result=null", output1.get(0));
    }

    @Test
    public void testReplaceFunction() throws Exception {
        String transformSql = "select replace(string1, string2, string3) from source";
        TransformConfig config = new TransformConfig(transformSql);
        TransformProcessor<String, String> processor = TransformProcessor
                .create(config, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        // case1: replace('hooray', 'oray', 'lly')
        List<String> output1 = processor.transform("hooray|oray|lly", new HashMap<>());
        Assert.assertEquals(1, output1.size());
        Assert.assertEquals(output1.get(0), "result=holly");
        // case2: replace('hooray', 'hook', 'hoor')
        List<String> output2 = processor.transform("hooray|hook|hoor", new HashMap<>());
        Assert.assertEquals(1, output2.size());
        Assert.assertEquals(output2.get(0), "result=hooray");
        // case3: replace('Hello World', 'World', '')
        List<String> output3 = processor.transform("Hello World|World|", new HashMap<>());
        Assert.assertEquals(1, output3.size());
        Assert.assertEquals(output3.get(0), "result=Hello ");
        // case4: replace('Hello World', '', 'J')
        List<String> output4 = processor.transform("Hello World||J", new HashMap<>());
        Assert.assertEquals(1, output4.size());
        Assert.assertEquals(output4.get(0), "result=JHJeJlJlJoJ JWJoJrJlJdJ");
        // case5: replace('', '', '')
        List<String> output5 = processor.transform("||", new HashMap<>());
        Assert.assertEquals(1, output5.size());
        Assert.assertEquals(output5.get(0), "result=");
        // case6: replace('abababab', 'ab', 'cd')
        List<String> output6 = processor.transform("abababab|ab|cd", new HashMap<>());
        Assert.assertEquals(1, output6.size());
        Assert.assertEquals(output6.get(0), "result=cdcdcdcd");
        // case7: replace('aaa', 'aa', 'd')
        List<String> output7 = processor.transform("aaa|aa|d", new HashMap<>());
        Assert.assertEquals(1, output7.size());
        Assert.assertEquals(output7.get(0), "result=da");
    }

    @Test
    public void testStrcmpFunction() throws Exception {
        String transformSql = null, data = null;
        TransformConfig config = null;
        TransformProcessor<String, String> processor = null;
        List<String> output = null;

        transformSql = "select strcmp(string1,string2) from source";
        config = new TransformConfig(transformSql);
        processor = TransformProcessor
                .create(config, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        // case1: strcmp('hello world','banana')
        data = "hello world|banana|cloud|5|3|3";
        output = processor.transform(data, new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=1", output.get(0));

        // case2: strcmp('hello world','hello world')
        data = "hello world|hello world|cloud|5|3|3";
        output = processor.transform(data, new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=0", output.get(0));

        // case3: strcmp('hello world','zzzzz')
        data = "hello world|zzzzz|cloud|5|3|3";
        output = processor.transform(data, new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=-1", output.get(0));

        // case4: strcmp('hello world',null)
        transformSql = "select strcmp(string1,xxd) from source";
        config = new TransformConfig(transformSql);
        processor = TransformProcessor
                .create(config, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        data = "hello world|zzzzz|cloud|5|3|3";
        output = processor.transform(data, new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=null", output.get(0));
    }

    @Test
    public void testRpadFunction() throws Exception {
        String transformSql = null, data = null;
        TransformConfig config = null;
        TransformProcessor<String, String> processor = null;
        List<String> output = null;

        transformSql = "select rpad(string1,numeric1,string2) from source";
        config = new TransformConfig(transformSql);
        processor = TransformProcessor
                .create(config, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        // case1: rpad('he',7,'xxd')
        data = "he|xxd|cloud|7|3|3";
        output = processor.transform(data, new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=hexxdxx", output.get(0));

        // case2: rpad('he',1,'xxd')
        data = "he|xxd|cloud|1|3|3";
        output = processor.transform(data, new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=h", output.get(0));

        // case3: rpad('he',1,'')
        data = "he||cloud|1|3|3";
        output = processor.transform(data, new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=h", output.get(0));

        // case4: rpad('he',-1,'xxd')
        data = "he|xxd|cloud|-1|3|3";
        output = processor.transform(data, new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=null", output.get(0));

        // case5: rpad(null,5,'xxd')
        transformSql = "select rpad(xxd,numeric1,string2) from source";
        config = new TransformConfig(transformSql);
        processor = TransformProcessor
                .create(config, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        data = "he|xxd|cloud|5|3|3";
        output = processor.transform(data, new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=null", output.get(0));

        // case6: rpad('he',null,'xxd')
        transformSql = "select rpad(string1,xxd,string2) from source";
        config = new TransformConfig(transformSql);
        processor = TransformProcessor
                .create(config, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        data = "he|xxd|cloud|5|3|3";
        output = processor.transform(data, new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=null", output.get(0));

        // case7: rpad('he',5,null)
        transformSql = "select rpad(string1,numeric1,xxd) from source";
        config = new TransformConfig(transformSql);
        processor = TransformProcessor
                .create(config, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        data = "he|xxd|cloud|5|3|3";
        output = processor.transform(data, new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=null", output.get(0));
    }

    @Test
    public void testLpadFunction() throws Exception {
        String transformSql = null, data = null;
        TransformConfig config = null;
        TransformProcessor<String, String> processor = null;
        List<String> output = null;

        transformSql = "select lpad(string1,numeric1,string2) from source";
        config = new TransformConfig(transformSql);
        processor = TransformProcessor
                .create(config, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        // case1: lpad('he',7,'xxd')
        data = "he|xxd|cloud|7|3|3";
        output = processor.transform(data, new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=xxdxxhe", output.get(0));

        // case2: lpad('he',1,'xxd')
        data = "he|xxd|cloud|1|3|3";
        output = processor.transform(data, new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=h", output.get(0));

        // case3: lpad('he',1,'')
        data = "he||cloud|1|3|3";
        output = processor.transform(data, new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=h", output.get(0));

        // case4: lpad('he',-1,'xxd')
        data = "he|xxd|cloud|-1|3|3";
        output = processor.transform(data, new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=null", output.get(0));

        // case5: lpad(null,5,'xxd')
        transformSql = "select lpad(xxd,numeric1,string2) from source";
        config = new TransformConfig(transformSql);
        processor = TransformProcessor
                .create(config, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        data = "he|xxd|cloud|5|3|3";
        output = processor.transform(data, new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=null", output.get(0));

        // case6: lpad('he',null,'xxd')
        transformSql = "select lpad(string1,xxd,string2) from source";
        config = new TransformConfig(transformSql);
        processor = TransformProcessor
                .create(config, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        data = "he|xxd|cloud|5|3|3";
        output = processor.transform(data, new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=null", output.get(0));

        // case7: lpad('he',5,null)
        transformSql = "select lpad(string1,numeric1,xxd) from source";
        config = new TransformConfig(transformSql);
        processor = TransformProcessor
                .create(config, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        data = "he|xxd|cloud|5|3|3";
        output = processor.transform(data, new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=null", output.get(0));
    }

    @Test
    public void testRightFunction() throws Exception {
        String transformSql = "select right(string1,numeric1) from source";
        TransformConfig config = new TransformConfig(transformSql);
        TransformProcessor<String, String> processor1 = TransformProcessor
                .create(config, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        // case1: right('hello world',5)
        String data = "hello world|banana|cloud|5|3|3";
        List<String> output1 = processor1.transform(data, new HashMap<>());
        Assert.assertEquals(1, output1.size());
        Assert.assertEquals("result=world", output1.get(0));

        // case2: right('hello world',-15)
        data = "hello world|banana|cloud|-15|3|3";
        output1 = processor1.transform(data, new HashMap<>());
        Assert.assertEquals(1, output1.size());
        Assert.assertEquals("result=", output1.get(0));

        // case3: right('hello world',100)
        data = "hello world|banana|cloud|100|3|3";
        output1 = processor1.transform(data, new HashMap<>());
        Assert.assertEquals(1, output1.size());
        Assert.assertEquals("result=hello world", output1.get(0));

        // case4: right(null,5)
        transformSql = "select right(xxd,numeric1) from source";
        config = new TransformConfig(transformSql);
        processor1 = TransformProcessor
                .create(config, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        data = "hello world|banana|cloud|5|3|3";
        output1 = processor1.transform(data, new HashMap<>());
        Assert.assertEquals(1, output1.size());
        Assert.assertEquals("result=null", output1.get(0));

        // case5: right('hello world',null)
        transformSql = "select right(string1,xxd) from source";
        config = new TransformConfig(transformSql);
        processor1 = TransformProcessor
                .create(config, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        data = "hello world|banana|cloud|5|3|3";
        output1 = processor1.transform(data, new HashMap<>());
        Assert.assertEquals(1, output1.size());
        Assert.assertEquals("result=null", output1.get(0));
    }

    @Test
    public void testLeftFunction() throws Exception {
        String transformSql = "select left(string1,numeric1) from source";
        TransformConfig config = new TransformConfig(transformSql);
        TransformProcessor<String, String> processor1 = TransformProcessor
                .create(config, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        // case1: left('hello world',5)
        String data = "hello world|banana|cloud|5|3|3";
        List<String> output1 = processor1.transform(data, new HashMap<>());
        Assert.assertEquals(1, output1.size());
        Assert.assertEquals("result=hello", output1.get(0));

        // case2: left('hello world',-15)
        data = "hello world|banana|cloud|-15|3|3";
        output1 = processor1.transform(data, new HashMap<>());
        Assert.assertEquals(1, output1.size());
        Assert.assertEquals("result=", output1.get(0));

        // case3: left('hello world',100)
        data = "hello world|banana|cloud|100|3|3";
        output1 = processor1.transform(data, new HashMap<>());
        Assert.assertEquals(1, output1.size());
        Assert.assertEquals("result=hello world", output1.get(0));

        // case4: left(null,5)
        transformSql = "select left(xxd,numeric1) from source";
        config = new TransformConfig(transformSql);
        processor1 = TransformProcessor
                .create(config, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        data = "hello world|banana|cloud|5|3|3";
        output1 = processor1.transform(data, new HashMap<>());
        Assert.assertEquals(1, output1.size());
        Assert.assertEquals("result=null", output1.get(0));

        // case5: left('hello world',null)
        transformSql = "select left(string1,xxd) from source";
        config = new TransformConfig(transformSql);
        processor1 = TransformProcessor
                .create(config, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        data = "hello world|banana|cloud|5|3|3";
        output1 = processor1.transform(data, new HashMap<>());
        Assert.assertEquals(1, output1.size());
        Assert.assertEquals("result=null", output1.get(0));
    }

    @Test
    public void testTranslateFunction() throws Exception {
        String transformSql1 = "select translate(string1, string2, string3) from source";
        TransformConfig config1 = new TransformConfig(transformSql1);
        TransformProcessor<String, String> processor1 = TransformProcessor
                .create(config1, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        // case1: translate("hello word!", "el", "EL")
        List<String> output1 = processor1.transform("hello word!|el|EL|2|1|3", new HashMap<>());
        Assert.assertEquals(1, output1.size());
        Assert.assertEquals(output1.get(0), "result=hELLo word!");
        String transformSql2 = "select translate(string3, string1, string2) from source";
        TransformConfig config2 = new TransformConfig(transformSql2);
        TransformProcessor<String, String> processor2 = TransformProcessor
                .create(config2, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        // case2: translate("hello word!", "el", "EL")
        List<String> output2 = processor2.transform("el|EL|hello word!|1|1|3", new HashMap<>());
        Assert.assertEquals(1, output2.size());
        Assert.assertEquals(output2.get(0), "result=hELLo word!");
        // case3: translate('ApaCHe Inlong', CH, ch)
        List<String> output3 = processor2.transform("CH|ch|ApaCHe Inlong|2|1|9", new HashMap<>());
        Assert.assertEquals(1, output3.size());
        Assert.assertEquals(output3.get(0), "result=Apache Inlong");
    }

}
