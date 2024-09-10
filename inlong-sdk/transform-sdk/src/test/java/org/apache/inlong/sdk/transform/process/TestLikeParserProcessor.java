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
 * TestLikeParserProcessor
 * description: test LikeParser in transform processor
 */
public class TestLikeParserProcessor {

    private static final List<FieldInfo> srcFields = new ArrayList<>();
    private static final List<FieldInfo> dstFields = new ArrayList<>();
    private static final CsvSourceInfo csvSource;
    private static final KvSinkInfo kvSink;

    static {
        for (int i = 1; i < 3; i++) {
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
    public void testLikeFunction() throws Exception {
        String transformSql = null, data = null;
        TransformConfig config = null;
        TransformProcessor<String, String> processor = null;
        List<String> output = null;

        transformSql = "select string1 like string2 from source";
        config = new TransformConfig(transformSql);
        processor = TransformProcessor
                .create(config, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        // case1: apple like %App%
        output = processor.transform("apple|%App%", new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=true", output.get(0));

        // case2: apple like /%App%
        // The reason why '\' is not used as an escape string here is that when processing CSV data,
        // the quote parameter defaults to the '\' character
        transformSql = "select string1 like string2 ESCAPE '/' from source";
        config = new TransformConfig(transformSql);
        processor = TransformProcessor
                .create(config, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));

        output = processor.transform("apple|/%App%", new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=false", output.get(0));

        // case3: %apple like /%App% ESCAPE '/'
        output = processor.transform("%apple|/%App%", new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=true", output.get(0));

        // case4: %apple like /%Apple_ ESCAPE '/'
        output = processor.transform("%apple|/%Apple_", new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=false", output.get(0));

        // case5: %apple like /%Appl_ ESCAPE '/'
        output = processor.transform("%apple|/%Appl_", new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=true", output.get(0));

        // case6: %ap_ple like /%Ap%_e ESCAPE '/'
        output = processor.transform("%ap_ple|/%Ap%_e", new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=true", output.get(0));

        // case7: %ap_ple/ like /%Ap%_e/ ESCAPE '/'
        output = processor.transform("%ap_ple/|/%Ap%_e/", new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=true", output.get(0));
    }
    @Test
    public void testNotLikeFunction() throws Exception {
        String transformSql = null, data = null;
        TransformConfig config = null;
        TransformProcessor<String, String> processor = null;
        List<String> output = null;

        transformSql = "select string1 not like string2 from source";
        config = new TransformConfig(transformSql);
        processor = TransformProcessor
                .create(config, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        // case1: apple not like %App%
        output = processor.transform("apple|%App%", new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=false", output.get(0));

        // case2: apple not like /%App%
        // The reason why '\' is not used as an escape string here is that when processing CSV data,
        // the quote parameter defaults to the '\' character
        transformSql = "select string1 not like string2 ESCAPE '/' from source";
        config = new TransformConfig(transformSql);
        processor = TransformProcessor
                .create(config, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));

        output = processor.transform("apple|/%App%", new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=true", output.get(0));

        // case3: %apple not like /%App% ESCAPE '/'
        output = processor.transform("%apple|/%App%", new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=false", output.get(0));

        // case4: %apple not like /%Apple_ ESCAPE '/'
        output = processor.transform("%apple|/%Apple_", new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=true", output.get(0));

        // case5: %apple not like /%Appl_ ESCAPE '/'
        output = processor.transform("%apple|/%Appl_", new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=false", output.get(0));

        // case6: %ap_ple not like /%Ap%_e ESCAPE '/'
        output = processor.transform("%ap_ple|/%Ap%_e", new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=false", output.get(0));

        // case7: %ap_ple/ not like /%Ap%_e/ ESCAPE '/'
        output = processor.transform("%ap_ple/|/%Ap%_e/", new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=false", output.get(0));
    }

}
