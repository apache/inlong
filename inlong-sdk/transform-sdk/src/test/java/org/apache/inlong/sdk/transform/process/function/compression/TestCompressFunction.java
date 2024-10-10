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

package org.apache.inlong.sdk.transform.process.function.compression;

import org.apache.inlong.sdk.transform.decode.SourceDecoderFactory;
import org.apache.inlong.sdk.transform.encode.SinkEncoderFactory;
import org.apache.inlong.sdk.transform.pojo.TransformConfig;
import org.apache.inlong.sdk.transform.process.TransformProcessor;

import org.apache.inlong.sdk.transform.process.function.string.AbstractFunctionStringTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;

public class TestCompressFunction extends AbstractFunctionStringTestBase {

    @Test
    public void testCompressFunction() throws Exception {
        String transformSql = "select length(compress(replicate(string1,100)),'ISO_8859_1') from source";
        TransformConfig config = new TransformConfig(transformSql);
        TransformProcessor<String, String> processor1 = TransformProcessor
                .create(config, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        // case1: length(compress(replicate(string1,100)),'ISO_8859_1')
        List<String> output1 = processor1.transform("abcdefghijk|apple|cloud|2|1|3", new HashMap<>());
        Assert.assertEquals(1, output1.size());
        Assert.assertEquals("result=33", output1.get(0));

        transformSql = "select length(compress(string1),'ISO_8859_1') from source";
        config = new TransformConfig(transformSql);
        processor1 = TransformProcessor
                .create(config, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        // case2: length(compress(''),'ISO_8859_1')
        output1 = processor1.transform("|apple|cloud|2|1|3", new HashMap<>());
        Assert.assertEquals(1, output1.size());
        Assert.assertEquals("result=0", output1.get(0));

        transformSql = "select length(compress(xxd),'ISO_8859_1') from source";
        config = new TransformConfig(transformSql);
        processor1 = TransformProcessor
                .create(config, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        // case3: length(compress(null),'ISO_8859_1')
        output1 = processor1.transform("hello world|apple|cloud|2|1|3", new HashMap<>());
        Assert.assertEquals(1, output1.size());
        Assert.assertEquals("result=", output1.get(0));

        transformSql = "select length(compress(string1,string2),'ISO_8859_1') from source";
        config = new TransformConfig(transformSql);
        processor1 = TransformProcessor
                .create(config, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        // case4: length(compress('hello world','Gzip'),'ISO_8859_1')
        output1 = processor1.transform("hello world|Gzip|cloud|2|1|3", new HashMap<>());
        Assert.assertEquals(1, output1.size());
        Assert.assertEquals("result=35", output1.get(0));

        // case5: length(compress('hello world','zip'),'ISO_8859_1')
        output1 = processor1.transform("hello world|zip|cloud|2|1|3", new HashMap<>());
        Assert.assertEquals(1, output1.size());
        Assert.assertEquals("result=131", output1.get(0));

        // case5: length(compress('hello world','undefinedType'),'ISO_8859_1')
        output1 = processor1.transform("hello world|undefinedType|cloud|2|1|3", new HashMap<>());
        Assert.assertEquals(1, output1.size());
        Assert.assertEquals("result=", output1.get(0));
    }
}
