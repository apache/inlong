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

public class TestUnCompressFunction extends AbstractFunctionStringTestBase {

    @Test
    public void testUnCompressFunction() throws Exception {
        String transformSql = "select length(uncompress(compress(replicate(string1,100)))) from source";
        TransformConfig config = new TransformConfig(transformSql);
        TransformProcessor<String, String> processor1 = TransformProcessor
                .create(config, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        // case1: length(uncompress(compress(repeat(string1,100))))
        List<String> output1 = processor1.transform("abcdefghijk|apple|cloud|2|1|3", new HashMap<>());
        Assert.assertEquals(1, output1.size());
        Assert.assertEquals("result=1100", output1.get(0));

        transformSql = "select uncompress(compress(replicate(string1,10))) from source";
        config = new TransformConfig(transformSql);
        processor1 = TransformProcessor
                .create(config, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        // case2: uncompress(compress(repeat('h',10)))
        output1 = processor1.transform("h|apple|cloud|2|1|3", new HashMap<>());
        Assert.assertEquals(1, output1.size());
        Assert.assertEquals("result=hhhhhhhhhh", output1.get(0));

        transformSql = "select uncompress(compress(xxd)) from source";
        config = new TransformConfig(transformSql);
        processor1 = TransformProcessor
                .create(config, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        // case3: uncompress(compress(null))
        output1 = processor1.transform("h|apple|cloud|2|1|3", new HashMap<>());
        Assert.assertEquals(1, output1.size());
        Assert.assertEquals("result=", output1.get(0));

        transformSql = "select uncompress(compress(string1)) from source";
        config = new TransformConfig(transformSql);
        processor1 = TransformProcessor
                .create(config, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        // case4: uncompress(compress(''))
        output1 = processor1.transform("|apple|cloud|2|1|3", new HashMap<>());
        Assert.assertEquals(1, output1.size());
        Assert.assertEquals("result=", output1.get(0));

        transformSql = "select uncompress(compress(string1,string2),string3) from source";
        config = new TransformConfig(transformSql);
        processor1 = TransformProcessor
                .create(config, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        // case5: uncompress(compress('hello world','Gzip'),'Gzip')
        output1 = processor1.transform("hello world|Gzip|Gzip|2|1|3", new HashMap<>());
        Assert.assertEquals(1, output1.size());
        Assert.assertEquals("result=hello world", output1.get(0));

        // case6: uncompress(compress('hello world','zip'),'zip')
        output1 = processor1.transform("hello world|zip|zip|2|1|3", new HashMap<>());
        Assert.assertEquals(1, output1.size());
        Assert.assertEquals("result=hello world", output1.get(0));

        transformSql = "select uncompress(compress(stringx,string2),string3) from source";
        config = new TransformConfig(transformSql);
        processor1 = TransformProcessor
                .create(config, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        // case7: uncompress(compress(null,'Gzip'),'Gzip')
        output1 = processor1.transform("hello world|Gzip|Gzip|2|1|3", new HashMap<>());
        Assert.assertEquals(1, output1.size());
        Assert.assertEquals("result=", output1.get(0));

        // case8: uncompress(compress(null,'zip'),'zip')
        output1 = processor1.transform("hello world|zip|zip|2|1|3", new HashMap<>());
        Assert.assertEquals(1, output1.size());
        Assert.assertEquals("result=", output1.get(0));

        // case8: uncompress(compress(null,'zip'),'undefinedType')
        output1 = processor1.transform("hello world|zip|undefinedType|2|1|3", new HashMap<>());
        Assert.assertEquals(1, output1.size());
        Assert.assertEquals("result=", output1.get(0));
    }
}
