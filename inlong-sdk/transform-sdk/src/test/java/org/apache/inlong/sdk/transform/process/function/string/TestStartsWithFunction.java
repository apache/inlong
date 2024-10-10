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

package org.apache.inlong.sdk.transform.process.function.string;

import org.apache.inlong.sdk.transform.decode.SourceDecoderFactory;
import org.apache.inlong.sdk.transform.encode.SinkEncoderFactory;
import org.apache.inlong.sdk.transform.pojo.TransformConfig;
import org.apache.inlong.sdk.transform.process.TransformProcessor;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;

public class TestStartsWithFunction extends AbstractFunctionStringTestBase {

    @Test
    public void testStartsWithFunction() throws Exception {
        String transformSql1 = "select startswith(encode(string1, string2), encode(string3, string2)) from source";
        TransformConfig config1 = new TransformConfig(transformSql1);
        TransformProcessor<String, String> processor1 = TransformProcessor
                .create(config1, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));

        // case1: startswith(encode('Hello','UTF-8'), encode('lo','UTF-8'))
        List<String> output1 = processor1.transform("Hello|UTF-8|Hel|cloud|1", new HashMap<>());
        Assert.assertEquals(1, output1.size());
        Assert.assertEquals(output1.get(0), "result=true");

        // case2: startswith(encode('Hello','UTF-8'), encode('o','UTF-8'))
        List<String> output2 = processor1.transform("Hello|UTF-8|o|cloud|1", new HashMap<>());
        Assert.assertEquals(1, output2.size());
        Assert.assertEquals(output2.get(0), "result=false");

        // case3: startswith(encode('Hello','UTF-8'), encode('','UTF-8'))
        List<String> output3 = processor1.transform("Hello|UTF-8||cloud|1", new HashMap<>());
        Assert.assertEquals(1, output3.size());
        Assert.assertEquals(output3.get(0), "result=true");

        // below are tests for String params; while upper are tests for byte[] params
        String transformSql2 = "select startswith(string1, stringX) from source";
        TransformConfig config2 = new TransformConfig(transformSql2);
        TransformProcessor<String, String> processor2 = TransformProcessor
                .create(config2, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        // case4: startswith('Apache InLong', null)
        List<String> output4 = processor2.transform("Apache InLong|UTF-16BE|UTF-16BE|cloud|1", new HashMap<>());
        Assert.assertEquals(1, output4.size());
        Assert.assertEquals(output4.get(0), "result=");

        String transformSql3 = "select startswith(stringX, string2) from source";
        TransformConfig config3 = new TransformConfig(transformSql3);
        TransformProcessor<String, String> processor3 = TransformProcessor
                .create(config3, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        // case5: startswith(null, 'Apache InLong')
        List<String> output5 = processor2.transform("Apache InLong|Apache InLong|UTF-16BE|cloud|1", new HashMap<>());
        Assert.assertEquals(1, output4.size());
        Assert.assertEquals(output4.get(0), "result=");

        String transformSql4 = "select startswith(string1, string2) from source";
        TransformConfig config4 = new TransformConfig(transformSql4);
        TransformProcessor<String, String> processor4 = TransformProcessor
                .create(config4, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));

        // case6: startswith('Apache InLong', 'Apache InLong')
        List<String> output6 = processor4.transform("Apache InLong|Apache InLong|UtF-16|cloud|1", new HashMap<>());
        Assert.assertEquals(1, output6.size());
        Assert.assertEquals(output6.get(0), "result=true");

        // case7: startswith('Apache InLong', ' [')
        List<String> output7 = processor4.transform("Apache InLong| [|UTF-16--|cloud|1", new HashMap<>());
        Assert.assertEquals(1, output7.size());
        Assert.assertEquals(output7.get(0), "result=false");

        // case8: startswith('Apache InLong', 'A')
        List<String> output8 = processor4.transform("Apache InLong|A|UTf-16LE|cloud|1", new HashMap<>());
        Assert.assertEquals(1, output8.size());
        Assert.assertEquals(output8.get(0), "result=true");

        // case8: startswith('Apache InLong', '')
        List<String> output9 = processor4.transform("Apache InLong||UTf-16LE|cloud|1", new HashMap<>());
        Assert.assertEquals(1, output9.size());
        Assert.assertEquals(output9.get(0), "result=true");
    }
}
