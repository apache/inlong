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

public class TestRepeatFunction extends AbstractFunctionStringTestBase {

    @Test
    public void testRepeatFunction() throws Exception {
        String transformSql1 = "select repeat(string1, numeric1) from source";
        TransformConfig config1 = new TransformConfig(transformSql1);
        TransformProcessor<String, String> processor1 = TransformProcessor
                .create(config1, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        // case1: repeat('apple', 2)
        List<String> output1 = processor1.transform("apple|banana|cloud|2|1|3", new HashMap<>());
        Assert.assertEquals(1, output1.size());
        Assert.assertEquals(output1.get(0), "result=appleapple");
        String transformSql2 = "select repeat(string2, numeric2) from source";
        TransformConfig config2 = new TransformConfig(transformSql2);
        TransformProcessor<String, String> processor2 = TransformProcessor
                .create(config2, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        // case2: repeat('banana', 3)
        List<String> output2 = processor2.transform("apple|banana|cloud|1|3|3", new HashMap<>());
        Assert.assertEquals(1, output2.size());
        Assert.assertEquals(output2.get(0), "result=bananabananabanana");
        // case3: repeat('banana', 1)
        List<String> output3 = processor2.transform("apple|banana|cloud|1|1|3", new HashMap<>());
        Assert.assertEquals(1, output3.size());
        Assert.assertEquals(output3.get(0), "result=banana");
        // case4: repeat('', 1)
        List<String> output4 = processor2.transform("apple||cloud|1|1|3", new HashMap<>());
        Assert.assertEquals(1, output4.size());
        Assert.assertEquals(output4.get(0), "result=");
        // case5: repeat('banana',)
        List<String> output5 = processor2.transform("apple|banana|cloud|1||3", new HashMap<>());
        Assert.assertEquals(1, output5.size());
        Assert.assertEquals(output5.get(0), "result=");
        // case6: repeat('banana',-2)
        List<String> output6 = processor2.transform("apple|banana|cloud|1|-2|3", new HashMap<>());
        Assert.assertEquals(1, output6.size());
        Assert.assertEquals(output6.get(0), "result=");
        String transformSql3 = "select replicate(string3, numeric3) from source";
        TransformConfig config3 = new TransformConfig(transformSql3);
        TransformProcessor<String, String> processor3 = TransformProcessor
                .create(config3, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        // case7: replicate('cloud', 0)
        List<String> output7 = processor3.transform("apple|banana|cloud|2|1|0", new HashMap<>());
        Assert.assertEquals(1, output7.size());
        Assert.assertEquals(output7.get(0), "result=");
        // case8: replicate('banana', 2)
        List<String> output8 = processor2.transform("apple|cloud|banana|1|2|3", new HashMap<>());
        Assert.assertEquals(1, output8.size());
        Assert.assertEquals(output8.get(0), "result=cloudcloud");
    }
}
