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

package org.apache.inlong.sdk.transform.process.function.arithmetic;

import org.apache.inlong.sdk.transform.decode.SourceDecoderFactory;
import org.apache.inlong.sdk.transform.encode.SinkEncoderFactory;
import org.apache.inlong.sdk.transform.pojo.TransformConfig;
import org.apache.inlong.sdk.transform.process.TransformProcessor;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Random;

public class TestRandIntegerFunction extends AbstractFunctionArithmeticTestBase {

    @Test
    public void testRandIntegerFunction() throws Exception {
        String transformSql1 = "select rand_integer(numeric1) from source";
        TransformConfig config1 = new TransformConfig(transformSql1);
        TransformProcessor<String, String> processor1 = TransformProcessor
                .create(config1, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        // case: rand_integer(10)
        List<String> output1 = processor1.transform("10|4|6|8", new HashMap<>());
        Assert.assertEquals(1, output1.size());
        int res1 = Integer.parseInt(output1.get(0).substring(7));
        Assert.assertTrue(res1 >= 0 && res1 < 10);
        // case: rand_integer(89)
        List<String> output2 = processor1.transform("89|4|6|8", new HashMap<>());
        Assert.assertEquals(1, output2.size());
        int res2 = Integer.parseInt(output2.get(0).substring(7));
        Assert.assertTrue(res2 >= 0 && res2 < 89);
        String transformSql2 = "select rand_integer(numeric1,numeric2) from source";
        TransformConfig config2 = new TransformConfig(transformSql2);
        TransformProcessor<String, String> processor2 = TransformProcessor
                .create(config2, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        // case: rand_integer(88, 89)
        List<String> output3 = processor2.transform("88|89||", new HashMap<>());
        Assert.assertEquals(1, output3.size());
        Assert.assertEquals(output3.get(0), "result=" + new Random(89).nextInt(88));
    }
}
