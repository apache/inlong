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

import java.util.List;

public class TestTruncateFunction extends AbstractFunctionArithmeticTestBase {

    @Test
    public void testTruncateFunction() throws Exception {
        String transformSql1 = "select truncate(numeric1,numeric2) from source";
        TransformConfig config1 = new TransformConfig(transformSql1);
        TransformProcessor<String, String> processor1 = TransformProcessor
                .create(config1, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));

        // case1: truncate(42.324, 2)
        List<String> output1 = processor1.transform("42.324|2|6|8");
        Assert.assertEquals(1, output1.size());
        Assert.assertEquals(output1.get(0), "result=42.32");

        // case2: truncate(42.324, -1)
        List<String> output2 = processor1.transform("42.324|-1|6|8");
        Assert.assertEquals(1, output2.size());
        Assert.assertEquals(output2.get(0), "result=40");

        // case3: truncate(12345.6789, -3)
        List<String> output3 = processor1.transform("12345.6789|-3|6|8");
        Assert.assertEquals(1, output3.size());
        Assert.assertEquals(output3.get(0), "result=12000");

        String transformSql2 = "select trunc(numeric1,numeric2) from source";
        TransformConfig config2 = new TransformConfig(transformSql2);
        TransformProcessor<String, String> processor2 = TransformProcessor
                .create(config2, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));

        // case1: trunc(42.324, 2)
        List<String> output4 = processor2.transform("42.324|2|6|8");
        Assert.assertEquals(1, output4.size());
        Assert.assertEquals(output4.get(0), "result=42.32");

        // case2: trunc(42.324, -1)
        List<String> output5 = processor2.transform("42.324|-1|6|8");
        Assert.assertEquals(1, output5.size());
        Assert.assertEquals(output5.get(0), "result=40");

        String transformSql3 = "select truncate(numeric1) from source";
        TransformConfig config3 = new TransformConfig(transformSql3);
        TransformProcessor<String, String> processor3 = TransformProcessor
                .create(config3, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));

        // case4: truncate(12345.6789)
        List<String> output6 = processor3.transform("12345.6789|-3|6|8");
        Assert.assertEquals(1, output6.size());
        Assert.assertEquals(output6.get(0), "result=12345");
    }
}
