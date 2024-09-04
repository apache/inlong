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

public class TestModuloFunction extends AbstractFunctionArithmeticTestBase {

    @Test
    public void testModuloFunction() throws Exception {
        String transformFunctionSql = "select mod(numeric1,100) from source";
        String transformExpressionSql = "select numeric1 % 100 from source";
        List<String> output1, output2;
        String data;
        TransformConfig functionConfig = new TransformConfig(transformFunctionSql);
        TransformProcessor<String, String> functionProcessor = TransformProcessor
                .create(functionConfig, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        TransformConfig expressionConfig = new TransformConfig(transformExpressionSql);
        TransformProcessor<String, String> expressionProcessor = TransformProcessor
                .create(expressionConfig, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));

        // case1: "mod(3.1415926,100)" and "3.1415926 % 100"
        data = "3.1415926|4a|4|8";
        output1 = functionProcessor.transform(data);
        Assert.assertEquals(1, output1.size());
        Assert.assertEquals("result=3.1415926", output1.get(0));
        output2 = expressionProcessor.transform(data);
        Assert.assertEquals(1, output2.size());
        Assert.assertEquals("result=3.1415926", output2.get(0));

        // case2: "mod(-3.1415926,100)" and "-3.1415926 % 100"
        data = "-3.1415926|4a|4|8";
        output1 = functionProcessor.transform(data);
        Assert.assertEquals(1, output1.size());
        Assert.assertEquals("result=-3.1415926", output1.get(0));
        output2 = expressionProcessor.transform(data);
        Assert.assertEquals(1, output2.size());
        Assert.assertEquals("result=-3.1415926", output2.get(0));

        // case3: "mod(320,100)" and "320 % 100"
        data = "320|4a|4|8";
        output1 = functionProcessor.transform(data);
        Assert.assertEquals(1, output1.size());
        Assert.assertEquals("result=20", output1.get(0));
        output2 = expressionProcessor.transform(data);
        Assert.assertEquals(1, output2.size());
        Assert.assertEquals("result=20", output2.get(0));

        // case4: "mod(-320,100)" and "-320 % 100"
        data = "-320|4a|4|8";
        output1 = functionProcessor.transform(data);
        Assert.assertEquals(1, output1.size());
        Assert.assertEquals("result=-20", output1.get(0));
        output2 = expressionProcessor.transform(data);
        Assert.assertEquals(1, output2.size());
        Assert.assertEquals("result=-20", output2.get(0));

        transformFunctionSql = "select mod(numeric1,-10) from source";
        transformExpressionSql = "select numeric1 % -10 from source";
        functionConfig = new TransformConfig(transformFunctionSql);
        functionProcessor = TransformProcessor
                .create(functionConfig, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        expressionConfig = new TransformConfig(transformExpressionSql);
        expressionProcessor = TransformProcessor
                .create(expressionConfig, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));

        // case5: "mod(9,-10)" and "9 % -10"
        data = "9|4a|4|8";
        output1 = functionProcessor.transform(data);
        Assert.assertEquals(1, output1.size());
        Assert.assertEquals("result=9", output1.get(0));
        output2 = expressionProcessor.transform(data);
        Assert.assertEquals(1, output2.size());
        Assert.assertEquals("result=9", output2.get(0));

        // case6: "mod(-13,-10)" and "-13 % -10"
        data = "-13|4a|4|8";
        output1 = functionProcessor.transform(data);
        Assert.assertEquals(1, output1.size());
        Assert.assertEquals("result=-3", output1.get(0));
        output2 = expressionProcessor.transform(data);
        Assert.assertEquals(1, output2.size());
        Assert.assertEquals("result=-3", output2.get(0));

        // case7: "mod(-13.14,-10)" and "-13.14 % -10"
        data = "-13.14|4a|4|8";
        output1 = functionProcessor.transform(data);
        Assert.assertEquals(1, output1.size());
        Assert.assertEquals("result=-3.14", output1.get(0));
        output2 = expressionProcessor.transform(data);
        Assert.assertEquals(1, output2.size());
        Assert.assertEquals("result=-3.14", output2.get(0));

        // case8: "mod(13.14,-10)" and "13.14 % -10"
        data = "13.14|4a|4|8";
        output1 = functionProcessor.transform(data);
        Assert.assertEquals(1, output1.size());
        Assert.assertEquals("result=3.14", output1.get(0));
        output2 = expressionProcessor.transform(data);
        Assert.assertEquals(1, output2.size());
        Assert.assertEquals("result=3.14", output2.get(0));

        transformFunctionSql = "select mod(numeric1,-3.14) from source";
        transformExpressionSql = "select numeric1 % -3.14 from source";
        functionConfig = new TransformConfig(transformFunctionSql);
        functionProcessor = TransformProcessor
                .create(functionConfig, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        expressionConfig = new TransformConfig(transformExpressionSql);
        expressionProcessor = TransformProcessor
                .create(expressionConfig, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));

        // case9: "mod(9,-3.14)" and "9 % -3.14"
        data = "9|4a|4|8";
        output1 = functionProcessor.transform(data);
        Assert.assertEquals(1, output1.size());
        Assert.assertEquals("result=2.72", output1.get(0));
        output2 = expressionProcessor.transform(data);
        Assert.assertEquals(1, output2.size());
        Assert.assertEquals("result=2.72", output2.get(0));

        // case10: "mod(-9,-3.14)" and "-9 % -3.14"
        data = "-9|4a|4|8";
        output1 = functionProcessor.transform(data);
        Assert.assertEquals(1, output1.size());
        Assert.assertEquals("result=-2.72", output1.get(0));
        output2 = expressionProcessor.transform(data);
        Assert.assertEquals(1, output2.size());
        Assert.assertEquals("result=-2.72", output2.get(0));

        // case11: "mod(-13.14,-3.14)" and "-13.14 % -3.14"
        data = "-13.14|4a|4|8";
        output1 = functionProcessor.transform(data);
        Assert.assertEquals(1, output1.size());
        Assert.assertEquals("result=-0.58", output1.get(0));
        output2 = expressionProcessor.transform(data);
        Assert.assertEquals(1, output2.size());
        Assert.assertEquals("result=-0.58", output2.get(0));

        // case12: "mod(13.14,-3.14)" and "13.14 % -3.14"
        data = "13.14|4a|4|8";
        output1 = functionProcessor.transform(data);
        Assert.assertEquals(1, output1.size());
        Assert.assertEquals("result=0.58", output1.get(0));
        output2 = expressionProcessor.transform(data);
        Assert.assertEquals(1, output2.size());
        Assert.assertEquals("result=0.58", output2.get(0));

    }
}
