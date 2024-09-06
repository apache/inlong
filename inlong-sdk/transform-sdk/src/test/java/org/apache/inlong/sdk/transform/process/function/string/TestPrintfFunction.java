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

public class TestPrintfFunction extends AbstractFunctionStringTestBase {

    @Test
    public void testPrintfFunction() throws Exception {
        String transformSql1 = "select printf(string1, numeric1) from source";
        TransformConfig config1 = new TransformConfig(transformSql1);
        TransformProcessor<String, String> processor1 = TransformProcessor
                .create(config1, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        // case1: printf("Hello, you have %d new messages.", 5)
        List<String> output1 = processor1.transform("Hello, you have %d new messages.|||5", new HashMap<>());
        Assert.assertEquals(1, output1.size());
        Assert.assertEquals(output1.get(0), "result=Hello, you have 5 new messages.");
        // case2: printf("Your total is %.2f dollars.", 123.456)
        String transformSql2 = "select printf(string1, numeric1) from source";
        TransformConfig config2 = new TransformConfig(transformSql2);
        TransformProcessor<String, String> processor2 = TransformProcessor
                .create(config2, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        List<String> output2 = processor2.transform("Your total is %.2f dollars.|||123.456", new HashMap<>());
        Assert.assertEquals(1, output2.size());
        Assert.assertEquals(output2.get(0), "result=Your total is 123.46 dollars.");
        // case3: printf("Welcome, %s!", "Alice")
        String transformSql3 = "select printf(string1, string2) from source";
        TransformConfig config3 = new TransformConfig(transformSql3);
        TransformProcessor<String, String> processor3 = TransformProcessor
                .create(config3, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        List<String> output3 = processor3.transform("Welcome, %s!|Alice", new HashMap<>());
        Assert.assertEquals(1, output3.size());
        Assert.assertEquals(output3.get(0), "result=Welcome, Alice!");
        // case4: printf("User %s has %d points and a balance of %.2f.", "Bob", 1500, 99.99)
        String transformSql4 = "select printf(string1, string2, numeric1, numeric2) from source";
        TransformConfig config4 = new TransformConfig(transformSql4);
        TransformProcessor<String, String> processor4 = TransformProcessor
                .create(config4, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        List<String> output4 =
                processor4.transform("User %s has %d points and a balance of %.2f.|Bob||1500|99.99", new HashMap<>());
        Assert.assertEquals(1, output4.size());
        Assert.assertEquals(output4.get(0), "result=User Bob has 1500 points and a balance of 99.99.");
        // case5: printf("Temperature is %d°C, and altitude is %.1f meters.", -20, 0.0)
        String transformSql5 = "select printf(string1, numeric1, numeric2) from source";
        TransformConfig config5 = new TransformConfig(transformSql5);
        TransformProcessor<String, String> processor5 = TransformProcessor
                .create(config5, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        List<String> output5 =
                processor5.transform("Temperature is %d°C, and altitude is %.1f meters.|||-20|0.0", new HashMap<>());
        Assert.assertEquals(1, output5.size());
        Assert.assertEquals(output5.get(0), "result=Temperature is -20°C, and altitude is 0.0 meters.");
        // case6: printf("Empty string: '%s'", "")
        String transformSql6 = "select printf(string1, string2) from source";
        TransformConfig config6 = new TransformConfig(transformSql6);
        TransformProcessor<String, String> processor6 = TransformProcessor
                .create(config6, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        List<String> output6 = processor6.transform("Empty string: '%s'|", new HashMap<>());
        Assert.assertEquals(1, output6.size());
        Assert.assertEquals(output6.get(0), "result=Empty string: ''");
        // case7: printf("Null value: '%s'", null)
        String transformSql7 = "select printf(string1, string2) from source";
        TransformConfig config7 = new TransformConfig(transformSql7);
        TransformProcessor<String, String> processor7 = TransformProcessor
                .create(config7, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        List<String> output7 = processor7.transform("Null value: '%s'", new HashMap<>());
        Assert.assertEquals(1, output7.size());
        Assert.assertEquals(output7.get(0), "result=Null value: 'null'");
    }
}
