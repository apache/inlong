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

package org.apache.inlong.sdk.transform.process.operator;

import org.apache.inlong.sdk.transform.decode.SourceDecoderFactory;
import org.apache.inlong.sdk.transform.encode.SinkEncoderFactory;
import org.apache.inlong.sdk.transform.pojo.TransformConfig;
import org.apache.inlong.sdk.transform.process.TransformProcessor;

import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class TestInOperator extends AbstractOperatorTestBase {

    @Test
    public void testAndOperator() throws Exception {
        String transformSql = "select if(string2 IN ('3a','5'),1,0) from source";
        TransformConfig config = new TransformConfig(transformSql);
        // case1: "3.14159265358979323846|3a|4|4"
        TransformProcessor<String, String> processor = TransformProcessor
                .create(config, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        List<String> output1 = processor.transform("3.14159265358979323846|3a|4|4");
        Assert.assertEquals(1, output1.size());
        Assert.assertEquals(output1.get(0), "result=1");
        // case2: "3.14159265358979323846|5|4|8"
        List<String> output2 = processor.transform("3.14159265358979323846|5|4|8");
        Assert.assertEquals(1, output2.size());
        Assert.assertEquals(output2.get(0), "result=1");
        // case3: "3.14159265358979323846|3|4|8"
        List<String> output3 = processor.transform("3.14159265358979323846|3|4|8");
        Assert.assertEquals(1, output3.size());
        Assert.assertEquals(output3.get(0), "result=0");

        transformSql = "select if(numeric3 IN (4,3.2),1,0) from source";
        config = new TransformConfig(transformSql);
        // case4: "3.14159265358979323846|4|4|8"
        processor = TransformProcessor
                .create(config, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        List<String> output4 = processor.transform("3.14159265358979323846|4|4|8");
        Assert.assertEquals(1, output4.size());
        Assert.assertEquals(output4.get(0), "result=1");
        // case5: "3.14159265358979323846|4|3.2|4"
        List<String> output5 = processor.transform("3.14159265358979323846|4|3.2|4");
        Assert.assertEquals(1, output5.size());
        Assert.assertEquals(output5.get(0), "result=1");
        // case6: "3.14159265358979323846|4|3.2|8"
        List<String> output6 = processor.transform("3.14159265358979323846|4|4.2|8");
        Assert.assertEquals(1, output6.size());
        Assert.assertEquals(output6.get(0), "result=0");

        // not in
        transformSql = "select if(numeric3 not IN (4,3.2),1,0) from source";
        config = new TransformConfig(transformSql);
        // case7: "3.14159265358979323846|4|4|8"
        processor = TransformProcessor
                .create(config, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        List<String> output7 = processor.transform("3.14159265358979323846|4|4|8");
        Assert.assertEquals(1, output7.size());
        Assert.assertEquals(output7.get(0), "result=0");
        // case8: "3.14159265358979323846|4|3.2|4"
        List<String> output8 = processor.transform("3.14159265358979323846|4|3.2|4");
        Assert.assertEquals(1, output8.size());
        Assert.assertEquals(output8.get(0), "result=0");
        // case9: "3.14159265358979323846|4|3.2|8"
        List<String> output9 = processor.transform("3.14159265358979323846|4|4.2|8");
        Assert.assertEquals(1, output9.size());
        Assert.assertEquals(output9.get(0), "result=1");
    }
}
