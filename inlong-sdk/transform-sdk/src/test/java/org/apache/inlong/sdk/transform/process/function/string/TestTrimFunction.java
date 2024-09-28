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

public class TestTrimFunction extends AbstractFunctionStringTestBase {

    @Test
    public void testTrimFunction() throws Exception {
        String transformSql1 = "select trim(string1) from source";
        TransformConfig config1 = new TransformConfig(transformSql1);
        TransformProcessor<String, String> processor1 = TransformProcessor
                .create(config1, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        // case1: trim(' in long')
        List<String> output1 = processor1.transform(" in long|in long | in long ", new HashMap<>());
        Assert.assertEquals(1, output1.size());
        Assert.assertEquals(output1.get(0), "result=in long");
        String transformSql2 = "select trim(string2) from source";
        TransformConfig config2 = new TransformConfig(transformSql2);
        TransformProcessor<String, String> processor2 = TransformProcessor
                .create(config2, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        // case2: trim('in long ')
        List<String> output2 = processor2.transform(" in long|in long | in long ", new HashMap<>());
        Assert.assertEquals(1, output2.size());
        Assert.assertEquals(output2.get(0), "result=in long");
        String transformSql3 = "select trim(string2) from source";
        TransformConfig config3 = new TransformConfig(transformSql3);
        TransformProcessor<String, String> processor3 = TransformProcessor
                .create(config3, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        // case3: trim(' in long ')
        List<String> output3 = processor3.transform(" in long|in long | in long ", new HashMap<>());
        Assert.assertEquals(1, output3.size());
        Assert.assertEquals(output3.get(0), "result=in long");

        String transformSql4 = "select btrim(string1) from source";
        TransformConfig config4 = new TransformConfig(transformSql4);
        TransformProcessor<String, String> processor4 = TransformProcessor
                .create(config4, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        // case4: btrim(' in long')
        List<String> output4 = processor4.transform(" in long|in long | in long ", new HashMap<>());
        Assert.assertEquals(1, output4.size());
        Assert.assertEquals(output4.get(0), "result=in long");
        String transformSql5 = "select btrim(string2) from source";
        TransformConfig config5 = new TransformConfig(transformSql5);
        TransformProcessor<String, String> processor5 = TransformProcessor
                .create(config5, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        // case5: btrim('in long ')
        List<String> output5 = processor5.transform(" in long|in long | in long ", new HashMap<>());
        Assert.assertEquals(1, output5.size());
        Assert.assertEquals(output5.get(0), "result=in long");
        String transformSql6 = "select btrim(string2) from source";
        TransformConfig config6 = new TransformConfig(transformSql6);
        TransformProcessor<String, String> processor6 = TransformProcessor
                .create(config6, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        // case6: btrim(' in long ')
        List<String> output6 = processor6.transform(" in long|in long | in long ", new HashMap<>());
        Assert.assertEquals(1, output6.size());
        Assert.assertEquals(output6.get(0), "result=in long");
    }
}
