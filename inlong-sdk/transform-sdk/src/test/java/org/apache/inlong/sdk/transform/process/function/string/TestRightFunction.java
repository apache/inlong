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

public class TestRightFunction extends AbstractFunctionStringTestBase {

    @Test
    public void testRightFunction() throws Exception {
        String transformSql = "select right(string1,numeric1) from source";
        TransformConfig config = new TransformConfig(transformSql);
        TransformProcessor<String, String> processor1 = TransformProcessor
                .create(config, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        // case1: right('hello world',5)
        String data = "hello world|banana|cloud|5|3|3";
        List<String> output1 = processor1.transform(data, new HashMap<>());
        Assert.assertEquals(1, output1.size());
        Assert.assertEquals("result=world", output1.get(0));

        // case2: right('hello world',-15)
        data = "hello world|banana|cloud|-15|3|3";
        output1 = processor1.transform(data, new HashMap<>());
        Assert.assertEquals(1, output1.size());
        Assert.assertEquals("result=", output1.get(0));

        // case3: right('hello world',100)
        data = "hello world|banana|cloud|100|3|3";
        output1 = processor1.transform(data, new HashMap<>());
        Assert.assertEquals(1, output1.size());
        Assert.assertEquals("result=hello world", output1.get(0));

        // case4: right(null,5)
        transformSql = "select right(xxd,numeric1) from source";
        config = new TransformConfig(transformSql);
        processor1 = TransformProcessor
                .create(config, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        data = "hello world|banana|cloud|5|3|3";
        output1 = processor1.transform(data, new HashMap<>());
        Assert.assertEquals(1, output1.size());
        Assert.assertEquals("result=", output1.get(0));

        // case5: right('hello world',null)
        transformSql = "select right(string1,xxd) from source";
        config = new TransformConfig(transformSql);
        processor1 = TransformProcessor
                .create(config, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        data = "hello world|banana|cloud|5|3|3";
        output1 = processor1.transform(data, new HashMap<>());
        Assert.assertEquals(1, output1.size());
        Assert.assertEquals("result=", output1.get(0));
    }
}
