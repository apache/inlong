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

public class TestDecodeFunction extends AbstractFunctionStringTestBase {

    @Test
    public void testDecodeFunction() throws Exception {
        String transformSql = "select decode(string1,string2) from source";
        TransformConfig config = new TransformConfig(transformSql);
        TransformProcessor<String, String> processor = TransformProcessor
                .create(config, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));

        // case1: decode('72 101 108 108 111','UTF-8')
        List<String> output1 = processor.transform("72 101 108 108 111|UTF-8|banana|cloud|1", new HashMap<>());
        Assert.assertEquals(1, output1.size());
        Assert.assertEquals(output1.get(0), "result=Hello");

        // case2: decode('72 101 108 108 111','US-ASCII')
        List<String> output2 = processor.transform("72 101 108 108 111|US-ASCII|banana|cloud|1", new HashMap<>());
        Assert.assertEquals(1, output2.size());
        Assert.assertEquals(output2.get(0), "result=Hello");

        // case3: decode('72 101 108 108 111','ISO-8859-1')
        List<String> output3 = processor.transform("72 101 108 108 111|ISO-8859-1|banana|cloud|1", new HashMap<>());
        Assert.assertEquals(1, output3.size());
        Assert.assertEquals(output3.get(0), "result=Hello");

        // case4: decode('0 72 0 101 0 108 0 108 0 111','UTF-16BE')
        List<String> output4 =
                processor.transform("0 72 0 101 0 108 0 108 0 111|UTF-16BE|banana|cloud|1", new HashMap<>());
        Assert.assertEquals(1, output4.size());
        Assert.assertEquals(output4.get(0), "result=Hello");

        // case5: decode('72 0 101 0 108 0 108 0 111 0','UTF-16LE')
        List<String> output5 =
                processor.transform("72 0 101 0 108 0 108 0 111 0|UTf-16LE|banana|cloud|1", new HashMap<>());
        Assert.assertEquals(1, output5.size());
        Assert.assertEquals(output5.get(0), "result=Hello");

        // case6: decode('-2 -1 0 72 0 101 0 108 0 108 0 111','UTF-16')
        List<String> output6 =
                processor.transform("-2 -1 0 72 0 101 0 108 0 108 0 111|UtF-16|banana|cloud|1", new HashMap<>());
        Assert.assertEquals(1, output6.size());
        Assert.assertEquals(output6.get(0), "result=Hello");

        // case7: decode('-2 -1 0 72 0 101 0 108 0 108 0 111','UTF-16--')
        List<String> output7 =
                processor.transform("-2 -1 0 72 0 101 0 108 0 108 0 111|UTF-16--|banana|cloud|1", new HashMap<>());
        Assert.assertEquals(1, output7.size());
        Assert.assertEquals(output7.get(0), "result=");
    }
}
