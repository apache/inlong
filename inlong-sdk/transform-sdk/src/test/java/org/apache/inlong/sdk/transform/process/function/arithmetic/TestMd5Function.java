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

public class TestMd5Function extends AbstractFunctionArithmeticTestBase {

    @Test
    public void testMd5Function() throws Exception {
        String transformSql = "select md5(numeric1) from source";
        TransformConfig config = new TransformConfig(transformSql);
        TransformProcessor<String, String> processor = TransformProcessor
                .create(config, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        // case1: md5("1")
        List<String> output1 = processor.transform("1|4|6|8");
        Assert.assertEquals(1, output1.size());
        Assert.assertEquals("result=c4ca4238a0b923820dcc509a6f75849b", output1.get(0));

        // case2: md5("-1")
        List<String> output2 = processor.transform("-1|4|6|8");
        Assert.assertEquals(1, output2.size());
        Assert.assertEquals("result=6bb61e3b7bce0931da574d19d1d82c88", output2.get(0));

        // case3: md5("")
        List<String> output3 = processor.transform("|4|6|8");
        Assert.assertEquals(1, output3.size());
        Assert.assertEquals("result=d41d8cd98f00b204e9800998ecf8427e", output3.get(0));

        // case4: md5(null)
        transformSql = "select md5(numericxx) from source";
        config = new TransformConfig(transformSql);
        processor = TransformProcessor
                .create(config, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        List<String> output4 = processor.transform("1|4|6|8");
        Assert.assertEquals(1, output4.size());
        Assert.assertEquals("result=", output4.get(0));
    }
}
