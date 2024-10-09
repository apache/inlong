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

public class TestLeastFunction extends AbstractFunctionArithmeticTestBase {

    @Test
    public void testLeastFunction() throws Exception {
        String transformSql1 = "select least(numeric1, least(numeric2, numeric3, numeric4)) from source";
        TransformConfig config1 = new TransformConfig(transformSql1);
        TransformProcessor<String, String> processor1 = TransformProcessor
                .create(config1, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));

        // case: least(3.14, least(7, 2, 1))
        List<String> output2 = processor1.transform("3.14|7|2|1", new HashMap<>());
        Assert.assertEquals(1, output2.size());
        Assert.assertEquals(output2.get(0), "result=1");

        String transformSql2 = "select least(numeric1, numeric2, least(numeric3, numeric4)) from source";
        TransformConfig config2 = new TransformConfig(transformSql2);
        TransformProcessor<String, String> processor2 = TransformProcessor
                .create(config2, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));

        // case: least(3.141592653589793, 4, least(3.33, 3.4))
        List<String> output3 = processor2.transform("3.141592653589793|4|3.33|3.4", new HashMap<>());
        Assert.assertEquals(1, output3.size());
        Assert.assertEquals(output3.get(0), "result=3.141592653589793");

        // case: least(-9223372036854775808, 1, least(-2, 3))
        List<String> output4 = processor2.transform("-9223372036854775808|1|-2|3", new HashMap<>());
        Assert.assertEquals(1, output4.size());
        Assert.assertEquals(output4.get(0), "result=-9223372036854775808");

        String transformSql3 =
                "select least(numeric1, least(numeric2, numeric3), least(numeric4, numeric5)) from source";
        TransformConfig config3 = new TransformConfig(transformSql3);
        TransformProcessor<String, String> processor3 = TransformProcessor
                .create(config3, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));

        // case: least(1, least(-2, -5), least(3.14836, 8))
        List<String> output5 = processor3.transform("1|-2|-5|3.14836|8", new HashMap<>());
        Assert.assertEquals(1, output5.size());
        Assert.assertEquals(output5.get(0), "result=-5");

        // case: least(1, least(-2, -5), least(, 8))
        List<String> output6 = processor3.transform("1|-2|-5||8", new HashMap<>());
        Assert.assertEquals(1, output6.size());
        Assert.assertEquals(output6.get(0), "result=");
    }

}
