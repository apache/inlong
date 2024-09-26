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

package org.apache.inlong.sdk.transform.process.function.temporal;

import org.apache.inlong.sdk.transform.decode.SourceDecoderFactory;
import org.apache.inlong.sdk.transform.encode.SinkEncoderFactory;
import org.apache.inlong.sdk.transform.pojo.TransformConfig;
import org.apache.inlong.sdk.transform.process.TransformProcessor;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;

public class TestTimestampAddFunction extends AbstractFunctionTemporalTestBase {

    @Test
    public void testTimestampAddFunction() throws Exception {
        String transformSql1 = "select timestamp_add('day',string2,string1) from source";
        TransformConfig config1 = new TransformConfig(transformSql1);
        TransformProcessor<String, String> processor1 = TransformProcessor
                .create(config1, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        // case1: timestamp_add('day',3,'1970-01-01')
        List<String> output1 = processor1.transform("1970-01-01|3", new HashMap<>());
        Assert.assertEquals(1, output1.size());
        Assert.assertEquals("result=1970-01-04", output1.get(0));

        // case2: timestamp_add('day',-3,'1970-01-01 00:00:44')
        List<String> output2 = processor1.transform("1970-01-01 00:00:44|-3", new HashMap<>());
        Assert.assertEquals(1, output2.size());
        Assert.assertEquals("result=1969-12-29 00:00:44", output2.get(0));

        String transformSql2 = "select timestamp_add('MINUTE',string2,string1) from source";
        TransformConfig config2 = new TransformConfig(transformSql2);
        TransformProcessor<String, String> processor2 = TransformProcessor
                .create(config2, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));

        // case3: timestamp_add('MINUTE',3,'1970-01-01 00:00:44')
        List<String> output3 = processor2.transform("1970-01-01 00:00:44|3", new HashMap<>());
        Assert.assertEquals(1, output3.size());
        Assert.assertEquals("result=1970-01-01 00:03:44", output3.get(0));

        // case4: timestamp_add('MINUTE',-3,'1970-01-01 00:00:44')
        List<String> output4 = processor2.transform("1970-01-01 00:00:44|-3", new HashMap<>());
        Assert.assertEquals(1, output4.size());
        Assert.assertEquals("result=1969-12-31 23:57:44", output4.get(0));

        // case5: timestamp_add('MINUTE',-3,'1970-01-01')
        List<String> output5 = processor2.transform("1970-01-01|-3", new HashMap<>());
        Assert.assertEquals(1, output5.size());
        Assert.assertEquals("result=1969-12-31 23:57:00", output5.get(0));

        String transformSql3 = "select timestamp_add('MICROSECOND',string2,string1) from source";
        TransformConfig config3 = new TransformConfig(transformSql3);
        TransformProcessor<String, String> processor3 = TransformProcessor
                .create(config3, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));

        // case6: timestamp_add('MICROSECOND',3,'1970-01-01 00:00:44.000001')
        List<String> output6 = processor3.transform("1970-01-01 00:00:44.000001|3", new HashMap<>());
        Assert.assertEquals(1, output6.size());
        Assert.assertEquals("result=1970-01-01 00:00:44.000004", output6.get(0));

        // case7: timestamp_add('MICROSECOND',3,'1970-01-01 00:00:44')
        List<String> output7 = processor3.transform("1970-01-01 00:00:44|3", new HashMap<>());
        Assert.assertEquals(1, output7.size());
        Assert.assertEquals("result=1970-01-01 00:00:44.000003", output7.get(0));
    }
}
