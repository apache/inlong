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

public class TestFromUnixTimeFunction extends AbstractFunctionTemporalTestBase {

    @Test
    public void testFromUnixTimeFunction() throws Exception {
        String transformSql1 = "select from_unix_time(numeric1) from source";
        TransformConfig config1 = new TransformConfig(transformSql1);
        TransformProcessor<String, String> processor1 = TransformProcessor
                .create(config1, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        // case1: from_unix_time(44)
        List<String> output1 = processor1.transform("can|apple|cloud|44|1|3", new HashMap<>());
        Assert.assertEquals(1, output1.size());
        Assert.assertEquals(output1.get(0), "result=1970-01-01 08:00:44");

        String transformSql2 = "select from_unix_time(numeric1, 'yyyy/MM/dd HH:mm:ss') from source";
        TransformConfig config2 = new TransformConfig(transformSql2);
        TransformProcessor<String, String> processor2 = TransformProcessor
                .create(config2, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        // case2: from_unix_time(44, 'yyyy/MM/dd HH:mm:ss')
        List<String> output2 = processor2.transform("can|apple|cloud|44|1|3", new HashMap<>());
        Assert.assertEquals(1, output2.size());
        Assert.assertEquals(output2.get(0), "result=1970/01/01 08:00:44");

        String transformSql3 = "select from_unix_time(numeric1, 'MMdd-yyyy') from source";
        TransformConfig config3 = new TransformConfig(transformSql3);
        TransformProcessor<String, String> processor3 = TransformProcessor
                .create(config3, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        // case3: from_unix_time(44, 'MMdd-yyyy')
        List<String> output3 = processor3.transform("can|apple|cloud|44|1|3", new HashMap<>());
        Assert.assertEquals(1, output3.size());
        Assert.assertEquals(output3.get(0), "result=0101-1970");

        String transformSql4 = "select from_unix_time(numeric1, 'yyyyMMddHHss') from source";
        TransformConfig config4 = new TransformConfig(transformSql4);
        TransformProcessor<String, String> processor4 = TransformProcessor
                .create(config4, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        // case4: from_unix_time(44, 'yyyyMMddHHss')
        List<String> output4 = processor4.transform("can|apple|cloud|44|1|3", new HashMap<>());
        Assert.assertEquals(1, output4.size());
        Assert.assertEquals(output4.get(0), "result=197001010844");

        String transformSql5 = "select concat(substr(from_unix_time(numeric1/1000),1,10),' 00:00:00') from source";
        TransformConfig config5 = new TransformConfig(transformSql5);
        TransformProcessor<String, String> processor5 = TransformProcessor
                .create(config5, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        // case5: from_unix_time(floor(1753353737123/86400000)*86400)
        List<String> output5 = processor5.transform("can|apple|cloud|1753353737123|1|3", new HashMap<>());
        Assert.assertEquals(1, output5.size());
        Assert.assertEquals(output5.get(0), "result=2025-07-24 00:00:00");

        String transformSql6 = "select concat(substr(from_unix_time(numeric1/1000),1,13),':00:00') from source";
        TransformConfig config6 = new TransformConfig(transformSql6);
        TransformProcessor<String, String> processor6 = TransformProcessor
                .create(config6, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        // case6: from_unix_time(floor(1753353737123/3600000)*3600)
        List<String> output6 = processor6.transform("can|apple|cloud|1753353737123|1|3", new HashMap<>());
        Assert.assertEquals(1, output6.size());
        Assert.assertEquals(output6.get(0), "result=2025-07-24 18:00:00");
    }
}
