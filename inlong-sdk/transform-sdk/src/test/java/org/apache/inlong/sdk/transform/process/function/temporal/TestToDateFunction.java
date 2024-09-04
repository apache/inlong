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

public class TestToDateFunction extends AbstractFunctionTemporalTestBase {

    @Test
    public void testToDateFunction() throws Exception {
        String transformSql1 = "select to_date(string1) from source";
        TransformConfig config1 = new TransformConfig(transformSql1);
        TransformProcessor<String, String> processor1 = TransformProcessor
                .create(config1, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        // case1: to_date('2024-08-15')
        List<String> output1 = processor1.transform("2024-08-15|apple|cloud|2|1|3", new HashMap<>());
        Assert.assertEquals(1, output1.size());
        Assert.assertEquals(output1.get(0), "result=2024-08-15");
        String transformSql2 = "select to_date(string1, string2) from source";
        TransformConfig config2 = new TransformConfig(transformSql2);
        TransformProcessor<String, String> processor2 = TransformProcessor
                .create(config2, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        // case2: to_date('20240815', 'yyyyMMdd')
        List<String> output2 = processor2.transform("20240815|yyyyMMdd|cloud|2|1|3", new HashMap<>());
        Assert.assertEquals(1, output2.size());
        Assert.assertEquals(output2.get(0), "result=2024-08-15");
        // case3: to_date('08152024', 'MMddyyyy')
        List<String> output3 = processor2.transform("08152024|MMddyyyy|cloud|2|1|3", new HashMap<>());
        Assert.assertEquals(1, output3.size());
        Assert.assertEquals(output3.get(0), "result=2024-08-15");
        // case4: to_date('2024/08/15', 'yyyy/MM/dd')
        List<String> output4 = processor2.transform("2024/08/15|yyyy/MM/dd|cloud|2|1|3", new HashMap<>());
        Assert.assertEquals(1, output4.size());
        Assert.assertEquals(output4.get(0), "result=2024-08-15");
    }
}
