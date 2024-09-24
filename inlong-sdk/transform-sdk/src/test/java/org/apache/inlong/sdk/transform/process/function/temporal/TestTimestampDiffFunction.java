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

public class TestTimestampDiffFunction extends AbstractFunctionTemporalTestBase {

    @Test
    public void testTimestampDiffFunction() throws Exception {
        String transformSql1 = "select timestampdiff(string1,string2,string3) from source";
        TransformConfig config1 = new TransformConfig(transformSql1);
        TransformProcessor<String, String> processor1 = TransformProcessor
                .create(config1, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));

        // case1: timestampdiff('MONTH','2003-02-01','2003-05-01')
        List<String> output1 = processor1.transform("MONTH|2003-02-01|2003-05-01", new HashMap<>());
        Assert.assertEquals(1, output1.size());
        Assert.assertEquals("result=3", output1.get(0));

        // case2: timestampdiff('YEAR','2002-05-01','2001-01-01')
        List<String> output2 = processor1.transform("YEAR|2002-05-01|2001-01-01", new HashMap<>());
        Assert.assertEquals(1, output2.size());
        Assert.assertEquals("result=-1", output2.get(0));

        // case3: timestampdiff('MICROSECOND','12:05:55.999999','2001-01-01')
        List<String> output3 = processor1.transform("MICROSECOND|12:05:55.999999|2003-05-01", new HashMap<>());
        Assert.assertEquals(1, output3.size());
        Assert.assertEquals("result=", output3.get(0));

        // case4: timestampdiff('MICROSECOND','2003-05-01 12:05:55.999999','2003-05-01')
        List<String> output4 =
                processor1.transform("MICROSECOND|2003-05-01 12:05:55.999999|2003-05-01", new HashMap<>());
        Assert.assertEquals(1, output4.size());
        Assert.assertEquals("result=-43555999999", output4.get(0));

        // case5: timestampdiff('QUARTER','2003-05-01 12:05:55.999999','2663-05-01')
        List<String> output5 = processor1.transform("QUARTER|2003-05-01 12:05:55.999999|2663-05-01", new HashMap<>());
        Assert.assertEquals(1, output5.size());
        Assert.assertEquals("result=2639", output5.get(0));

        String transformSql2 = "select timestampdiff(string1,stringx,string3) from source";
        TransformConfig config2 = new TransformConfig(transformSql2);
        TransformProcessor<String, String> processor2 = TransformProcessor
                .create(config2, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));

        // case6: timestampdiff('MONTH',null,'2003-05-01')
        List<String> output6 = processor2.transform("MONTH|2003-02-01|2003-05-01", new HashMap<>());
        Assert.assertEquals(1, output6.size());
        Assert.assertEquals("result=", output6.get(0));

    }
}
