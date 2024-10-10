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

public class TestTimeDiffFunction extends AbstractFunctionTemporalTestBase {

    @Test
    public void testTimeDiffFunction() throws Exception {
        String transformSql = null;
        TransformConfig config = null;
        TransformProcessor<String, String> processor = null;
        List<String> output = null;

        transformSql = "select timediff(string1,string2) from source";
        config = new TransformConfig(transformSql);
        processor = TransformProcessor
                .create(config, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        // case1: TIMEDIFF('2000-01-01 00:00:00','2000-01-01 00:00:00.000001')
        output = processor.transform("2000-01-01 00:00:00|2000-01-01 00:00:00.000001", new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=-00:00:00.000001", output.get(0));

        // case2: TIMEDIFF('2008-12-31 23:59:59.000001','2008-12-30 01:01:01.000002')
        output = processor.transform("2008-12-31 23:59:59|2008-12-30 01:01:01.000002", new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=46:58:57.999998", output.get(0));

        // case3: TIMEDIFF('00:00:00','00:00:01')
        output = processor.transform("00:00:00|00:00:01", new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=-00:00:01", output.get(0));

        // case4: TIMEDIFF('23:59:59.000001','01:01:01.000002')
        output = processor.transform("23:59:59|01:01:01.000002", new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=22:58:57.999998", output.get(0));

        // case5: TIMEDIFF('2008-12-31 23:59:59.000001','01:01:01.000002')
        output = processor.transform("2008-12-31 23:59:59|01:01:01.000002", new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=", output.get(0));

        // case6: TIMEDIFF('2008-12-31 23:59:59.000001','0001-12-31 01:01:01.000002')
        output = processor.transform("2008-12-31 23:59:59|0001-12-31 01:01:01.000002", new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=", output.get(0));
    }
}
