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

public class TestTimestampFunction extends AbstractFunctionTemporalTestBase {

    @Test
    public void testTimestamp() throws Exception {
        String transformSql = null;
        TransformConfig config = null;
        TransformProcessor<String, String> processor = null;
        List<String> output = null;

        transformSql = "select timestamp(string1,string2) from source";
        config = new TransformConfig(transformSql);
        processor = TransformProcessor
                .create(config, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        // case1: timestamp('2003-12-31 12:00:00.600000','12:00:00')
        output = processor.transform("2003-12-31 12:00:00.600000|12:00:00", new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=2004-01-01 00:00:00.600000", output.get(0));

        // case2: timestamp('2003-12-31 12:00:00','12:00:00.600000')
        output = processor.transform("2003-12-31 12:00:00|12:00:00.600000", new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=2004-01-01 00:00:00.600000", output.get(0));

        // case3: timestamp('2003-12-31','12:00:00.600000')
        output = processor.transform("2003-12-31|12:00:00.600000", new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=2003-12-31 12:00:00.600000", output.get(0));

        transformSql = "select timestamp(string1,stringx) from source";
        config = new TransformConfig(transformSql);
        processor = TransformProcessor
                .create(config, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        // case4: timestamp('2003-12-31 12:00:00.600000',null)
        output = processor.transform("2003-12-31 12:00:00.600000|12:00:00", new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=", output.get(0));

        transformSql = "select timestamp(string1) from source";
        config = new TransformConfig(transformSql);
        processor = TransformProcessor
                .create(config, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        // case5: timestamp('2003-12-31 12:00:00')
        output = processor.transform("2003-12-31 12:00:00", new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=2003-12-31 12:00:00", output.get(0));

        // case6: timestamp('2003-12-31')
        output = processor.transform("2003-12-31", new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=2003-12-31 00:00:00", output.get(0));

    }
}
