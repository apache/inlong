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

public class TestConvertTzFunction extends AbstractFunctionTemporalTestBase {

    @Test
    public void testConvertTzFunction() throws Exception {
        String transformSql = null;
        TransformConfig config = null;
        TransformProcessor<String, String> processor = null;
        List<String> output = null;

        transformSql = "select convert_tz(string1,string2,string3) from source";
        config = new TransformConfig(transformSql);
        processor = TransformProcessor
                .create(config, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        // case1: convert_tz('1970-01-01 00:00:00', 'UTC', 'America/Los_Angeles')
        output = processor.transform("1970-01-01 00:00:00|UTC|America/Los_Angeles", new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=1969-12-31 16:00:00", output.get(0));

        // case1: convert_tz('1970-01-01 00:00:00', 'UTC', 'America/Los_Angeles')
        output = processor.transform("2024-09-29 12:00:00|Asia/Tokyo|UTC", new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=2024-09-29 03:00:00", output.get(0));

        transformSql = "select convert_tz(string1,string2,string3) from source";
        config = new TransformConfig(transformSql);
        processor = TransformProcessor
                .create(config, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        // case1: convert_tz('2024-12-31 23:59:59', 'Europe/London', 'America/New_York')
        output = processor.transform("2024-12-31 23:59:59|Europe/London|America/New_York", new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=2024-12-31 18:59:59", output.get(0));

        transformSql = "select convert_tz(string1,string2,string3) from source";
        config = new TransformConfig(transformSql);
        processor = TransformProcessor
                .create(config, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        // case1: convert_tz('2024-07-15 10:30:00', 'America/New_York', 'Australia/Sydney')
        output = processor.transform("2024-07-15 10:30:00|America/New_York|Australia/Sydney", new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=2024-07-16 00:30:00", output.get(0));

        // case1: convert_tz('2024-03-31 02:00:00', 'Europe/Berlin', 'Asia/Kolkata')
        output = processor.transform("2024-03-31 02:00:00|Europe/Berlin|Asia/Kolkata", new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=2024-03-31 06:30:00", output.get(0));

        // case1: convert_tz('2024-11-01 15:45:00', 'Australia/Sydney', 'UTC')
        output = processor.transform("2024-11-01 15:45:00|Australia/Sydney|UTC", new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=2024-11-01 04:45:00", output.get(0));

        // case1: convert_tz('2024-06-21 00:00:00', 'America/Los_Angeles', 'Europe/Paris')
        output = processor.transform("2024-06-21 00:00:00|America/Los_Angeles|Europe/Paris", new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=2024-06-21 09:00:00", output.get(0));

        // case1: convert_tz('2024-01-01 10:00:00', 'GMT-03:00', 'Asia/Shanghai')
        output = processor.transform("2024-01-01 10:00:00|GMT-03:00|Asia/Shanghai", new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=2024-01-01 21:00:00", output.get(0));

    }
}
