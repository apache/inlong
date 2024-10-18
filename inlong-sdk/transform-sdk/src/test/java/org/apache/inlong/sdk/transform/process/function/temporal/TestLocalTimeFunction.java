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

import java.time.Duration;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.HashMap;
import java.util.List;

public class TestLocalTimeFunction extends AbstractFunctionTemporalTestBase {

    @Test
    public void testLocalTimeFunction() throws Exception {
        String transformSql1 = "select localtime() from source";
        TransformConfig config1 = new TransformConfig(transformSql1);
        TransformProcessor<String, String> processor1 = TransformProcessor
                .create(config1, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        DateTimeFormatter fomatter = new DateTimeFormatterBuilder()
                .appendPattern("HH:mm")
                .optionalStart()
                .appendPattern(":ss")
                .optionalEnd()
                .parseDefaulting(ChronoField.SECOND_OF_MINUTE, 0)
                .toFormatter();
        // case1: localTime()
        List<String> output1 = processor1.transform("", new HashMap<>());
        LocalTime expectedTime1 = LocalTime.now().withNano(0);
        LocalTime actualTime1 = LocalTime.parse(output1.get(0).split("=")[1], fomatter);
        Duration duration1 = Duration.between(expectedTime1, actualTime1);
        Assert.assertEquals(1, output1.size());
        Assert.assertTrue(duration1.getSeconds() < 1);

        // case2: currentTime("UTC")
        String transformSql2 = "select current_time('UTC') from source";
        TransformConfig config2 = new TransformConfig(transformSql2);
        TransformProcessor<String, String> processor2 = TransformProcessor
                .create(config2, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        List<String> output2 = processor2.transform("", new HashMap<>());
        LocalTime expectedTime2 = LocalTime.now(ZoneId.of("UTC")).withNano(0);
        LocalTime actualTime2 = LocalTime.parse(output2.get(0).split("=")[1], fomatter);
        Duration duration2 = Duration.between(expectedTime2, actualTime2);
        Assert.assertEquals(1, output2.size());
        Assert.assertTrue(duration2.getSeconds() < 1);

        // case 3: localTime("America/New_York")
        String transformSql3 = "select localtime('America/New_York') from source";
        TransformConfig config3 = new TransformConfig(transformSql3);
        TransformProcessor<String, String> processor3 = TransformProcessor
                .create(config3, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        List<String> output3 = processor3.transform("", new HashMap<>());
        LocalTime expectedTime3 = LocalTime.now(ZoneId.of("America/New_York")).withNano(0);
        LocalTime actualTime3 = LocalTime.parse(output3.get(0).split("=")[1], fomatter);
        Duration duration3 = Duration.between(expectedTime3, actualTime3);
        Assert.assertEquals(1, output3.size());
        Assert.assertTrue(duration3.getSeconds() < 1);
    }
}
