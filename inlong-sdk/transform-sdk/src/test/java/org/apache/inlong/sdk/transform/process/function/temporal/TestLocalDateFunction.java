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

import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;

public class TestLocalDateFunction extends AbstractFunctionTemporalTestBase {

    @Test
    public void testLocalDateFunction() throws Exception {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");

        // case1: localDate() - default system time zone
        String transformSql1 = "select localdate() from source";
        TransformConfig config1 = new TransformConfig(transformSql1);
        TransformProcessor<String, String> processor1 = TransformProcessor
                .create(config1, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        List<String> output1 = processor1.transform("", new HashMap<>());
        LocalDate expectedDate1 = LocalDate.now(ZoneId.systemDefault());
        LocalDate actualDate1 = LocalDate.parse(output1.get(0).split("=")[1], formatter);
        Assert.assertEquals(1, output1.size());
        Assert.assertEquals(expectedDate1, actualDate1);

        // case2: localDate("UTC")
        String transformSql2 = "select localdate('UTC') from source";
        TransformConfig config2 = new TransformConfig(transformSql2);
        TransformProcessor<String, String> processor2 = TransformProcessor
                .create(config2, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        List<String> output2 = processor2.transform("", new HashMap<>());
        LocalDate expectedDate2 = LocalDate.now(ZoneId.of("UTC"));
        LocalDate actualDate2 = LocalDate.parse(output2.get(0).split("=")[1], formatter);
        Assert.assertEquals(1, output2.size());
        Assert.assertEquals(expectedDate2, actualDate2);

        // case3: localDate("UTC-12")
        String transformSql3 = "select localdate('UTC-12') from source";
        TransformConfig config3 = new TransformConfig(transformSql3);
        TransformProcessor<String, String> processor3 = TransformProcessor
                .create(config3, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        List<String> output3 = processor3.transform("", new HashMap<>());
        LocalDate expectedDate3 = LocalDate.now(ZoneId.of("UTC-12"));
        LocalDate actualDate3 = LocalDate.parse(output3.get(0).split("=")[1], formatter);
        Assert.assertEquals(1, output3.size());
        Assert.assertEquals(expectedDate3, actualDate3);
    }
}
