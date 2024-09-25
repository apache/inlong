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

public class TestDateSubFunction extends AbstractFunctionTemporalTestBase {

    @Test
    public void testDateSubFunction() throws Exception {
        String transformSql = null;
        TransformConfig config = null;
        TransformProcessor<String, String> processor = null;
        List<String> output = null;

        transformSql = "select DATE_SUB(string1,  INTERVAL string2 DAY) from source";
        config = new TransformConfig(transformSql);
        processor = TransformProcessor
                .create(config, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        // case1: DATE_SUB('2020-12-31 23:59:59',INTERVAL 999 DAY)
        output = processor.transform("2020-12-31 23:59:59|999", new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=2018-04-07 23:59:59", output.get(0));

        // case2: DATE_SUB('2020-12-31 23:59:59',INTERVAL -999 DAY)
        output = processor.transform("2020-12-31 23:59:59|-999", new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=2023-09-26 23:59:59", output.get(0));

        transformSql = "select DATE_SUB(string1,  INTERVAL string2 MINUTE_SECOND) from source";
        config = new TransformConfig(transformSql);
        processor = TransformProcessor
                .create(config, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        // case3: DATE_SUB('2020-12-31',INTERVAL '-1:1' MINUTE_SECOND)
        output = processor.transform("2020-12-31|-1:1", new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=2020-12-31 00:01:01", output.get(0));

        transformSql = "select DATE_SUB(string1,  INTERVAL string2 SECOND_MICROSECOND) from source";
        config = new TransformConfig(transformSql);
        processor = TransformProcessor
                .create(config, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        // case4: DATE_SUB('1992-12-31 23:59:59', INTERVAL '-1.999999' SECOND_MICROSECOND)
        output = processor.transform("1992-12-31 23:59:59|-1.999999", new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=1993-01-01 00:00:00.999999", output.get(0));

    }
}
