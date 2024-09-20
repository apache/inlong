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

package org.apache.inlong.sdk.transform.process.parser;

import org.apache.inlong.sdk.transform.decode.SourceDecoderFactory;
import org.apache.inlong.sdk.transform.encode.SinkEncoderFactory;
import org.apache.inlong.sdk.transform.pojo.TransformConfig;
import org.apache.inlong.sdk.transform.process.TransformProcessor;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;

public class TestAdditionParser extends AbstractParserTestBase {

    @Test
    public void testAdditionParser() throws Exception {
        String transformSql = null;
        TransformConfig config = null;
        TransformProcessor<String, String> processor = null;
        List<String> output = null;

        transformSql = "select string1 + INTERVAL string2 SECOND_MICROSECOND from source";
        config = new TransformConfig(transformSql);
        processor = TransformProcessor
                .create(config, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        // case1: '1992-12-31 23:59:59' + INTERVAL 1.999999 SECOND_MICROSECOND
        output = processor.transform("||||1992-12-31 23:59:59|1.999999", new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=1993-01-01 00:00:00.999999", output.get(0));
        // case1: '1992-12-31 23:59:59' + INTERVAL -1.999999 SECOND_MICROSECOND
        output = processor.transform("||||1992-12-31 23:59:59|-1.999999", new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=1992-12-31 23:59:57.000001", output.get(0));

        // case2: '1992-12-31' + INTERVAL 1.999999 SECOND_MICROSECOND
        output = processor.transform("||||1992-12-31|1.999999", new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=1992-12-31 00:00:01.999999", output.get(0));
        // case2: '1992-12-31' + INTERVAL -1.999999 SECOND_MICROSECOND
        output = processor.transform("||||1992-12-31|-1.999999", new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=1992-12-30 23:59:58.000001", output.get(0));

        transformSql = "select string1 + INTERVAL string2 YEAR from source";
        config = new TransformConfig(transformSql);
        processor = TransformProcessor
                .create(config, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        // case3: '1992-12-31 23:59:59' + INTERVAL 1 YEAR
        output = processor.transform("||||1992-12-31 23:59:59|1", new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=1993-12-31 23:59:59", output.get(0));
        // case3: '1992-12-31 23:59:59' + INTERVAL -1 YEAR
        output = processor.transform("||||1992-12-31 23:59:59|-1", new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=1991-12-31 23:59:59", output.get(0));

        // case4: '23:59:59' + INTERVAL 1 YEAR
        output = processor.transform("||||23:59:59|1", new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=", output.get(0));
        // case4: '23:59:59' + INTERVAL -1 YEAR
        output = processor.transform("||||23:59:59|-1", new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=", output.get(0));

        transformSql = "select string1 + INTERVAL string2 WEEK from source";
        config = new TransformConfig(transformSql);
        processor = TransformProcessor
                .create(config, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        // case5: '1992-12-31 23:59:59' + INTERVAL 13 WEEK
        output = processor.transform("||||1992-12-31 23:59:59|13", new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=1993-04-01 23:59:59", output.get(0));
        // case5: '1992-12-31 23:59:59' + INTERVAL -13 WEEK
        output = processor.transform("||||1992-12-31 23:59:59|-13", new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=1992-10-01 23:59:59", output.get(0));

        transformSql = "select string1 + INTERVAL string2 QUARTER from source";
        config = new TransformConfig(transformSql);
        processor = TransformProcessor
                .create(config, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        // case6: '1992-12-31 23:59:59' + INTERVAL 13 QUARTER
        output = processor.transform("||||1992-12-31 23:59:59|13", new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=1996-03-31 23:59:59", output.get(0));
        // case6: '1992-12-31 23:59:59' + INTERVAL -13 QUARTER
        output = processor.transform("||||1992-12-31 23:59:59|-13", new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=1989-09-30 23:59:59", output.get(0));

        transformSql = "select string1 + INTERVAL xxd QUARTER from source";
        config = new TransformConfig(transformSql);
        processor = TransformProcessor
                .create(config, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        // case7: '1992-12-31 23:59:59' + INTERVAL null QUARTER
        output = processor.transform("||||1992-12-31 23:59:59|13", new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=", output.get(0));

    }
}
