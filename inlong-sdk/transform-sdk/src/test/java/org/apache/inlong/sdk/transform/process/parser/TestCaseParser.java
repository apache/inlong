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
import org.apache.inlong.sdk.transform.process.function.arithmetic.AbstractFunctionArithmeticTestBase;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;

public class TestCaseParser extends AbstractFunctionArithmeticTestBase {

    @Test
    public void testCaseFunction() throws Exception {
        String transformSql = null, data = null;
        TransformConfig config = null;
        TransformProcessor<String, String> processor = null;
        List<String> output = null;

        // case1: CASE WHEN numeric1 > numeric2 THEN 'greater' ELSE 'lesser' END
        transformSql = "select case " +
                "when numeric1 > numeric2 then 'greater' " +
                "else 'lesser' end " +
                "from source";
        config = new TransformConfig(transformSql);
        processor = TransformProcessor
                .create(config, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        data = "5|3|3|5";
        output = processor.transform(data, new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=greater", output.get(0));

        // case2: CASE numeric1 WHEN 5 THEN 'five' ELSE 'other' END
        transformSql = "select case numeric1 " +
                "when 5 then 'five' " +
                "else 'other' end " +
                "from source";
        config = new TransformConfig(transformSql);
        processor = TransformProcessor
                .create(config, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        data = "5|0|3|5";
        output = processor.transform(data, new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=five", output.get(0));

        // case3: CASE numeric1 WHEN 3 THEN 'three' WHEN 5 THEN 'five' ELSE 'other' END
        transformSql = "select case numeric1 " +
                "when 3 then 'three' " +
                "when 5 then 'five' " +
                "else 'other' end " +
                "from source";
        config = new TransformConfig(transformSql);
        processor = TransformProcessor
                .create(config, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        data = "6|0|3|5";
        output = processor.transform(data, new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=other", output.get(0));

        // case4: CASE WHEN numeric1 >= 90 THEN 'Excellent' WHEN numeric1 >= 80 THEN 'Good' WHEN numeric1 >= 60 THEN
        // 'Pass' ELSE 'Fail' END
        transformSql = "select case " +
                "when numeric1 >= 90 then 'Excellent' " +
                "when numeric1 >= 80 then 'Good' " +
                "when numeric1 >= 60 then 'Pass' " +
                "else 'Fail' end " +
                "from source";
        config = new TransformConfig(transformSql);
        processor = TransformProcessor
                .create(config, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        data = "85|0|3|5";
        output = processor.transform(data, new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=Good", output.get(0));

        // case5: CASE WHEN numeric1 = 5 THEN 'five' WHEN numeric1 > 3 THEN 'greater' END
        transformSql = "select case " +
                "when numeric1 = 5 then 'five' " +
                "when numeric1 > 3 then 'greater' " +
                "end " +
                "from source";
        config = new TransformConfig(transformSql);
        processor = TransformProcessor
                .create(config, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        data = "5|0|3|5";
        output = processor.transform(data, new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=five", output.get(0));

        // case6: CASE numeric1 + numeric2 WHEN 3 THEN 3 WHEN 5 THEN 'five' WHEN 5.4 THEN 5.4 ELSE 'other' END
        transformSql = "select case numeric1 + numeric2 " +
                "when 3 then 3 " +
                "when 5 then 'five' " +
                "when 5.4 then 5.4 " +
                "else 'other' end " +
                "from source";
        config = new TransformConfig(transformSql);
        processor = TransformProcessor
                .create(config, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        data = "3.2|2.2|3|5";
        output = processor.transform(data, new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=5.4", output.get(0));

        // case7: CASE numeric1 / numeric2 WHEN 3 THEN 'three' WHEN 5 THEN 'five' ELSE 'other' END
        transformSql = "select case numeric1 / numeric2 " +
                "when 3 then 'three' " +
                "when 5 then 'five' " +
                "else 'other' end " +
                "from source";
        config = new TransformConfig(transformSql);
        processor = TransformProcessor
                .create(config, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        data = "3|0|3|5";
        output = processor.transform(data, new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=", output.get(0));

        // case8: Nested CASE Expression
        transformSql = "select case " +
                "when numeric1 >= 90 then 'Excellent' " +
                "when numeric1 >= 80 then 'Good' " +
                "when numeric1 >= 60 then case " +
                "when numeric2 >= 70 then 'Pass with Merit' " +
                "else 'Pass' end " +
                "else 'Fail' end " +
                "from source";
        config = new TransformConfig(transformSql);
        processor = TransformProcessor
                .create(config, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        data = "65|75|3|5";
        output = processor.transform(data, new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=Pass with Merit", output.get(0));
    }
}