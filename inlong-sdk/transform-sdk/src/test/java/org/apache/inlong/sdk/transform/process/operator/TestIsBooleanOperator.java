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

package org.apache.inlong.sdk.transform.process.operator;

import org.apache.inlong.sdk.transform.decode.SourceDecoderFactory;
import org.apache.inlong.sdk.transform.encode.SinkEncoderFactory;
import org.apache.inlong.sdk.transform.pojo.TransformConfig;
import org.apache.inlong.sdk.transform.process.TransformProcessor;
import org.apache.inlong.sdk.transform.process.function.arithmetic.AbstractFunctionArithmeticTestBase;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;

public class TestIsBooleanOperator extends AbstractFunctionArithmeticTestBase {

    @Test
    public void testIsBooleanOperator() throws Exception {
        String transformSql = null, data = null;
        TransformConfig config = null;
        TransformProcessor<String, String> processor = null;
        List<String> output = null;

        // case1: if(true is true,'true','false')
        transformSql = "select if(booleanVal is true,'true','false') from source";
        config = new TransformConfig(transformSql);
        processor = TransformProcessor
                .create(config, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        data = "5|3|||true";
        output = processor.transform(data, new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=true", output.get(0));

        // case2: if(false is true,'true','false')
        data = "5|3|||false";
        output = processor.transform(data, new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=false", output.get(0));

        // case3: if(true is not true,'true','false')
        transformSql = "select if(booleanVal is not true,'true','false') from source";
        config = new TransformConfig(transformSql);
        processor = TransformProcessor
                .create(config, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        data = "5|3|||true";
        output = processor.transform(data, new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=false", output.get(0));

        // case4: if(false is not true,'true','false')
        data = "5|3|||false";
        output = processor.transform(data, new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=true", output.get(0));

        // case5: if(null is not true,'true','false')
        transformSql = "select if(booleanValX is not true,'true','false') from source";
        config = new TransformConfig(transformSql);
        processor = TransformProcessor
                .create(config, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        data = "5|3|||true";
        output = processor.transform(data, new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=true", output.get(0));

        // case6: if((5 = 3) is true,'true','false')
        transformSql = "select if((numeric1 = numeric2) is true,'true','false') from source";
        config = new TransformConfig(transformSql);
        processor = TransformProcessor
                .create(config, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        data = "5|3|||false";
        output = processor.transform(data, new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=false", output.get(0));

        // case7: if((5 / 2) is not true,'true','false')
        transformSql = "select if((numeric1 / numeric2) is not true,'true','false') from source";
        config = new TransformConfig(transformSql);
        processor = TransformProcessor
                .create(config, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        data = "5|2|||false";
        output = processor.transform(data, new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=false", output.get(0));

    }
}
