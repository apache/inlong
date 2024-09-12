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

package org.apache.inlong.sdk.transform.process.function.string;

import org.apache.inlong.sdk.transform.decode.SourceDecoderFactory;
import org.apache.inlong.sdk.transform.encode.SinkEncoderFactory;
import org.apache.inlong.sdk.transform.pojo.TransformConfig;
import org.apache.inlong.sdk.transform.process.TransformProcessor;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;

public class TestIsDecimalFunction extends AbstractFunctionStringTestBase {

    @Test
    public void testIsDecimalFunction() throws Exception {
        String transformSql = null, data = null;
        TransformConfig config = null;
        TransformProcessor<String, String> processor = null;
        List<String> output = null;

        transformSql = "select is_decimal(string1) from source";
        config = new TransformConfig(transformSql);
        processor = TransformProcessor
                .create(config, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        // case1: is_decimal('3he')
        data = "3he|xxd|cloud|7|3|3";
        output = processor.transform(data, new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=false", output.get(0));

        // case2: is_decimal('3.5')
        data = "3.5|xxd|cloud|7|3|3";
        output = processor.transform(data, new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=true", output.get(0));

        // case3: is_decimal('35')
        data = "35|xxd|cloud|7|3|3";
        output = processor.transform(data, new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=true", output.get(0));

        // case4: is_decimal('')
        data = "|xxd|cloud|7|3|3";
        output = processor.transform(data, new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=false", output.get(0));

        transformSql = "select is_decimal(xxd) from source";
        config = new TransformConfig(transformSql);
        processor = TransformProcessor
                .create(config, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        // case5: is_decimal(null)
        data = "|xxd|cloud|7|3|3";
        output = processor.transform(data, new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=false", output.get(0));
    }
}
