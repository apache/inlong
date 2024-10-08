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

public class TestJsonQuoteFunction extends AbstractFunctionStringTestBase {

    @Test
    public void testJsonQuoteFunction() throws Exception {
        String transformSql = null, data = null;
        TransformConfig config = null;
        TransformProcessor<String, String> processor = null;
        List<String> output = null;

        transformSql = "select json_quote(string1) from source";
        config = new TransformConfig(transformSql);
        processor = TransformProcessor
                .create(config, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        // case1: json_quote('Hello, World!')
        data = "Hello, World!|xxd|cloud|7|3|3";
        output = processor.transform(data, new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=\"Hello, World!\"", output.get(0));

        // case2: json_quote('This is a "quoted" string')
        data = "This is a \"quoted\" string|xxd|cloud|7|3|3";
        output = processor.transform(data, new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=\"This is a quoted string\"", output.get(0));

        // case3: json_quote('A back\slash:')
        data = "A back\\slash:|xxd|cloud|7|3|3";
        output = processor.transform(data, new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=\"A backslash:\"", output.get(0));

        // case4: json_quote('Column1\tColumn2)
        data = "Column1\tColumn2|xxd|cloud|7|3|3";
        output = processor.transform(data, new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=\"Column1\\tColumn2\"", output.get(0));

        // case5: json_quote('Quotes ' and double quotes \"')
        data = "Quotes ' and double quotes \\\"|xxd|cloud|7|3|3";
        output = processor.transform(data, new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=\"Quotes ' and double quotes \\\"\"", output.get(0));

        // case6: json_quote(null)
        data = "|xxd|cloud|7|3|3";
        output = processor.transform(data, new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=\"\"", output.get(0));

        // case7: json_quote('Complex string with / and \\')
        data = "Complex string with / and \\\\";
        output = processor.transform(data, new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=\"Complex string with / and \\\\\"", output.get(0));

        // case8: json_quote('Unicode test: ሴ噸')
        data = "Unicode test: ሴ噸|xxd|cloud|7|3|3";
        output = processor.transform(data, new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=\"Unicode test: ሴ噸\"", output.get(0));

        transformSql = "select json_quote(xxd) from source";
        config = new TransformConfig(transformSql);
        processor = TransformProcessor
                .create(config, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        // case9: json_quote()
        data = "|xxd|cloud|7|3|3";
        output = processor.transform(data, new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=", output.get(0));
    }

    @Test
    public void testJsonStringFunction() throws Exception {
        String transformSql = null, data = null;
        TransformConfig config = null;
        TransformProcessor<String, String> processor = null;
        List<String> output = null;

        transformSql = "select json_string(string1) from source";
        config = new TransformConfig(transformSql);
        processor = TransformProcessor
                .create(config, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        // case1: json_string('true')
        data = "true|xxd|cloud|7|3|3";
        output = processor.transform(data, new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=\"true\"", output.get(0));

        // case2: json_string('This is a "quoted" string')
        data = "This is a \"quoted\" string|xxd|cloud|7|3|3";
        output = processor.transform(data, new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=\"This is a quoted string\"", output.get(0));

        // case3: json_string('A back\slash:')
        data = "A back\\slash:|xxd|cloud|7|3|3";
        output = processor.transform(data, new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=\"A backslash:\"", output.get(0));

        // case4: json_string('Column1\tColumn2)
        data = "Column1\tColumn2|xxd|cloud|7|3|3";
        output = processor.transform(data, new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=\"Column1\\tColumn2\"", output.get(0));

        // case5: json_string('Quotes ' and double quotes \"')
        data = "Quotes ' and double quotes \\\"|xxd|cloud|7|3|3";
        output = processor.transform(data, new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=\"Quotes ' and double quotes \\\"\"", output.get(0));

        // case6: json_string(null)
        data = "|xxd|cloud|7|3|3";
        output = processor.transform(data, new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=\"\"", output.get(0));

        // case7: json_string('Complex string with / and \\')
        data = "Complex string with / and \\\\";
        output = processor.transform(data, new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=\"Complex string with / and \\\\\"", output.get(0));

        // case8: json_string('Unicode test: ሴ噸')
        data = "Unicode test: ሴ噸|xxd|cloud|7|3|3";
        output = processor.transform(data, new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=\"Unicode test: ሴ噸\"", output.get(0));

        transformSql = "select json_quote(xxd) from source";
        config = new TransformConfig(transformSql);
        processor = TransformProcessor
                .create(config, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        // case9: json_string()
        data = "|xxd|cloud|7|3|3";
        output = processor.transform(data, new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=", output.get(0));
    }
}
