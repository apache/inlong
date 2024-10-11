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

public class TestJsonArrayAppendFunction extends AbstractFunctionStringTestBase {

    @Test
    public void testJsonArrayAppendFunction() throws Exception {
        String transformSql = null, data = null;
        TransformConfig config = null;
        TransformProcessor<String, String> processor = null;
        List<String> output = null;

        transformSql = "select json_array_append(string1,string2,string3) from source";
        config = new TransformConfig(transformSql);
        processor = TransformProcessor
                .create(config, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));

        // case1: json_array_append(["a", ["b", "c"], "d"], $[1],1)
        data = "[\\\"a\\\",[\\\"b\\\",\\\"c\\\"],\\\"d\\\"]|$[1]|1|";
        output = processor.transform(data, new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=[\"a\",[\"b\",\"c\",\"1\"],\"d\"]", output.get(0));

        // case2: json_array_append(["a", ["b", "c"], "d"], $[0],2)
        data = "[\\\"a\\\",[\\\"b\\\",\\\"c\\\"],\\\"d\\\"]|$[0]|2|";
        output = processor.transform(data, new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=[[\"a\",\"2\"],[\"b\",\"c\"],\"d\"]", output.get(0));

        // case3: json_array_append(["a", ["b", "c"], "d"],$[1][0],3)
        data = "[\\\"a\\\",[\\\"b\\\",\\\"c\\\"],\\\"d\\\"]|$[1][0]|3|";
        output = processor.transform(data, new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=[\"a\",[[\"b\",\"3\"],\"c\"],\"d\"]", output.get(0));

        // case4: json_array_append("{\"a\": 1, \"b\": [2, 3], \"c\": 4}",$.b,"x")
        data = "{\\\"a\\\": 1, \\\"b\\\": [2, 3], \\\"c\\\": 4}|$.b|x|";
        output = processor.transform(data, new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result={\"a\":1,\"b\":[2,3,\"x\"],\"c\":4}", output.get(0));

        // case5: json_array_append("{\"a\": 1, \"b\": [2, 3], \"c\": 4}",$.c,"y")
        data = "{\\\"a\\\": 1, \\\"b\\\": [2, 3], \\\"c\\\": 4}|$.c|y|";
        output = processor.transform(data, new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result={\"a\":1,\"b\":[2,3],\"c\":[4,\"y\"]}", output.get(0));

        // case6: json_array_append("{"a": 1}",$,"z")
        data = "{\\\"a\\\": 1}|$|z|";
        output = processor.transform(data, new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=[{\"a\":1},\"z\"]", output.get(0));

        transformSql = "select json_array_append(string1,string2,string3,numeric1,numeric2) from source";
        config = new TransformConfig(transformSql);
        processor = TransformProcessor
                .create(config, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        // case7: json_array_append(["a", ["b", "c"], "d"],$[0],2,$[1],3)
        data = "[\\\"a\\\", [\\\"b\\\", \\\"c\\\"], \\\"d\\\"]|$[0]|2|$[1]|3";
        output = processor.transform(data, new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=[[\"a\",\"2\"],[\"b\",\"c\",\"3\"],\"d\"]", output.get(0));
    }
}
