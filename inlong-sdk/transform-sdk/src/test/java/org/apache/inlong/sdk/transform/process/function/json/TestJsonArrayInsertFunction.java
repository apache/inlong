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

package org.apache.inlong.sdk.transform.process.function.json;

import org.apache.inlong.sdk.transform.decode.SourceDecoderFactory;
import org.apache.inlong.sdk.transform.encode.SinkEncoderFactory;
import org.apache.inlong.sdk.transform.pojo.TransformConfig;
import org.apache.inlong.sdk.transform.process.TransformProcessor;
import org.apache.inlong.sdk.transform.process.function.string.AbstractFunctionStringTestBase;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;

public class TestJsonArrayInsertFunction extends AbstractFunctionStringTestBase {

    @Test
    public void testJsonArrayInsertFunction() throws Exception {
        String transformSql = null, data = null;
        TransformConfig config = null;
        TransformProcessor<String, String> processor = null;
        List<String> output = null;

        transformSql = "select json_array_insert(string1,string2,string3) from source";
        config = new TransformConfig(transformSql);
        processor = TransformProcessor
                .create(config, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));

        // case1: json_array_append(["a", {"b": [1, 2]}, [3, 4]], $[1], x)
        data = "[\\\"a\\\", {\\\"b\\\": [1, 2]}, [3, 4]]|$[1]|x|";
        output = processor.transform(data, new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=[\"a\",\"x\",{\"b\":[1,2]},[3,4]]", output.get(0));

        // case2: json_array_append(["a", {"b": [1, 2]}, [3, 4]], $[100], x)
        data = "[\\\"a\\\", {\\\"b\\\": [1, 2]}, [3, 4]]|$[100]|x|";
        output = processor.transform(data, new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=[\"a\",{\"b\":[1,2]},[3,4],\"x\"]", output.get(0));

        // case3: json_array_append(["a", {"b": [1, 2]}, [3, 4]], $[100], x)
        data = "[\\\"a\\\", {\\\"b\\\": [1, 2]}, [3, 4]]|$[1].b[0]|x|";
        output = processor.transform(data, new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=[\"a\",{\"b\":[\"x\",1,2]},[3,4]]", output.get(0));

        // case4: json_array_append(["a", {"b": [1, 2]}, [3, 4]], $[100], y)
        data = "[\\\"a\\\", {\\\"b\\\": [1, 2]}, [3, 4]]|$[2][1]|y|";
        output = processor.transform(data, new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=[\"a\",{\"b\":[1,2]},[3,\"y\",4]]", output.get(0));

        transformSql = "select json_array_insert(string1,string2,string3,numeric1,numeric2) from source";
        config = new TransformConfig(transformSql);
        processor = TransformProcessor
                .create(config, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));

        // case5: json_array_append(["a", {"b": [1, 2]}, [3, 4]], "$[0]", "x", "$[2][1]", "y")
        data = "[\\\"a\\\", {\\\"b\\\": [1, 2]}, [3, 4]]|$[0]|x|$[2][1]|y";
        output = processor.transform(data, new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=[\"x\",\"a\",{\"b\":[1,2]},[3,4]]", output.get(0));

        // case6: json_array_append(["a", {"b": [1, 2]}, [3, 4]], "$[0]", "x", "$[3][1]", "y")
        data = "[\\\"a\\\", {\\\"b\\\": [1, 2]}, [3, 4]]|$[0]|x|$[3][1]|y";
        output = processor.transform(data, new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=[\"x\",\"a\",{\"b\":[1,2]},[3,\"y\",4]]", output.get(0));

    }
}
