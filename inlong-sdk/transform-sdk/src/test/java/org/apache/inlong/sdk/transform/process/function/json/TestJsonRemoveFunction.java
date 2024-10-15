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

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;

public class TestJsonRemoveFunction extends AbstractFunctionJsonTestBase {

    @Test
    public void testJsonRemoveFunction() throws Exception {
        String transformSql = null, data = null;
        TransformConfig config = null;
        TransformProcessor<String, String> processor = null;
        List<String> output = null;

        String json_doc =
                "{\\\"name\\\":\\\"Alice\\\",\\\"age\\\":30,\\\"address\\\":{\\\"city\\\":\\\"Wonderland\\\",\\\"zip\\\":\\\"12345\\\"}}";
        transformSql = "select json_remove(string1,string2) from source";
        config = new TransformConfig(transformSql);
        processor = TransformProcessor
                .create(config, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        // case1: json_remove("{\"name\":\"Alice\",\"age\":30,\"address\":{\"city\":\"Wonderland\",\"zip\":\"12345\"}}",
        // "$.age")
        data = json_doc + "|$.age|";
        output = processor.transform(data, new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result={\"address\":{\"zip\":\"12345\",\"city\":\"Wonderland\"},\"name\":\"Alice\"}",
                output.get(0));

        // case2: json_remove("{\"name\":\"Alice\",\"age\":30,\"address\":{\"city\":\"Wonderland\",\"zip\":\"12345\"}}",
        // "$.address.zip")
        data = json_doc + "|$.address.zip|";
        output = processor.transform(data, new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result={\"address\":{\"city\":\"Wonderland\"},\"name\":\"Alice\",\"age\":30}",
                output.get(0));

        json_doc =
                "{\\\"name\\\":\\\"Charlie\\\",\\\"hobbies\\\":[[\\\"swimming1\\\",\\\"swimming2\\\"],\\\"reading\\\",\\\"coding\\\"]}";
        transformSql = "select json_remove(string1,string2,string3) from source";
        config = new TransformConfig(transformSql);
        processor = TransformProcessor
                .create(config, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        // case3:
        // json_remove("{\"name\":\"Charlie\",\"hobbies\":[[\"swimming1\",\"swimming2\"],\"reading\",\"coding\"]}","$.age")
        data = json_doc + "|$.hobbies[1]|$.hobbies[0][0]|";
        output = processor.transform(data, new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result={\"hobbies\":[[\"swimming2\"],\"coding\"],\"name\":\"Charlie\"}", output.get(0));
    }
}
