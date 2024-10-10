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

public class TestJsonExistsFunction extends AbstractFunctionStringTestBase {

    @Test
    public void testJsonExistsFunction() throws Exception {
        String transformSql = null, data = null;
        TransformConfig config = null;
        TransformProcessor<String, String> processor = null;
        List<String> output = null;
        transformSql = "select json_exists(string1,string2) from source";
        config = new TransformConfig(transformSql);
        processor = TransformProcessor
                .create(config, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));

        // case1: json_exists({"a": 1}, $.a)
        data = "{\"a\": 1}|$.a|3|5";
        output = processor.transform(data, new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=true", output.get(0));

        // case2: json_exists({"a": null}, $.a)
        data = "{\"a\": null}|$.a|3|5";
        output = processor.transform(data, new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=false", output.get(0));

        // case3: json_exists({"a": true}, $.b)
        data = "{\"a\": true}|$.b|3|5";
        output = processor.transform(data, new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=false", output.get(0));

        // case4: json_exists(null, $.a)
        data = "|$.a|3|5";
        output = processor.transform(data, new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=false", output.get(0));

        // case5: json_exists({"a": true}, null)
        data = "{\"a\": true}||3|5";
        output = processor.transform(data, new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=false", output.get(0));

        // case6: json_exists({\"person\": {\"name\": \"Alice\" ,\"age\": 30}}, $.person.name)
        data = "{\\\"person\\\": {\\\"name\\\": \\\"Alice\\\" ,\\\"age\\\": 30}}|$.person.name|3|5";
        output = processor.transform(data, new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=true", output.get(0));

        // case7: json_exists({\"person\": {\"name\": \"Alice\", \"age\": 30}}, $.person.address)
        data = "{\\\"person\\\": {\\\"name\\\": \\\"Alice\\\", \\\"age\\\": 30}}|$.person.address|3|5";
        output = processor.transform(data, new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=false", output.get(0));

        // case8: json_exists({\"person\" : {\"name\" : \"Amy\" }}, $.person.name)
        data = "{\\\"person\\\" : {\\\"name\\\" : \\\"Amy\\\" }}|$.person.name|3|5";
        output = processor.transform(data, new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=true", output.get(0));

        // case9: json_exists({\"people\": [{\"name\": \"Alice\"}, {\"name\": \"Bob\"}]}, $.people[0].name)
        data = "{\\\"people\\\": [{\\\"name\\\": \\\"Alice\\\"}, {\\\"name\\\": \\\"Bob\\\"}]}|$.people[0].name|3|5";
        output = processor.transform(data, new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=true", output.get(0));

        // case10: json_exists({\"people\": [{\"name\": \"Alice\"}, {\"name\": \"Bob\"}]}, $.people[2].name)
        data = "{\\\"people\\\": [{\\\"name\\\": \\\"Alice\\\"}, {\\\"name\\\": \\\"Bob\\\"}]}|$.people[2].name|3|5";
        output = processor.transform(data, new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=false", output.get(0));

        // case11: json_exists({\"data\": [{\"details\": {\"status\": \"active\"}},
        // {\"details\": {\"status\": \"inactive\"}}]}, $.data[1].details.status)
        data = "{\\\"data\\\": [{\\\"details\\\": {\\\"status\\\": \\\"active\\\"}}, " +
                "{\\\"details\\\": {\\\"status\\\": \\\"inactive\\\"}}]}|$.data[1].details.status|3|5";
        output = processor.transform(data, new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=true", output.get(0));

        // case12: json_exists({\"records\": [{\"id\": 1, \"info\": {\"valid\": true}},
        // {\"id\": 2, \"info\": {\"valid\": false}}]}, $.records[0].info.valid)
        data = "{\\\"records\\\": [{\\\"id\\\": 1, \\\"info\\\": {\\\"valid\\\": true}}, " +
                "{\\\"id\\\": 2, \\\"info\\\": {\\\"valid\\\": false}}]}|$.records[0].info.valid|3|5";
        output = processor.transform(data, new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=true", output.get(0));

        // case13: json_exists({"data": {"items": [{"id": 1, "details": {"price": 100}},
        // {"id": 2, "details": {"price": 200}}]}}, $.data.items[1].details.discount)
        data = "{\"data\": {\"items\": [{\"id\": 1, \"details\": {\"price\": 100}}, " +
                "{\"id\": 2, \"details\": {\"price\": 200}}]}}|$.data.items[1].details.discount|3|5";
        output = processor.transform(data, new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=false", output.get(0));

        // case14: json_exists({"items": []}, $.items[0])
        data = "{\"items\": []}|$.items[0]|3|5";
        output = processor.transform(data, new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=false", output.get(0));

        // case15: json_exists({\"list\": [null, {\"name\": \"John\"}]}, $.list[0])
        data = "{\\\"list\\\": [null, {\\\"name\\\": \\\"John\\\"}]}|$.list[0]|3|5";
        output = processor.transform(data, new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=false", output.get(0));

        // case16: json_exists({\"list\": [null, {\"name\": \"John\"}]}, $.list[1].name)
        data = "{\\\"list\\\": [null, {\\\"name\\\": \\\"John\\\"}]}|$.list[1].name|3|5";
        output = processor.transform(data, new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=true", output.get(0));
    }
}
