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

public class TestJsonQueryFunction extends AbstractFunctionJsonTestBase {

    @Test
    public void testJsonQueryFunction() throws Exception {
        String transformSql = null, data = null;
        TransformConfig config = null;
        TransformProcessor<String, String> processor = null;
        List<String> output = null;
        transformSql = "select json_query(string1,string2) from source";
        config = new TransformConfig(transformSql);
        processor = TransformProcessor
                .create(config, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));

        // case1: json_query({"a": 1}, $.a)
        data = "{\"a\": 1}|$.a|3|5";
        output = processor.transform(data, new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=1", output.get(0));

        // case2: json_query({"a": null}, $.a)
        data = "{\"a\": null}|$.a|3|5";
        output = processor.transform(data, new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=", output.get(0));

        // case3: json_query({"a": true}, $.b)
        data = "{\"a\": true}|$.b|3|5";
        output = processor.transform(data, new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=", output.get(0));

        // case4: json_query(null, $.a)
        data = "|$.a|3|5";
        output = processor.transform(data, new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=", output.get(0));

        // case5: json_query({"a": true}, null)
        data = "{\"a\": true}||3|5";
        output = processor.transform(data, new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=", output.get(0));

        // case6: json_query({\"person\": {\"name\": \"Alice\" ,\"age\": 30}}, $.person.name)
        data = "{\\\"person\\\": {\\\"name\\\": \\\"Alice\\\" ,\\\"age\\\": 30}}|$.person.name|3|5";
        output = processor.transform(data, new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=Alice", output.get(0));

        // case7: json_query({\"person\": {\"name\": \"Alice\", \"age\": 30}}, $.person.address)
        data = "{\\\"person\\\": {\\\"name\\\": \\\"Alice\\\", \\\"age\\\": 30}}|$.person.address|3|5";
        output = processor.transform(data, new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=", output.get(0));

        // case8: json_query({\"person\" : {\"name\" : \"Amy\" }}, $.person.name)
        data = "{\\\"person\\\" : {\\\"name\\\" : \\\"Amy\\\" }}|$.person.name|3|5";
        output = processor.transform(data, new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=Amy", output.get(0));

        // case9: json_query({\"people\": [{\"name\": \"Alice\"}, {\"name\": \"Bob\"}]}, $.people)
        data = "{\\\"people\\\": [{\\\"name\\\": \\\"Alice\\\"}, {\\\"name\\\": \\\"Bob\\\"}]}|$.people|3|5";
        output = processor.transform(data, new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=[{\"name\":\"Alice\"},{\"name\":\"Bob\"}]", output.get(0));

        // case10: json_query({\"people\": [{\"name\": \"Alice\"}, {\"name\": \"Bob\"}]}, $.people[2].name)
        data = "{\\\"people\\\": [{\\\"name\\\": \\\"Alice\\\"}, {\\\"name\\\": \\\"Bob\\\"}]}|$.people[2].name|3|5";
        output = processor.transform(data, new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=", output.get(0));

        // case11: json_query({\"data\": [{\"details\": {\"status\": \"active\"}},
        // {\"details\": {\"status\": \"inactive\"}}]}, $.data[1].details)
        data = "{\\\"data\\\": [{\\\"details\\\": {\\\"status\\\": \\\"active\\\"}}, " +
                "{\\\"details\\\": {\\\"status\\\": \\\"inactive\\\"}}]}|$.data[1].details|3|5";
        output = processor.transform(data, new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result={\"status\":\"inactive\"}", output.get(0));

        // case12: json_query({\"records\": [{\"id\": 1, \"info\": {\"valid\": true}},
        // {\"id\": 2, \"info\": {\"valid\": false}}]}, $.records[0].info)
        data = "{\\\"records\\\": [{\\\"id\\\": 1, \\\"info\\\": {\\\"valid\\\": true}}, " +
                "{\\\"id\\\": 2, \\\"info\\\": {\\\"valid\\\": false}}]}|$.records[0].info|3|5";
        output = processor.transform(data, new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result={\"valid\":true}", output.get(0));

        // case13: json_query({"data": {"items": [{"id": 1, "details": {"price": 100}},
        // {"id": 2, "details": {"price": 200}}]}}, $.data.items[1].discount)
        data = "{\"data\": {\"items\": [{\"id\": 1, \"details\": {\"price\": 100}}, " +
                "{\"id\": 2, \"details\": {\"price\": 200}}]}}|$.data.items[1].discount|3|5";
        output = processor.transform(data, new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=", output.get(0));

        // case14: json_query({"items": []}, $.items[0])
        data = "{\"items\": []}|$.items[0]|3|5";
        output = processor.transform(data, new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=", output.get(0));

        // case15: json_query({\"list\": [null, {\"name\": \"John\"}]}, $.list[0])
        data = "{\\\"list\\\": [null, {\\\"name\\\": \\\"John\\\"}]}|$.list[0]|3|5";
        output = processor.transform(data, new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=", output.get(0));

        // case16: json_query({\"list\": [null, {\"name\": \"John\"}]}, $.list[1].name)
        data = "{\\\"list\\\": [null, {\\\"name\\\": \\\"John\\\"}]}|$.list[1].name|3|5";
        output = processor.transform(data, new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=John", output.get(0));
    }
}
