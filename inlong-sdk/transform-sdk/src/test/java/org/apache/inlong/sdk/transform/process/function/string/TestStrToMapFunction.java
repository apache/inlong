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

public class TestStrToMapFunction extends AbstractFunctionStringTestBase {

    @Test
    public void testStrToMapFunction() throws Exception {
        String transformSql = null, data = null;
        TransformConfig config = null;
        TransformProcessor<String, String> processor = null;
        List<String> output = null;

        transformSql = "select str_to_map(string1) from source";
        config = new TransformConfig(transformSql);
        processor = TransformProcessor
                .create(config, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));

        // case1: str_to_map('key1=value1,key2=value2,key3=value3')
        data = "key1=value1,key2=value2,key3=value3|xxd|cloud|7|3|3";
        output = processor.transform(data, new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result={key1=value1, key2=value2, key3=value3}", output.get(0));

        // case2: str_to_map('key1=,key2=,key3=')
        data = "key1=,key2=,key3=|xxd|cloud|7|3|3";
        output = processor.transform(data, new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result={key1=, key2=, key3=}", output.get(0));

        // case3: str_to_map('key1value1,key2value2,key3value3')
        data = "key1value1,key2value2,key3value3|xxd|cloud|7|3|3";
        output = processor.transform(data, new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result={}", output.get(0));

        // case4: str_to_map("")
        data = "|xxd|cloud|7|3|3";
        output = processor.transform(data, new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result={}", output.get(0));

        transformSql = "select str_to_map(string1,string2,string3) from source";
        config = new TransformConfig(transformSql);
        processor = TransformProcessor
                .create(config, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));

        // case5: str_to_map("item1:10,item2:5,item3" , "," , ":")
        data = "item1:10,item2:5,item3:2|,|:|7|3|3";
        output = processor.transform(data, new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result={item1=10, item2=5, item3=2}", output.get(0));

        // case6: str_to_map("name->John!age->30!city->China" , "!" , "->")
        data = "name->John!age->30!city->China|!|->|7|3|3";
        output = processor.transform(data, new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result={name=John, age=30, city=China}", output.get(0));

        // case7: str_to_map("name->John!age->30!city->China" , null , "->")
        data = "name->John,age->30,city->China||->|7|3|3";
        output = processor.transform(data, new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result={name=John, age=30, city=China}", output.get(0));
    }
}
