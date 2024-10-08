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

public class TestArrayAppendFunction extends AbstractFunctionStringTestBase {

    @Test
    public void testArrayAppendFunction() throws Exception {
        String transformSql = null, data = null;
        TransformConfig config = null;
        TransformProcessor<String, String> processor = null;
        List<String> output = null;

        transformSql = "select array_append(array(string1,numeric1,string2),string3) from source";
        config = new TransformConfig(transformSql);
        processor = TransformProcessor
                .create(config, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));

        // case1: array_append(array('he',7,'xxd'), 'cloud')
        data = "he|xxd|cloud|7|3|3";
        output = processor.transform(data, new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=[he, 7, xxd, cloud]", output.get(0));

        // case2: array_append(array('he',-1,'xxd'),'')
        data = "he|xxd||-1|3|3";
        output = processor.transform(data, new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=[he, -1, xxd, ]", output.get(0));

        transformSql = "select array_append(array(string1), array(numeric1)) from source";
        config = new TransformConfig(transformSql);
        processor = TransformProcessor
                .create(config, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));

        // case3: array_append(array('he'), array(5))
        data = "he|xxd|cloud|5|3|3";
        output = processor.transform(data, new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=[he, [5]]", output.get(0));

        transformSql = "select array_append(array(),numeric1) from source";
        config = new TransformConfig(transformSql);
        processor = TransformProcessor
                .create(config, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));

        // case4: array_append(array(),5)
        data = "he|xxd|cloud|5|3|3";
        output = processor.transform(data, new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=", output.get(0));

        // case5: array_append(array(),'')
        data = "||";
        output = processor.transform(data, new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=", output.get(0));
    }
}
