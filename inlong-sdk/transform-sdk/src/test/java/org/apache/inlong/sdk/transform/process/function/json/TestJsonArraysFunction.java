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
import org.apache.inlong.sdk.transform.pojo.CsvSourceInfo;
import org.apache.inlong.sdk.transform.pojo.FieldInfo;
import org.apache.inlong.sdk.transform.pojo.KvSinkInfo;
import org.apache.inlong.sdk.transform.pojo.TransformConfig;
import org.apache.inlong.sdk.transform.process.TransformProcessor;

import org.apache.inlong.sdk.transform.process.function.string.AbstractFunctionStringTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class TestJsonArraysFunction extends AbstractFunctionStringTestBase {

    private static final List<FieldInfo> srcFields = new ArrayList<>();
    private static final List<FieldInfo> dstFields = new ArrayList<>();
    private static final CsvSourceInfo csvSource;
    private static final KvSinkInfo kvSink;

    static {
        for (int i = 1; i < 4; i++) {
            FieldInfo field1 = new FieldInfo();
            field1.setName("string" + i);
            srcFields.add(field1);
            FieldInfo field2 = new FieldInfo();
            field2.setName("numeric" + i);
            srcFields.add(field2);
        }
        FieldInfo field = new FieldInfo();
        field.setName("result");
        dstFields.add(field);
        csvSource = new CsvSourceInfo("UTF-8", '|', '\\', srcFields);
        kvSink = new KvSinkInfo("UTF-8", dstFields);
    }

    @Test
    public void testJsonArraysFunction() throws Exception {
        String transformSql = "select json_arrays(string1,numeric1,string2) from source";
        TransformConfig config = new TransformConfig(transformSql);
        TransformProcessor<String, String> processor = TransformProcessor
                .create(config, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));

        // case1: json_array('apple', '1', 'good')
        List<String> output1 = processor.transform("apple|1|good|cloud|1", new HashMap<>());
        Assert.assertEquals(1, output1.size());
        Assert.assertEquals(output1.get(0), "result=[\"apple\",\"1\",\"good\"]");

        String transformSql2 = "select json_arrays(numeric1,numeric2,numeric3) from source";
        TransformConfig config2 = new TransformConfig(transformSql2);
        TransformProcessor<String, String> processor2 = TransformProcessor
                .create(config2, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));

        // case2: json_array('1', '2', '3')
        List<String> output2 = processor2.transform("aa|1|bb|2|cc|3", new HashMap<>());
        Assert.assertEquals(1, output2.size());
        Assert.assertEquals(output2.get(0), "result=[\"1\",\"2\",\"3\"]");

        String transformSql3 = "select json_arrays(truncate(numeric1, numeric2),numeric3) from source";
        TransformConfig config3 = new TransformConfig(transformSql3);
        TransformProcessor<String, String> processor3 = TransformProcessor
                .create(config3, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));

        // case3: json_arrays(truncate(43.324, -1), '6')
        List<String> output3 = processor3.transform("aa|42.324|bb|-1|cc|6|", new HashMap<>());
        Assert.assertEquals(1, output3.size());
        Assert.assertEquals(output3.get(0), "result=[40,\"6\"]");

        String transformSql4 = "select json_arrays(json_arrays(truncate(numeric1, numeric2),numeric3)) from source";
        TransformConfig config4 = new TransformConfig(transformSql4);
        TransformProcessor<String, String> processor4 = TransformProcessor
                .create(config4, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));

        // case3: json_arrays(json_arrays(truncate(42.324, -1), '6'))
        List<String> output4 = processor4.transform("aa|42.324|bb|-1|cc|6|", new HashMap<>());
        Assert.assertEquals(1, output4.size());
        Assert.assertEquals(output4.get(0), "result=[[40,\"6\"]]");

        String transformSql5 = "select json_arrays() from source";
        TransformConfig config5 = new TransformConfig(transformSql5);
        TransformProcessor<String, String> processor5 = TransformProcessor
                .create(config5, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));

        // case3: json_arrays()
        List<String> output5 = processor5.transform("aa|42.324|bb|-1|cc|6|", new HashMap<>());
        Assert.assertEquals(1, output5.size());
        Assert.assertEquals(output5.get(0), "result=[]");

    }
}
