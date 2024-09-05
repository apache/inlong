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

package org.apache.inlong.sdk.transform.process;

import org.apache.inlong.sdk.transform.decode.SourceDecoderFactory;
import org.apache.inlong.sdk.transform.encode.SinkEncoderFactory;
import org.apache.inlong.sdk.transform.pojo.CsvSourceInfo;
import org.apache.inlong.sdk.transform.pojo.FieldInfo;
import org.apache.inlong.sdk.transform.pojo.KvSinkInfo;
import org.apache.inlong.sdk.transform.pojo.TransformConfig;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * TestTransformFromBase64FunctionProcessor
 * description: test the from_base64 function in transform processor
 */
public class TestTransformFromBase64FunctionProcessor {

    private static final List<FieldInfo> srcFields = new ArrayList<>();
    private static final List<FieldInfo> dstFields = new ArrayList<>();
    private static final CsvSourceInfo csvSource;
    private static final KvSinkInfo kvSink;

    static {
        for (int i = 1; i < 4; i++) {
            FieldInfo field = new FieldInfo();
            field.setName("string" + i);
            srcFields.add(field);
        }
        for (int i = 1; i < 4; i++) {
            FieldInfo field = new FieldInfo();
            field.setName("numeric" + i);
            srcFields.add(field);
        }
        FieldInfo field = new FieldInfo();
        field.setName("result");
        dstFields.add(field);
        csvSource = new CsvSourceInfo("UTF-8", '|', '\\', srcFields);
        kvSink = new KvSinkInfo("UTF-8", dstFields);
    }
    @Test
    public void testFromBase64Function() throws Exception {
        String transformSql = "select from_base64(string1) from source";
        TransformConfig config = new TransformConfig(transformSql);
        TransformProcessor<String, String> processor = TransformProcessor
                .create(config, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));

        // case1: from_base64('aGVsbG8gd29ybGQ=') -> 'hello world'
        List<String> output1 = processor.transform("aGVsbG8gd29ybGQ=|apple|banana|cloud|1", new HashMap<>());
        Assert.assertEquals(1, output1.size());
        Assert.assertEquals(output1.get(0), "result=hello world");

        String transformSql2 = "select from_base64(stringX) from source";
        TransformConfig config2 = new TransformConfig(transformSql2);
        TransformProcessor<String, String> processor2 = TransformProcessor
                .create(config2, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        // case2: from_base64(null) -> null
        List<String> output2 = processor2.transform("|apple|banana|cloud|1", new HashMap<>());
        Assert.assertEquals(1, output2.size());
        Assert.assertEquals(output2.get(0), "result=null");

        // case3: from_base64('QXBhY2hlIEluTG9uZw==') -> 'Apache InLong'
        List<String> output3 = processor.transform("QXBhY2hlIEluTG9uZw==|apple|banana|cloud|1", new HashMap<>());
        Assert.assertEquals(1, output3.size());
        Assert.assertEquals(output3.get(0), "result=Apache InLong");
    }
}
