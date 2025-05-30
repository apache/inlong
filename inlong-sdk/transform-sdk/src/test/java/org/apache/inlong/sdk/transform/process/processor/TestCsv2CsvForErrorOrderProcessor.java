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

package org.apache.inlong.sdk.transform.process.processor;

import org.apache.inlong.sdk.transform.decode.SourceDecoderFactory;
import org.apache.inlong.sdk.transform.encode.SinkEncoderFactory;
import org.apache.inlong.sdk.transform.pojo.CsvSinkInfo;
import org.apache.inlong.sdk.transform.pojo.CsvSourceInfo;
import org.apache.inlong.sdk.transform.pojo.FieldInfo;
import org.apache.inlong.sdk.transform.pojo.KvSinkInfo;
import org.apache.inlong.sdk.transform.pojo.KvSourceInfo;
import org.apache.inlong.sdk.transform.pojo.TransformConfig;
import org.apache.inlong.sdk.transform.process.TransformProcessor;

import net.sf.jsqlparser.JSQLParserException;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestCsv2CsvForErrorOrderProcessor extends AbstractProcessorTestBase {

    @Test
    public void testCsv2CsvForErrorOrder() throws Exception {
        List<FieldInfo> sourceFields = this.getTestFieldList("ftime", "extinfo", "data");
        CsvSourceInfo csvSource = new CsvSourceInfo("UTF-8", '|', '\\', sourceFields);
        List<FieldInfo> sinkFields = this.getTestFieldList("field1", "field2", "field3");
        CsvSinkInfo csvSink = new CsvSinkInfo("UTF-8", '|', '\\', sinkFields);
        String transformSql =
                "select ftime as field2,data as field3,extinfo as field4,$ctx.partition as field1 from source where extinfo='ok'";
        TransformConfig config = new TransformConfig(transformSql, false);
        // case1
        TransformProcessor<String, String> processor1 = TransformProcessor
                .create(config, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createCsvEncoder(csvSink));

        Map<String, Object> extParams = new HashMap<>();
        extParams.put("partition", "2024042801");
        List<String> output1 = processor1.transform("2024-04-28 00:00:00|ok|data1", extParams);
        Assert.assertEquals(1, output1.size());
        Assert.assertEquals("2024042801|2024-04-28 00:00:00|data1", output1.get(0));
    }

    @Test
    public void testKv2KvForErrorOrder() throws Exception {
        List<FieldInfo> sourceFields = this.getTestFieldList("key1", "key2", "key3", "key4");
        KvSourceInfo kvSource = new KvSourceInfo("UTF-8", sourceFields);
        List<FieldInfo> sinkFields = this.getTestFieldList("field1", "field2", "field3");
        KvSinkInfo kvSink = new KvSinkInfo("UTF-8", sinkFields);
        String transformSql = "select key4 as field3, key2 as field6, key1 as field1";
        TransformConfig config = new TransformConfig(transformSql, false);
        // case1
        TransformProcessor<String, String> processor1 = TransformProcessor
                .create(config, SourceDecoderFactory.createKvDecoder(kvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));

        List<String> output1 = processor1.transform("key1=string11&key2=string12&key3=number11&key4=number12",
                new HashMap<>());
        Assert.assertEquals(1, output1.size());
        Assert.assertEquals("field1=string11&field2=&field3=number12", output1.get(0));
    }

    @Test
    public void testKv2KvForConfigError() throws Exception {
        List<FieldInfo> sourceFields = this.getTestFieldList("key1", "key2", "key3", "key4");
        KvSourceInfo kvSource = new KvSourceInfo("UTF-8", sourceFields);
        List<FieldInfo> sinkFields = this.getTestFieldList("field1", "field2", "field3");
        KvSinkInfo kvSink = new KvSinkInfo("UTF-8", sinkFields);
        String transformSql = "select key4 as field3, key2 as field6, key1 as field1";
        TransformConfig config = new TransformConfig(transformSql, new HashMap<>(), false, false);
        // case1
        Assert.assertThrows(JSQLParserException.class, () -> {
            TransformProcessor
                    .create(config, SourceDecoderFactory.createKvDecoder(kvSource),
                            SinkEncoderFactory.createKvEncoder(kvSink));
        });
    }
}
